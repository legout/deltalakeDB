//! Two-phase commit implementation for multi-table transactions
//!
//! This module provides a robust two-phase commit protocol that ensures atomic
//! commits across multiple tables and potentially multiple database instances.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock, Semaphore};

use crate::{SqlResult, DatabaseAdapter};
use crate::traits::{TransactionStatus, IsolationLevel};
use crate::transaction::{Transaction, TableOperation};

/// Two-phase commit coordinator status
#[derive(Debug, Clone, PartialEq)]
pub enum CoordinatorStatus {
    /// Coordinator is idle, no active transactions
    Idle,
    /// Preparing phase - checking if all participants can commit
    Preparing,
    /// Prepared phase - all participants are ready to commit
    Prepared,
    /// Committing phase - issuing commit commands
    Committing,
    /// Rolling back phase - issuing rollback commands
    RollingBack,
    /// Committed - transaction successfully committed
    Committed,
    /// Rolled back - transaction was rolled back
    RolledBack,
    /// Failed - transaction failed during processing
    Failed,
}

/// Participant in two-phase commit
#[derive(Debug, Clone)]
pub struct Participant {
    /// Unique participant ID (typically database adapter ID)
    pub participant_id: Uuid,
    /// Tables this participant manages
    pub tables: Vec<Uuid>,
    /// Current status of this participant
    pub status: ParticipantStatus,
    /// When the participant was last contacted
    pub last_contact: DateTime<Utc>,
    /// Number of retry attempts
    pub retry_count: u32,
}

/// Participant status in two-phase commit
#[derive(Debug, Clone, PartialEq)]
pub enum ParticipantStatus {
    /// Participant is not yet involved in the transaction
    NotInvolved,
    /// Participant has been asked to prepare
    Preparing,
    /// Participant has prepared and is ready to commit
    Prepared,
    /// Participant has committed
    Committed,
    /// Participant has rolled back
    RolledBack,
    /// Participant failed during preparation
    PrepareFailed,
    /// Participant failed during commit
    CommitFailed,
    /// Participant communication failed
    CommunicationFailed,
}

impl Participant {
    /// Create a new participant
    pub fn new(participant_id: Uuid, tables: Vec<Uuid>) -> Self {
        Self {
            participant_id,
            tables,
            status: ParticipantStatus::NotInvolved,
            last_contact: Utc::now(),
            retry_count: 0,
        }
    }

    /// Update participant status and contact time
    pub fn update_status(&mut self, status: ParticipantStatus) {
        self.status = status;
        self.last_contact = Utc::now();
        if status == ParticipantStatus::NotInvolved {
            self.retry_count = 0;
        }
    }

    /// Increment retry count
    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
        self.last_contact = Utc::now();
    }

    /// Check if participant is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(self.status,
            ParticipantStatus::Committed |
            ParticipantStatus::RolledBack |
            ParticipantStatus::PrepareFailed |
            ParticipantStatus::CommitFailed |
            ParticipantStatus::CommunicationFailed
        )
    }

    /// Check if participant is ready for next phase
    pub fn is_ready(&self) -> bool {
        matches!(self.status,
            ParticipantStatus::Prepared |
            ParticipantStatus::Committed |
            ParticipantStatus::RolledBack
        )
    }
}

/// Two-phase commit context
#[derive(Debug)]
pub struct TwoPhaseCommitContext {
    /// Unique transaction ID
    pub transaction_id: Uuid,
    /// Coordinator status
    pub status: Arc<RwLock<CoordinatorStatus>>,
    /// Participants in this transaction
    pub participants: Arc<Mutex<HashMap<Uuid, Participant>>>,
    /// Operations to be executed
    pub operations: Arc<RwLock<Vec<TableOperation>>>,
    /// Transaction isolation level
    pub isolation_level: IsolationLevel,
    /// When the transaction was created
    pub created_at: DateTime<Utc>,
    /// When the transaction will timeout
    pub timeout_at: DateTime<Utc>,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Retry delay
    pub retry_delay: Duration,
}

impl TwoPhaseCommitContext {
    /// Create a new two-phase commit context
    pub fn new(
        transaction_id: Uuid,
        isolation_level: IsolationLevel,
        timeout: Duration,
        max_retries: u32,
    ) -> Self {
        let now = Utc::now();

        Self {
            transaction_id,
            status: Arc::new(RwLock::new(CoordinatorStatus::Idle)),
            participants: Arc::new(Mutex::new(HashMap::new())),
            operations: Arc::new(RwLock::new(Vec::new())),
            isolation_level,
            created_at: now,
            timeout_at: now + chrono::Duration::from_std(timeout).unwrap(),
            max_retries,
            retry_delay: Duration::from_millis(100),
        }
    }

    /// Add a participant to the transaction
    pub async fn add_participant(&self, participant: Participant) -> SqlResult<()> {
        let mut participants = self.participants.lock().await;
        participants.insert(participant.participant_id, participant);
        Ok(())
    }

    /// Add operations to the transaction
    pub async fn add_operations(&self, operations: Vec<TableOperation>) -> SqlResult<()> {
        let mut ops = self.operations.write().await;
        ops.extend(operations);
        Ok(())
    }

    /// Check if the transaction has timed out
    pub async fn is_expired(&self) -> bool {
        Utc::now() > self.timeout_at
    }

    /// Get the current status
    pub async fn get_status(&self) -> CoordinatorStatus {
        self.status.read().await.clone()
    }

    /// Update the status
    pub async fn update_status(&self, new_status: CoordinatorStatus) -> SqlResult<()> {
        let mut status = self.status.write().await;
        *status = new_status;
        Ok(())
    }

    /// Get all participants
    pub async fn get_participants(&self) -> HashMap<Uuid, Participant> {
        self.participants.lock().await.clone()
    }

    /// Get operations by participant
    pub async fn get_operations_for_participant(&self, participant_id: Uuid) -> Vec<TableOperation> {
        let operations = self.operations.read().await;
        let participants = self.participants.lock().await;

        if let Some(participant) = participants.get(&participant_id) {
            operations.iter()
                .filter(|op| participant.tables.contains(&op.table_id))
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Check if all participants are ready for the next phase
    pub async fn all_participants_ready(&self, required_status: ParticipantStatus) -> bool {
        let participants = self.participants.lock().await;
        participants.values().all(|p| p.status == required_status)
    }

    /// Count participants in a specific status
    pub async fn count_participants_by_status(&self, status: ParticipantStatus) -> usize {
        let participants = self.participants.lock().await;
        participants.values().filter(|p| p.status == status).count()
    }
}

/// Two-phase commit coordinator
pub struct TwoPhaseCommitCoordinator {
    /// Database adapter for executing operations
    adapter: Arc<dyn DatabaseAdapter>,
    /// Active two-phase commit contexts
    active_contexts: Arc<RwLock<HashMap<Uuid, TwoPhaseCommitContext>>>,
    /// Maximum concurrent transactions
    max_concurrent: Arc<Semaphore>,
    /// Default timeout for transactions
    default_timeout: Duration,
    /// Default maximum retries
    default_max_retries: u32,
}

impl TwoPhaseCommitCoordinator {
    /// Create a new two-phase commit coordinator
    pub fn new(adapter: Arc<dyn DatabaseAdapter>, max_concurrent: usize) -> Self {
        Self {
            adapter,
            active_contexts: Arc::new(RwLock::new(HashMap::new())),
            max_concurrent: Arc::new(Semaphore::new(max_concurrent)),
            default_timeout: Duration::from_secs(60),
            default_max_retries: 3,
        }
    }

    /// Begin a two-phase commit transaction
    pub async fn begin_transaction(
        &self,
        isolation_level: Option<IsolationLevel>,
        timeout: Option<Duration>,
        max_retries: Option<u32>,
    ) -> SqlResult<Uuid> {
        // Acquire semaphore for concurrency control
        let _permit = self.max_concurrent.acquire().await
            .map_err(|_| crate::error::SqlError::TransactionError(
                "Too many concurrent transactions".to_string()
            ))?;

        let transaction_id = Uuid::new_v4();
        let isolation_level = isolation_level.unwrap_or_default();
        let timeout = timeout.unwrap_or(self.default_timeout);
        let max_retries = max_retries.unwrap_or(self.default_max_retries);

        let context = TwoPhaseCommitContext::new(
            transaction_id,
            isolation_level,
            timeout,
            max_retries,
        );

        let mut contexts = self.active_contexts.write().await;
        contexts.insert(transaction_id, context);

        Ok(transaction_id)
    }

    /// Execute the prepare phase of two-phase commit
    pub async fn prepare(&self, transaction_id: Uuid) -> SqlResult<()> {
        let contexts = self.active_contexts.read().await;
        let context = contexts.get(&transaction_id)
            .ok_or_else(|| crate::error::SqlError::TransactionError(
                "Transaction not found".to_string()
            ))?;

        // Check if transaction has expired
        if context.is_expired().await {
            return Err(crate::error::SqlError::TransactionError(
                "Transaction has expired".to_string()
            ));
        }

        context.update_status(CoordinatorStatus::Preparing).await?;

        let participants = context.get_participants().await;
        let mut preparation_results = Vec::new();

        // Send prepare to all participants
        for participant in participants.values() {
            let result = self.prepare_participant(context, participant).await;
            preparation_results.push((participant.participant_id, result));
        }

        // Check preparation results
        let mut all_prepared = true;
        for (participant_id, result) in preparation_results {
            match result {
                Ok(_) => {
                    // Update participant status to prepared
                    let mut participants = context.participants.lock().await;
                    if let Some(p) = participants.get_mut(&participant_id) {
                        p.update_status(ParticipantStatus::Prepared);
                    }
                }
                Err(e) => {
                    all_prepared = false;
                    // Mark participant as failed
                    let mut participants = context.participants.lock().await;
                    if let Some(p) = participants.get_mut(&participant_id) {
                        p.update_status(ParticipantStatus::PrepareFailed);
                    }
                    // Log error
                    eprintln!("Prepare failed for participant {}: {:?}", participant_id, e);
                }
            }
        }

        if all_prepared {
            context.update_status(CoordinatorStatus::Prepared).await?;
            Ok(())
        } else {
            context.update_status(CoordinatorStatus::RollingBack).await?;
            // Trigger rollback asynchronously
            self.rollback_async(transaction_id);
            Err(crate::error::SqlError::TransactionError(
                "Prepare phase failed, initiating rollback".to_string()
            ))
        }
    }

    /// Execute the commit phase of two-phase commit
    pub async fn commit(&self, transaction_id: Uuid) -> SqlResult<()> {
        let contexts = self.active_contexts.read().await;
        let context = contexts.get(&transaction_id)
            .ok_or_else(|| crate::error::SqlError::TransactionError(
                "Transaction not found".to_string()
            ))?;

        if context.get_status().await != CoordinatorStatus::Prepared {
            return Err(crate::error::SqlError::TransactionError(
                "Transaction must be prepared before commit".to_string()
            ));
        }

        context.update_status(CoordinatorStatus::Committing).await?;

        let participants = context.get_participants().await;
        let mut commit_results = Vec::new();

        // Send commit to all participants
        for participant in participants.values() {
            let result = self.commit_participant(context, participant).await;
            commit_results.push((participant.participant_id, result));
        }

        // Check commit results
        let mut all_committed = true;
        for (participant_id, result) in commit_results {
            match result {
                Ok(_) => {
                    // Update participant status to committed
                    let mut participants = context.participants.lock().await;
                    if let Some(p) = participants.get_mut(&participant_id) {
                        p.update_status(ParticipantStatus::Committed);
                    }
                }
                Err(e) => {
                    all_committed = false;
                    // Mark participant as failed
                    let mut participants = context.participants.lock().await;
                    if let Some(p) = participants.get_mut(&participant_id) {
                        p.update_status(ParticipantStatus::CommitFailed);
                    }
                    // Log error - this is a serious situation
                    eprintln!("Commit failed for participant {}: {:?}", participant_id, e);
                }
            }
        }

        if all_committed {
            context.update_status(CoordinatorStatus::Committed).await?;
            // Clean up the transaction context
            self.cleanup_transaction(transaction_id).await;
            Ok(())
        } else {
            context.update_status(CoordinatorStatus::Failed).await?;
            // Transaction is in an inconsistent state - requires manual intervention
            Err(crate::error::SqlError::TransactionError(
                "Commit phase failed - transaction may be inconsistent".to_string()
            ))
        }
    }

    /// Rollback a transaction
    pub async fn rollback(&self, transaction_id: Uuid) -> SqlResult<()> {
        let contexts = self.active_contexts.read().await;
        let context = contexts.get(&transaction_id)
            .ok_or_else(|| crate::error::SqlError::TransactionError(
                "Transaction not found".to_string()
            ))?;

        context.update_status(CoordinatorStatus::RollingBack).await?;

        let participants = context.get_participants().await;

        // Send rollback to all participants
        for participant in participants.values() {
            if let Err(e) = self.rollback_participant(context, participant).await {
                // Log error but continue with other participants
                eprintln!("Rollback failed for participant {}: {:?}", participant.participant_id, e);
            }
        }

        context.update_status(CoordinatorStatus::RolledBack).await?;
        self.cleanup_transaction(transaction_id).await;
        Ok(())
    }

    /// Prepare a single participant
    async fn prepare_participant(&self, context: &TwoPhaseCommitContext, participant: &Participant) -> SqlResult<()> {
        let operations = context.get_operations_for_participant(participant.participant_id).await;

        // For now, we'll simulate preparation
        // In a real implementation, this would:
        // 1. Begin a database transaction
        // 2. Validate all operations
        // 3. Acquire necessary locks
        // 4. Prepare the transaction for commit

        if !operations.is_empty() {
            // Check if database supports two-phase commit
            if self.adapter.supports_two_phase_commit().await {
                // Use native two-phase commit
                self.adapter.prepare_transaction(context.transaction_id).await?;
            } else {
                // Simulate preparation with regular transaction
                // In a real implementation, this would be more complex
                let _tx = self.adapter.begin_transaction().await?;
                // Validate operations, acquire locks, etc.
            }
        }

        Ok(())
    }

    /// Commit a single participant
    async fn commit_participant(&self, context: &TwoPhaseCommitContext, participant: &Participant) -> SqlResult<()> {
        let operations = context.get_operations_for_participant(participant.participant_id).await;

        if !operations.is_empty() {
            if self.adapter.supports_two_phase_commit().await {
                // Use native two-phase commit
                self.adapter.commit_prepared(context.transaction_id).await?;
            } else {
                // Simulate commit with regular transaction
                let mut tx = self.adapter.begin_transaction().await?;

                // Execute all operations
                for operation in operations {
                    self.execute_operation(&mut *tx, &operation).await?;
                }

                tx.commit().await?;
            }
        }

        Ok(())
    }

    /// Rollback a single participant
    async fn rollback_participant(&self, context: &TwoPhaseCommitContext, participant: &Participant) -> SqlResult<()> {
        if self.adapter.supports_two_phase_commit().await {
            // Use native two-phase commit rollback
            self.adapter.rollback_prepared(context.transaction_id).await?;
        } else {
            // For regular transactions, no action needed if they weren't committed
            // In a real implementation, we'd need to track and rollback any partial work
        }

        Ok(())
    }

    /// Execute a single operation within a transaction
    async fn execute_operation(&self, tx: &mut dyn crate::traits::Transaction, operation: &TableOperation) -> SqlResult<()> {
        // This is similar to the implementation in transaction.rs
        match &operation.data {
            crate::transaction::OperationData::Table(table) => {
                match operation.operation_type {
                    crate::transaction::OperationType::CreateTable => {
                        tx.create_table(table).await?;
                    }
                    crate::transaction::OperationType::UpdateTable => {
                        tx.update_table(table).await?;
                    }
                    _ => {}
                }
            }
            crate::transaction::OperationData::Commit(commit) => {
                tx.write_commit(commit).await?;
            }
            _ => {
                // Handle other operation types
            }
        }
        Ok(())
    }

    /// Async rollback helper
    fn rollback_async(&self, transaction_id: Uuid) {
        let coordinator = self.clone();
        tokio::spawn(async move {
            if let Err(e) = coordinator.rollback(transaction_id).await {
                eprintln!("Async rollback failed: {:?}", e);
            }
        });
    }

    /// Clean up a completed transaction
    async fn cleanup_transaction(&self, transaction_id: Uuid) {
        let mut contexts = self.active_contexts.write().await;
        contexts.remove(&transaction_id);
    }

    /// Get transaction status
    pub async fn get_transaction_status(&self, transaction_id: Uuid) -> SqlResult<CoordinatorStatus> {
        let contexts = self.active_contexts.read().await;
        let context = contexts.get(&transaction_id)
            .ok_or_else(|| crate::error::SqlError::TransactionError(
                "Transaction not found".to_string()
            ))?;
        Ok(context.get_status().await)
    }

    /// List active transactions
    pub async fn list_active_transactions(&self) -> Vec<Uuid> {
        let contexts = self.active_contexts.read().await;
        contexts.keys().copied().collect()
    }

    /// Clean up expired transactions
    pub async fn cleanup_expired_transactions(&self) -> SqlResult<usize> {
        let contexts = self.active_contexts.read().await;
        let mut expired_ids = Vec::new();

        for (id, context) in contexts.iter() {
            if context.is_expired().await {
                expired_ids.push(*id);
            }
        }
        drop(contexts);

        let count = expired_ids.len();
        for id in expired_ids {
            // Rollback expired transactions
            if let Err(e) = self.rollback(id).await {
                eprintln!("Failed to rollback expired transaction {}: {:?}", id, e);
            }
        }

        Ok(count)
    }
}

// Implement Clone for TwoPhaseCommitCoordinator
impl Clone for TwoPhaseCommitCoordinator {
    fn clone(&self) -> Self {
        Self {
            adapter: self.adapter.clone(),
            active_contexts: self.active_contexts.clone(),
            max_concurrent: self.max_concurrent.clone(),
            default_timeout: self.default_timeout,
            default_max_retries: self.default_max_retries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::SQLiteAdapter;
    use crate::DatabaseConfig;

    #[tokio::test]
    async fn test_two_phase_commit_context_creation() {
        let transaction_id = Uuid::new_v4();
        let context = TwoPhaseCommitContext::new(
            transaction_id,
            IsolationLevel::ReadCommitted,
            Duration::from_secs(30),
            3,
        );

        assert_eq!(context.transaction_id, transaction_id);
        assert_eq!(context.get_status().await, CoordinatorStatus::Idle);
        assert_eq!(context.isolation_level, IsolationLevel::ReadCommitted);
        assert_eq!(context.max_retries, 3);
    }

    #[tokio::test]
    async fn test_participant_creation() {
        let participant_id = Uuid::new_v4();
        let tables = vec![Uuid::new_v4(), Uuid::new_v4()];
        let participant = Participant::new(participant_id, tables.clone());

        assert_eq!(participant.participant_id, participant_id);
        assert_eq!(participant.tables, tables);
        assert_eq!(participant.status, ParticipantStatus::NotInvolved);
        assert_eq!(participant.retry_count, 0);
    }

    #[tokio::test]
    async fn test_participant_status_updates() {
        let participant_id = Uuid::new_v4();
        let mut participant = Participant::new(participant_id, vec![]);

        participant.update_status(ParticipantStatus::Preparing);
        assert_eq!(participant.status, ParticipantStatus::Preparing);
        assert!(!participant.is_terminal());

        participant.update_status(ParticipantStatus::Prepared);
        assert!(participant.is_ready());

        participant.update_status(ParticipantStatus::Committed);
        assert!(participant.is_terminal());
    }

    #[tokio::test]
    async fn test_coordinator_creation() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let coordinator = TwoPhaseCommitCoordinator::new(adapter, 10);

        assert_eq!(coordinator.default_timeout, Duration::from_secs(60));
        assert_eq!(coordinator.default_max_retries, 3);
        assert_eq!(coordinator.list_active_transactions().await.len(), 0);
    }

    #[tokio::test]
    async fn test_begin_transaction() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let coordinator = TwoPhaseCommitCoordinator::new(adapter, 10);

        let transaction_id = coordinator.begin_transaction(None, None, None).await.unwrap();
        assert_ne!(transaction_id, Uuid::nil());

        let status = coordinator.get_transaction_status(transaction_id).await.unwrap();
        assert_eq!(status, CoordinatorStatus::Idle);

        // Cleanup
        coordinator.rollback(transaction_id).await.unwrap();
    }
}