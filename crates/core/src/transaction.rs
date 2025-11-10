//! Transaction management types for Delta Lake operations.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Transaction isolation levels for multi-table transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionIsolationLevel {
    /// Read Committed: Only committed data is visible, allows non-repeatable reads
    ReadCommitted,
    /// Repeatable Read: Guarantees repeatable reads within transaction
    RepeatableRead,
    /// Serializable: Full isolation, prevents all phenomena
    Serializable,
}

impl Default for TransactionIsolationLevel {
    fn default() -> Self {
        Self::ReadCommitted // Balanced performance and consistency
    }
}

impl std::fmt::Display for TransactionIsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionIsolationLevel::ReadCommitted => write!(f, "READ COMMITTED"),
            TransactionIsolationLevel::RepeatableRead => write!(f, "REPEATABLE READ"),
            TransactionIsolationLevel::Serializable => write!(f, "SERIALIZABLE"),
        }
    }
}

/// Transaction state for multi-table transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Transaction is active and can accept new actions
    Active,
    /// Transaction is being committed
    Committing,
    /// Transaction has been committed
    Committed,
    /// Transaction has been aborted
    Aborted,
    /// Transaction is being rolled back
    RollingBack,
    /// Transaction has been rolled back
    RolledBack,
}

/// Represents an active transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// Table ID.
    pub table_id: String,
    /// Transaction ID.
    pub transaction_id: String,
    /// Start timestamp.
    pub started_at: DateTime<Utc>,
    /// Current version.
    pub current_version: i64,
    /// Staged actions.
    pub staged_actions: Vec<crate::DeltaAction>,
    /// Transaction state.
    pub state: TransactionState,
}

impl Transaction {
    /// Create a new transaction.
    pub fn new(
        table_id: String,
        transaction_id: String,
        current_version: i64,
    ) -> Self {
        Self {
            table_id,
            transaction_id,
            started_at: Utc::now(),
            current_version,
            staged_actions: Vec::new(),
            state: TransactionState::Active,
        }
    }

    /// Get the transaction age in seconds.
    pub fn age_seconds(&self) -> i64 {
        Utc::now().signed_duration_since(self.started_at).num_seconds()
    }

    /// Check if transaction is active.
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    /// Check if transaction is committed.
    pub fn is_committed(&self) -> bool {
        self.state == TransactionState::Committed
    }

    /// Check if transaction is rolled back.
    pub fn is_rolled_back(&self) -> bool {
        matches!(self.state, TransactionState::Aborted | TransactionState::RolledBack)
    }

    /// Mark transaction as committing.
    pub fn mark_committing(&mut self) -> Result<(), String> {
        if self.state != TransactionState::Active {
            return Err(format!("Cannot commit transaction in state: {:?}", self.state));
        }
        self.state = TransactionState::Committing;
        Ok(())
    }

    /// Mark transaction as committed.
    pub fn mark_committed(&mut self) {
        self.state = TransactionState::Committed;
    }

    /// Mark transaction as rolled back.
    pub fn mark_rolled_back(&mut self) {
        self.state = TransactionState::RolledBack;
    }

    /// Add an action to the transaction.
    pub fn add_action(&mut self, action: crate::DeltaAction) {
        self.staged_actions.push(action);
    }

    /// Get the number of staged actions.
    pub fn action_count(&self) -> usize {
        self.staged_actions.len()
    }

    /// Stage an action.
    pub fn stage_action(&mut self, action: crate::DeltaAction) {
        self.staged_actions.push(action);
    }

    /// Stage multiple actions.
    pub fn stage_actions(&mut self, actions: Vec<crate::DeltaAction>) {
        self.staged_actions.extend(actions);
    }

    /// Get the number of staged actions.
    pub fn staged_count(&self) -> usize {
        self.staged_actions.len()
    }

    /// Clear staged actions.
    pub fn clear_staged(&mut self) {
        self.staged_actions.clear();
    }

    /// Get the next version number.
    pub fn next_version(&self) -> i64 {
        self.current_version + 1
    }
}