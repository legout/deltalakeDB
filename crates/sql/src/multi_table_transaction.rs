//! High-level Multi-table Transaction API
//!
//! This module provides a user-friendly high-level API for multi-table ACID transactions.
//! It wraps the low-level TransactionManager to provide a simple, intuitive interface
//! for coordinating operations across multiple Delta Lake tables.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use async_trait::async_trait;

use crate::{SqlResult, DatabaseAdapter};
use crate::transaction::{TransactionManager, Transaction, TransactionState, IsolationLevel};
use crate::two_phase_commit::TwoPhaseCommitCoordinator;
use deltalakedb_core::{Table, Commit, Protocol, Metadata, Action, DeltaError};
use deltalakedb_core::table::TableBuilder;

/// High-level multi-table transaction coordinator
#[derive(Debug)]
pub struct MultiTableTransaction {
    /// Underlying transaction manager
    transaction_manager: Arc<TransactionManager>,
    /// Two-phase commit coordinator
    coordinator: Arc<TwoPhaseCommitCoordinator>,
    /// Current transaction
    current_transaction: Option<Arc<Transaction>>,
    /// Registered tables in this transaction
    registered_tables: HashMap<String, Uuid>,
    /// Transaction options
    options: TransactionOptions,
    /// Transaction state
    state: TransactionState,
}

/// Options for multi-table transactions
#[derive(Debug, Clone)]
pub struct TransactionOptions {
    /// Isolation level for the transaction
    pub isolation_level: Option<IsolationLevel>,
    /// Transaction timeout
    pub timeout: Option<Duration>,
    /// Whether to enable auto-commit
    pub auto_commit: bool,
    /// Maximum retry attempts for conflicts
    pub max_retries: u32,
    /// Conflict resolution strategy
    pub conflict_resolution: ConflictResolutionStrategy,
    /// Whether to enable optimistic locking
    pub optimistic_locking: bool,
}

/// Conflict resolution strategies
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictResolutionStrategy {
    /// Abort the transaction on conflicts
    Abort,
    /// Retry the transaction with exponential backoff
    RetryWithBackoff,
    /// Automatically merge compatible changes
    AutoMerge,
    /// Let user handle conflicts manually
    Manual,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            isolation_level: Some(IsolationLevel::Serializable),
            timeout: Some(Duration::from_secs(30)),
            auto_commit: false,
            max_retries: 3,
            conflict_resolution: ConflictResolutionStrategy::RetryWithBackoff,
            optimistic_locking: true,
        }
    }
}

/// Table operation types for high-level API
#[derive(Debug, Clone)]
pub enum TableOperation {
    /// Create a new table
    CreateTable {
        name: String,
        schema: Option<Metadata>,
        partition_columns: Vec<String>,
        description: Option<String>,
    },
    /// Update table metadata
    UpdateTable {
        table_id: Uuid,
        new_metadata: Metadata,
    },
    /// Write data to a table
    WriteData {
        table_id: Uuid,
        actions: Vec<Action>,
        version: Option<i64>,
    },
    /// Delete a table
    DeleteTable {
        table_id: Uuid,
    },
    /// Update table protocol
    UpdateProtocol {
        table_id: Uuid,
        protocol: Protocol,
    },
}

/// Transaction result
#[derive(Debug, Clone)]
pub struct TransactionResult {
    /// Whether the transaction was successful
    pub success: bool,
    /// Number of tables affected
    pub tables_affected: usize,
    /// Number of operations performed
    pub operations_performed: usize,
    /// Transaction duration
    pub duration: Duration,
    /// Transaction ID
    pub transaction_id: Uuid,
    /// Any error that occurred
    pub error: Option<String>,
}

impl MultiTableTransaction {
    /// Create a new multi-table transaction coordinator
    pub fn new(adapter: Arc<dyn DatabaseAdapter>) -> Self {
        let transaction_manager = Arc::new(TransactionManager::new(adapter.clone()));
        let coordinator = Arc::new(TwoPhaseCommitCoordinator::new(adapter));

        Self {
            transaction_manager,
            coordinator,
            current_transaction: None,
            registered_tables: HashMap::new(),
            options: TransactionOptions::default(),
            state: TransactionState::Created,
        }
    }

    /// Create a new multi-table transaction with custom options
    pub fn with_options(adapter: Arc<dyn DatabaseAdapter>, options: TransactionOptions) -> Self {
        let transaction_manager = Arc::new(TransactionManager::new(adapter.clone()));
        let coordinator = Arc::new(TwoPhaseCommitCoordinator::new(adapter));

        Self {
            transaction_manager,
            coordinator,
            current_transaction: None,
            registered_tables: HashMap::new(),
            options,
            state: TransactionState::Created,
        }
    }

    /// Begin a new multi-table transaction
    pub async fn begin(&mut self) -> SqlResult<Uuid> {
        if self.current_transaction.is_some() {
            return Err(crate::error::SqlError::TransactionError(
                "Transaction already active".to_string()
            ));
        }

        let transaction = self.transaction_manager
            .begin_transaction(
                self.options.isolation_level,
                self.options.timeout
            )
            .await?;

        let transaction_id = transaction.transaction_id;
        self.current_transaction = Some(transaction);
        self.state = TransactionState::Active;

        Ok(transaction_id)
    }

    /// Begin a transaction with custom isolation level and timeout
    pub async fn begin_with_config(
        &mut self,
        isolation_level: Option<IsolationLevel>,
        timeout: Option<Duration>,
    ) -> SqlResult<Uuid> {
        if self.current_transaction.is_some() {
            return Err(crate::error::SqlError::TransactionError(
                "Transaction already active".to_string()
            ));
        }

        let transaction = self.transaction_manager
            .begin_transaction(isolation_level, timeout)
            .await?;

        let transaction_id = transaction.transaction_id;
        self.current_transaction = Some(transaction);
        self.state = TransactionState::Active;

        Ok(transaction_id)
    }

    /// Register a table for participation in this transaction
    pub async fn register_table(&mut self, table_name: &str, table_id: Uuid) -> SqlResult<()> {
        if !self.is_active() {
            return Err(crate::error::SqlError::TransactionError(
                "No active transaction".to_string()
            ));
        }

        self.registered_tables.insert(table_name.to_string(), table_id);

        // Create a consistency snapshot for the registered table
        let transaction = self.current_transaction.as_ref().unwrap();
        transaction.create_consistency_snapshot(table_id).await?;

        Ok(())
    }

    /// Register a table by name (will resolve table ID automatically)
    pub async fn register_table_by_name(&mut self, table_name: &str) -> SqlResult<()> {
        if !self.is_active() {
            return Err(crate::error::SqlError::TransactionError(
                "No active transaction".to_string()
            ));
        }

        // Find table by name - this would require adapter support
        // For now, we'll create a placeholder implementation
        let table_id = self.resolve_table_id(table_name).await?;
        self.register_table(table_name, table_id).await
    }

    /// Execute a table operation within the transaction
    pub async fn execute_operation(&mut self, operation: TableOperation) -> SqlResult<Uuid> {
        if !self.is_active() {
            return Err(crate::error::SqlError::TransactionError(
                "No active transaction".to_string()
            ));
        }

        let transaction = self.current_transaction.as_ref().unwrap();

        match operation {
            TableOperation::CreateTable { name, schema, partition_columns, description } => {
                let table = self.create_table_internal(name, schema, partition_columns, description).await?;
                let operation_id = transaction.stage_create_table(&table, vec![]).await?;
                self.registered_tables.insert(name, table.table_id);
                Ok(operation_id)
            },
            TableOperation::UpdateTable { table_id, new_metadata } => {
                // Create a temporary table with new metadata
                let mut table = self.get_table_by_id(table_id).await?;
                table.metadata = new_metadata;
                transaction.stage_update_table(&table, vec![]).await
            },
            TableOperation::WriteData { table_id, actions, version } => {
                // Create a commit for the write operation
                let commit = self.create_commit_for_write(table_id, actions, version).await?;
                transaction.stage_write_commit(&commit, vec![]).await
            },
            TableOperation::DeleteTable { table_id } => {
                // Remove from registered tables
                self.registered_tables.retain(|_, &mut tid| tid != table_id);
                transaction.stage_delete_table(table_id, vec![]).await
            },
            TableOperation::UpdateProtocol { table_id, protocol } => {
                transaction.stage_update_protocol(table_id, &protocol, vec![]).await
            },
        }
    }

    /// Execute multiple operations atomically
    pub async fn execute_operations(&mut self, operations: Vec<TableOperation>) -> SqlResult<Vec<Uuid>> {
        let mut operation_ids = Vec::new();

        for operation in operations {
            let operation_id = self.execute_operation(operation).await?;
            operation_ids.push(operation_id);
        }

        Ok(operation_ids)
    }

    /// Commit the transaction
    pub async fn commit(&mut self) -> SqlResult<TransactionResult> {
        if !self.is_active() {
            return Err(crate::error::SqlError::TransactionError(
                "No active transaction".to_string()
            ));
        }

        let start_time = std::time::Instant::now();
        let transaction = self.current_transaction.take().unwrap();
        let transaction_id = transaction.transaction_id;

        // Prepare the transaction
        self.state = TransactionState::Preparing;
        let context = self.coordinator
            .prepare_transaction(transaction.transaction_id)
            .await?;

        // Commit the transaction
        let commit_result = self.coordinator
            .commit_transaction(transaction.transaction_id)
            .await;

        let duration = start_time.elapsed();
        let tables_affected = self.registered_tables.len();
        let operations_performed = transaction.staged_operations.read().await.len();

        let result = match commit_result {
            Ok(_) => {
                self.state = TransactionState::Committed;
                TransactionResult {
                    success: true,
                    tables_affected,
                    operations_performed,
                    duration,
                    transaction_id,
                    error: None,
                }
            },
            Err(e) => {
                self.state = TransactionState::RolledBack;
                TransactionResult {
                    success: false,
                    tables_affected,
                    operations_performed,
                    duration,
                    transaction_id,
                    error: Some(e.to_string()),
                }
            }
        };

        // Clear registered tables
        self.registered_tables.clear();

        Ok(result)
    }

    /// Rollback the transaction
    pub async fn rollback(&mut self) -> SqlResult<TransactionResult> {
        if !self.is_active() {
            return Err(crate::error::SqlError::TransactionError(
                "No active transaction".to_string()
            ));
        }

        let start_time = std::time::Instant::now();
        let transaction = self.current_transaction.take().unwrap();
        let transaction_id = transaction.transaction_id;

        // Rollback the transaction
        let rollback_result = self.coordinator
            .rollback_transaction(transaction.transaction_id)
            .await;

        let duration = start_time.elapsed();
        let tables_affected = self.registered_tables.len();
        let operations_performed = transaction.staged_operations.read().await.len();

        let result = match rollback_result {
            Ok(_) => {
                self.state = TransactionState::RolledBack;
                TransactionResult {
                    success: true,
                    tables_affected,
                    operations_performed,
                    duration,
                    transaction_id,
                    error: None,
                }
            },
            Err(e) => {
                TransactionResult {
                    success: false,
                    tables_affected,
                    operations_performed,
                    duration,
                    transaction_id,
                    error: Some(e.to_string()),
                }
            }
        };

        // Clear registered tables
        self.registered_tables.clear();

        Ok(result)
    }

    /// Get transaction status
    pub fn get_status(&self) -> TransactionState {
        self.state.clone()
    }

    /// Check if transaction is active
    pub fn is_active(&self) -> bool {
        matches!(self.state, TransactionState::Active)
    }

    /// Get the transaction ID
    pub fn get_transaction_id(&self) -> Option<Uuid> {
        self.current_transaction.as_ref().map(|tx| tx.transaction_id)
    }

    /// Get registered tables
    pub fn get_registered_tables(&self) -> HashMap<String, Uuid> {
        self.registered_tables.clone()
    }

    /// Get transaction options
    pub fn get_options(&self) -> &TransactionOptions {
        &self.options
    }

    /// Update transaction options
    pub fn update_options(&mut self, options: TransactionOptions) {
        self.options = options;
    }

    // Private helper methods

    async fn resolve_table_id(&self, table_name: &str) -> SqlResult<Uuid> {
        // This would need to be implemented using the database adapter
        // For now, return a placeholder UUID
        // In a real implementation, this would query the database for the table ID
        Ok(Uuid::new_v4())
    }

    async fn create_table_internal(
        &self,
        name: String,
        schema: Option<Metadata>,
        partition_columns: Vec<String>,
        description: Option<String>,
    ) -> SqlResult<Table> {
        // Create a new table with the given parameters
        let table_id = Uuid::new_v4();
        let created_at = Utc::now();

        let table = Table {
            table_id,
            name,
            version: 0,
            metadata: schema.unwrap_or_default(),
            protocol: Protocol::default(),
            created_at,
            updated_at: created_at,
            description,
            partition_columns,
        };

        Ok(table)
    }

    async fn get_table_by_id(&self, table_id: Uuid) -> SqlResult<Table> {
        // This would need to be implemented using the database adapter
        // For now, return a placeholder table
        // In a real implementation, this would query the database for the table
        Ok(Table {
            table_id,
            name: "placeholder".to_string(),
            version: 0,
            metadata: Metadata::default(),
            protocol: Protocol::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            description: None,
            partition_columns: vec![],
        })
    }

    async fn create_commit_for_write(
        &self,
        table_id: Uuid,
        actions: Vec<Action>,
        version: Option<i64>,
    ) -> SqlResult<Commit> {
        let commit = Commit {
            commit_id: Uuid::new_v4(),
            table_id,
            version: version.unwrap_or(0),
            timestamp: Utc::now(),
            actions,
            user_id: None,
            user_name: None,
        };

        Ok(commit)
    }
}

/// Context manager for multi-table transactions
pub struct TransactionContext {
    transaction: MultiTableTransaction,
    auto_commit: bool,
}

impl TransactionContext {
    /// Create a new transaction context
    pub fn new(transaction: MultiTableTransaction) -> Self {
        Self {
            transaction,
            auto_commit: true,
        }
    }

    /// Create a transaction context with custom auto-commit setting
    pub fn with_auto_commit(transaction: MultiTableTransaction, auto_commit: bool) -> Self {
        Self {
            transaction,
            auto_commit,
        }
    }

    /// Begin the transaction
    pub async fn begin(&mut self) -> SqlResult<Uuid> {
        self.transaction.begin().await
    }

    /// Get mutable reference to the transaction
    pub fn transaction(&mut self) -> &mut MultiTableTransaction {
        &mut self.transaction
    }

    /// Commit the transaction
    pub async fn commit(&mut self) -> SqlResult<TransactionResult> {
        self.transaction.commit().await
    }

    /// Rollback the transaction
    pub async fn rollback(&mut self) -> SqlResult<TransactionResult> {
        self.transaction.rollback().await
    }

    /// Execute operations within the transaction
    pub async fn execute<F, R>(&mut self, operations: F) -> SqlResult<R>
    where
        F: FnOnce(&mut MultiTableTransaction) -> SqlResult<R>,
    {
        self.transaction.begin().await?;

        let result = match operations(&mut self.transaction) {
            Ok(result) => {
                if self.auto_commit {
                    self.transaction.commit().await?;
                }
                Ok(result)
            },
            Err(e) => {
                let _ = self.transaction.rollback().await;
                Err(e)
            }
        };

        result
    }
}

#[async_trait]
pub trait TransactionExecutor {
    async fn execute_in_transaction<F, R>(&self, operations: F) -> SqlResult<R>
    where
        F: FnOnce(&mut MultiTableTransaction) -> SqlResult<R> + Send,
        R: Send;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::SQLiteAdapter;
    use crate::DatabaseConfig;

    #[tokio::test]
    async fn test_multi_table_transaction_creation() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let tx = MultiTableTransaction::new(adapter);

        assert_eq!(tx.get_status(), TransactionState::Created);
        assert!(!tx.is_active());
        assert!(tx.get_transaction_id().is_none());
    }

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let mut tx = MultiTableTransaction::new(adapter);

        // Begin transaction
        let transaction_id = tx.begin().await.unwrap();
        assert_eq!(tx.get_status(), TransactionState::Active);
        assert!(tx.is_active());
        assert_eq!(tx.get_transaction_id(), Some(transaction_id));

        // Commit transaction
        let result = tx.commit().await.unwrap();
        assert!(result.success);
        assert_eq!(tx.get_status(), TransactionState::Committed);
        assert!(!tx.is_active());
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let mut tx = MultiTableTransaction::new(adapter);

        // Begin transaction
        tx.begin().await.unwrap();
        assert!(tx.is_active());

        // Rollback transaction
        let result = tx.rollback().await.unwrap();
        assert!(result.success);
        assert_eq!(tx.get_status(), TransactionState::RolledBack);
        assert!(!tx.is_active());
    }

    #[tokio::test]
    async fn test_table_registration() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let mut tx = MultiTableTransaction::new(adapter);

        // Begin transaction
        tx.begin().await.unwrap();

        // Register table
        let table_id = Uuid::new_v4();
        tx.register_table("test_table", table_id).await.unwrap();

        let registered_tables = tx.get_registered_tables();
        assert_eq!(registered_tables.len(), 1);
        assert_eq!(registered_tables.get("test_table"), Some(&table_id));
    }

    #[tokio::test]
    async fn test_transaction_context() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let tx = MultiTableTransaction::new(adapter);
        let mut context = TransactionContext::new(tx);

        let result = context.execute(|transaction| {
            async {
                // This would be where you perform operations
                Ok("test_result")
            }
        }).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test_result");
    }
}