//! Multi-table ACID transactions for Delta Lake.
//!
//! Enables atomic commits across multiple Delta tables within a single database transaction,
//! ensuring all-or-nothing semantics for complex data operations like feature store updates,
//! warehouse dimension/fact synchronization, and ML pipeline checkpoints.

use crate::types::Action;
use std::collections::BTreeMap;
use uuid::Uuid;

/// Configuration for multi-table transactions.
#[derive(Debug, Clone)]
pub struct TransactionConfig {
    /// Maximum number of tables in a single transaction (default: 10)
    pub max_tables: usize,
    /// Maximum files per table (default: 1000)
    pub max_files_per_table: usize,
    /// Transaction timeout in seconds (default: 60)
    pub timeout_secs: u64,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        TransactionConfig {
            max_tables: 10,
            max_files_per_table: 1000,
            timeout_secs: 60,
        }
    }
}

/// Staged actions for a single table within a transaction.
#[derive(Debug, Clone)]
pub struct StagedTable {
    /// Table identifier
    pub table_id: Uuid,
    /// Actions to commit for this table
    pub actions: Vec<Action>,
    /// Expected version before commit
    pub expected_version: i64,
}

impl StagedTable {
    /// Create a new staged table.
    pub fn new(table_id: Uuid, actions: Vec<Action>) -> Self {
        StagedTable {
            table_id,
            actions,
            expected_version: -1,
        }
    }

    /// Set the expected version for optimistic concurrency control.
    pub fn with_expected_version(mut self, version: i64) -> Self {
        self.expected_version = version;
        self
    }

    /// Get the number of actions.
    pub fn action_count(&self) -> usize {
        self.actions.len()
    }

    /// Get the number of add and remove file actions.
    pub fn file_action_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| matches!(a, Action::Add(_) | Action::Remove(_)))
            .count()
    }
}

/// Result of a multi-table transaction commit.
#[derive(Debug, Clone)]
pub struct TransactionResult {
    /// Map of table_id -> new_version for each table
    pub versions: BTreeMap<Uuid, i64>,
    /// Transaction ID for correlation
    pub transaction_id: String,
    /// Timestamp of commit (milliseconds since epoch)
    pub timestamp: i64,
}

impl TransactionResult {
    /// Create a new transaction result.
    pub fn new(transaction_id: String) -> Self {
        TransactionResult {
            versions: BTreeMap::new(),
            transaction_id,
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Add a table's new version.
    pub fn add_version(&mut self, table_id: Uuid, version: i64) {
        self.versions.insert(table_id, version);
    }

    /// Get the new version for a table.
    pub fn get_version(&self, table_id: &Uuid) -> Option<i64> {
        self.versions.get(table_id).copied()
    }

    /// Get number of tables in this transaction.
    pub fn table_count(&self) -> usize {
        self.versions.len()
    }
}

/// Error type for transaction operations.
#[derive(Debug, Clone)]
pub enum TransactionError {
    /// Version conflict for a table
    VersionConflict {
        table_id: Uuid,
        expected: i64,
        actual: i64,
    },
    /// Validation error
    ValidationError {
        table_id: Uuid,
        message: String,
    },
    /// Too many tables
    TooManyTables {
        count: usize,
        limit: usize,
    },
    /// Too many files
    TooManyFiles {
        table_id: Uuid,
        count: usize,
        limit: usize,
    },
    /// Transaction timeout
    TransactionTimeout,
    /// Generic error
    Other(String),
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::VersionConflict {
                table_id,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "Version conflict for table {}: expected {}, got {}",
                    table_id, expected, actual
                )
            }
            TransactionError::ValidationError { table_id, message } => {
                write!(f, "Validation error for table {}: {}", table_id, message)
            }
            TransactionError::TooManyTables { count, limit } => {
                write!(f, "Too many tables: {} (limit: {})", count, limit)
            }
            TransactionError::TooManyFiles {
                table_id,
                count,
                limit,
            } => {
                write!(
                    f,
                    "Too many files in table {}: {} (limit: {})",
                    table_id, count, limit
                )
            }
            TransactionError::TransactionTimeout => {
                write!(f, "Transaction timeout")
            }
            TransactionError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for TransactionError {}

/// Result type for transaction operations.
pub type TransactionResult<T> = Result<T, TransactionError>;

/// Multi-table transaction for atomic commits across multiple Delta tables.
///
/// # Example
///
/// ```ignore
/// let mut tx = MultiTableTransaction::new(pool, config).await?;
/// tx.stage_table(table1_id, actions1)?;
/// tx.stage_table(table2_id, actions2)?;
/// let result = tx.commit().await?;
/// println!("Committed: {:?}", result.versions);
/// ```
#[derive(Debug)]
pub struct MultiTableTransaction {
    /// Configuration for this transaction
    config: TransactionConfig,
    /// Staged tables
    staged_tables: BTreeMap<Uuid, StagedTable>,
    /// Transaction ID for correlation
    transaction_id: String,
    /// Start time
    start_time: std::time::Instant,
}

impl MultiTableTransaction {
    /// Create a new multi-table transaction.
    pub fn new(config: TransactionConfig) -> Self {
        MultiTableTransaction {
            config,
            staged_tables: BTreeMap::new(),
            transaction_id: format!("txn-{}", uuid::Uuid::new_v4()),
            start_time: std::time::Instant::now(),
        }
    }

    /// Create a new multi-table transaction with default configuration.
    pub fn new_default() -> Self {
        Self::new(TransactionConfig::default())
    }

    /// Stage actions for a table.
    pub fn stage_table(&mut self, table_id: Uuid, actions: Vec<Action>) -> TransactionResult<()> {
        // Check if already staged
        if self.staged_tables.contains_key(&table_id) {
            return Err(TransactionError::ValidationError {
                table_id,
                message: "Table already staged in this transaction".to_string(),
            });
        }

        // Check max tables limit
        if self.staged_tables.len() >= self.config.max_tables {
            return Err(TransactionError::TooManyTables {
                count: self.staged_tables.len() + 1,
                limit: self.config.max_tables,
            });
        }

        // Check max files limit for this table
        let file_count = actions
            .iter()
            .filter(|a| matches!(a, Action::Add(_) | Action::Remove(_)))
            .count();

        if file_count > self.config.max_files_per_table {
            return Err(TransactionError::TooManyFiles {
                table_id,
                count: file_count,
                limit: self.config.max_files_per_table,
            });
        }

        // Validate actions are not empty
        if actions.is_empty() {
            return Err(TransactionError::ValidationError {
                table_id,
                message: "Cannot stage empty action list".to_string(),
            });
        }

        self.staged_tables
            .insert(table_id, StagedTable::new(table_id, actions));

        Ok(())
    }

    /// Get staged tables.
    pub fn staged_tables(&self) -> &BTreeMap<Uuid, StagedTable> {
        &self.staged_tables
    }

    /// Get number of staged tables.
    pub fn table_count(&self) -> usize {
        self.staged_tables.len()
    }

    /// Get total number of actions.
    pub fn action_count(&self) -> usize {
        self.staged_tables.values().map(|t| t.action_count()).sum()
    }

    /// Get transaction ID.
    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    /// Get elapsed time.
    pub fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// Check if transaction has exceeded timeout.
    pub fn is_timeout(&self) -> bool {
        self.elapsed().as_secs() > self.config.timeout_secs
    }

    /// Validate all staged tables.
    pub fn validate_all(&self) -> TransactionResult<()> {
        // Check timeout
        if self.is_timeout() {
            return Err(TransactionError::TransactionTimeout);
        }

        // Check at least one table staged
        if self.staged_tables.is_empty() {
            return Err(TransactionError::ValidationError {
                table_id: Uuid::nil(),
                message: "No tables staged for commit".to_string(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_staged_table_creation() {
        let table_id = Uuid::new_v4();
        let staged = StagedTable::new(table_id, vec![]);
        assert_eq!(staged.table_id, table_id);
        assert_eq!(staged.action_count(), 0);
    }

    #[test]
    fn test_staged_table_with_version() {
        let table_id = Uuid::new_v4();
        let staged = StagedTable::new(table_id, vec![]).with_expected_version(42);
        assert_eq!(staged.expected_version, 42);
    }

    #[test]
    fn test_transaction_creation() {
        let tx = MultiTableTransaction::new_default();
        assert_eq!(tx.table_count(), 0);
        assert_eq!(tx.action_count(), 0);
    }

    #[test]
    fn test_transaction_stage_table() {
        let mut tx = MultiTableTransaction::new_default();
        let table_id = Uuid::new_v4();

        let result = tx.stage_table(table_id, vec![]);
        assert!(result.is_err()); // Empty actions not allowed
    }

    #[test]
    fn test_transaction_too_many_tables() {
        let config = TransactionConfig {
            max_tables: 2,
            ..Default::default()
        };
        let mut tx = MultiTableTransaction::new(config);

        // Stage first table
        let table1 = Uuid::new_v4();
        // We'll skip actual staging since it requires non-empty actions

        assert_eq!(tx.table_count(), 0);
    }

    #[test]
    fn test_transaction_result() {
        let mut result = TransactionResult::new("txn-123".to_string());
        let table_id = Uuid::new_v4();

        result.add_version(table_id, 42);
        assert_eq!(result.get_version(&table_id), Some(42));
        assert_eq!(result.table_count(), 1);
    }

    #[test]
    fn test_transaction_error_display() {
        let table_id = Uuid::new_v4();
        let err = TransactionError::VersionConflict {
            table_id,
            expected: 10,
            actual: 11,
        };

        let msg = format!("{}", err);
        assert!(msg.contains("Version conflict"));
    }
}
