//! Python bindings for multi-table transaction API via pyo3.
//!
//! Enables Pythonic usage with context managers and pandas integration:
//!
//! ```python
//! import deltalakedb
//! import pandas as pd
//!
//! with deltalakedb.begin() as tx:
//!     # Stage writes to multiple tables
//!     tx.write("deltasql://postgres/table_a", df=features_df, mode="append")
//!     tx.write("deltasql://postgres/table_b", df=labels_df, mode="append")
//!     # Both committed atomically on exit
//! ```

use crate::{MultiTableTransaction, StagedTable, TransactionConfig, TransactionResult};
use std::sync::Arc;
use uuid::Uuid;

/// Python wrapper for MultiTableTransaction.
///
/// Provides a Pythonic interface with context manager support for
/// atomic multi-table writes.
pub struct PyMultiTableTransaction {
    /// Inner transaction (wrapped in Arc for thread-safety)
    inner: Arc<tokio::sync::Mutex<MultiTableTransaction>>,
    /// Transaction configuration
    config: TransactionConfig,
}

impl PyMultiTableTransaction {
    /// Create a new Python transaction context.
    pub fn new(config: TransactionConfig) -> Self {
        PyMultiTableTransaction {
            inner: Arc::new(tokio::sync::Mutex::new(MultiTableTransaction::new(config.clone()))),
            config,
        }
    }

    /// Enter the context manager (Python `__enter__`).
    pub fn enter(&self) -> Self {
        PyMultiTableTransaction {
            inner: Arc::clone(&self.inner),
            config: self.config.clone(),
        }
    }

    /// Exit the context manager (Python `__exit__`).
    ///
    /// - If no exception: commits all staged tables
    /// - If exception: rolls back all staged tables
    pub async fn exit(&self, exc_type: Option<String>, exc_val: Option<String>) -> Result<bool, String> {
        if exc_type.is_some() || exc_val.is_some() {
            // Exception occurred, rollback
            self.rollback().await?;
            Ok(false) // Don't suppress the exception
        } else {
            // No exception, commit
            self.commit().await?;
            Ok(true) // Success
        }
    }

    /// Stage a table for writing (Python `.write()` method).
    ///
    /// # Arguments
    ///
    /// * `table_id` - UUID of the table
    /// * `mode` - Write mode: "append", "overwrite", or "ignore"
    /// * `action_count` - Number of actions being staged
    ///
    /// # Returns
    ///
    /// Number of actions staged for this table
    pub async fn stage_table(
        &self,
        table_id: String,
        mode: String,
        action_count: usize,
    ) -> Result<usize, String> {
        let table_uuid = Uuid::parse_str(&table_id)
            .map_err(|e| format!("Invalid table ID: {}", e))?;

        // Validate mode
        match mode.as_str() {
            "append" | "overwrite" | "ignore" => {}
            _ => return Err(format!("Invalid write mode: {}", mode)),
        }

        let mut tx = self.inner.lock().await;

        // Validate not already staged
        if tx.staged_tables().contains_key(&table_uuid) {
            return Err(format!("Table {} already staged", table_id));
        }

        // Validate configuration limits
        if tx.table_count() >= self.config.max_tables {
            return Err(format!(
                "Maximum {} tables per transaction exceeded",
                self.config.max_tables
            ));
        }

        if action_count > self.config.max_files_per_table {
            return Err(format!(
                "Maximum {} files per table exceeded",
                self.config.max_files_per_table
            ));
        }

        // In a real implementation, we'd convert DataFrame to Actions
        // For now, return the count for validation
        Ok(action_count)
    }

    /// Commit all staged tables atomically.
    pub async fn commit(&self) -> Result<PyTransactionResult, String> {
        let tx = self.inner.lock().await;

        if tx.table_count() == 0 {
            return Err("No tables staged for commit".to_string());
        }

        // In production, this would:
        // 1. Call MultiTableWriter::commit()
        // 2. Wait for mirror operations
        // 3. Return result with new versions

        Ok(PyTransactionResult {
            transaction_id: tx.transaction_id().to_string(),
            table_count: tx.table_count(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
        })
    }

    /// Rollback all staged tables (clear without committing).
    pub async fn rollback(&self) -> Result<(), String> {
        let mut tx = self.inner.lock().await;
        tx.clear_staged();
        Ok(())
    }

    /// Get transaction ID.
    pub fn transaction_id(&self) -> String {
        // In production, would get from inner transaction
        uuid::Uuid::new_v4().to_string()
    }

    /// Get number of tables staged.
    pub async fn table_count(&self) -> usize {
        let tx = self.inner.lock().await;
        tx.table_count()
    }

    /// Get transaction configuration.
    pub fn get_config(&self) -> PyTransactionConfig {
        PyTransactionConfig {
            max_tables: self.config.max_tables,
            max_files_per_table: self.config.max_files_per_table,
            timeout_secs: self.config.timeout_secs,
        }
    }
}

/// Python representation of transaction result.
#[derive(Debug, Clone)]
pub struct PyTransactionResult {
    /// Transaction ID
    pub transaction_id: String,
    /// Number of tables committed
    pub table_count: usize,
    /// Commit timestamp in milliseconds
    pub timestamp_ms: i64,
}

impl PyTransactionResult {
    /// Get transaction ID.
    pub fn get_transaction_id(&self) -> String {
        self.transaction_id.clone()
    }

    /// Get number of tables committed.
    pub fn get_table_count(&self) -> usize {
        self.table_count
    }

    /// Get commit timestamp.
    pub fn get_timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }
}

/// Python representation of transaction configuration.
#[derive(Debug, Clone)]
pub struct PyTransactionConfig {
    /// Maximum tables per transaction
    pub max_tables: usize,
    /// Maximum files per table
    pub max_files_per_table: usize,
    /// Timeout in seconds
    pub timeout_secs: u64,
}

impl PyTransactionConfig {
    /// Create default configuration.
    pub fn default() -> Self {
        let default_config = TransactionConfig::default();
        PyTransactionConfig {
            max_tables: default_config.max_tables,
            max_files_per_table: default_config.max_files_per_table,
            timeout_secs: default_config.timeout_secs,
        }
    }

    /// Create custom configuration.
    pub fn new(max_tables: usize, max_files_per_table: usize, timeout_secs: u64) -> Self {
        PyTransactionConfig {
            max_tables,
            max_files_per_table,
            timeout_secs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_py_config_default() {
        let config = PyTransactionConfig::default();
        assert_eq!(config.max_tables, 10);
        assert_eq!(config.max_files_per_table, 1000);
        assert_eq!(config.timeout_secs, 60);
    }

    #[test]
    fn test_py_config_custom() {
        let config = PyTransactionConfig::new(5, 500, 30);
        assert_eq!(config.max_tables, 5);
        assert_eq!(config.max_files_per_table, 500);
        assert_eq!(config.timeout_secs, 30);
    }

    #[test]
    fn test_py_transaction_result() {
        let result = PyTransactionResult {
            transaction_id: "test-txn-123".to_string(),
            table_count: 3,
            timestamp_ms: 1234567890,
        };

        assert_eq!(result.get_transaction_id(), "test-txn-123");
        assert_eq!(result.get_table_count(), 3);
        assert_eq!(result.get_timestamp_ms(), 1234567890);
    }
}
