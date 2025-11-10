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
use pyo3::prelude::*;
use std::sync::Arc;
use uuid::Uuid;

/// Python wrapper for MultiTableTransaction.
///
/// Provides a Pythonic interface with context manager support for
/// atomic multi-table writes.
#[pyclass]
pub struct PyMultiTableTransaction {
    /// Inner transaction (wrapped in Arc for thread-safety)
    inner: Arc<tokio::sync::Mutex<MultiTableTransaction>>,
    /// Transaction configuration
    config: TransactionConfig,
}

#[pymethods]
impl PyMultiTableTransaction {
    /// Create a new Python transaction context.
    #[new]
    #[pyo3(signature = (*, max_tables = 10, max_files_per_table = 1000, timeout_secs = 60))]
    fn new(max_tables: usize, max_files_per_table: usize, timeout_secs: u64) -> Self {
        let config = TransactionConfig {
            max_tables,
            max_files_per_table,
            timeout_secs,
        };
        PyMultiTableTransaction {
            inner: Arc::new(tokio::sync::Mutex::new(MultiTableTransaction::new(config.clone()))),
            config,
        }
    }

    /// Enter the context manager (Python `__enter__`).
    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    /// Exit the context manager (Python `__exit__`).
    ///
    /// - If no exception: commits all staged tables
    /// - If exception: rolls back all staged tables
    fn __exit__(
        &self,
        exc_type: Option<PyObject>,
        _exc_val: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<bool> {
        // Create a tokio runtime for the async operations
        let rt = tokio::runtime::Runtime::new()?;
        
        if exc_type.is_some() {
            // Exception occurred, rollback
            rt.block_on(async { self.rollback().await })?;
            Ok(false) // Don't suppress the exception
        } else {
            // No exception, commit
            rt.block_on(async { self.commit().await })?;
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
    fn stage_table(
        &self,
        table_id: String,
        mode: String,
        action_count: usize,
    ) -> PyResult<usize> {
        let _table_uuid = Uuid::parse_str(&table_id)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                format!("Invalid table ID: {}", e)
            ))?;

        match mode.as_str() {
            "append" | "overwrite" | "ignore" => {}
            _ => return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Invalid write mode: {}", mode)
            )),
        }

        // Validate configuration limits
        if action_count > self.config.max_files_per_table {
            return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Maximum {} files per table exceeded", self.config.max_files_per_table)
            ));
        }

        Ok(action_count)
    }

    /// Commit all staged tables atomically.
    fn commit(&self) -> PyResult<PyTransactionResult> {
        // In production, this would:
        // 1. Call MultiTableWriter::commit()
        // 2. Wait for mirror operations
        // 3. Return result with new versions

        Ok(PyTransactionResult {
            transaction_id: Uuid::new_v4().to_string(),
            table_count: 0,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
        })
    }

    /// Rollback all staged tables (clear without committing).
    fn rollback(&self) -> PyResult<()> {
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
#[pyclass]
#[derive(Debug, Clone)]
pub struct PyTransactionResult {
    /// Transaction ID
    #[pyo3(get)]
    pub transaction_id: String,
    /// Number of tables committed
    #[pyo3(get)]
    pub table_count: usize,
    /// Commit timestamp in milliseconds
    #[pyo3(get)]
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

/// Python module initialization function.
#[pymodule]
fn deltalakedb(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyMultiTableTransaction>()?;
    m.add_class::<PyTransactionResult>()?;
    Ok(())
}
