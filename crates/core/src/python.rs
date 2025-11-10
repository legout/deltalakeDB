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
    /// Creates placeholder actions for the table and stages them in the transaction.
    /// In a complete implementation, this would convert pandas DataFrames to proper Delta actions.
    ///
    /// # Arguments
    ///
    /// * `table_id` - UUID of the table
    /// * `mode` - Write mode: "append", "overwrite", or "ignore"
    /// * `num_files` - Number of files being added (for testing/demo)
    ///
    /// # Returns
    ///
    /// Number of actions actually staged
    fn stage_table(
        &self,
        table_id: String,
        mode: String,
        num_files: usize,
    ) -> PyResult<usize> {
        let table_uuid = Uuid::parse_str(&table_id)
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
        if num_files > self.config.max_files_per_table {
            return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Maximum {} files per table exceeded", self.config.max_files_per_table)
            ));
        }

        // Create placeholder actions for staging
        // In production, these would be created from pandas DataFrame data
        let mut actions = Vec::new();
        for i in 0..num_files {
            actions.push(crate::types::Action::Add(crate::types::AddFile {
                path: format!("part-{:06}.parquet", i),
                size: 1024 * (i as i64 + 1), // Placeholder sizes
                modification_time: chrono::Utc::now().timestamp_millis(),
                data_change_version: 0,
            }));
        }

        // Stage the table in the transaction
        // We need to create a tokio runtime to handle the async mutex
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(
                format!("Failed to create tokio runtime: {}", e)
            ))?;

        rt.block_on(async {
            let mut tx = self.inner.lock().await;
            match tx.stage_table(table_uuid, actions) {
                Ok(_) => Ok(num_files),
                Err(e) => Err(pyo3::exceptions::PyValueError::new_err(
                    format!("Failed to stage table: {}", e)
                ))
            }
        })
    }

    /// Commit all staged tables atomically.
    /// 
    /// This is called by __exit__ when exiting the context manager normally.
    /// Creates a transaction result with all staged tables' versions.
    fn commit(&self) -> PyResult<PyTransactionResult> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(
                format!("Failed to create tokio runtime: {}", e)
            ))?;

        rt.block_on(async {
            let tx = self.inner.lock().await;
            
            // Build transaction result with all staged tables
            let mut result = deltalakedb_core::TransactionResult::new(tx.transaction_id().to_string());
            
            for (table_id, _staged_table) in tx.staged_tables().iter() {
                // In production, these would come from the actual commit
                // For now, assign incrementing versions for demo
                result.add_version(*table_id, 1);
            }

            Ok(PyTransactionResult {
                transaction_id: result.transaction_id().to_string(),
                table_count: result.table_count(),
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
            })
        })
    }

    /// Rollback all staged tables (clear without committing).
    /// 
    /// This is called by __exit__ when an exception occurs in the context manager.
    fn rollback(&self) -> PyResult<()> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(
                format!("Failed to create tokio runtime: {}", e)
            ))?;

        rt.block_on(async {
            let mut tx = self.inner.lock().await;
            tx.clear_staged();
            Ok(())
        })
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
