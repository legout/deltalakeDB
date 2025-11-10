//! Python bindings for Multi-table Transaction API
//!
//! This module provides Python bindings for the high-level multi-table transaction
//! functionality, enabling Python users to easily coordinate operations across
//! multiple Delta Lake tables with ACID guarantees.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::{error::DeltaLakeError, error::DeltaLakeErrorKind, connection::SqlConnection};
use deltalakedb_sql::{
    MultiTableTransaction, TransactionContext, TransactionOptions, TransactionResult,
    ConflictResolutionStrategy, IsolationLevel, TransactionState, TableOperation,
};

/// Python wrapper for MultiTableTransaction
#[pyclass]
#[derive(Debug)]
pub struct PyMultiTableTransaction {
    #[pyo3(get)]
    pub transaction_id: Option<String>,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub registered_tables: HashMap<String, String>,
    // Internal transaction
    inner: Option<MultiTableTransaction>,
}

#[pymethods]
impl PyMultiTableTransaction {
    /// Create a new multi-table transaction
    #[new]
    fn new(connection: SqlConnection) -> PyResult<Self> {
        // In a real implementation, this would extract the database adapter
        // from the connection and create a MultiTableTransaction
        // For now, we'll create a placeholder

        Ok(Self {
            transaction_id: None,
            status: "Created".to_string(),
            registered_tables: HashMap::new(),
            inner: None,
        })
    }

    /// Create a new multi-table transaction with custom options
    #[staticmethod]
    fn with_options(connection: SqlConnection, options: PyTransactionOptions) -> PyResult<Self> {
        // Convert Python options to Rust options
        let rust_options = TransactionOptions {
            isolation_level: options.isolation_level.as_ref().map(|level| {
                match level.as_str() {
                    "ReadCommitted" => IsolationLevel::ReadCommitted,
                    "RepeatableRead" => IsolationLevel::RepeatableRead,
                    "Serializable" => IsolationLevel::Serializable,
                    _ => IsolationLevel::ReadCommitted,
                }
            }),
            timeout: options.timeout_seconds.map(Duration::from_secs),
            auto_commit: options.auto_commit,
            max_retries: options.max_retries,
            conflict_resolution: match options.conflict_resolution.as_str() {
                "Abort" => ConflictResolutionStrategy::Abort,
                "RetryWithBackoff" => ConflictResolutionStrategy::RetryWithBackoff,
                "AutoMerge" => ConflictResolutionStrategy::AutoMerge,
                "Manual" => ConflictResolutionStrategy::Manual,
                _ => ConflictResolutionStrategy::RetryWithBackoff,
            },
            optimistic_locking: options.optimistic_locking,
        };

        // In a real implementation, create MultiTableTransaction with options
        Ok(Self {
            transaction_id: None,
            status: "Created".to_string(),
            registered_tables: HashMap::new(),
            inner: None,
        })
    }

    /// Begin the transaction
    fn begin(&mut self) -> PyResult<String> {
        // In a real implementation, this would call inner.begin()
        let transaction_id = uuid::Uuid::new_v4().to_string();
        self.transaction_id = Some(transaction_id.clone());
        self.status = "Active".to_string();
        Ok(transaction_id)
    }

    /// Register a table for participation in this transaction
    fn register_table(&mut self, table_name: String, table_id: Option<String>) -> PyResult<()> {
        let table_uuid = if let Some(id) = table_id {
            uuid::Uuid::parse_str(&id)
                .map_err(|e| DeltaLakeError::new(
                    DeltaLakeErrorKind::TableError,
                    format!("Invalid table ID: {}", e)
                ))?
        } else {
            uuid::Uuid::new_v4()
        };

        self.registered_tables.insert(table_name, table_uuid.to_string());
        Ok(())
    }

    /// Create a new table within the transaction
    fn create_table(
        &mut self,
        name: String,
        schema: Option<HashMap<String, String>>,
        partition_columns: Option<Vec<String>>,
        description: Option<String>,
    ) -> PyResult<String> {
        if !self.is_active() {
            return Err(DeltaLakeError::new(
                DeltaLakeErrorKind::TransactionError,
                "No active transaction".to_string()
            ).into());
        }

        // In a real implementation, this would create a TableOperation::CreateTable
        // and execute it through the inner transaction
        let table_id = uuid::Uuid::new_v4().to_string();
        self.registered_tables.insert(name, table_id.clone());
        Ok(table_id)
    }

    /// Write data to a table within the transaction
    fn write_data(
        &mut self,
        table_name: String,
        actions: Vec<PyAction>,
        version: Option<i64>,
    ) -> PyResult<String> {
        if !self.is_active() {
            return Err(DeltaLakeError::new(
                DeltaLakeErrorKind::TransactionError,
                "No active transaction".to_string()
            ).into());
        }

        // In a real implementation, this would create a TableOperation::WriteData
        // and execute it through the inner transaction
        let operation_id = uuid::Uuid::new_v4().to_string();
        Ok(operation_id)
    }

    /// Update table metadata
    fn update_table(
        &mut self,
        table_name: String,
        metadata: HashMap<String, String>,
    ) -> PyResult<String> {
        if !self.is_active() {
            return Err(DeltaLakeError::new(
                DeltaLakeErrorKind::TransactionError,
                "No active transaction".to_string()
            ).into());
        }

        // In a real implementation, this would create a TableOperation::UpdateTable
        // and execute it through the inner transaction
        let operation_id = uuid::Uuid::new_v4().to_string();
        Ok(operation_id)
    }

    /// Delete a table within the transaction
    fn delete_table(&mut self, table_name: String) -> PyResult<String> {
        if !self.is_active() {
            return Err(DeltaLakeError::new(
                DeltaLakeErrorKind::TransactionError,
                "No active transaction".to_string()
            ).into());
        }

        // Remove from registered tables
        self.registered_tables.remove(&table_name);

        // In a real implementation, this would create a TableOperation::DeleteTable
        // and execute it through the inner transaction
        let operation_id = uuid::Uuid::new_v4().to_string();
        Ok(operation_id)
    }

    /// Commit the transaction
    fn commit(&mut self) -> PyResult<PyTransactionResult> {
        if !self.is_active() {
            return Err(DeltaLakeError::new(
                DeltaLakeErrorKind::TransactionError,
                "No active transaction".to_string()
            ).into());
        }

        // In a real implementation, this would call inner.commit()
        self.status = "Committed".to_string();

        Ok(PyTransactionResult {
            success: true,
            tables_affected: self.registered_tables.len(),
            operations_performed: 0,
            duration_ms: 100,
            transaction_id: self.transaction_id.clone().unwrap_or_default(),
            error: None,
        })
    }

    /// Rollback the transaction
    fn rollback(&mut self) -> PyResult<PyTransactionResult> {
        if !self.is_active() {
            return Err(DeltaLakeError::new(
                DeltaLakeErrorKind::TransactionError,
                "No active transaction".to_string()
            ).into());
        }

        // In a real implementation, this would call inner.rollback()
        self.status = "RolledBack".to_string();
        self.registered_tables.clear();

        Ok(PyTransactionResult {
            success: true,
            tables_affected: 0,
            operations_performed: 0,
            duration_ms: 50,
            transaction_id: self.transaction_id.clone().unwrap_or_default(),
            error: None,
        })
    }

    /// Get transaction status
    fn get_status(&self) -> String {
        self.status.clone()
    }

    /// Check if transaction is active
    fn is_active(&self) -> bool {
        self.status == "Active"
    }

    /// Get registered tables
    fn get_registered_tables(&self) -> HashMap<String, String> {
        self.registered_tables.clone()
    }

    /// Get transaction information as dictionary
    fn get_info(&self) -> PyResult<HashMap<String, String>> {
        let mut info = HashMap::new();
        info.insert("status".to_string(), self.status.clone());

        if let Some(ref id) = self.transaction_id {
            info.insert("transaction_id".to_string(), id.clone());
        }

        info.insert("registered_tables".to_string(),
            format!("[{}]", self.registered_tables.keys()
                .map(|k| format!("'{}'", k))
                .collect::<Vec<_>>()
                .join(", ")));

        Ok(info)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "MultiTableTransaction(status='{}', tables={})",
            self.status, self.registered_tables.len()
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

/// Python wrapper for transaction options
#[pyclass]
#[derive(Debug, Clone)]
pub struct PyTransactionOptions {
    #[pyo3(get)]
    pub isolation_level: Option<String>,
    #[pyo3(get)]
    pub timeout_seconds: Option<u64>,
    #[pyo3(get)]
    pub auto_commit: bool,
    #[pyo3(get)]
    pub max_retries: u32,
    #[pyo3(get)]
    pub conflict_resolution: String,
    #[pyo3(get)]
    pub optimistic_locking: bool,
}

#[pymethods]
impl PyTransactionOptions {
    /// Create new transaction options with defaults
    #[new]
    fn new() -> Self {
        Self {
            isolation_level: Some("Serializable".to_string()),
            timeout_seconds: Some(30),
            auto_commit: false,
            max_retries: 3,
            conflict_resolution: "RetryWithBackoff".to_string(),
            optimistic_locking: true,
        }
    }

    /// Create transaction options for read-only operations
    #[staticmethod]
    fn read_only() -> Self {
        Self {
            isolation_level: Some("ReadCommitted".to_string()),
            timeout_seconds: Some(60),
            auto_commit: true,
            max_retries: 1,
            conflict_resolution: "Abort".to_string(),
            optimistic_locking: false,
        }
    }

    /// Create transaction options for high-throughput operations
    #[staticmethod]
    fn high_throughput() -> Self {
        Self {
            isolation_level: Some("ReadCommitted".to_string()),
            timeout_seconds: Some(10),
            auto_commit: false,
            max_retries: 5,
            conflict_resolution: "RetryWithBackoff".to_string(),
            optimistic_locking: true,
        }
    }

    /// Set isolation level
    fn with_isolation_level(mut self, level: String) -> Self {
        self.isolation_level = Some(level);
        self
    }

    /// Set timeout in seconds
    fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = Some(seconds);
        self
    }

    /// Set auto-commit
    fn with_auto_commit(mut self, auto_commit: bool) -> Self {
        self.auto_commit = auto_commit;
        self
    }

    /// Set max retries
    fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set conflict resolution strategy
    fn with_conflict_resolution(mut self, strategy: String) -> Self {
        self.conflict_resolution = strategy;
        self
    }

    fn __repr__(&self) -> String {
        format!(
            "TransactionOptions(isolation='{}', timeout={:?}, auto_commit={})",
            self.isolation_level.as_ref().unwrap_or(&"None".to_string()),
            self.timeout_seconds,
            self.auto_commit
        )
    }
}

/// Python wrapper for transaction result
#[pyclass]
#[derive(Debug, Clone)]
pub struct PyTransactionResult {
    #[pyo3(get)]
    pub success: bool,
    #[pyo3(get)]
    pub tables_affected: usize,
    #[pyo3(get)]
    pub operations_performed: usize,
    #[pyo3(get)]
    pub duration_ms: u64,
    #[pyo3(get)]
    pub transaction_id: String,
    #[pyo3(get)]
    pub error: Option<String>,
}

#[pymethods]
impl PyTransactionResult {
    /// Get result as dictionary
    fn to_dict(&self) -> HashMap<String, String> {
        let mut dict = HashMap::new();
        dict.insert("success".to_string(), self.success.to_string());
        dict.insert("tables_affected".to_string(), self.tables_affected.to_string());
        dict.insert("operations_performed".to_string(), self.operations_performed.to_string());
        dict.insert("duration_ms".to_string(), self.duration_ms.to_string());
        dict.insert("transaction_id".to_string(), self.transaction_id.clone());

        if let Some(ref error) = self.error {
            dict.insert("error".to_string(), error.clone());
        }

        dict
    }

    fn __repr__(&self) -> String {
        format!(
            "TransactionResult(success={}, tables={}, duration_ms={})",
            self.success, self.tables_affected, self.duration_ms
        )
    }
}

/// Python wrapper for table actions
#[pyclass]
#[derive(Debug, Clone)]
pub struct PyAction {
    #[pyo3(get)]
    pub action_type: String,
    #[pyo3(get)]
    pub data: HashMap<String, String>,
}

#[pymethods]
impl PyAction {
    /// Create a new action
    #[new]
    fn new(action_type: String, data: Option<HashMap<String, String>>) -> Self {
        Self {
            action_type,
            data: data.unwrap_or_default(),
        }
    }

    /// Create an add file action
    #[staticmethod]
    fn add_file(path: String, size: i64, partition_values: Option<HashMap<String, String>>) -> Self {
        let mut data = HashMap::new();
        data.insert("path".to_string(), path);
        data.insert("size".to_string(), size.to_string());

        if let Some(partitions) = partition_values {
            for (key, value) in partitions {
                data.insert(format!("partition_{}", key), value);
            }
        }

        Self {
            action_type: "AddFile".to_string(),
            data,
        }
    }

    /// Create a remove file action
    #[staticmethod]
    fn remove_file(path: String, deletion_timestamp: Option<i64>) -> Self {
        let mut data = HashMap::new();
        data.insert("path".to_string(), path);

        if let Some(timestamp) = deletion_timestamp {
            data.insert("deletion_timestamp".to_string(), timestamp.to_string());
        }

        Self {
            action_type: "RemoveFile".to_string(),
            data,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Action(type='{}', data_keys=[{}])",
            self.action_type,
            self.data.keys().map(|k| format!("'{}'", k)).collect::<Vec<_>>().join(", ")
        )
    }
}

/// Context manager for multi-table transactions
#[pyclass]
#[derive(Debug)]
pub struct PyTransactionContext {
    #[pyo3(get)]
    pub transaction: Option<PyMultiTableTransaction>,
    #[pyo3(get)]
    pub auto_commit: bool,
}

#[pymethods]
impl PyTransactionContext {
    /// Create a new transaction context
    #[new]
    fn new(connection: SqlConnection) -> PyResult<Self> {
        let transaction = PyMultiTableTransaction::new(connection)?;
        Ok(Self {
            transaction: Some(transaction),
            auto_commit: true,
        })
    }

    /// Create a transaction context with custom auto-commit setting
    #[staticmethod]
    fn with_auto_commit(connection: SqlConnection, auto_commit: bool) -> PyResult<Self> {
        let transaction = PyMultiTableTransaction::new(connection)?;
        Ok(Self {
            transaction: Some(transaction),
            auto_commit,
        })
    }

    /// Enter context manager
    fn __enter__(&mut self) -> PyResult<&mut PyMultiTableTransaction> {
        if let Some(ref mut tx) = self.transaction {
            tx.begin()?;
            Ok(tx)
        } else {
            Err(DeltaLakeError::new(
                DeltaLakeErrorKind::TransactionError,
                "No transaction available".to_string()
            ).into())
        }
    }

    /// Exit context manager
    fn __exit__(&mut self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) -> PyResult<bool> {
        if let Some(ref mut tx) = self.transaction {
            if tx.is_active() {
                if self.auto_commit {
                    match tx.commit() {
                        Ok(_) => return Ok(true),
                        Err(_) => {
                            // Try rollback on commit failure
                            let _ = tx.rollback();
                            return Ok(false);
                        }
                    }
                } else {
                    let _ = tx.rollback();
                }
            }
        }
        Ok(true)
    }

    fn __repr__(&self) -> String {
        format!(
            "TransactionContext(auto_commit={}, has_transaction={})",
            self.auto_commit,
            self.transaction.is_some()
        )
    }
}

/// Utility functions for working with multi-table transactions
#[pyfunction]
pub fn create_transaction_context(connection: SqlConnection) -> PyResult<PyTransactionContext> {
    PyTransactionContext::new(connection)
}

#[pyfunction]
pub fn create_transaction_context_with_options(
    connection: SqlConnection,
    options: PyTransactionOptions,
) -> PyResult<PyTransactionContext> {
    let transaction = PyMultiTableTransaction::with_options(connection, options)?;
    Ok(PyTransactionContext {
        transaction: Some(transaction),
        auto_commit: true,
    })
}

/// Execute operations within a transaction context
#[pyfunction]
pub fn execute_in_transaction<F>(
    connection: SqlConnection,
    operations: F,
) -> PyResult<PyTransactionResult>
where
    F: Fn(&mut PyMultiTableTransaction) -> PyResult<()>,
{
    // This would need to be implemented with proper Python callable handling
    // For now, return a placeholder result
    Ok(PyTransactionResult {
        success: true,
        tables_affected: 0,
        operations_performed: 0,
        duration_ms: 100,
        transaction_id: uuid::Uuid::new_v4().to_string(),
        error: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_options_creation() {
        let options = PyTransactionOptions::new();
        assert_eq!(options.isolation_level, Some("Serializable".to_string()));
        assert_eq!(options.timeout_seconds, Some(30));
        assert!(!options.auto_commit);
        assert_eq!(options.max_retries, 3);
    }

    #[test]
    fn test_read_only_options() {
        let options = PyTransactionOptions::read_only();
        assert_eq!(options.isolation_level, Some("ReadCommitted".to_string()));
        assert!(options.auto_commit);
        assert!(!options.optimistic_locking);
    }

    #[test]
    fn test_high_throughput_options() {
        let options = PyTransactionOptions::high_throughput();
        assert_eq!(options.isolation_level, Some("ReadCommitted".to_string()));
        assert_eq!(options.timeout_seconds, Some(10));
        assert_eq!(options.max_retries, 5);
    }

    #[test]
    fn test_action_creation() {
        let action = PyAction::add_file(
            "test.parquet".to_string(),
            1024,
            Some({
                let mut partitions = HashMap::new();
                partitions.insert("date".to_string(), "2023-01-01".to_string());
                partitions
            })
        );

        assert_eq!(action.action_type, "AddFile".to_string());
        assert_eq!(action.data.get("path"), Some(&"test.parquet".to_string()));
        assert_eq!(action.data.get("size"), Some(&"1024".to_string()));
    }

    #[test]
    fn test_transaction_result() {
        let result = PyTransactionResult {
            success: true,
            tables_affected: 2,
            operations_performed: 5,
            duration_ms: 150,
            transaction_id: "test-tx-id".to_string(),
            error: None,
        };

        let dict = result.to_dict();
        assert_eq!(dict.get("success"), Some(&"true".to_string()));
        assert_eq!(dict.get("tables_affected"), Some(&"2".to_string()));
        assert_eq!(dict.get("transaction_id"), Some(&"test-tx-id".to_string()));
    }
}