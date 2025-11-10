use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use deltalakedb_sql::{
    MultiTableWriter, MultiTableConfig, MultiTableTransaction,
    connection::{DatabaseConnection, DatabaseConfig, DatabaseEngine},
    schema::{DatabaseSchema, TableSchema},
};
use deltalakedb_core::{TxnLogResult, TxnLogError};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Python wrapper for MultiTableConfig
#[pyclass]
#[derive(Clone, Debug)]
pub struct PyMultiTableConfig {
    pub inner: MultiTableConfig,
}

#[pymethods]
impl PyMultiTableConfig {
    #[new]
    #[pyo3(signature = (max_retry_attempts=3, retry_base_delay_ms=100, retry_max_delay_ms=5000, enable_ordered_mirroring=true, enable_consistency_validation=true, max_tables_per_transaction=100, max_actions_per_transaction=1000))]
    fn new(
        max_retry_attempts: u32,
        retry_base_delay_ms: u64,
        retry_max_delay_ms: u64,
        enable_ordered_mirroring: bool,
        enable_consistency_validation: bool,
        max_tables_per_transaction: usize,
        max_actions_per_transaction: usize,
    ) -> Self {
        Self {
            inner: MultiTableConfig {
                max_retry_attempts,
                retry_base_delay_ms,
                retry_max_delay_ms,
                enable_ordered_mirroring,
                enable_consistency_validation,
                max_tables_per_transaction,
                max_actions_per_transaction,
            },
        }
    }
}

/// Python wrapper for DatabaseConfig
#[pyclass]
#[derive(Clone)]
pub struct PyDatabaseConfig {
    pub inner: DatabaseConfig,
}

#[pymethods]
impl PyDatabaseConfig {
    #[new]
    #[pyo3(signature = (engine, connection_string))]
    fn new(engine: &str, connection_string: &str) -> PyResult<Self> {
        let db_engine = match engine {
            "sqlite" => DatabaseEngine::Sqlite,
            "postgres" => DatabaseEngine::Postgres,
            "mysql" => DatabaseEngine::MySQL,
            _ => return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Unsupported database engine: {}", engine)
            )),
        };

        Ok(Self {
            inner: DatabaseConfig::new(db_engine, connection_string),
        })
    }
}

/// Python wrapper for MultiTableWriter
#[pyclass]
pub struct PyMultiTableWriter {
    pub inner: MultiTableWriter,
}

#[pymethods]
impl PyMultiTableWriter {
    #[new]
    #[pyo3(signature = (db_config, config=None))]
    fn new(db_config: PyDatabaseConfig, config: Option<PyMultiTableConfig>) -> PyResult<Self> {
        // Initialize tokio runtime for this thread
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;

        let config = config.unwrap_or_else(|| PyMultiTableConfig::new(
            3, 100, 5000, true, true, 100, 1000
        ));

        let connection = rt.block_on(async {
            DatabaseConnection::connect(db_config.inner).await
        }).map_err(|e| pyo3::exceptions::PyConnectionError::new_err(format!("Failed to connect: {}", e)))?;

        let schema = DatabaseSchema::new();
        let writer = MultiTableWriter::new(
            Arc::new(connection),
            None, // No mirror engine for now
            config.inner,
        );

        Ok(Self { inner: writer })
    }

    /// Begin a new multi-table transaction
    fn begin_transaction(&self) -> PyMultiTableTransaction {
        PyMultiTableTransaction {
            inner: self.inner.begin_transaction(),
        }
    }

    /// Begin a transaction and stage actions for multiple tables in one call
    fn begin_and_stage<'py>(
        &self,
        py: Python<'py>,
        table_actions: Vec<PyTableActions>,
    ) -> PyResult<&'py PyAny> {
        let actions: Vec<_> = table_actions.into_iter().map(|pa| pa.inner).collect();
        let writer = self.inner.clone();
        
        future_into_py(py, async move {
            writer.begin_and_stage(actions)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to begin and stage: {}", e)))
                .map(|tx| PyMultiTableTransaction { inner: tx })
        })
    }

    /// Commit a multi-table transaction atomically
    fn commit_transaction<'py>(
        &self,
        py: Python<'py>,
        transaction: PyMultiTableTransaction,
    ) -> PyResult<&'py PyAny> {
        let writer = self.inner.clone();
        
        future_into_py(py, async move {
            writer.commit_transaction(transaction.inner)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to commit transaction: {}", e)))
                .map(|result| PyMultiTableCommitResult { inner: result })
        })
    }

    /// Validate cross-table consistency for a transaction
    fn validate_cross_table_consistency<'py>(
        &self,
        py: Python<'py>,
        transaction: &PyMultiTableTransaction,
    ) -> PyResult<&'py PyAny> {
        let writer = self.inner.clone();
        let tx = transaction.inner.clone();
        
        future_into_py(py, async move {
            writer.validate_cross_table_consistency(&tx)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Validation failed: {}", e)))
                .map(|violations| {
                    violations.into_iter().map(|v| PyConsistencyViolation { inner: v }).collect::<Vec<_>>()
                })
        })
    }
}

/// Python wrapper for MultiTableTransaction
#[pyclass]
#[derive(Clone)]
pub struct PyMultiTableTransaction {
    pub inner: MultiTableTransaction,
}

#[pymethods]
impl PyMultiTableTransaction {
    /// Get the transaction ID
    #[getter]
    fn transaction_id(&self) -> String {
        self.inner.transaction_id.clone()
    }

    /// Get the number of tables in this transaction
    fn table_count(&self) -> usize {
        self.inner.table_count()
    }

    /// Get the total number of actions in this transaction
    fn total_action_count(&self) -> usize {
        self.inner.total_action_count()
    }

    /// Add actions for a table
    fn add_actions(&mut self, table_actions: PyTableActions) -> PyResult<()> {
        self.inner.stage_actions(table_actions.inner)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to add actions: {}", e)))
    }

    /// Remove all staged actions for a table
    fn unstage_actions(&mut self, table_id: &str) -> PyResult<PyTableActions> {
        self.inner.unstage_actions(table_id)
            .map(|actions| PyTableActions { inner: actions })
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to unstage actions: {}", e)))
    }

    /// Clear all staged actions
    fn clear_all_staged(&mut self) -> PyResult<()> {
        self.inner.clear_all_staged()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to clear staged actions: {}", e)))
    }

    /// Get a summary of the transaction
    fn summary(&self) -> PyTransactionSummary {
        PyTransactionSummary {
            inner: self.inner.summary(),
        }
    }
}

/// Python wrapper for TableActions
#[pyclass]
#[derive(Clone)]
pub struct PyTableActions {
    pub inner: deltalakedb_sql::TableActions,
}

#[pymethods]
impl PyTableActions {
    #[new]
    #[pyo3(signature = (table_id, expected_version))]
    fn new(table_id: String, expected_version: i64) -> Self {
        Self {
            inner: deltalakedb_sql::TableActions::new(table_id, expected_version),
        }
    }

    /// Get the table ID
    #[getter]
    fn table_id(&self) -> String {
        self.inner.table_id().to_string()
    }

    /// Get the expected version
    #[getter]
    fn expected_version(&self) -> i64 {
        self.inner.expected_version()
    }

    /// Get the number of actions
    fn action_count(&self) -> usize {
        self.inner.action_count()
    }

    /// Add files to the table
    fn add_files(&mut self, files: Vec<PyAddFile>) -> PyResult<()> {
        let add_files: Vec<_> = files.into_iter().map(|f| f.inner).collect();
        self.inner.add_files(add_files)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to add files: {}", e)))
    }

    /// Remove files from the table
    fn remove_files(&mut self, files: Vec<PyRemoveFile>) -> PyResult<()> {
        let remove_files: Vec<_> = files.into_iter().map(|f| f.inner).collect();
        self.inner.remove_files(remove_files)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to remove files: {}", e)))
    }

    /// Update table metadata
    fn update_metadata(&mut self, metadata: PyMetadata) -> PyResult<()> {
        self.inner.update_metadata(metadata.inner)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to update metadata: {}", e)))
    }

    /// Update table protocol
    fn update_protocol(&mut self, protocol: PyProtocol) -> PyResult<()> {
        self.inner.update_protocol(protocol.inner)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to update protocol: {}", e)))
    }

    /// Set operation
    fn with_operation(&mut self, operation: String) -> PyResult<()> {
        self.inner.with_operation(operation)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to set operation: {}", e)))
    }

    /// Set operation parameters
    fn with_operation_params(&mut self, params: std::collections::HashMap<String, String>) -> PyResult<()> {
        self.inner.with_operation_params(params)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to set operation params: {}", e)))
    }
}

/// Python wrapper for AddFile
#[pyclass]
#[derive(Clone)]
pub struct PyAddFile {
    pub inner: deltalakedb_core::AddFile,
}

#[pymethods]
impl PyAddFile {
    #[new]
    #[pyo3(signature = (path, size, modification_time=None, data_change=true, partition_values=None, stats=None, tags=None))]
    fn new(
        path: String,
        size: i64,
        modification_time: Option<DateTime<Utc>>,
        data_change: bool,
        partition_values: Option<std::collections::HashMap<String, String>>,
        stats: Option<String>,
        tags: Option<std::collections::HashMap<String, String>>,
    ) -> Self {
        Self {
            inner: deltalakedb_core::AddFile {
                path,
                size,
                modification_time: modification_time.unwrap_or_else(Utc::now),
                data_change,
                partition_values: partition_values.unwrap_or_default(),
                stats,
                tags,
            },
        }
    }
}

/// Python wrapper for RemoveFile
#[pyclass]
#[derive(Clone)]
pub struct PyRemoveFile {
    pub inner: deltalakedb_core::RemoveFile,
}

#[pymethods]
impl PyRemoveFile {
    #[new]
    #[pyo3(signature = (path, deletion_timestamp=None, data_change=true, extended_file_metadata=None, partition_values=None, size=None))]
    fn new(
        path: String,
        deletion_timestamp: Option<DateTime<Utc>>,
        data_change: bool,
        extended_file_metadata: Option<bool>,
        partition_values: Option<std::collections::HashMap<String, String>>,
        size: Option<i64>,
    ) -> Self {
        Self {
            inner: deltalakedb_core::RemoveFile {
                path,
                deletion_timestamp: deletion_timestamp.unwrap_or_else(Utc::now),
                data_change,
                extended_file_metadata,
                partition_values: partition_values.unwrap_or_default(),
                size,
            },
        }
    }
}

/// Python wrapper for Metadata
#[pyclass]
#[derive(Clone)]
pub struct PyMetadata {
    pub inner: deltalakedb_core::Metadata,
}

#[pymethods]
impl PyMetadata {
    #[new]
    #[pyo3(signature = (id, name, description=None, format=None, schema_string=None, partition_columns=None, created_time=None, configuration=None))]
    fn new(
        id: String,
        name: String,
        description: Option<String>,
        format: Option<PyFormat>,
        schema_string: Option<String>,
        partition_columns: Option<Vec<String>>,
        created_time: Option<DateTime<Utc>>,
        configuration: Option<std::collections::HashMap<String, String>>,
    ) -> Self {
        Self {
            inner: deltalakedb_core::Metadata {
                id,
                name,
                description,
                format: format.map(|f| f.inner),
                schema_string,
                partition_columns,
                created_time,
                configuration: configuration.unwrap_or_default(),
            },
        }
    }
}

/// Python wrapper for Format
#[pyclass]
#[derive(Clone)]
pub struct PyFormat {
    pub inner: deltalakedb_core::Format,
}

#[pymethods]
impl PyFormat {
    #[new]
    #[pyo3(signature = (provider=None, options=None))]
    fn new(
        provider: Option<String>,
        options: Option<std::collections::HashMap<String, String>>,
    ) -> Self {
        Self {
            inner: deltalakedb_core::Format {
                provider,
                options: options.unwrap_or_default(),
            },
        }
    }
}

/// Python wrapper for Protocol
#[pyclass]
#[derive(Clone)]
pub struct PyProtocol {
    pub inner: deltalakedb_core::Protocol,
}

#[pymethods]
impl PyProtocol {
    #[new]
    #[pyo3(signature = (min_reader_version=None, min_writer_version=None))]
    fn new(
        min_reader_version: Option<i32>,
        min_writer_version: Option<i32>,
    ) -> Self {
        Self {
            inner: deltalakedb_core::Protocol {
                min_reader_version,
                min_writer_version,
            },
        }
    }
}

/// Python wrapper for MultiTableCommitResult
#[pyclass]
pub struct PyMultiTableCommitResult {
    pub inner: deltalakedb_sql::MultiTableCommitResult,
}

#[pymethods]
impl PyMultiTableCommitResult {
    /// Get the transaction summary
    #[getter]
    fn transaction(&self) -> PyTransactionSummary {
        PyTransactionSummary {
            inner: self.inner.transaction.clone(),
        }
    }

    /// Get the table results
    #[getter]
    fn table_results(&self) -> Vec<PyTableCommitResult> {
        self.inner.table_results.iter()
            .map(|r| PyTableCommitResult { inner: r.clone() })
            .collect()
    }

    /// Get the mirroring results
    #[getter]
    fn mirroring_results(&self) -> Option<Vec<PyMirroringResult>> {
        self.inner.mirroring_results.as_ref()
            .map(|results| results.iter()
                .map(|r| PyMirroringResult { inner: r.clone() })
                .collect())
    }
}

/// Python wrapper for TransactionSummary
#[pyclass]
#[derive(Clone)]
pub struct PyTransactionSummary {
    pub inner: deltalakedb_sql::TransactionSummary,
}

#[pymethods]
impl PyTransactionSummary {
    /// Get the transaction ID
    #[getter]
    fn transaction_id(&self) -> String {
        self.inner.transaction_id.clone()
    }

    /// Get the number of tables
    #[getter]
    fn table_count(&self) -> usize {
        self.inner.table_count
    }

    /// Get the total action count
    #[getter]
    fn total_action_count(&self) -> usize {
        self.inner.total_action_count
    }

    /// Get the status
    #[getter]
    fn status(&self) -> String {
        format!("{:?}", self.inner.status)
    }

    /// Get the created time
    #[getter]
    fn created_time(&self) -> DateTime<Utc> {
        self.inner.created_time
    }
}

/// Python wrapper for TableCommitResult
#[pyclass]
#[derive(Clone)]
pub struct PyTableCommitResult {
    pub inner: deltalakedb_sql::TableCommitResult,
}

#[pymethods]
impl PyTableCommitResult {
    /// Get the table ID
    #[getter]
    fn table_id(&self) -> String {
        self.inner.table_id.clone()
    }

    /// Get the version
    #[getter]
    fn version(&self) -> i64 {
        self.inner.version
    }

    /// Get the action count
    #[getter]
    fn action_count(&self) -> usize {
        self.inner.action_count
    }

    /// Get success status
    #[getter]
    fn success(&self) -> bool {
        self.inner.success
    }

    /// Get the error
    #[getter]
    fn error(&self) -> Option<String> {
        self.inner.error.clone()
    }

    /// Get mirroring triggered status
    #[getter]
    fn mirroring_triggered(&self) -> bool {
        self.inner.mirroring_triggered
    }
}

/// Python wrapper for MirroringResult
#[pyclass]
#[derive(Clone)]
pub struct PyMirroringResult {
    pub inner: deltalakedb_sql::MirroringResult,
}

#[pymethods]
impl PyMirroringResult {
    /// Get the table ID
    #[getter]
    fn table_id(&self) -> String {
        self.inner.table_id.clone()
    }

    /// Get the version
    #[getter]
    fn version(&self) -> i64 {
        self.inner.version
    }

    /// Get success status
    #[getter]
    fn success(&self) -> bool {
        self.inner.success
    }

    /// Get the error
    #[getter]
    fn error(&self) -> Option<String> {
        self.inner.error.clone()
    }

    /// Get the timestamp
    #[getter]
    fn timestamp(&self) -> DateTime<Utc> {
        self.inner.timestamp
    }
}

/// Python wrapper for ConsistencyViolation
#[pyclass]
pub struct PyConsistencyViolation {
    pub inner: deltalakedb_sql::ConsistencyViolation,
}

#[pymethods]
impl PyConsistencyViolation {
    /// Get the violation type
    #[getter]
    fn violation_type(&self) -> String {
        format!("{:?}", self.inner.violation_type)
    }

    /// Get the table ID
    #[getter]
    fn table_id(&self) -> String {
        self.inner.table_id.clone()
    }

    /// Get the description
    #[getter]
    fn description(&self) -> String {
        self.inner.description.clone()
    }
}

/// Python module definition
#[pymodule]
fn delkalakedb(_py: Python, m: &PyModule) -> PyResult<()> {
    // Classes
    m.add_class::<PyMultiTableConfig>()?;
    m.add_class::<PyDatabaseConfig>()?;
    m.add_class::<PyMultiTableWriter>()?;
    m.add_class::<PyMultiTableTransaction>()?;
    m.add_class::<PyTableActions>()?;
    m.add_class::<PyAddFile>()?;
    m.add_class::<PyRemoveFile>()?;
    m.add_class::<PyMetadata>()?;
    m.add_class::<PyFormat>()?;
    m.add_class::<PyProtocol>()?;
    m.add_class::<PyMultiTableCommitResult>()?;
    m.add_class::<PyTransactionSummary>()?;
    m.add_class::<PyTableCommitResult>()?;
    m.add_class::<PyMirroringResult>()?;
    m.add_class::<PyConsistencyViolation>()?;

    // Module-level functions
    m.add_function(wrap_pyfunction!(create_test_schema, m)?)?;

    Ok(())
}

/// Create a test database schema for testing purposes
#[pyfunction]
fn create_test_schema() -> PyDatabaseSchema {
    PyDatabaseSchema {
        inner: DatabaseSchema::new(),
    }
}

/// Python wrapper for DatabaseSchema
#[pyclass]
pub struct PyDatabaseSchema {
    pub inner: DatabaseSchema,
}

#[pymethods]
impl PyDatabaseSchema {
    #[new]
    fn new() -> Self {
        Self {
            inner: DatabaseSchema::new(),
        }
    }

    /// Add a table to the schema
    fn add_table(&mut self, table_id: String, table_schema: PyTableSchema) -> PyResult<()> {
        self.inner.add_table(&table_id, table_schema.inner)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to add table: {}", e)))
    }

    /// Get a table from the schema
    fn get_table(&self, table_id: &str) -> Option<PyTableSchema> {
        self.inner.get_table(table_id)
            .map(|schema| PyTableSchema { inner: schema.clone() })
    }

    /// List all tables in the schema
    fn list_tables(&self) -> Vec<String> {
        self.inner.list_tables()
    }

    /// Check if a table exists
    fn has_table(&self, table_id: &str) -> bool {
        self.inner.has_table(table_id)
    }
}

/// Python wrapper for TableSchema
#[pyclass]
pub struct PyTableSchema {
    pub inner: TableSchema,
}

#[pymethods]
impl PyTableSchema {
    #[new]
    #[pyo3(signature = (table_id, latest_version=None, created_time=None))]
    fn new(
        table_id: String,
        latest_version: Option<i64>,
        created_time: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            inner: TableSchema {
                table_id,
                latest_version: latest_version.unwrap_or(-1),
                created_time: created_time.unwrap_or_else(Utc::now),
            },
        }
    }

    /// Get the table ID
    #[getter]
    fn table_id(&self) -> String {
        self.inner.table_id.clone()
    }

    /// Get the latest version
    #[getter]
    fn latest_version(&self) -> i64 {
        self.inner.latest_version
    }

    /// Get the created time
    #[getter]
    fn created_time(&self) -> DateTime<Utc> {
        self.inner.created_time
    }
}