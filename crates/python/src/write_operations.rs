//! Write operations integration with deltalake package
//!
//! This module provides seamless integration with the deltalake package's write_deltalake function
//! while using SQL-backed metadata for Delta table operations.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyAny, PyTuple};
use std::collections::HashMap;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{error::DeltaLakeError, error::DeltaLakeErrorKind, connection::SqlConnection};
use deltalakedb_core::{Table, Commit, Protocol, Metadata, Action, DeltaError, File};
use deltalakedb_sql::{DatabaseAdapter, TableSnapshot, ConsistencySnapshot};
use serde_json::{json, Value};

/// Write operation modes for Delta tables
#[pyclass]
#[derive(Debug, Clone)]
pub enum WriteMode {
    Append,
    Overwrite,
    Merge,
    ErrorIfExists,
    Ignore,
}

/// Write configuration for Delta table operations
#[pyclass]
#[derive(Debug, Clone)]
pub struct WriteConfig {
    #[pyo3(get)]
    pub mode: WriteMode,
    #[pyo3(get)]
    pub partition_by: Option<Vec<String>>,
    #[pyo3(get)]
    pub overwrite_schema: bool,
    #[pyo3(get)]
    pub schema_mode: String,  // "merge", "overwrite", etc.
    #[pyo3(get)]
    pub predicate: Option<String>,
    #[pyo3(get)]
    pub writer_properties: HashMap<String, String>,
}

#[pymethods]
impl WriteConfig {
    /// Create a new WriteConfig with default settings
    #[new]
    fn new(mode: WriteMode) -> Self {
        Self {
            mode,
            partition_by: None,
            overwrite_schema: false,
            schema_mode: "merge".to_string(),
            predicate: None,
            writer_properties: HashMap::new(),
        }
    }

    /// Set partition columns
    fn partition_columns(mut self, columns: Vec<String>) -> Self {
        self.partition_by = Some(columns);
        self
    }

    /// Enable schema overwriting
    fn enable_schema_overwrite(mut self) -> Self {
        self.overwrite_schema = true;
        self.schema_mode = "overwrite".to_string();
        self
    }

    /// Set write predicate (for merge operations)
    fn with_predicate(mut self, predicate: String) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Add writer properties
    fn with_writer_property(mut self, key: String, value: String) -> Self {
        self.writer_properties.insert(key, value);
        self
    }
}

/// Represents a file action within a write operation
#[pyclass]
#[derive(Debug, Clone)]
pub struct FileAction {
    #[pyo3(get)]
    pub path: String,
    #[pyo3(get)]
    pub size: i64,
    #[pyo3(get)]
    pub modification_time: i64,
    #[pyo3(get)]
    pub data_change: bool,
    #[pyo3(get)]
    pub is_add: bool,
    #[pyo3(get)]
    pub partition_values: HashMap<String, String>,
    #[pyo3(get)]
    pub stats: Option<String>,
    #[pyo3(get)]
    pub tags: Option<HashMap<String, String>>,
}

impl FileAction {
    fn from_core_file(file: &File) -> Self {
        Self {
            path: file.path.clone(),
            size: file.size,
            modification_time: file.modification_time,
            data_change: file.data_change,
            is_add: file.is_add,
            partition_values: file.partition_values
                .as_object()
                .map(|obj| obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect())
                .unwrap_or_default(),
            stats: file.stats.as_ref().map(|s| s.to_string()),
            tags: file.tags.as_ref().and_then(|t| {
                t.as_object().map(|obj| obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect())
            }),
        }
    }

    fn to_core_file(&self, table_id: Uuid, commit_id: Uuid) -> File {
        let partition_values = if self.partition_values.is_empty() {
            Value::Null
        } else {
            json!(self.partition_values)
        };

        let stats = self.stats.as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(Value::Null);

        let tags = if let Some(ref tags) = self.tags {
            json!(tags)
        } else {
            Value::Null
        };

        File {
            id: Uuid::new_v4(),
            table_id,
            commit_id,
            path: self.path.clone(),
            size: self.size,
            modification_time: self.modification_time,
            is_add: self.is_add,
            data_change: self.data_change,
            stats: Some(stats),
            tags: Some(tags),
            partition_values,
        }
    }
}

/// Result of a write operation
#[pyclass]
#[derive(Debug)]
pub struct WriteResult {
    #[pyo3(get)]
    pub table_path: String,
    #[pyo3(get)]
    pub version: i64,
    #[pyo3(get)]
    pub files_added: i64,
    #[pyo3(get)]
    pub files_removed: i64,
    #[pyo3(get)]
    pub bytes_added: i64,
    #[pyo3(get)]
    pub bytes_removed: i64,
    #[pyo3(get)]
    pub commit_timestamp: DateTime<Utc>,
    #[pyo3(get)]
    pub operation: String,
    #[pyo3(get)]
    pub partition_values: HashMap<String, String>,
}

impl WriteResult {
    fn new(
        table_path: String,
        version: i64,
        files_added: i64,
        files_removed: i64,
        bytes_added: i64,
        bytes_removed: i64,
        operation: String,
    ) -> Self {
        Self {
            table_path,
            version,
            files_added,
            files_removed,
            bytes_added,
            bytes_removed,
            commit_timestamp: Utc::now(),
            operation,
            partition_values: HashMap::new(),
        }
    }
}

/// Delta Lake writer that handles write operations with SQL-backed metadata
#[pyclass]
#[derive(Debug)]
pub struct DeltaWriter {
    #[pyo3(get)]
    pub table_path: String,
    #[pyo3(get)]
    pub table_id: String,
    connection: Option<SqlConnection>,
    current_version: i64,
}

#[pymethods]
impl DeltaWriter {
    /// Create a new DeltaWriter for a table
    #[new]
    fn new(table_path: String, table_id: String) -> PyResult<Self> {
        Ok(Self {
            table_path,
            table_id,
            connection: None,
            current_version: 0,
        })
    }

    /// Initialize writer with connection
    fn with_connection(&mut self, connection: SqlConnection) {
        self.connection = Some(connection);
    }

    /// Get current table version
    fn current_version(&self) -> PyResult<i64> {
        Ok(self.current_version)
    }

    /// Write data to the Delta table
    fn write(
        &mut self,
        py: Python,
        data: &PyAny,
        config: &WriteConfig,
    ) -> PyResult<WriteResult> {
        // This method integrates with deltalake's write_deltalake function
        py.allow_threads(|| {
            self.write_internal(data, config)
        })
    }

    /// Write data from pandas DataFrame
    fn write_pandas(
        &mut self,
        py: Python,
        df: &PyAny,
        config: &WriteConfig,
    ) -> PyResult<WriteResult> {
        // Import pandas and convert to arrow table
        let pandas = py.import("pandas")?;
        let arrow = py.import("pyarrow")?;

        // Convert pandas DataFrame to Arrow Table
        let arrow_table = arrow.call_method1("Table", (df,))?;

        self.write(py, arrow_table, config)
    }

    /// Write data from Arrow Table
    fn write_arrow(
        &mut self,
        py: Python,
        table: &PyAny,
        config: &WriteConfig,
    ) -> PyResult<WriteResult> {
        self.write(py, table, config)
    }

    /// Create merge operation
    fn create_merge_operation(
        &mut self,
        source: &PyAny,
        predicate: String,
        matched_clauses: Vec<&PyAny>,
        not_matched_clauses: Vec<&PyAny>,
    ) -> PyResult<WriteResult> {
        // Implementation for MERGE operations
        // This is a complex operation that requires careful handling of predicates and clauses
        todo!("Implement merge operation")
    }

    /// Vacuum old files from the table
    fn vacuum(
        &mut self,
        dry_run: Option<bool>,
        retain_hours: Option<i64>,
    ) -> PyResult<WriteResult> {
        let dry_run = dry_run.unwrap_or(true);
        let retain_hours = retain_hours.unwrap_or(168); // 7 days default

        // Implementation for VACUUM operation
        // This requires identifying expired files and removing them
        todo!("Implement vacuum operation")
    }

    /// Optimize the table
    fn optimize(
        &mut self,
        z_order_columns: Option<Vec<String>>,
        min_file_size: Option<i64>,
        max_concurrent_tasks: Option<i64>,
    ) -> PyResult<WriteResult> {
        // Implementation for OPTIMIZE operation
        // This requires file compaction and reorganization
        todo!("Implement optimize operation")
    }
}

impl DeltaWriter {
    /// Internal write implementation
    fn write_internal(
        &mut self,
        data: &PyAny,
        config: &WriteConfig,
    ) -> PyResult<WriteResult> {
        // Check if connection is available
        if self.connection.is_none() {
            return Err(DeltaLakeError::new(
                DeltaLakeErrorKind::ConnectionError,
                "No database connection available for write operation"
            ).into());
        }

        let connection = self.connection.as_ref().unwrap();
        let table_uuid = Uuid::parse_str(&self.table_id)
            .map_err(|e| DeltaLakeError::new(
                DeltaLakeErrorKind::InvalidData,
                format!("Invalid table UUID: {}", e)
            ))?;

        // Check table existence and handle different modes
        match config.mode {
            WriteMode::Append => self.handle_append_write(data, config, connection, table_uuid)?,
            WriteMode::Overwrite => self.handle_overwrite_write(data, config, connection, table_uuid)?,
            WriteMode::Merge => self.handle_merge_write(data, config, connection, table_uuid)?,
            WriteMode::ErrorIfExists => self.handle_error_if_exists(data, config, connection, table_uuid)?,
            WriteMode::Ignore => self.handle_ignore_write(data, config, connection, table_uuid)?,
        }

        // Create result
        Ok(WriteResult::new(
            self.table_path.clone(),
            self.current_version,
            0, // files_added - to be calculated
            0, // files_removed - to be calculated
            0, // bytes_added - to be calculated
            0, // bytes_removed - to be calculated
            format!("{:?}", config.mode),
        ))
    }

    /// Handle append write mode
    fn handle_append_write(
        &mut self,
        data: &PyAny,
        config: &WriteConfig,
        _connection: &SqlConnection,
        _table_uuid: Uuid,
    ) -> PyResult<()> {
        // For append mode, we need to:
        // 1. Convert data to Arrow format
        // 2. Write data to Parquet files
        // 3. Create file add actions
        // 4. Create commit metadata
        // 5. Write commit to SQL database

        // This is a complex operation that requires integration with
        // the deltalake package's write functionality

        todo!("Implement append write logic")
    }

    /// Handle overwrite write mode
    fn handle_overwrite_write(
        &mut self,
        data: &PyAny,
        config: &WriteConfig,
        _connection: &SqlConnection,
        _table_uuid: Uuid,
    ) -> PyResult<()> {
        // For overwrite mode, we need to:
        // 1. Get current active files
        // 2. Create remove actions for all files (if predicate is None)
        // 3. Convert data to Arrow format
        // 4. Write data to Parquet files
        // 5. Create file add actions
        // 6. Create commit metadata
        // 7. Write commit to SQL database

        todo!("Implement overwrite write logic")
    }

    /// Handle merge write mode
    fn handle_merge_write(
        &mut self,
        data: &PyAny,
        config: &WriteConfig,
        _connection: &SqlConnection,
        _table_uuid: Uuid,
    ) -> PyResult<()> {
        // For merge mode, we need to:
        // 1. Apply merge predicate
        // 2. Process matched and not-matched clauses
        // 3. Create appropriate file add/remove actions
        // 4. Create commit metadata
        // 5. Write commit to SQL database

        todo!("Implement merge write logic")
    }

    /// Handle error-if-exists write mode
    fn handle_error_if_exists(
        &mut self,
        data: &PyAny,
        config: &WriteConfig,
        _connection: &SqlConnection,
        _table_uuid: Uuid,
    ) -> PyResult<()> {
        // Check if table exists and has data
        if self.current_version > 0 {
            return Err(DeltaLakeError::new(
                DeltaLakeErrorKind::TableExists,
                "Table already exists and mode is 'error_if_exists'"
            ).into());
        }

        // Fall back to append for new tables
        self.handle_append_write(data, config, _connection, _table_uuid)
    }

    /// Handle ignore write mode
    fn handle_ignore_write(
        &mut self,
        data: &PyAny,
        config: &WriteConfig,
        _connection: &SqlConnection,
        _table_uuid: Uuid,
    ) -> PyResult<()> {
        // If table exists and has data, ignore the operation
        if self.current_version > 0 {
            // Create empty result indicating no operation was performed
            return Ok(());
        }

        // Fall back to append for new tables
        self.handle_append_write(data, config, _connection, _table_uuid)
    }

    /// Get the database adapter from connection
    fn get_adapter(&self) -> PyResult<Arc<dyn DatabaseAdapter>> {
        let connection = self.connection.as_ref()
            .ok_or_else(|| DeltaLakeError::new(
                DeltaLakeErrorKind::ConnectionError,
                "No database connection available"
            ))?;

        Ok(connection.adapter.clone())
    }

    /// Create commit metadata for write operations
    fn create_commit_metadata(
        &self,
        operation: &str,
        file_actions: Vec<FileAction>,
        config: &WriteConfig,
    ) -> PyResult<Commit> {
        let commit_info = json!({
            "operation": operation,
            "mode": format!("{:?}", config.mode),
            "timestamp": Utc::now().timestamp_millis(),
            "userId": None,
            "userName": None,
            "jobId": None,
            "jobName": None,
            "notebookId": None,
            "notebookInfo": None,
            "clusterId": None,
            "readVersion": self.current_version,
            "isolationLevel": "WriteSerializable",
            "isBlindAppend": matches!(config.mode, WriteMode::Append),
            "operationParameters": {
                "mode": format!("{:?}", config.mode),
                "partitionBy": config.partition_by,
                "predicate": config.predicate,
                "overwriteSchema": config.overwrite_schema,
            },
            "engineInfo": "deltalakedb-sql",
            "txnId": Uuid::new_v4().to_string(),
        });

        Ok(Commit {
            id: Uuid::new_v4(),
            table_id: Uuid::parse_str(&self.table_id)
                .map_err(|e| DeltaLakeError::new(
                    DeltaLakeErrorKind::InvalidData,
                    format!("Invalid table UUID: {}", e)
                ))?,
            version: self.current_version + 1,
            timestamp: Utc::now(),
            operation_type: operation.to_string(),
            operation_parameters: json!({
                "mode": format!("{:?}", config.mode),
                "partitionBy": config.partition_by,
            }),
            commit_info,
            checksum: None,
            in_progress: false,
        })
    }

    /// Process schema evolution
    fn process_schema_evolution(
        &mut self,
        new_schema: &Value,
        config: &WriteConfig,
    ) -> PyResult<()> {
        // Implement schema evolution logic
        // This includes:
        // 1. Compare existing schema with new schema
        // 2. Apply appropriate evolution strategy based on config.schema_mode
        // 3. Update table metadata if schema changes

        todo!("Implement schema evolution")
    }

    /// Apply partitioning to data
    fn apply_partitioning(
        &self,
        data: &PyAny,
        partition_columns: &[String],
    ) -> PyResult<Vec<HashMap<String, String>>> {
        // Implement partitioning logic
        // This includes:
        // 1. Extract partition column values from data
        // 2. Group data by partition values
        // 3. Return partition specifications for file writing

        todo!("Implement partitioning")
    }
}

/// Transaction handler for write operations
#[pyclass]
#[derive(Debug)]
pub struct WriteTransaction {
    #[pyo3(get)]
    pub transaction_id: String,
    writer: DeltaWriter,
    operations: Vec<WriteOperation>,
}

#[derive(Debug, Clone)]
enum WriteOperation {
    AddFile(FileAction),
    RemoveFile(String), // path
    UpdateMetadata(Metadata),
    UpdateProtocol(Protocol),
}

#[pymethods]
impl WriteTransaction {
    /// Create a new write transaction
    #[new]
    fn new(writer: DeltaWriter) -> Self {
        Self {
            transaction_id: Uuid::new_v4().to_string(),
            writer,
            operations: Vec::new(),
        }
    }

    /// Add file to transaction
    fn add_file(&mut self, file_action: FileAction) {
        self.operations.push(WriteOperation::AddFile(file_action));
    }

    /// Remove file from transaction
    fn remove_file(&mut self, path: String) {
        self.operations.push(WriteOperation::RemoveFile(path));
    }

    /// Update table metadata in transaction
    fn update_metadata(&mut self, metadata: Metadata) {
        self.operations.push(WriteOperation::UpdateMetadata(metadata));
    }

    /// Update protocol in transaction
    fn update_protocol(&mut self, protocol: Protocol) {
        self.operations.push(WriteOperation::UpdateProtocol(protocol));
    }

    /// Commit the transaction
    fn commit(&mut self) -> PyResult<WriteResult> {
        // Execute all operations atomically
        // This requires careful coordination with the SQL adapter's transaction support

        todo!("Implement transaction commit")
    }

    /// Rollback the transaction
    fn rollback(&mut self) -> PyResult<()> {
        // Clear all operations without committing
        self.operations.clear();
        Ok(())
    }

    /// Get transaction size (number of operations)
    fn size(&self) -> PyResult<usize> {
        Ok(self.operations.len())
    }
}

/// Error handling and recovery utilities
#[pyclass]
#[derive(Debug)]
pub struct WriteErrorHandler {
    #[pyo3(get)]
    pub max_retries: i32,
    #[pyo3(get)]
    pub retry_delay_ms: i64,
    #[pyo3(get)]
    pub exponential_backoff: bool,
}

#[pymethods]
impl WriteErrorHandler {
    /// Create new error handler
    #[new]
    fn new(max_retries: Option<i32>, retry_delay_ms: Option<i64>, exponential_backoff: Option<bool>) -> Self {
        Self {
            max_retries: max_retries.unwrap_or(3),
            retry_delay_ms: retry_delay_ms.unwrap_or(1000),
            exponential_backoff: exponential_backoff.unwrap_or(true),
        }
    }

    /// Handle write operation with retry logic
    fn handle_with_retry<F, T>(&self, operation: F) -> PyResult<T>
    where
        F: Fn() -> PyResult<T>,
    {
        let mut last_error = None;
        let mut delay = self.retry_delay_ms;

        for attempt in 0..=self.max_retries {
            match operation() {
                Ok(result) => return Ok(result),
                Err(error) => {
                    last_error = Some(error.clone());

                    // Check if this is a retryable error
                    if !self.is_retryable_error(&error) || attempt == self.max_retries {
                        break;
                    }

                    // Sleep before retry
                    std::thread::sleep(std::time::Duration::from_millis(delay as u64));

                    // Update delay for exponential backoff
                    if self.exponential_backoff {
                        delay *= 2;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Unknown error")))
    }

    /// Check if an error is retryable
    fn is_retryable_error(&self, error: &PyErr) -> bool {
        // Check for retryable error types
        // This includes connection errors, timeouts, and certain database errors

        if error.is_instance::<pyo3::exceptions::PyConnectionError>() {
            return true;
        }

        if error.is_instance::<pyo3::exceptions::PyTimeoutError>() {
            return true;
        }

        // Check for DeltaLake specific retryable errors
        if let Some(delta_error) = error.downcast_ref::<DeltaLakeError>() {
            match delta_error.kind {
                DeltaLakeErrorKind::ConnectionError |
                DeltaLakeErrorKind::TimeoutError |
                DeltaLakeErrorKind::TransactionError => return true,
                _ => return false,
            }
        }

        false
    }
}

/// Integration function to bridge with deltalake package
#[pyfunction]
pub fn write_deltalake(
    py: Python,
    table_or_uri: &str,
    data: &PyAny,
    mode: Option<&str>,
    partition_by: Option<Vec<String>>,
    overwrite_schema: Option<bool>,
    schema_mode: Option<&str>,
    predicate: Option<&str>,
) -> PyResult<WriteResult> {
    // Parse the mode
    let write_mode = match mode.unwrap_or("append") {
        "append" => WriteMode::Append,
        "overwrite" => WriteMode::Overwrite,
        "merge" => WriteMode::Merge,
        "error" | "error_if_exists" => WriteMode::ErrorIfExists,
        "ignore" => WriteMode::Ignore,
        _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Invalid mode. Must be one of: append, overwrite, merge, error, ignore"
        )),
    };

    // Create write configuration
    let mut config = WriteConfig::new(write_mode);
    if let Some(partitions) = partition_by {
        config = config.partition_columns(partitions);
    }
    if overwrite_schema.unwrap_or(false) {
        config = config.enable_schema_overwrite();
    }
    if let Some(schema_mode) = schema_mode {
        config.schema_mode = schema_mode.to_string();
    }
    if let Some(predicate) = predicate {
        config = config.with_predicate(predicate.to_string());
    }

    // Create writer and perform write operation
    let mut writer = DeltaWriter::new(table_or_uri.to_string(), Uuid::new_v4().to_string())?;

    // Check if this is a deltalake:// URI that we can handle directly
    if table_or_uri.starts_with("deltasql://") {
        // Parse URI and establish connection
        let uri = crate::uri::DeltaSqlUri::parse(table_or_uri)?;
        let connection = crate::connection::SqlConnection::new(uri.to_string(), None)?;
        writer.with_connection(connection);
    } else {
        // Try to integrate with standard deltalake package
        return integrate_with_deltalake_package(py, table_or_uri, data, &config);
    }

    writer.write(py, data, &config)
}

/// Integration with the standard deltalake package
fn integrate_with_deltalake_package(
    py: Python,
    table_or_uri: &str,
    data: &PyAny,
    config: &WriteConfig,
) -> PyResult<WriteResult> {
    // Try to import deltalake package
    let deltalake = match py.import("deltalake") {
        Ok(module) => module,
        Err(_) => {
            return Err(PyErr::new::<pyo3::exceptions::PyImportError, _>(
                "deltalake package not available. Install with: pip install deltalake"
            ));
        }
    };

    // Call deltalake.write_deltalake function
    let write_func = deltalake.getattr("write_deltalake")?;

    // Convert our config to deltalake parameters
    let kwargs = PyDict::new(py);
    kwargs.set_item("mode", format!("{:?}", config.mode).to_lowercase())?;

    if let Some(ref partitions) = config.partition_by {
        kwargs.set_item("partition_by", partitions)?;
    }

    if config.overwrite_schema {
        kwargs.set_item("overwrite_schema", true)?;
    }

    kwargs.set_item("schema_mode", config.schema_mode)?;

    if let Some(ref predicate) = config.predicate {
        kwargs.set_item("predicate", predicate)?;
    }

    // Call deltalake.write_deltalake
    let result = write_func.call((table_or_uri, data), Some(kwargs))?;

    // Convert deltalake result to our WriteResult
    convert_deltalake_result(py, result)
}

/// Convert deltalake write result to our WriteResult
fn convert_deltalake_result(py: Python, deltalake_result: &PyAny) -> PyResult<WriteResult> {
    let table_path = deltalake_result.getattr("table_path")?.extract::<String>()?;
    let version = deltalake_result.getattr("version")?.extract::<i64>()?;
    let files_added = deltalake_result.getattr("files_added")?.extract::<i64>()?;
    let files_removed = deltalake_result.getattr("files_removed")?.extract::<i64>()?;
    let bytes_added = deltalake_result.getattr("bytes_added")?.extract::<i64>()?;
    let bytes_removed = deltalake_result.getattr("bytes_removed")?.extract::<i64>()?;
    let operation = deltalake_result.getattr("operation")?.extract::<String>()?;

    Ok(WriteResult {
        table_path,
        version,
        files_added,
        files_removed,
        bytes_added,
        bytes_removed,
        commit_timestamp: Utc::now(),
        operation,
        partition_values: HashMap::new(),
    })
}