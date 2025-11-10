//! Core Python bindings for Delta Lake domain models
//!
//! This module provides Python bindings for the core Delta Lake entities including
//! tables, commits, files, protocols, and metadata using PyO3.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

use deltalakedb_core::{Table, Commit, Protocol, Metadata, Action};
use deltalakedb_sql::DatabaseConfig;
use crate::{error::DeltaLakeError, connection::SqlConnection, config::SqlConfig};

/// Python wrapper for Delta Table
#[pyclass]
#[derive(Debug, Clone)]
pub struct Table {
    #[pyo3(get)]
    pub table_id: String,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub description: Option<String>,
    #[pyo3(get)]
    pub created_at: DateTime<Utc>,
    #[pyo3(get)]
    pub updated_at: DateTime<Utc>,
    #[pyo3(get)]
    pub version: i64,
}

#[pymethods]
impl Table {
    /// Create a new Table instance
    #[new]
    fn new(
        table_id: String,
        name: String,
        description: Option<String>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        version: i64,
    ) -> Self {
        Self {
            table_id,
            name,
            description,
            created_at,
            updated_at,
            version,
        }
    }

    /// Get table as a dictionary
    fn to_dict(&self) -> PyResult<HashMap<String, String>> {
        let mut dict = HashMap::new();
        dict.insert("table_id".to_string(), self.table_id.clone());
        dict.insert("name".to_string(), self.name.clone());
        dict.insert("version".to_string(), self.version.to_string());
        dict.insert("created_at".to_string(), self.created_at.to_rfc3339());
        dict.insert("updated_at".to_string(), self.updated_at.to_rfc3339());

        if let Some(desc) = &self.description {
            dict.insert("description".to_string(), desc.clone());
        }

        Ok(dict)
    }

    /// Get table representation as string
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "DeltaTable(table_id='{}', name='{}', version={})",
            self.table_id, self.name, self.version
        ))
    }

    /// Get table representation as string
    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

/// Python wrapper for Delta Commit
#[pyclass]
#[derive(Debug, Clone)]
pub struct Commit {
    #[pyo3(get)]
    pub commit_id: String,
    #[pyo3(get)]
    pub table_id: String,
    #[pyo3(get)]
    pub version: i64,
    #[pyo3(get)]
    pub timestamp: DateTime<Utc>,
    #[pyo3(get)]
    pub operation: String,
    #[pyo3(get)]
    pub user_id: Option<String>,
    #[pyo3(get)]
    pub user_name: Option<String>,
}

#[pymethods]
impl Commit {
    /// Create a new Commit instance
    #[new]
    fn new(
        commit_id: String,
        table_id: String,
        version: i64,
        timestamp: DateTime<Utc>,
        operation: String,
        user_id: Option<String>,
        user_name: Option<String>,
    ) -> Self {
        Self {
            commit_id,
            table_id,
            version,
            timestamp,
            operation,
            user_id,
            user_name,
        }
    }

    /// Get commit as a dictionary
    fn to_dict(&self) -> PyResult<HashMap<String, String>> {
        let mut dict = HashMap::new();
        dict.insert("commit_id".to_string(), self.commit_id.clone());
        dict.insert("table_id".to_string(), self.table_id.clone());
        dict.insert("version".to_string(), self.version.to_string());
        dict.insert("timestamp".to_string(), self.timestamp.to_rfc3339());
        dict.insert("operation".to_string(), self.operation.clone());

        if let Some(user_id) = &self.user_id {
            dict.insert("user_id".to_string(), user_id.clone());
        }
        if let Some(user_name) = &self.user_name {
            dict.insert("user_name".to_string(), user_name.clone());
        }

        Ok(dict)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "DeltaCommit(commit_id='{}', table_id='{}', version={})",
            self.commit_id, self.table_id, self.version
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

/// Python wrapper for Delta File
#[pyclass]
#[derive(Debug, Clone)]
pub struct File {
    #[pyo3(get)]
    pub path: String,
    #[pyo3(get)]
    pub size: i64,
    #[pyo3(get)]
    pub modification_time: DateTime<Utc>,
    #[pyo3(get)]
    pub data_change: bool,
    #[pyo3(get)]
    pub partition_values: HashMap<String, String>,
    #[pyo3(get)]
    pub stats: Option<HashMap<String, serde_json::Value>>,
}

#[pymethods]
impl File {
    /// Create a new File instance
    #[new]
    fn new(
        path: String,
        size: i64,
        modification_time: DateTime<Utc>,
        data_change: bool,
        partition_values: HashMap<String, String>,
        stats: Option<HashMap<String, serde_json::Value>>,
    ) -> Self {
        Self {
            path,
            size,
            modification_time,
            data_change,
            partition_values,
            stats,
        }
    }

    /// Get file as a dictionary
    fn to_dict(&self) -> PyResult<HashMap<String, String>> {
        let mut dict = HashMap::new();
        dict.insert("path".to_string(), self.path.clone());
        dict.insert("size".to_string(), self.size.to_string());
        dict.insert("modification_time".to_string(), self.modification_time.to_rfc3339());
        dict.insert("data_change".to_string(), self.data_change.to_string());

        // Convert partition values to string representation
        let partition_str = self.partition_values
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",");
        dict.insert("partition_values".to_string(), partition_str);

        if let Some(stats) = &self.stats {
            dict.insert("stats".to_string(),
                serde_json::to_string(stats).unwrap_or_default());
        }

        Ok(dict)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "DeltaFile(path='{}', size={}, data_change={})",
            self.path, self.size, self.data_change
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

/// Python wrapper for Delta Protocol
#[pyclass]
#[derive(Debug, Clone)]
pub struct Protocol {
    #[pyo3(get)]
    pub min_reader_version: i32,
    #[pyo3(get)]
    pub min_writer_version: i32,
    #[pyo3(get)]
    pub reader_features: Vec<String>,
    #[pyo3(get)]
    pub writer_features: Vec<String>,
}

#[pymethods]
impl Protocol {
    /// Create a new Protocol instance
    #[new]
    fn new(
        min_reader_version: i32,
        min_writer_version: i32,
        reader_features: Vec<String>,
        writer_features: Vec<String>,
    ) -> Self {
        Self {
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        }
    }

    /// Get protocol as a dictionary
    fn to_dict(&self) -> PyResult<HashMap<String, String>> {
        let mut dict = HashMap::new();
        dict.insert("min_reader_version".to_string(), self.min_reader_version.to_string());
        dict.insert("min_writer_version".to_string(), self.min_writer_version.to_string());

        dict.insert("reader_features".to_string(),
            format!("[{}]", self.reader_features.join(", ")));
        dict.insert("writer_features".to_string(),
            format!("[{}]", self.writer_features.join(", ")));

        Ok(dict)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "DeltaProtocol(min_reader={}, min_writer={})",
            self.min_reader_version, self.min_writer_version
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

/// Python wrapper for Delta Metadata
#[pyclass]
#[derive(Debug, Clone)]
pub struct Metadata {
    #[pyo3(get)]
    pub configuration: HashMap<String, String>,
    #[pyo3(get)]
    pub partition_columns: Vec<String>,
    #[pyo3(get)]
    pub created_time: Option<DateTime<Utc>>,
    #[pyo3(get)]
    pub description: Option<String>,
    #[pyo3(get)]
    pub format_version: String,
}

#[pymethods]
impl Metadata {
    /// Create a new Metadata instance
    #[new]
    fn new(
        configuration: HashMap<String, String>,
        partition_columns: Vec<String>,
        created_time: Option<DateTime<Utc>>,
        description: Option<String>,
        format_version: String,
    ) -> Self {
        Self {
            configuration,
            partition_columns,
            created_time,
            description,
            format_version,
        }
    }

    /// Get metadata as a dictionary
    fn to_dict(&self) -> PyResult<HashMap<String, String>> {
        let mut dict = HashMap::new();

        dict.insert("format_version".to_string(), self.format_version.clone());
        dict.insert("partition_columns".to_string(),
            format!("[{}]", self.partition_columns.join(", ")));

        if let Some(created_time) = self.created_time {
            dict.insert("created_time".to_string(), created_time.to_rfc3339());
        }
        if let Some(description) = &self.description {
            dict.insert("description".to_string(), description.clone());
        }

        // Convert configuration to string representation
        let config_str = self.configuration
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",");
        dict.insert("configuration".to_string(), config_str);

        Ok(dict)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "DeltaMetadata(format='{}', partitions={})",
            self.format_version, self.partition_columns.len()
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

/// Connect to a Delta table using a URI
#[pyfunction]
pub fn connect_to_table(
    uri: &str,
    config: Option<SqlConfig>,
) -> PyResult<(Table, SqlConnection)> {
    // Parse URI and establish connection
    let parsed_uri = crate::uri::parse_uri(uri)
        .map_err(|e| DeltaLakeError::new(crate::error::DeltaLakeErrorKind::InvalidUri, e))?;

    let sql_config = config.unwrap_or_default();
    let connection = SqlConnection::connect(&parsed_uri, sql_config)
        .map_err(|e| DeltaLakeError::new(crate::error::DeltaLakeErrorKind::ConnectionError, e))?;

    // Load table metadata
    let table = connection.get_table()
        .map_err(|e| DeltaLakeError::new(crate::error::DeltaLakeErrorKind::MetadataError, e))?;

    Ok((table, connection))
}

/// Create a new Delta table
#[pyfunction]
pub fn create_table(
    uri: &str,
    name: String,
    description: Option<String>,
    partition_columns: Option<Vec<String>>,
    config: Option<SqlConfig>,
) -> PyResult<Table> {
    let parsed_uri = crate::uri::parse_uri(uri)
        .map_err(|e| DeltaLakeError::new(crate::error::DeltaLakeErrorKind::InvalidUri, e))?;

    let sql_config = config.unwrap_or_default();
    let mut connection = SqlConnection::connect(&parsed_uri, sql_config)
        .map_err(|e| DeltaLakeError::new(crate::error::DeltaLakeErrorKind::ConnectionError, e))?;

    let table = connection.create_table(name, description, partition_columns)
        .map_err(|e| DeltaLakeError::new(crate::error::DeltaLakeErrorKind::TableError, e))?;

    Ok(table)
}

/// List all Delta tables in a database
#[pyfunction]
pub fn list_tables(uri: &str, config: Option<SqlConfig>) -> PyResult<Vec<Table>> {
    let parsed_uri = crate::uri::parse_uri(uri)
        .map_err(|e| DeltaLakeError::new(crate::error::DeltaLakeErrorKind::InvalidUri, e))?;

    let sql_config = config.unwrap_or_default();
    let connection = SqlConnection::connect(&parsed_uri, sql_config)
        .map_err(|e| DeltaLakeError::new(crate::error::DeltaLakeErrorKind::ConnectionError, e))?;

    let tables = connection.list_tables()
        .map_err(|e| DeltaLakeError::new(crate::error::DeltaLakeErrorKind::MetadataError, e))?;

    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_table_creation() {
        let table = Table::new(
            "test-id".to_string(),
            "test-table".to_string(),
            Some("Test table".to_string()),
            Utc::now(),
            Utc::now(),
            1,
        );

        assert_eq!(table.name, "test-table");
        assert_eq!(table.version, 1);
        assert!(table.description.is_some());
    }

    #[test]
    fn test_table_to_dict() {
        let table = Table::new(
            "test-id".to_string(),
            "test-table".to_string(),
            Some("Test table".to_string()),
            Utc::now(),
            Utc::now(),
            1,
        );

        let dict = table.to_dict().unwrap();
        assert_eq!(dict.get("name"), Some(&"test-table".to_string()));
        assert_eq!(dict.get("version"), Some(&"1".to_string()));
        assert!(dict.contains_key("created_at"));
    }

    #[test]
    fn test_commit_creation() {
        let commit = Commit::new(
            "commit-id".to_string(),
            "table-id".to_string(),
            1,
            Utc::now(),
            "WRITE".to_string(),
            Some("user-123".to_string()),
            Some("test-user".to_string()),
        );

        assert_eq!(commit.operation, "WRITE");
        assert_eq!(commit.version, 1);
        assert!(commit.user_id.is_some());
        assert!(commit.user_name.is_some());
    }

    #[test]
    fn test_file_creation() {
        let mut partition_values = HashMap::new();
        partition_values.insert("date".to_string(), "2023-01-01".to_string());
        partition_values.insert("country".to_string(), "US".to_string());

        let file = File::new(
            "/path/to/file.parquet".to_string(),
            1024,
            Utc::now(),
            true,
            partition_values.clone(),
            None,
        );

        assert_eq!(file.path, "/path/to/file.parquet");
        assert_eq!(file.size, 1024);
        assert_eq!(file.partition_values, partition_values);
    }

    #[test]
    fn test_protocol_creation() {
        let protocol = Protocol::new(
            1,
            2,
            vec!["feature1".to_string(), "feature2".to_string()],
            vec!["writer_feature1".to_string()],
        );

        assert_eq!(protocol.min_reader_version, 1);
        assert_eq!(protocol.min_writer_version, 2);
        assert_eq!(protocol.reader_features.len(), 2);
        assert_eq!(protocol.writer_features.len(), 1);
    }

    #[test]
    fn test_metadata_creation() {
        let mut config = HashMap::new();
        config.insert("key1".to_string(), "value1".to_string());

        let metadata = Metadata::new(
            config,
            vec!["date".to_string(), "country".to_string()],
            Some(Utc::now()),
            Some("Test metadata".to_string()),
            "1.0".to_string(),
        );

        assert_eq!(metadata.format_version, "1.0");
        assert_eq!(metadata.partition_columns.len(), 2);
        assert!(metadata.description.is_some());
        assert!(metadata.configuration.contains_key("key1"));
    }
}