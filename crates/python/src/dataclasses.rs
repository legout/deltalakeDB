//! Python dataclass representations for Delta Lake domain models
//!
//! This module provides Python dataclass representations of core Delta Lake entities
//! to enable better type safety, IDE support, and data validation in Python.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use serde::{Deserialize, Serialize};

/// Dataclass representation for Delta Table metadata
#[pyclass]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableData {
    #[pyo3(get)]
    pub table_id: String,
    #[pyo3(get)]
    pub table_path: String,
    #[pyo3(get)]
    pub table_name: String,
    #[pyo3(get)]
    pub table_uuid: String,
    #[pyo3(get)]
    pub created_at: Option<DateTime<Utc>>,
    #[pyo3(get)]
    pub updated_at: Option<DateTime<Utc>>,
    #[pyo3(get)]
    pub deleted_at: Option<DateTime<Utc>>,
    #[pyo3(get)]
    pub description: Option<String>,
    #[pyo3(get)]
    pub partition_columns: Vec<String>,
    #[pyo3(get)]
    pub configuration: HashMap<String, serde_json::Value>,
    #[pyo3(get)]
    pub metadata: Option<HashMap<String, String>>,
}

#[pymethods]
impl TableData {
    /// Create a new TableData instance
    #[new]
    fn new(
        table_id: String,
        table_path: String,
        table_name: String,
        table_uuid: String,
    ) -> Self {
        Self {
            table_id,
            table_path,
            table_name,
            table_uuid,
            created_at: None,
            updated_at: None,
            deleted_at: None,
            description: None,
            partition_columns: Vec::new(),
            configuration: HashMap::new(),
            metadata: None,
        }
    }

    /// Set created timestamp
    fn with_created_at(mut self, created_at: DateTime<Utc>) -> Self {
        self.created_at = Some(created_at);
        self
    }

    /// Set updated timestamp
    fn with_updated_at(mut self, updated_at: DateTime<Utc>) -> Self {
        self.updated_at = Some(updated_at);
        self
    }

    /// Set description
    fn with_description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// Set partition columns
    fn with_partition_columns(mut self, partition_columns: Vec<String>) -> Self {
        self.partition_columns = partition_columns;
        self
    }

    /// Set configuration
    fn with_configuration(mut self, configuration: HashMap<String, serde_json::Value>) -> Self {
        self.configuration = configuration;
        self
    }

    /// Mark as deleted
    fn mark_deleted(mut self, deleted_at: DateTime<Utc>) -> Self {
        self.deleted_at = Some(deleted_at);
        self
    }

    /// Check if table is active (not deleted)
    fn is_active(&self) -> bool {
        self.deleted_at.is_none()
    }

    /// Get table age in days
    fn age_days(&self) -> Option<i64> {
        if let Some(created_at) = self.created_at {
            let duration = Utc::now() - created_at;
            Some(duration.num_days())
        } else {
            None
        }
    }

    /// Convert to dictionary
    fn to_dict(&self, py: Python) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);

        dict.set_item("table_id", &self.table_id)?;
        dict.set_item("table_path", &self.table_path)?;
        dict.set_item("table_name", &self.table_name)?;
        dict.set_item("table_uuid", &self.table_uuid)?;

        if let Some(ref created_at) = self.created_at {
            dict.set_item("created_at", created_at.to_rfc3339())?;
        }

        if let Some(ref updated_at) = self.updated_at {
            dict.set_item("updated_at", updated_at.to_rfc3339())?;
        }

        if let Some(ref deleted_at) = self.deleted_at {
            dict.set_item("deleted_at", deleted_at.to_rfc3339())?;
        }

        if let Some(ref description) = self.description {
            dict.set_item("description", description)?;
        }

        dict.set_item("partition_columns", &self.partition_columns)?;
        dict.set_item("configuration", &self.configuration)?;

        if let Some(ref metadata) = self.metadata {
            dict.set_item("metadata", metadata)?;
        }

        Ok(dict.into())
    }

    /// Create from dictionary
    #[staticmethod]
    fn from_dict(py: Python, data: &PyDict) -> PyResult<Self> {
        let table_id: String = data.get_item("table_id")?.extract()?;
        let table_path: String = data.get_item("table_path")?.extract()?;
        let table_name: String = data.get_item("table_name")?.extract()?;
        let table_uuid: String = data.get_item("table_uuid")?.extract()?;

        let mut instance = Self::new(table_id, table_path, table_name, table_uuid);

        // Optional fields
        if let Some(created_at) = data.get_item("created_at") {
            let created_str: String = created_at.extract()?;
            instance.created_at = DateTime::parse_from_rfc3339(&created_str)
                .map(|dt| dt.with_timezone(&Utc))
                .ok();
        }

        if let Some(updated_at) = data.get_item("updated_at") {
            let updated_str: String = updated_at.extract()?;
            instance.updated_at = DateTime::parse_from_rfc3339(&updated_str)
                .map(|dt| dt.with_timezone(&Utc))
                .ok();
        }

        if let Some(description) = data.get_item("description") {
            instance.description = Some(description.extract()?);
        }

        if let Some(partition_columns) = data.get_item("partition_columns") {
            instance.partition_columns = partition_columns.extract()?;
        }

        if let Some(configuration) = data.get_item("configuration") {
            instance.configuration = configuration.extract()?;
        }

        Ok(instance)
    }

    fn __repr__(&self) -> String {
        format!(
            "TableData(name='{}', path='{}', active={})",
            self.table_name,
            self.table_path,
            self.is_active()
        )
    }
}

/// Dataclass representation for Delta Commit
#[pyclass]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitData {
    #[pyo3(get)]
    pub commit_id: String,
    #[pyo3(get)]
    pub table_id: String,
    #[pyo3(get)]
    pub version: i64,
    #[pyo3(get)]
    pub timestamp: DateTime<Utc>,
    #[pyo3(get)]
    pub operation_type: String,
    #[pyo3(get)]
    pub operation_parameters: HashMap<String, serde_json::Value>,
    #[pyo3(get)]
    pub commit_info: HashMap<String, serde_json::Value>,
    #[pyo3(get)]
    pub checksum: Option<String>,
    #[pyo3(get)]
    pub in_progress: bool,
}

#[pymethods]
impl CommitData {
    /// Create a new CommitData instance
    #[new]
    fn new(
        commit_id: String,
        table_id: String,
        version: i64,
        timestamp: DateTime<Utc>,
        operation_type: String,
    ) -> Self {
        Self {
            commit_id,
            table_id,
            version,
            timestamp,
            operation_type,
            operation_parameters: HashMap::new(),
            commit_info: HashMap::new(),
            checksum: None,
            in_progress: false,
        }
    }

    /// Set operation parameters
    fn with_operation_parameters(mut self, params: HashMap<String, serde_json::Value>) -> Self {
        self.operation_parameters = params;
        self
    }

    /// Set commit info
    fn with_commit_info(mut self, info: HashMap<String, serde_json::Value>) -> Self {
        self.commit_info = info;
        self
    }

    /// Set checksum
    fn with_checksum(mut self, checksum: String) -> Self {
        self.checksum = Some(checksum);
        self
    }

    /// Mark as in progress
    fn mark_in_progress(mut self) -> Self {
        self.in_progress = true;
        self
    }

    /// Complete the commit
    fn complete(mut self) -> Self {
        self.in_progress = false;
        self
    }

    /// Check if this is a data-changing commit
    fn is_data_change(&self) -> bool {
        self.commit_info.get("isBlindAppend")
            .and_then(|v| v.as_bool())
            .map(|blinded| !blinded)
            .unwrap_or(true)  // Assume data change if not specified
    }

    /// Get commit age in hours
    fn age_hours(&self) -> i64 {
        let duration = Utc::now() - self.timestamp;
        duration.num_hours()
    }

    /// Convert to dictionary
    fn to_dict(&self, py: Python) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);

        dict.set_item("commit_id", &self.commit_id)?;
        dict.set_item("table_id", &self.table_id)?;
        dict.set_item("version", &self.version)?;
        dict.set_item("timestamp", self.timestamp.to_rfc3339())?;
        dict.set_item("operation_type", &self.operation_type)?;
        dict.set_item("operation_parameters", &self.operation_parameters)?;
        dict.set_item("commit_info", &self.commit_info)?;

        if let Some(ref checksum) = self.checksum {
            dict.set_item("checksum", checksum)?;
        }

        dict.set_item("in_progress", self.in_progress)?;
        dict.set_item("is_data_change", self.is_data_change())?;
        dict.set_item("age_hours", self.age_hours())?;

        Ok(dict.into())
    }

    fn __repr__(&self) -> String {
        format!(
            "CommitData(id='{}', table='{}', version={}, operation='{}')",
            self.commit_id[..8].to_string(),  // Shorten ID for display
            self.table_id,
            self.version,
            self.operation_type
        )
    }
}

/// Dataclass representation for Delta File
#[pyclass]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileData {
    #[pyo3(get)]
    pub file_id: String,
    #[pyo3(get)]
    pub table_id: String,
    #[pyo3(get)]
    pub commit_id: String,
    #[pyo3(get)]
    pub path: String,
    #[pyo3(get)]
    pub size: i64,
    #[pyo3(get)]
    pub modification_time: i64,
    #[pyo3(get)]
    pub is_add: bool,
    #[pyo3(get)]
    pub data_change: bool,
    #[pyo3(get)]
    pub stats: Option<HashMap<String, serde_json::Value>>,
    #[pyo3(get)]
    pub tags: Option<HashMap<String, String>>,
    #[pyo3(get)]
    pub partition_values: HashMap<String, String>,
}

#[pymethods]
impl FileData {
    /// Create a new FileData instance
    #[new]
    fn new(
        file_id: String,
        table_id: String,
        commit_id: String,
        path: String,
        size: i64,
        modification_time: i64,
    ) -> Self {
        Self {
            file_id,
            table_id,
            commit_id,
            path,
            size,
            modification_time,
            is_add: true,
            data_change: true,
            stats: None,
            tags: None,
            partition_values: HashMap::new(),
        }
    }

    /// Mark as file removal
    fn as_removal(mut self) -> Self {
        self.is_add = false;
        self
    }

    /// Set data change flag
    fn with_data_change(mut self, data_change: bool) -> Self {
        self.data_change = data_change;
        self
    }

    /// Set partition values
    fn with_partition_values(mut self, values: HashMap<String, String>) -> Self {
        self.partition_values = values;
        self
    }

    /// Set file statistics
    fn with_stats(mut self, stats: HashMap<String, serde_json::Value>) -> Self {
        self.stats = Some(stats);
        self
    }

    /// Set file tags
    fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Get file size in human readable format
    fn size_human(&self) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
        let mut size = self.size as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        format!("{:.2} {}", size, UNITS[unit_index])
    }

    /// Check if this file matches partition filter
    fn matches_partition_filter(&self, filter: HashMap<String, String>) -> bool {
        for (key, value) in &filter {
            if let Some(file_value) = self.partition_values.get(key) {
                if file_value != value {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    /// Get modification time as DateTime
    fn modification_datetime(&self, py: Python) -> PyResult<DateTime<Utc>> {
        DateTime::from_timestamp(self.modification_time / 1000, 0)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Invalid modification time"
            ))
    }

    /// Convert to dictionary
    fn to_dict(&self, py: Python) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);

        dict.set_item("file_id", &self.file_id)?;
        dict.set_item("table_id", &self.table_id)?;
        dict.set_item("commit_id", &self.commit_id)?;
        dict.set_item("path", &self.path)?;
        dict.set_item("size", &self.size)?;
        dict.set_item("size_human", self.size_human())?;
        dict.set_item("modification_time", &self.modification_time)?;
        dict.set_item("is_add", self.is_add)?;
        dict.set_item("data_change", self.data_change)?;

        if let Some(ref stats) = self.stats {
            dict.set_item("stats", stats)?;
        }

        if let Some(ref tags) = self.tags {
            dict.set_item("tags", tags)?;
        }

        dict.set_item("partition_values", &self.partition_values)?;

        Ok(dict.into())
    }

    fn __repr__(&self) -> String {
        let action = if self.is_add { "ADD" } else { "REMOVE" };
        format!(
            "FileData(action='{}', path='{}', size={})",
            action,
            self.path,
            self.size_human()
        )
    }
}

/// Dataclass representation for Protocol version
#[pyclass]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProtocolData {
    #[pyo3(get)]
    pub min_reader_version: i32,
    #[pyo3(get)]
    pub min_writer_version: i32,
    #[pyo3(get)]
    pub reader_features: Option<Vec<String>>,
    #[pyo3(get)]
    pub writer_features: Option<Vec<String>>,
}

#[pymethods]
impl ProtocolData {
    /// Create a new ProtocolData instance
    #[new]
    fn new(min_reader_version: i32, min_writer_version: i32) -> Self {
        Self {
            min_reader_version,
            min_writer_version,
            reader_features: None,
            writer_features: None,
        }
    }

    /// Set reader features
    fn with_reader_features(mut self, features: Vec<String>) -> Self {
        self.reader_features = Some(features);
        self
    }

    /// Set writer features
    fn with_writer_features(mut self, features: Vec<String>) -> Self {
        self.writer_features = Some(features);
        self
    }

    /// Check if protocol supports reader version
    fn supports_reader_version(&self, version: i32) -> bool {
        version >= self.min_reader_version
    }

    /// Check if protocol supports writer version
    fn supports_writer_version(&self, version: i32) -> bool {
        version >= self.min_writer_version
    }

    /// Check if protocol supports specific feature
    fn supports_feature(&self, feature: &str, is_writer: bool) -> bool {
        let features = if is_writer {
            &self.writer_features
        } else {
            &self.reader_features
        };

        features.as_ref()
            .map(|f| f.contains(&feature.to_string()))
            .unwrap_or(false)
    }

    /// Get protocol description
    fn description(&self) -> String {
        format!(
            "Protocol(reader={}, writer={})",
            self.min_reader_version,
            self.min_writer_version
        )
    }

    /// Convert to dictionary
    fn to_dict(&self, py: Python) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);

        dict.set_item("min_reader_version", &self.min_reader_version)?;
        dict.set_item("min_writer_version", &self.min_writer_version)?;
        dict.set_item("description", self.description())?;

        if let Some(ref reader_features) = self.reader_features {
            dict.set_item("reader_features", reader_features)?;
        }

        if let Some(ref writer_features) = self.writer_features {
            dict.set_item("writer_features", writer_features)?;
        }

        Ok(dict.into())
    }

    fn __repr__(&self) -> String {
        format!(
            "ProtocolData(reader={}, writer={})",
            self.min_reader_version,
            self.min_writer_version
        )
    }
}

/// Dataclass representation for Metadata
#[pyclass]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetadataData {
    #[pyo3(get)]
    pub metadata_id: String,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub description: Option<String>,
    #[pyo3(get)]
    pub format: String,
    #[pyo3(get)]
    pub schema_string: Option<String>,
    #[pyo3(get)]
    pub partition_columns: Vec<String>,
    #[pyo3(get)]
    pub created_time: Option<i64>,
    #[pyo3(get)]
    pub configuration: HashMap<String, serde_json::Value>,
}

#[pymethods]
impl MetadataData {
    /// Create a new MetadataData instance
    #[new]
    fn new(metadata_id: String, name: String, format: String) -> Self {
        Self {
            metadata_id,
            name,
            description: None,
            format,
            schema_string: None,
            partition_columns: Vec::new(),
            created_time: None,
            configuration: HashMap::new(),
        }
    }

    /// Set description
    fn with_description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// Set schema string
    fn with_schema_string(mut self, schema: String) -> Self {
        self.schema_string = Some(schema);
        self
    }

    /// Set partition columns
    fn with_partition_columns(mut self, columns: Vec<String>) -> Self {
        self.partition_columns = columns;
        self
    }

    /// Set created time
    fn with_created_time(mut self, created_time: i64) -> Self {
        self.created_time = Some(created_time);
        self
    }

    /// Set configuration
    fn with_configuration(mut self, config: HashMap<String, serde_json::Value>) -> Self {
        self.configuration = config;
        self
    }

    /// Check if table is partitioned
    fn is_partitioned(&self) -> bool {
        !self.partition_columns.is_empty()
    }

    /// Get number of partition columns
    fn partition_count(&self) -> usize {
        self.partition_columns.len()
    }

    /// Parse schema if available
    fn parse_schema(&self, py: Python) -> PyResult<Option<HashMap<String, serde_json::Value>>> {
        if let Some(ref schema_str) = self.schema_string {
            serde_json::from_str(schema_str)
                .map(Some)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    format!("Failed to parse schema: {}", e)
                ))
        } else {
            Ok(None)
        }
    }

    /// Get created time as DateTime
    fn created_datetime(&self, py: Python) -> PyResult<Option<DateTime<Utc>>> {
        if let Some(created_time) = self.created_time {
            DateTime::from_timestamp(created_time / 1000, 0)
                .map(Some)
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid created time"
                ))
        } else {
            Ok(None)
        }
    }

    /// Convert to dictionary
    fn to_dict(&self, py: Python) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);

        dict.set_item("metadata_id", &self.metadata_id)?;
        dict.set_item("name", &self.name)?;

        if let Some(ref description) = self.description {
            dict.set_item("description", description)?;
        }

        dict.set_item("format", &self.format)?;
        dict.set_item("is_partitioned", self.is_partitioned())?;
        dict.set_item("partition_count", self.partition_count())?;

        if let Some(ref schema_string) = self.schema_string {
            dict.set_item("schema_string", schema_string)?;
        }

        dict.set_item("partition_columns", &self.partition_columns)?;

        if let Some(created_time) = self.created_time {
            dict.set_item("created_time", created_time)?;
        }

        dict.set_item("configuration", &self.configuration)?;

        Ok(dict.into())
    }

    fn __repr__(&self) -> String {
        format!(
            "MetadataData(name='{}', format='{}', partitioned={})",
            self.name,
            self.format,
            self.is_partitioned()
        )
    }
}

/// Collection factory functions for creating dataclass instances
#[pyclass]
#[derive(Debug, Clone)]
pub struct DataClassFactory;

#[pymethods]
impl DataClassFactory {
    /// Create TableData from components
    #[staticmethod]
    fn create_table(
        table_id: String,
        table_path: String,
        table_name: String,
        table_uuid: String,
    ) -> TableData {
        TableData::new(table_id, table_path, table_name, table_uuid)
    }

    /// Create CommitData from components
    #[staticmethod]
    fn create_commit(
        commit_id: String,
        table_id: String,
        version: i64,
        operation_type: String,
    ) -> CommitData {
        CommitData::new(
            commit_id,
            table_id,
            version,
            Utc::now(),
            operation_type,
        )
    }

    /// Create FileData from components
    #[staticmethod]
    fn create_file(
        file_id: String,
        table_id: String,
        commit_id: String,
        path: String,
        size: i64,
    ) -> FileData {
        FileData::new(
            file_id,
            table_id,
            commit_id,
            path,
            size,
            Utc::now().timestamp_millis(),
        )
    }

    /// Create ProtocolData from components
    #[staticmethod]
    fn create_protocol(min_reader_version: i32, min_writer_version: i32) -> ProtocolData {
        ProtocolData::new(min_reader_version, min_writer_version)
    }

    /// Create MetadataData from components
    #[staticmethod]
    fn create_metadata(metadata_id: String, name: String, format: String) -> MetadataData {
        MetadataData::new(metadata_id, name, format)
    }
}

/// Utility functions for working with dataclasses
#[pyfunction]
pub fn validate_table_data(data: &TableData) -> PyResult<bool> {
    // Validate required fields
    if data.table_id.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "table_id cannot be empty"
        ));
    }

    if data.table_name.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "table_name cannot be empty"
        ));
    }

    if data.table_path.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "table_path cannot be empty"
        ));
    }

    // Validate UUID format
    Uuid::parse_str(&data.table_uuid)
        .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Invalid table_uuid format"
        ))?;

    Ok(true)
}

#[pyfunction]
pub fn validate_commit_data(data: &CommitData) -> PyResult<bool> {
    // Validate required fields
    if data.commit_id.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "commit_id cannot be empty"
        ));
    }

    if data.version < 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "version must be non-negative"
        ));
    }

    if data.operation_type.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "operation_type cannot be empty"
        ));
    }

    // Validate UUID format
    Uuid::parse_str(&data.table_id)
        .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Invalid table_id format"
        ))?;

    Ok(true)
}

#[pyfunction]
pub fn validate_file_data(data: &FileData) -> PyResult<bool> {
    // Validate required fields
    if data.file_id.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "file_id cannot be empty"
        ));
    }

    if data.path.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "path cannot be empty"
        ));
    }

    if data.size < 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "size must be non-negative"
        ));
    }

    // Validate UUID formats
    Uuid::parse_str(&data.table_id)
        .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Invalid table_id format"
        ))?;

    Uuid::parse_str(&data.commit_id)
        .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Invalid commit_id format"
        ))?;

    Ok(true)
}