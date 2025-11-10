//! Delta Lake action data structures.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a Delta Lake action.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum DeltaAction {
    /// Add file action.
    Add(AddFile),
    /// Remove file action.
    Remove(RemoveFile),
    /// Metadata update action.
    Metadata(Metadata),
    /// Protocol update action.
    Protocol(Protocol),
    /// Transaction action.
    Transaction(Txn),
}

/// Add file action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddFile {
    /// Path to the file.
    pub path: String,
    /// Size of the file in bytes.
    pub size: i64,
    /// Modification timestamp.
    pub modification_time: i64,
    /// Whether this is a data change.
    pub data_change: bool,
    /// Partition values.
    pub partition_values: HashMap<String, String>,
    /// File statistics.
    pub stats: Option<String>,
    /// Tags.
    pub tags: Option<HashMap<String, String>>,
}

/// Remove file action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveFile {
    /// Path to the file.
    pub path: String,
    /// Deletion timestamp.
    pub deletion_timestamp: Option<i64>,
    /// Whether this is a data change.
    pub data_change: bool,
    /// Extended file metadata.
    pub extended_file_metadata: Option<bool>,
    /// Partition values.
    pub partition_values: Option<HashMap<String, String>>,
    /// Size of the file in bytes.
    pub size: Option<i64>,
    /// Tags.
    pub tags: Option<HashMap<String, String>>,
}

/// Metadata update action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    /// Unique identifier for the metadata.
    pub id: String,
    /// Format information.
    pub format: Format,
    /// Schema string.
    pub schema_string: String,
    /// Partition columns.
    pub partition_columns: Vec<String>,
    /// Configuration.
    pub configuration: HashMap<String, String>,
    /// Created time.
    pub created_time: Option<i64>,
}

/// Format information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Format {
    /// Provider format (e.g., "parquet").
    pub provider: String,
    /// Format options.
    pub options: HashMap<String, String>,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        }
    }
}

/// Protocol update action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Protocol {
    /// Minimum reader version.
    pub min_reader_version: i32,
    /// Minimum writer version.
    pub min_writer_version: i32,
}

impl Default for Protocol {
    fn default() -> Self {
        Self {
            min_reader_version: 1,
            min_writer_version: 1,
        }
    }
}

/// Transaction action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Txn {
    /// Application ID.
    pub app_id: String,
    /// Version.
    pub version: i64,
    /// Last updated timestamp.
    pub last_updated: i64,
}

/// Table metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Table ID.
    pub table_id: String,
    /// Table name.
    pub name: String,
    /// Table location.
    pub location: String,
    /// Current version.
    pub version: i64,
    /// Protocol information.
    pub protocol: Protocol,
    /// Metadata.
    pub metadata: Metadata,
    /// Created timestamp.
    pub created_at: DateTime<Utc>,
}

/// Table version information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableVersion {
    /// Table ID.
    pub table_id: String,
    /// Version number.
    pub version: i64,
    /// Committed timestamp.
    pub committed_at: DateTime<Utc>,
    /// Committer.
    pub committer: Option<String>,
    /// Operation type.
    pub operation: Option<String>,
    /// Operation parameters.
    pub operation_params: Option<HashMap<String, String>>,
}

/// Active file information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveFile {
    /// File path.
    pub path: String,
    /// File size.
    pub size: i64,
    /// Modification time.
    pub modification_time: i64,
    /// Partition values.
    pub partition_values: HashMap<String, String>,
    /// File statistics.
    pub stats: Option<String>,
    /// Version when added.
    pub version: i64,
}

impl DeltaAction {
    /// Get the action type as a string.
    pub fn action_type(&self) -> &'static str {
        match self {
            DeltaAction::Add(_) => "add",
            DeltaAction::Remove(_) => "remove",
            DeltaAction::Metadata(_) => "metadata",
            DeltaAction::Protocol(_) => "protocol",
            DeltaAction::Transaction(_) => "txn",
        }
    }
}

impl AddFile {
    /// Create a new add file action.
    pub fn new(
        path: String,
        size: i64,
        modification_time: i64,
        data_change: bool,
    ) -> Self {
        Self {
            path,
            size,
            modification_time,
            data_change,
            partition_values: HashMap::new(),
            stats: None,
            tags: None,
        }
    }

    /// Add partition value.
    pub fn with_partition_value(mut self, key: String, value: String) -> Self {
        self.partition_values.insert(key, value);
        self
    }

    /// Add statistics.
    pub fn with_stats(mut self, stats: String) -> Self {
        self.stats = Some(stats);
        self
    }
}

impl RemoveFile {
    /// Create a new remove file action.
    pub fn new(path: String, data_change: bool) -> Self {
        Self {
            path,
            deletion_timestamp: None,
            data_change,
            extended_file_metadata: None,
            partition_values: None,
            size: None,
            tags: None,
        }
    }

    /// Set deletion timestamp.
    pub fn with_deletion_timestamp(mut self, timestamp: i64) -> Self {
        self.deletion_timestamp = Some(timestamp);
        self
    }
}

impl Metadata {
    /// Create new metadata.
    pub fn new(id: String, schema_string: String, format: Format) -> Self {
        Self {
            id,
            format,
            schema_string,
            partition_columns: Vec::new(),
            configuration: HashMap::new(),
            created_time: None,
        }
    }

    /// Add partition column.
    pub fn with_partition_column(mut self, column: String) -> Self {
        self.partition_columns.push(column);
        self
    }

    /// Add configuration.
    pub fn with_configuration(mut self, key: String, value: String) -> Self {
        self.configuration.insert(key, value);
        self
    }
}

impl Format {
    /// Create new format.
    pub fn new(provider: String) -> Self {
        Self {
            provider,
            options: HashMap::new(),
        }
    }

    /// Add format option.
    pub fn with_option(mut self, key: String, value: String) -> Self {
        self.options.insert(key, value);
        self
    }
}

impl Protocol {
    /// Create new protocol.
    pub fn new(min_reader_version: i32, min_writer_version: i32) -> Self {
        Self {
            min_reader_version,
            min_writer_version,
        }
    }
}

impl Txn {
    /// Create new transaction.
    pub fn new(app_id: String, version: i64, last_updated: i64) -> Self {
        Self {
            app_id,
            version,
            last_updated,
        }
    }
}