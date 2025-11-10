//! Delta file actions and the main Action enum.

use crate::{DeltaError, ValidationError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents the different types of actions in a Delta transaction log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "add", rename_all = "camelCase")]
pub enum Action {
    /// Add a file to the table
    AddFile(AddFile),

    /// Remove a file from the table
    RemoveFile(RemoveFile),

    /// Update table metadata
    Metadata(Metadata),

    /// Update protocol version
    Protocol(Protocol),

    /// Commit information
    CommitInfo(CommitInfo),

    /// Transaction information for idempotent writes
    Txn(Txn),
}

/// Represents adding a file to the Delta table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddFile {
    /// Relative path to the data file
    pub path: String,

    /// File size in bytes
    pub size: i64,

    /// When this file was added (milliseconds since epoch)
    #[serde(default)]
    pub modification_time: i64,

    /// Whether the file should be considered a data file (true) or not (false)
    #[serde(default = "default_true")]
    pub data_change: bool,

    /// Partition values for this file
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub partition_values: HashMap<String, String>,

    /// File statistics in JSON format
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<String>,

    /// Number of records in the file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_records: Option<i64>,

    /// File tags
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub tags: HashMap<String, String>,

    /// Default values for columns
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_row_commit_version: Option<i64>,

    /// Additional metadata
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Represents removing a file from the Delta table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveFile {
    /// Relative path to the data file
    pub path: String,

    /// When this file was deleted (milliseconds since epoch)
    pub deletion_timestamp: i64,

    /// Whether this removal represents a data change
    #[serde(default = "default_true")]
    pub data_change: bool,

    /// Whether the file was removed by an update operation
    #[serde(default)]
    pub extended_file_metadata: bool,

    /// Partition values for the removed file
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub partition_values: HashMap<String, String>,

    /// File size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,

    /// File statistics in JSON format
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<String>,

    /// Number of records in the file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_records: Option<i64>,

    /// Additional metadata
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
}

// Re-export types from dedicated modules to avoid duplication
pub use crate::metadata::Metadata;
pub use crate::metadata::Format;
pub use crate::protocol::Protocol;

/// Represents commit information.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitInfo {
    /// Timestamp when the commit was made
    pub timestamp: i64,

    /// User ID who made the commit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    /// Username of the committer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,

    /// Operation performed (WRITE, UPDATE, DELETE, etc.)
    pub operation: Option<String>,

    /// Additional operation parameters
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub operation_parameters: HashMap<String, serde_json::Value>,

    /// Job information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job: Option<JobInfo>,

    /// Notebook information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notebook: Option<NotebookInfo>,

    /// Cluster information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_info: Option<ClusterInfo>,

    /// Read version this commit was built on
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_version: Option<i64>,

    /// Isolation level
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isolation_level: Option<String>,

    /// Whether this commit contains data changes
    #[serde(default = "default_true")]
    pub is_blind_append: bool,

    /// Additional metadata
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Job information for commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobInfo {
    /// Job ID
    pub job_id: String,

    /// Job name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,

    /// Run ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,

    /// Job owner
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_owner: Option<String>,

    /// Job trigger type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_trigger_type: Option<String>,

    /// Additional job parameters
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub job_parameters: HashMap<String, String>,
}

/// Notebook information for commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NotebookInfo {
    /// Notebook ID
    pub notebook_id: String,
}

/// Cluster information for commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterInfo {
    /// Cluster ID
    pub cluster_id: String,

    /// Cluster name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_name: Option<String>,
}

/// Transaction information for idempotent writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Txn {
    /// Application ID
    pub app_id: String,

    /// Transaction version
    pub version: i64,

    /// When this transaction was last updated
    #[serde(rename = "lastUpdated")]
    pub last_updated: i64,
}

// Helper function for default values
fn default_true() -> bool {
    true
}

impl AddFile {
    /// Creates a new AddFile action.
    ///
    /// # Arguments
    /// * `path` - Relative path to the data file
    /// * `size` - File size in bytes
    ///
    /// # Errors
    /// Returns `ValidationError` if the path is empty or size is negative
    pub fn new(path: String, size: i64) -> Result<Self, ValidationError> {
        if path.trim().is_empty() {
            return Err(ValidationError::InvalidFilePath { path });
        }
        if size < 0 {
            return Err(ValidationError::NegativeFileSize { size });
        }

        Ok(Self {
            path,
            size,
            modification_time: chrono::Utc::now().timestamp_millis(),
            data_change: true,
            partition_values: HashMap::new(),
            stats: None,
            num_records: None,
            tags: HashMap::new(),
            default_row_commit_version: None,
            metadata: HashMap::new(),
        })
    }

    /// Creates a new AddFile with all parameters specified.
    pub fn complete(
        path: String,
        size: i64,
        modification_time: i64,
        data_change: bool,
        partition_values: HashMap<String, String>,
        stats: Option<String>,
        num_records: Option<i64>,
        tags: HashMap<String, String>,
        default_row_commit_version: Option<i64>,
    ) -> Result<Self, ValidationError> {
        if path.trim().is_empty() {
            return Err(ValidationError::InvalidFilePath { path });
        }
        if size < 0 {
            return Err(ValidationError::NegativeFileSize { size });
        }

        Ok(Self {
            path,
            size,
            modification_time,
            data_change,
            partition_values,
            stats,
            num_records,
            tags,
            default_row_commit_version,
            metadata: HashMap::new(),
        })
    }

    /// Adds a partition value.
    pub fn with_partition_value(mut self, column: String, value: String) -> Self {
        if !column.trim().is_empty() {
            self.partition_values.insert(column, value);
        }
        self
    }

    /// Sets the file statistics.
    pub fn with_stats(mut self, stats: String) -> Self {
        self.stats = Some(stats);
        self
    }

    /// Sets the number of records.
    pub fn with_num_records(mut self, num_records: i64) -> Self {
        self.num_records = Some(num_records);
        self
    }

    /// Adds a tag.
    pub fn with_tag(mut self, key: String, value: String) -> Self {
        if !key.trim().is_empty() {
            self.tags.insert(key, value);
        }
        self
    }

    /// Validates the AddFile action.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.path.trim().is_empty() {
            return Err(ValidationError::InvalidFilePath {
                path: self.path.clone(),
            });
        }
        if self.size < 0 {
            return Err(ValidationError::NegativeFileSize { size: self.size });
        }
        if self.modification_time < 0 {
            return Err(ValidationError::InvalidTimestamp {
                timestamp: self.modification_time,
            });
        }

        // Validate stats if present
        if let Some(stats) = &self.stats {
            if let Err(_) = serde_json::from_str::<serde_json::Value>(stats) {
                return Err(ValidationError::InvalidStatistics {
                    error: format!("Invalid JSON: {}", stats),
                });
            }
        }

        Ok(())
    }
}

impl RemoveFile {
    /// Creates a new RemoveFile action.
    ///
    /// # Arguments
    /// * `path` - Relative path to the data file
    /// * `deletion_timestamp` - When the file was deleted (milliseconds since epoch)
    ///
    /// # Errors
    /// Returns `ValidationError` if the path is empty
    pub fn new(path: String, deletion_timestamp: i64) -> Result<Self, ValidationError> {
        if path.trim().is_empty() {
            return Err(ValidationError::InvalidFilePath { path });
        }
        if deletion_timestamp < 0 {
            return Err(ValidationError::InvalidTimestamp {
                timestamp: deletion_timestamp,
            });
        }

        Ok(Self {
            path,
            deletion_timestamp,
            data_change: true,
            extended_file_metadata: false,
            partition_values: HashMap::new(),
            size: None,
            stats: None,
            num_records: None,
            metadata: HashMap::new(),
        })
    }

    /// Creates a new RemoveFile with all parameters specified.
    pub fn complete(
        path: String,
        deletion_timestamp: i64,
        data_change: bool,
        extended_file_metadata: bool,
        partition_values: HashMap<String, String>,
        size: Option<i64>,
        stats: Option<String>,
        num_records: Option<i64>,
    ) -> Result<Self, ValidationError> {
        if path.trim().is_empty() {
            return Err(ValidationError::InvalidFilePath { path });
        }
        if deletion_timestamp < 0 {
            return Err(ValidationError::InvalidTimestamp {
                timestamp: deletion_timestamp,
            });
        }
        if let Some(size) = size {
            if size < 0 {
                return Err(ValidationError::NegativeFileSize { size });
            }
        }

        Ok(Self {
            path,
            deletion_timestamp,
            data_change,
            extended_file_metadata,
            partition_values,
            size,
            stats,
            num_records,
            metadata: HashMap::new(),
        })
    }

    /// Adds a partition value.
    pub fn with_partition_value(mut self, column: String, value: String) -> Self {
        if !column.trim().is_empty() {
            self.partition_values.insert(column, value);
        }
        self
    }

    /// Sets the file size.
    pub fn with_size(mut self, size: i64) -> Self {
        self.size = Some(size);
        self
    }

    /// Sets the file statistics.
    pub fn with_stats(mut self, stats: String) -> Self {
        self.stats = Some(stats);
        self
    }

    /// Validates the RemoveFile action.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.path.trim().is_empty() {
            return Err(ValidationError::InvalidFilePath {
                path: self.path.clone(),
            });
        }
        if self.deletion_timestamp < 0 {
            return Err(ValidationError::InvalidTimestamp {
                timestamp: self.deletion_timestamp,
            });
        }
        if let Some(size) = self.size {
            if size < 0 {
                return Err(ValidationError::NegativeFileSize { size });
            }
        }

        Ok(())
    }
}

// Format Default implementation is now in the metadata module

impl Action {
    /// Serializes the action to Delta JSON format.
    pub fn to_delta_json(&self) -> Result<String, DeltaError> {
        // Validate the action first
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    /// Deserializes an action from Delta JSON format.
    pub fn from_delta_json(json: &str) -> Result<Self, DeltaError> {
        let action: Action = serde_json::from_str(json)?;
        action.validate()?;
        Ok(action)
    }

    /// Validates the action.
    pub fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Action::AddFile(add_file) => add_file.validate()?,
            Action::RemoveFile(remove_file) => remove_file.validate()?,
            Action::Metadata(_) => {
                // Metadata validation would be handled by the Metadata struct
            }
            Action::Protocol(_) => {
                // Protocol validation would be handled by the Protocol struct
            }
            Action::CommitInfo(_) => {
                // CommitInfo validation
            }
            Action::Txn(txn) => {
                if txn.app_id.trim().is_empty() {
                    return Err(ValidationError::MissingField {
                        field: "txn.appId".to_string(),
                    });
                }
                if txn.version < 0 {
                    return Err(ValidationError::InvalidCommitVersion {
                        version: txn.version,
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_add_file_new() {
        let add_file = AddFile::new("data/file1.parquet".to_string(), 1024).unwrap();
        assert_eq!(add_file.path, "data/file1.parquet");
        assert_eq!(add_file.size, 1024);
        assert!(add_file.data_change);
        assert!(add_file.partition_values.is_empty());
    }

    #[test]
    fn test_add_file_validation() {
        let result = AddFile::new("".to_string(), 1024);
        assert!(result.is_err());

        let result = AddFile::new("file.parquet".to_string(), -1);
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_file_new() {
        let remove_file = RemoveFile::new(
            "data/file1.parquet".to_string(),
            1234567890,
        ).unwrap();
        assert_eq!(remove_file.path, "data/file1.parquet");
        assert_eq!(remove_file.deletion_timestamp, 1234567890);
        assert!(remove_file.data_change);
    }

    #[test]
    fn test_add_file_builder() {
        let add_file = AddFile::new("data/file1.parquet".to_string(), 1024)
            .unwrap()
            .with_partition_value("date".to_string(), "2023-01-01".to_string())
            .with_stats("{\"numRecords\": 100}".to_string())
            .with_num_records(100)
            .with_tag("source".to_string(), "batch".to_string());

        assert_eq!(add_file.partition_values.get("date"), Some(&"2023-01-01".to_string()));
        assert_eq!(add_file.stats, Some("{\"numRecords\": 100}".to_string()));
        assert_eq!(add_file.num_records, Some(100));
        assert_eq!(add_file.tags.get("source"), Some(&"batch".to_string()));
    }

    #[test]
    fn test_action_serialization() {
        let add_file = AddFile::new("data/file1.parquet".to_string(), 1024).unwrap();
        let action = Action::AddFile(add_file);

        let json = action.to_delta_json().unwrap();
        let deserialized = Action::from_delta_json(&json).unwrap();
        assert_eq!(action, deserialized);
    }

    #[test]
    fn test_protocol_action() {
        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
            metadata: std::collections::HashMap::new(),
        };
        let action = Action::Protocol(protocol);

        let json = action.to_delta_json().unwrap();
        let deserialized = Action::from_delta_json(&json).unwrap();
        assert_eq!(action, deserialized);
    }

    #[test]
    fn test_metadata_action() {
        let metadata = Metadata {
            id: "test-id".to_string(),
            name: Some("test-table".to_string()),
            description: None,
            schema_string: json!({"type": "struct", "fields": []}).to_string(),
            format: Format::default(),
            partition_columns: vec![],
            configuration: HashMap::new(),
            created_time: Some(1234567890),
            additional_metadata: HashMap::new(),
        };
        let action = Action::Metadata(metadata);

        let json = action.to_delta_json().unwrap();
        let deserialized = Action::from_delta_json(&json).unwrap();
        assert_eq!(action, deserialized);
    }
}