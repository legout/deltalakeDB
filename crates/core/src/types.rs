//! Core domain types for Delta Lake transaction log operations.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// File added to a Delta table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddFile {
    /// Path to the file in object storage
    pub path: String,
    /// File size in bytes
    pub size: i64,
    /// Modification timestamp (milliseconds since epoch)
    pub modification_time: i64,
    /// Version when this file was added
    pub data_change_version: i64,
    /// Optional statistics about the file (e.g., row count, column stats)
    pub stats: Option<String>,
    /// Optional statistics truncated flag
    pub stats_truncated: Option<bool>,
    /// Optional additional properties
    pub tags: Option<HashMap<String, String>>,
}

/// File removed from a Delta table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoveFile {
    /// Path to the file in object storage
    pub path: String,
    /// Deletion timestamp (milliseconds since epoch)
    pub deletion_timestamp: i64,
    /// Whether this file deletion was a data change
    pub data_change: bool,
    /// Extended file metadata
    pub extended_file_metadata: Option<bool>,
    /// Optional deletion vector for deletion-based versioning
    pub deletion_vector: Option<String>,
    /// Version when this file was removed
    pub removed_at_version: Option<i64>,
}

/// Metadata update for a Delta table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataUpdate {
    /// Table description
    pub description: Option<String>,
    /// Table schema in JSON format
    pub schema: Option<String>,
    /// Table partition columns
    pub partition_columns: Option<Vec<String>>,
    /// Table creation timestamp (milliseconds since epoch)
    pub created_time: Option<i64>,
    /// Additional metadata properties
    pub configuration: Option<HashMap<String, String>>,
}

/// Protocol update for a Delta table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProtocolUpdate {
    /// Minimum reader version required to read this table
    pub min_reader_version: Option<i32>,
    /// Minimum writer version required to write this table
    pub min_writer_version: Option<i32>,
}

/// Transaction action for multi-table ACID operations.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxnAction {
    /// Unique application ID for this transaction
    pub app_id: String,
    /// Transaction version number
    pub version: i64,
    /// Timestamp of transaction (milliseconds since epoch)
    pub timestamp: i64,
}

/// Action in a Delta transaction log.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Action {
    /// File addition
    Add(AddFile),
    /// File removal
    Remove(RemoveFile),
    /// Metadata update
    Metadata(MetadataUpdate),
    /// Protocol update
    Protocol(ProtocolUpdate),
    /// Transaction action
    Txn(TxnAction),
}

/// Snapshot of a Delta table at a specific version.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    /// Current version of the table
    pub version: i64,
    /// Timestamp of this version (milliseconds since epoch)
    pub timestamp: i64,
    /// Active files in this version
    pub files: Vec<AddFile>,
    /// Table metadata
    pub metadata: MetadataUpdate,
    /// Protocol requirements
    pub protocol: ProtocolUpdate,
}

/// Handle to an in-progress commit transaction.
/// This is an opaque handle that implementations can use to track
/// the state of a transaction without exposing internal details.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CommitHandle {
    /// Unique transaction ID
    txn_id: String,
}

impl CommitHandle {
    /// Create a new commit handle with the given transaction ID.
    pub fn new(txn_id: String) -> Self {
        CommitHandle { txn_id }
    }

    /// Get the transaction ID.
    pub fn txn_id(&self) -> &str {
        &self.txn_id
    }
}

impl Default for MetadataUpdate {
    fn default() -> Self {
        MetadataUpdate {
            description: None,
            schema: None,
            partition_columns: None,
            created_time: None,
            configuration: None,
        }
    }
}

impl Default for ProtocolUpdate {
    fn default() -> Self {
        ProtocolUpdate {
            min_reader_version: None,
            min_writer_version: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_file_creation() {
        let file = AddFile {
            path: "s3://bucket/path/to/file.parquet".to_string(),
            size: 1024,
            modification_time: 1234567890,
            data_change_version: 1,
            stats: None,
            stats_truncated: None,
            tags: None,
        };
        assert_eq!(file.path, "s3://bucket/path/to/file.parquet");
        assert_eq!(file.size, 1024);
    }

    #[test]
    fn test_remove_file_creation() {
        let file = RemoveFile {
            path: "s3://bucket/path/to/file.parquet".to_string(),
            deletion_timestamp: 1234567900,
            data_change: true,
            extended_file_metadata: None,
            deletion_vector: None,
            removed_at_version: Some(2),
        };
        assert_eq!(file.path, "s3://bucket/path/to/file.parquet");
        assert!(file.data_change);
    }

    #[test]
    fn test_snapshot_creation() {
        let snapshot = Snapshot {
            version: 5,
            timestamp: 1234567890,
            files: vec![],
            metadata: MetadataUpdate {
                description: Some("Test table".to_string()),
                schema: None,
                partition_columns: None,
                created_time: None,
                configuration: None,
            },
            protocol: ProtocolUpdate {
                min_reader_version: Some(1),
                min_writer_version: Some(2),
            },
        };
        assert_eq!(snapshot.version, 5);
        assert_eq!(snapshot.files.len(), 0);
    }

    #[test]
    fn test_commit_handle() {
        let handle = CommitHandle::new("txn-123".to_string());
        assert_eq!(handle.txn_id(), "txn-123");
    }

    #[test]
    fn test_serialization_add_file() {
        let file = AddFile {
            path: "s3://bucket/file.parquet".to_string(),
            size: 512,
            modification_time: 1000000,
            data_change_version: 1,
            stats: None,
            stats_truncated: None,
            tags: None,
        };
        let json = serde_json::to_string(&file).unwrap();
        let deserialized: AddFile = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.path, file.path);
    }
}
