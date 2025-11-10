use std::collections::HashMap;

use chrono::Utc;
use deltalakedb_core::txn_log::{Protocol, RemovedFile, TableMetadata};
use serde::Serialize;
use serde_json::Value;

use crate::MirrorError;

/// Canonical serializer for Delta JSON commits.
pub struct JsonCommitSerializer;

impl JsonCommitSerializer {
    /// Serializes the provided action list into newline-delimited JSON bytes.
    pub fn serialize(actions: &[Action]) -> Result<Vec<u8>, MirrorError> {
        let mut buf = Vec::new();
        for (idx, action) in actions.iter().enumerate() {
            let line = serde_json::to_vec(action)?;
            buf.extend_from_slice(&line);
            if idx + 1 < actions.len() {
                buf.push(b'\n');
            }
        }
        buf.push(b'\n');
        Ok(buf)
    }
}

/// JSON action that will be written to the Delta commit file.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Action {
    /// Commit info metadata.
    CommitInfo {
        #[serde(rename = "commitInfo")]
        commit_info: CommitInfo,
    },
    /// Protocol update.
    Protocol { protocol: ProtocolPayload },
    /// Metadata update.
    MetaData {
        #[serde(rename = "metaData")]
        meta_data: MetaDataPayload,
    },
    /// Added data file.
    Add { add: AddPayload },
    /// Removed data file.
    Remove { remove: RemovePayload },
}

/// Commit metadata that records the operation.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitInfo {
    /// Millisecond timestamp for the commit.
    pub timestamp: i64,
    /// Operation descriptor (e.g., WRITE, MERGE).
    pub operation: String,
    /// Arbitrary operation parameters serialized as JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_parameters: Option<Value>,
}

/// Protocol action payload.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolPayload {
    /// Minimum reader version.
    pub min_reader_version: u32,
    /// Minimum writer version.
    pub min_writer_version: u32,
}

/// Metadata payload.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetaDataPayload {
    /// JSON schema for the table.
    #[serde(rename = "schemaString")]
    pub schema_string: String,
    /// Partition columns.
    pub partition_columns: Vec<String>,
    /// Table configuration.
    pub configuration: HashMap<String, String>,
}

/// Add file payload.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddPayload {
    /// File path relative to the table root.
    pub path: String,
    /// File size in bytes.
    pub size: i64,
    /// Partition column values.
    pub partition_values: HashMap<String, Value>,
    /// Optional stats blob (not yet populated).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<Value>,
    /// Millisecond modification time.
    pub modification_time: i64,
    /// Whether this add changed data (always true for now).
    pub data_change: bool,
}

/// Remove file payload.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemovePayload {
    /// File path relative to the table root.
    pub path: String,
    /// Millisecond timestamp when the deletion occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<i64>,
    /// Data change flag (always true for now).
    pub data_change: bool,
}

impl From<&Protocol> for ProtocolPayload {
    fn from(value: &Protocol) -> Self {
        Self {
            min_reader_version: value.min_reader_version,
            min_writer_version: value.min_writer_version,
        }
    }
}

impl From<&TableMetadata> for MetaDataPayload {
    fn from(value: &TableMetadata) -> Self {
        Self {
            schema_string: value.schema_json.clone(),
            partition_columns: value.partition_columns.clone(),
            configuration: value.configuration.clone(),
        }
    }
}

impl RemovePayload {
    /// Builds a payload from the provided removed file.
    pub fn from_removed(file: &RemovedFile) -> Self {
        Self {
            path: file.path.clone(),
            deletion_timestamp: file.deletion_timestamp,
            data_change: true,
        }
    }
}

impl CommitInfo {
    /// Creates a default commit info record for a write operation.
    pub fn new(operation: impl Into<String>, params: Option<Value>, timestamp: i64) -> Self {
        Self {
            timestamp,
            operation: operation.into(),
            operation_parameters: params,
        }
    }
}

impl Default for CommitInfo {
    fn default() -> Self {
        Self {
            timestamp: Utc::now().timestamp_millis(),
            operation: "WRITE".into(),
            operation_parameters: None,
        }
    }
}
