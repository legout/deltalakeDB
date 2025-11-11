//! Delta JSON/Parquet action representations shared across crates.

use std::collections::HashMap;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::txn_log::{Protocol, RemovedFile, TableMetadata};

/// Canonical Delta JSON action used in `_delta_log` files.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DeltaAction {
    /// Commit metadata describing the operation.
    CommitInfo {
        /// Commit info payload.
        #[serde(rename = "commitInfo")]
        commit_info: CommitInfo,
    },
    /// Protocol update.
    Protocol {
        /// Protocol payload.
        protocol: ProtocolPayload,
    },
    /// Metadata update.
    MetaData {
        /// Metadata payload.
        #[serde(rename = "metaData")]
        meta_data: MetaDataPayload,
    },
    /// Added data file.
    Add {
        /// Add payload.
        add: AddPayload,
    },
    /// Removed data file.
    Remove {
        /// Remove payload.
        remove: RemovePayload,
    },
}

/// Commit metadata that records the operation.
#[derive(Debug, Serialize, Deserialize)]
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

impl CommitInfo {
    /// Creates a commit info record for the provided operation.
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

/// Protocol action payload.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolPayload {
    /// Minimum reader version.
    pub min_reader_version: u32,
    /// Minimum writer version.
    pub min_writer_version: u32,
}

impl From<&Protocol> for ProtocolPayload {
    fn from(value: &Protocol) -> Self {
        Self {
            min_reader_version: value.min_reader_version,
            min_writer_version: value.min_writer_version,
        }
    }
}

/// Metadata payload.
#[derive(Debug, Serialize, Deserialize, Clone)]
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

impl From<&TableMetadata> for MetaDataPayload {
    fn from(value: &TableMetadata) -> Self {
        Self {
            schema_string: value.schema_json.clone(),
            partition_columns: value.partition_columns.clone(),
            configuration: value.configuration.clone(),
        }
    }
}

/// Added file payload.
#[derive(Debug, Serialize, Deserialize)]
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
    /// Whether this add changed data.
    pub data_change: bool,
}

/// Removed file payload.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemovePayload {
    /// File path relative to the table root.
    pub path: String,
    /// Millisecond timestamp when the deletion occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<i64>,
    /// Data change flag.
    pub data_change: bool,
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

/// Utility that converts a JSON value into a string.
pub fn json_value_to_string(value: Value) -> String {
    match value {
        Value::String(s) => s,
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}
