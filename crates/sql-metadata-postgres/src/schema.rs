//! PostgreSQL schema definitions and utilities.
//!
//! This module provides Rust types that correspond to the PostgreSQL schema,
//! making it easier to work with the metadata tables in type-safe ways.

use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Default PostgreSQL schema configuration.
#[derive(Debug, Clone, Default)]
pub struct PostgresSchema;

impl PostgresSchema {
    /// Get the schema version.
    pub fn version() -> &'static str {
        "0.0.1"
    }

    /// Get the minimum PostgreSQL version required.
    pub fn min_postgres_version() -> &'static str {
        "12.0"
    }

    /// Get all required extensions.
    pub fn required_extensions() -> &'static [&'static str] {
        &["uuid-ossp"]
    }
}

/// Represents a row in the `dl_tables` table.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DeltaTable {
    /// Unique identifier for the table
    pub table_id: Uuid,
    /// Name of the table
    pub table_name: String,
    /// Object storage location (e.g., s3://bucket/path)
    pub location: String,
    /// Current version for optimistic concurrency control
    pub current_version: i64,
    /// Minimum reader protocol version
    pub min_reader_version: i32,
    /// Minimum writer protocol version
    pub min_writer_version: i32,
    /// Additional properties as JSON
    pub properties: sqlx::types::JsonValue,
    /// Creation timestamp
    pub created_at: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,
    /// Last update timestamp
    pub updated_at: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,
}

/// Represents a row in the `dl_table_versions` table.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TableVersion {
    /// Table identifier
    pub table_id: Uuid,
    /// Version number
    pub version: i64,
    /// Commit timestamp in milliseconds since epoch
    pub commit_timestamp: i64,
    /// Type of operation in this version
    pub operation_type: String,
    /// Number of actions in this version
    pub num_actions: i32,
    /// Commit metadata as JSON
    pub commit_info: sqlx::types::JsonValue,
    /// When this version was recorded
    pub recorded_at: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,
}

/// Represents a row in the `dl_add_files` table.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AddFile {
    /// Table identifier
    pub table_id: Uuid,
    /// Version in which this file was added
    pub version: i64,
    /// Path to the file in object storage
    pub file_path: String,
    /// File size in bytes
    pub file_size_bytes: i64,
    /// Modification timestamp in milliseconds since epoch
    pub modification_time: i64,
    /// Partition values as JSON (for flexible partition schemes)
    pub partition_values: Option<sqlx::types::JsonValue>,
    /// File statistics as JSON
    pub stats: Option<sqlx::types::JsonValue>,
    /// Whether statistics are truncated
    pub stats_truncated: Option<bool>,
    /// Additional tags/metadata as JSON
    pub tags: Option<sqlx::types::JsonValue>,
}

/// Represents a row in the `dl_remove_files` table.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct RemoveFile {
    /// Table identifier
    pub table_id: Uuid,
    /// Version in which this file was removed
    pub version: i64,
    /// Path to the removed file
    pub file_path: String,
    /// Deletion timestamp in milliseconds since epoch
    pub deletion_timestamp: i64,
    /// Whether this removal changed data
    pub data_change: bool,
    /// Whether extended file metadata is present
    pub extended_file_metadata: Option<bool>,
    /// Deletion vector (if applicable)
    pub deletion_vector: Option<sqlx::types::JsonValue>,
}

/// Represents a row in the `dl_metadata_updates` table.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct MetadataUpdate {
    /// Table identifier
    pub table_id: Uuid,
    /// Version in which metadata was updated
    pub version: i64,
    /// Table description
    pub description: Option<String>,
    /// Schema as JSON (Delta StructType)
    pub schema_json: Option<sqlx::types::JsonValue>,
    /// Partition columns
    pub partition_columns: Option<Vec<String>>,
    /// Configuration properties
    pub configuration: Option<sqlx::types::JsonValue>,
    /// Creation time in milliseconds since epoch
    pub created_time: Option<i64>,
}

/// Represents a row in the `dl_protocol_updates` table.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ProtocolUpdate {
    /// Table identifier
    pub table_id: Uuid,
    /// Version in which protocol was updated
    pub version: i64,
    /// Minimum reader protocol version
    pub min_reader_version: Option<i32>,
    /// Minimum writer protocol version
    pub min_writer_version: Option<i32>,
}

/// Represents a row in the `dl_txn_actions` table.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TransactionAction {
    /// Table identifier
    pub table_id: Uuid,
    /// Version of this transaction action
    pub version: i64,
    /// Application identifier (e.g., streaming app name)
    pub app_id: String,
    /// Last update value/offset
    pub last_update_value: Option<String>,
}

/// Represents the result of querying active files (v_active_files view).
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ActiveFile {
    /// Table identifier
    pub table_id: Uuid,
    /// Latest version containing this file
    pub version: i64,
    /// Path to the file
    pub file_path: String,
    /// File size in bytes
    pub file_size_bytes: i64,
    /// Partition values
    pub partition_values: Option<sqlx::types::JsonValue>,
    /// File statistics
    pub stats: Option<sqlx::types::JsonValue>,
    /// Modification timestamp
    pub modification_time: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_version() {
        assert_eq!(PostgresSchema::version(), "0.0.1");
    }

    #[test]
    fn test_min_postgres_version() {
        assert_eq!(PostgresSchema::min_postgres_version(), "12.0");
    }

    #[test]
    fn test_required_extensions() {
        let exts = PostgresSchema::required_extensions();
        assert!(exts.contains(&"uuid-ossp"));
    }
}
