//! Trait for reading Delta Lake transaction logs.

use crate::error::TxnLogResult;
use crate::{ActiveFile, DeltaAction, TableMetadata, TableVersion};
use chrono::{DateTime, Utc};

/// Trait for reading Delta Lake transaction logs.
/// 
/// This trait abstracts the source of Delta metadata, allowing implementations
/// for file-based logs, SQL databases, or other storage backends.
#[async_trait::async_trait]
pub trait TxnLogReader: Send + Sync {
    /// Get the latest table metadata.
    async fn get_table_metadata(&self, table_id: &str) -> TxnLogResult<TableMetadata>;

    /// Get table metadata at a specific version.
    async fn get_table_metadata_at_version(
        &self,
        table_id: &str,
        version: i64,
    ) -> TxnLogResult<TableMetadata>;

    /// Get table metadata at a specific timestamp.
    async fn get_table_metadata_at_timestamp(
        &self,
        table_id: &str,
        timestamp: DateTime<Utc>,
    ) -> TxnLogResult<TableMetadata>;

    /// Get the latest version of a table.
    async fn get_latest_version(&self, table_id: &str) -> TxnLogResult<i64>;

    /// Get the version at or before a specific timestamp.
    async fn get_version_at_timestamp(
        &self,
        table_id: &str,
        timestamp: DateTime<Utc>,
    ) -> TxnLogResult<i64>;

    /// List active files for a table at the latest version.
    async fn list_active_files(&self, table_id: &str) -> TxnLogResult<Vec<ActiveFile>>;

    /// List active files for a table at a specific version.
    async fn list_active_files_at_version(
        &self,
        table_id: &str,
        version: i64,
    ) -> TxnLogResult<Vec<ActiveFile>>;

    /// List active files for a table at a specific timestamp.
    async fn list_active_files_at_timestamp(
        &self,
        table_id: &str,
        timestamp: DateTime<Utc>,
    ) -> TxnLogResult<Vec<ActiveFile>>;

    /// Get actions for a specific version.
    async fn get_version_actions(
        &self,
        table_id: &str,
        version: i64,
    ) -> TxnLogResult<Vec<DeltaAction>>;

    /// Get table version information.
    async fn get_table_version(
        &self,
        table_id: &str,
        version: i64,
    ) -> TxnLogResult<TableVersion>;

    /// List all versions of a table.
    async fn list_table_versions(&self, table_id: &str) -> TxnLogResult<Vec<TableVersion>>;

    /// Check if a table exists.
    async fn table_exists(&self, table_id: &str) -> TxnLogResult<bool>;

    /// Get table history between versions.
    async fn get_table_history(
        &self,
        table_id: &str,
        start_version: Option<i64>,
        end_version: Option<i64>,
    ) -> TxnLogResult<Vec<TableVersion>>;

    /// Get table history between timestamps.
    async fn get_table_history_by_timestamp(
        &self,
        table_id: &str,
        start_timestamp: Option<DateTime<Utc>>,
        end_timestamp: Option<DateTime<Utc>>,
    ) -> TxnLogResult<Vec<TableVersion>>;
}

/// Extension trait for common reader operations.
#[async_trait::async_trait]
pub trait TxnLogReaderExt {
    /// Get the latest snapshot of a table.
    async fn get_latest_snapshot(&self, table_id: &str) -> TxnLogResult<TableSnapshot>;
}

/// A snapshot of a Delta table at a specific point in time.
#[derive(Debug, Clone)]
pub struct TableSnapshot {
    /// Table metadata.
    pub metadata: TableMetadata,
    /// Active files.
    pub files: Vec<ActiveFile>,
    /// Version number.
    pub version: i64,
    /// Timestamp of the snapshot.
    pub timestamp: DateTime<Utc>,
}

impl TableSnapshot {
    /// Create a new table snapshot.
    pub fn new(
        metadata: TableMetadata,
        files: Vec<ActiveFile>,
        version: i64,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            metadata,
            files,
            version,
            timestamp,
        }
    }

    /// Get the table ID.
    pub fn table_id(&self) -> &str {
        &self.metadata.table_id
    }

    /// Get the table location.
    pub fn location(&self) -> &str {
        &self.metadata.location
    }

    /// Get the schema string.
    pub fn schema(&self) -> &str {
        &self.metadata.metadata.schema_string
    }

    /// Get the partition columns.
    pub fn partition_columns(&self) -> &[String] {
        &self.metadata.metadata.partition_columns
    }

    /// Get the total size of all files.
    pub fn total_size(&self) -> i64 {
        self.files.iter().map(|f| f.size).sum()
    }

    /// Get the number of files.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }
}

