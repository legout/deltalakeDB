//! Trait for writing Delta Lake transaction logs.

use crate::error::TxnLogResult;
use crate::{DeltaAction, TableMetadata, TableVersion};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// Trait for writing Delta Lake transaction logs.
/// 
/// This trait abstracts the destination of Delta metadata, allowing implementations
/// for file-based logs, SQL databases, or other storage backends.
#[async_trait::async_trait]
pub trait TxnLogWriter: Send + Sync {
    /// Begin a new transaction.
    async fn begin_transaction(&self, table_id: &str) -> TxnLogResult<Transaction>;

    /// Commit actions for a table.
    async fn commit_actions(
        &self,
        table_id: &str,
        actions: Vec<DeltaAction>,
        operation: Option<String>,
        operation_params: Option<HashMap<String, String>>,
    ) -> TxnLogResult<i64>;

    /// Commit actions with explicit version.
    async fn commit_actions_with_version(
        &self,
        table_id: &str,
        version: i64,
        actions: Vec<DeltaAction>,
        operation: Option<String>,
        operation_params: Option<HashMap<String, String>>,
    ) -> TxnLogResult<()>;

    /// Create a new table.
    async fn create_table(
        &self,
        table_id: &str,
        name: &str,
        location: &str,
        metadata: TableMetadata,
    ) -> TxnLogResult<()>;

    /// Update table metadata.
    async fn update_table_metadata(
        &self,
        table_id: &str,
        metadata: TableMetadata,
    ) -> TxnLogResult<()>;

    /// Delete a table.
    async fn delete_table(&self, table_id: &str) -> TxnLogResult<()>;

    /// Get the next version for a table.
    async fn get_next_version(&self, table_id: &str) -> TxnLogResult<i64>;

    /// Check if a table exists.
    async fn table_exists(&self, table_id: &str) -> TxnLogResult<bool>;

    /// Get table metadata.
    async fn get_table_metadata(&self, table_id: &str) -> TxnLogResult<TableMetadata>;

    /// Vacuum old versions.
    async fn vacuum(
        &self,
        table_id: &str,
        retain_last_n_versions: Option<i64>,
        retain_hours: Option<i64>,
    ) -> TxnLogResult<()>;

    /// Optimize table.
    async fn optimize(
        &self,
        table_id: &str,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<i32>,
    ) -> TxnLogResult<()>;
}

// Re-export Transaction from the transaction module
pub use crate::transaction::Transaction;

/// Extension trait for common writer operations.
#[async_trait::async_trait]
pub trait TxnLogWriterExt {
    /// Add files to a table.
    async fn add_files(
        &self,
        table_id: &str,
        files: Vec<crate::AddFile>,
    ) -> TxnLogResult<i64>;

    /// Remove files from a table.
    async fn remove_files(
        &self,
        table_id: &str,
        files: Vec<crate::RemoveFile>,
    ) -> TxnLogResult<i64>;

    /// Update table metadata.
    async fn update_metadata(
        &self,
        table_id: &str,
        metadata: crate::Metadata,
    ) -> TxnLogResult<i64>;

    /// Update protocol.
    async fn update_protocol(
        &self,
        table_id: &str,
        protocol: crate::Protocol,
    ) -> TxnLogResult<i64>;
}

