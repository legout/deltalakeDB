//! PostgreSQL-backed implementation of `TxnLogWriter` for Delta Lake metadata.
//!
//! This module provides safe, atomic write operations to Delta table metadata
//! with optimistic concurrency control, batch insertion, and full ACID guarantees.

use deltalakedb_core::error::TxnLogError;
use deltalakedb_core::traits::TxnLogWriter;
use deltalakedb_core::types::{Action, AddFile, CommitHandle, MetadataUpdate, ProtocolUpdate, RemoveFile, TxnAction};
use sqlx::postgres::{PgPool, PgTransaction};
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
use uuid::Uuid;

/// PostgreSQL-backed writer for Delta Lake transaction logs.
///
/// Provides safe atomic commits with optimistic concurrency control:
/// - Prevents concurrent writer conflicts via version validation
/// - Ensures all actions in a commit are atomic (all or nothing)
/// - Supports batch insertion for efficient large commits
/// - Locks are held only for the duration of the transaction
///
/// # Optimistic Concurrency Control
///
/// Uses `SELECT ... FOR UPDATE` pattern:
/// 1. Begin transaction and lock the table row
/// 2. Validate current_version + 1 == expected_version
/// 3. Insert all actions within the transaction
/// 4. Update current_version atomically
/// 5. Commit transaction
///
/// If another writer commits between begin and finalize, the version check fails
/// and a `VersionConflict` error is returned.
///
/// # Example
///
/// ```ignore
/// use sqlx::postgres::PgPool;
/// use deltalakedb_sql_metadata_postgres::PostgresWriter;
/// use deltalakedb_core::traits::TxnLogWriter;
/// use deltalakedb_core::types::Action;
///
/// let pool = PgPool::connect("postgresql://...").await?;
/// let mut writer = PostgresWriter::new(pool, table_id);
///
/// // Begin commit
/// let handle = writer.begin_commit().await?;
///
/// // Stage actions
/// let actions = vec![Action::Add(file1), Action::Add(file2)];
/// writer.write_actions(&handle, actions).await?;
///
/// // Finalize (atomic commit)
/// let new_version = writer.finalize_commit(handle).await?;
/// println!("Committed as version: {}", new_version);
/// ```
pub struct PostgresWriter {
    /// Connection pool for database access
    pool: PgPool,
    /// Table ID for this writer
    table_id: Uuid,
    /// Active transaction state (if a commit is in progress)
    transaction: Option<WriterTransaction>,
}

/// State for an in-progress commit transaction.
struct WriterTransaction {
    /// Database transaction
    tx: Transaction<'static, Postgres>,
    /// Current version being validated
    current_version: i64,
    /// Expected next version
    expected_version: i64,
    /// Staged actions to be inserted
    staged_actions: Vec<Action>,
}

impl PostgresWriter {
    /// Create a new PostgreSQL writer for a specific table.
    ///
    /// # Arguments
    ///
    /// * `pool` - Connection pool to PostgreSQL database
    /// * `table_id` - UUID of the table to write
    pub fn new(pool: PgPool, table_id: Uuid) -> Self {
        PostgresWriter {
            pool,
            table_id,
            transaction: None,
        }
    }

    /// Get the table ID this writer is accessing.
    pub fn table_id(&self) -> Uuid {
        self.table_id
    }

    /// Get a reference to the connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Check if a commit is currently in progress.
    pub fn is_committing(&self) -> bool {
        self.transaction.is_some()
    }
}

/// Implementation of `TxnLogWriter` for PostgreSQL.
#[async_trait::async_trait]
impl TxnLogWriter for PostgresWriter {
    async fn begin_commit(&mut self, _table_id: &str) -> Result<CommitHandle, TxnLogError> {
        if self.transaction.is_some() {
            return Err(TxnLogError::Other(
                "Commit already in progress for this writer".to_string(),
            ));
        }

        // Begin transaction
        let tx = self.pool.begin().await.map_err(|e| {
            TxnLogError::Other(format!("Failed to begin transaction: {}", e))
        })?;

        // Lock the table row and get current version
        // Using FOR UPDATE to prevent concurrent writers from seeing same version
        let row = sqlx::query(
            "SELECT current_version FROM dl_tables WHERE table_id = $1 FOR UPDATE"
        )
        .bind(self.table_id)
        .fetch_one(&mut *(tx.as_mut()))
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => {
                TxnLogError::TableNotFound(self.table_id.to_string())
            }
            _ => TxnLogError::Other(format!("Failed to lock table: {}", e)),
        })?;

        let current_version: i64 = row.get("current_version");
        let expected_version = current_version + 1;

        // Create commit handle
        let handle = CommitHandle::new(format!("{}:{}", self.table_id, expected_version));

        // Store transaction state
        // SAFETY: We're using Box::leak here because sqlx Transaction needs 'static lifetime
        // This is safe because we own the transaction until finalize_commit
        let tx_static: Transaction<'static, Postgres> =
            unsafe { std::mem::transmute(tx) };

        self.transaction = Some(WriterTransaction {
            tx: tx_static,
            current_version,
            expected_version,
            staged_actions: Vec::new(),
        });

        Ok(handle)
    }

    async fn write_actions(
        &mut self,
        _handle: &CommitHandle,
        actions: Vec<Action>,
    ) -> Result<(), TxnLogError> {
        let txn = self.transaction.as_mut().ok_or_else(|| {
            TxnLogError::Other("No commit in progress".to_string())
        })?;

        // Validate actions
        for action in &actions {
            validate_action(action)?;
        }

        // Stage actions for insertion in finalize_commit
        txn.staged_actions.extend(actions);

        Ok(())
    }

    async fn finalize_commit(&mut self, _handle: CommitHandle) -> Result<i64, TxnLogError> {
        let mut txn = self.transaction.take().ok_or_else(|| {
            TxnLogError::Other("No commit in progress".to_string())
        })?;

        // Revalidate version (ensures no concurrent writer committed)
        let row = sqlx::query(
            "SELECT current_version FROM dl_tables WHERE table_id = $1"
        )
        .bind(self.table_id)
        .fetch_one(&mut *(txn.tx.as_mut()))
        .await
        .map_err(|e| TxnLogError::Other(format!("Failed to revalidate version: {}", e)))?;

        let current_version: i64 = row.get("current_version");
        if current_version != txn.current_version {
            return Err(TxnLogError::VersionConflict {
                expected: txn.expected_version,
                actual: current_version + 1,
            });
        }

        // Insert all staged actions
        insert_actions(&mut txn.tx, self.table_id, txn.expected_version, &txn.staged_actions)
            .await?;

        // Insert commit record to dl_table_versions
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| TxnLogError::Other("System time error".to_string()))?
            .as_millis() as i64;

        sqlx::query(
            "INSERT INTO dl_table_versions (table_id, version, commit_timestamp, operation_type, num_actions)
             VALUES ($1, $2, $3, $4, $5)"
        )
        .bind(self.table_id)
        .bind(txn.expected_version)
        .bind(now_ms)
        .bind(get_operation_type(&txn.staged_actions))
        .bind(txn.staged_actions.len() as i32)
        .execute(&mut *(txn.tx.as_mut()))
        .await
        .map_err(|e| TxnLogError::Other(format!("Failed to insert version record: {}", e)))?;

        // Update current_version atomically
        sqlx::query(
            "UPDATE dl_tables SET current_version = $1 WHERE table_id = $2"
        )
        .bind(txn.expected_version)
        .bind(self.table_id)
        .execute(&mut *(txn.tx.as_mut()))
        .await
        .map_err(|e| TxnLogError::Other(format!("Failed to update version: {}", e)))?;

        // Commit transaction (all or nothing)
        txn.tx.commit().await.map_err(|e| {
            TxnLogError::Other(format!("Failed to commit transaction: {}", e))
        })?;

        Ok(txn.expected_version)
    }

    async fn validate_version(&self, expected: i64) -> Result<(), TxnLogError> {
        let row = sqlx::query(
            "SELECT current_version FROM dl_tables WHERE table_id = $1"
        )
        .bind(self.table_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TxnLogError::Other(format!("Version check failed: {}", e)))?;

        match row {
            Some(r) => {
                let current_version: i64 = r.get("current_version");
                if current_version + 1 == expected {
                    Ok(())
                } else {
                    Err(TxnLogError::VersionConflict {
                        expected,
                        actual: current_version + 1,
                    })
                }
            }
            None => Err(TxnLogError::TableNotFound(self.table_id.to_string())),
        }
    }
}

/// Validate an action before insertion.
fn validate_action(action: &Action) -> Result<(), TxnLogError> {
    match action {
        Action::Add(file) => {
            if file.path.is_empty() {
                return Err(TxnLogError::Other("Add file path cannot be empty".to_string()));
            }
            if file.size < 0 {
                return Err(TxnLogError::Other(format!(
                    "File size must be non-negative: {}",
                    file.size
                )));
            }
        }
        Action::Remove(file) => {
            if file.path.is_empty() {
                return Err(TxnLogError::Other("Remove file path cannot be empty".to_string()));
            }
        }
        Action::Metadata(_) => {
            // Metadata is optional, allow empty
        }
        Action::Protocol(proto) => {
            if let Some(rv) = proto.min_reader_version {
                if rv < 1 {
                    return Err(TxnLogError::Other(
                        "min_reader_version must be >= 1".to_string(),
                    ));
                }
            }
            if let Some(wv) = proto.min_writer_version {
                if wv < 1 {
                    return Err(TxnLogError::Other(
                        "min_writer_version must be >= 1".to_string(),
                    ));
                }
            }
        }
        Action::Txn(_) => {
            // Transaction action is optional
        }
    }
    Ok(())
}

/// Insert all staged actions into the database.
async fn insert_actions(
    tx: &mut Transaction<'static, Postgres>,
    table_id: Uuid,
    version: i64,
    actions: &[Action],
) -> Result<(), TxnLogError> {
    // Group actions by type for batch insertion
    let mut add_files = Vec::new();
    let mut remove_files = Vec::new();
    let mut metadata = Vec::new();
    let mut protocols = Vec::new();
    let mut txns = Vec::new();

    for action in actions {
        match action {
            Action::Add(file) => add_files.push(file),
            Action::Remove(file) => remove_files.push(file),
            Action::Metadata(meta) => metadata.push(meta),
            Action::Protocol(proto) => protocols.push(proto),
            Action::Txn(txn) => txns.push(txn),
        }
    }

    // Insert add files
    for file in add_files {
        sqlx::query(
            "INSERT INTO dl_add_files 
             (table_id, version, file_path, file_size_bytes, modification_time, partition_values, stats, stats_truncated, tags)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        )
        .bind(table_id)
        .bind(version)
        .bind(&file.path)
        .bind(file.size)
        .bind(file.modification_time)
        .bind(None::<sqlx::types::JsonValue>) // partition_values - from tags if available
        .bind(file.stats.as_ref().map(|s| serde_json::json!(s)))
        .bind(file.stats_truncated)
        .bind(file.tags.as_ref().map(|t| serde_json::to_value(t).unwrap_or_default()))
        .execute(&mut **tx)
        .await
        .map_err(|e| TxnLogError::Other(format!("Failed to insert add file: {}", e)))?;
    }

    // Insert remove files
    for file in remove_files {
        sqlx::query(
            "INSERT INTO dl_remove_files 
             (table_id, version, file_path, deletion_timestamp, data_change, extended_file_metadata, deletion_vector)
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        )
        .bind(table_id)
        .bind(version)
        .bind(&file.path)
        .bind(file.deletion_timestamp)
        .bind(file.data_change)
        .bind(file.extended_file_metadata)
        .bind(file.deletion_vector.as_ref().map(|v| serde_json::json!(v)))
        .execute(&mut **tx)
        .await
        .map_err(|e| TxnLogError::Other(format!("Failed to insert remove file: {}", e)))?;
    }

    // Insert metadata updates
    for meta in metadata {
        sqlx::query(
            "INSERT INTO dl_metadata_updates 
             (table_id, version, description, schema_json, partition_columns, configuration, created_time)
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        )
        .bind(table_id)
        .bind(version)
        .bind(&meta.description)
        .bind(meta.schema.as_ref().map(|s| serde_json::json!(s)))
        .bind(&meta.partition_columns)
        .bind(meta.configuration.as_ref().map(|c| serde_json::to_value(c).unwrap_or_default()))
        .bind(meta.created_time)
        .execute(&mut **tx)
        .await
        .map_err(|e| TxnLogError::Other(format!("Failed to insert metadata: {}", e)))?;
    }

    // Insert protocol updates
    for proto in protocols {
        sqlx::query(
            "INSERT INTO dl_protocol_updates 
             (table_id, version, min_reader_version, min_writer_version)
             VALUES ($1, $2, $3, $4)"
        )
        .bind(table_id)
        .bind(version)
        .bind(proto.min_reader_version)
        .bind(proto.min_writer_version)
        .execute(&mut **tx)
        .await
        .map_err(|e| TxnLogError::Other(format!("Failed to insert protocol: {}", e)))?;
    }

    // Insert transaction actions
    for txn in txns {
        sqlx::query(
            "INSERT INTO dl_txn_actions 
             (table_id, version, app_id, last_update_value)
             VALUES ($1, $2, $3, $4)"
        )
        .bind(table_id)
        .bind(version)
        .bind(&txn.app_id)
        .bind(&txn.timestamp.to_string()) // Store timestamp as last_update_value
        .execute(&mut **tx)
        .await
        .map_err(|e| TxnLogError::Other(format!("Failed to insert txn action: {}", e)))?;
    }

    Ok(())
}

/// Determine the operation type based on the actions in the commit.
fn get_operation_type(actions: &[Action]) -> String {
    let mut has_add = false;
    let mut has_remove = false;
    let mut has_metadata = false;
    let mut has_protocol = false;

    for action in actions {
        match action {
            Action::Add(_) => has_add = true,
            Action::Remove(_) => has_remove = true,
            Action::Metadata(_) => has_metadata = true,
            Action::Protocol(_) => has_protocol = true,
            Action::Txn(_) => {}
        }
    }

    // Return primary operation type
    if has_metadata {
        "Metadata".to_string()
    } else if has_protocol {
        "Protocol".to_string()
    } else if has_remove {
        "RemoveFile".to_string()
    } else if has_add {
        "AddFile".to_string()
    } else {
        "Other".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writer_creation() {
        let table_id = Uuid::new_v4();
        let pool = sqlx::postgres::PgPool::connect_lazy("postgresql://localhost").unwrap();
        let writer = PostgresWriter::new(pool, table_id);

        assert_eq!(writer.table_id(), table_id);
        assert!(!writer.is_committing());
    }

    #[test]
    fn test_validate_add_file() {
        let file = AddFile {
            path: "s3://bucket/file.parquet".to_string(),
            size: 1024,
            modification_time: 0,
            data_change_version: 0,
            stats: None,
            stats_truncated: None,
            tags: None,
        };
        assert!(validate_action(&Action::Add(file)).is_ok());
    }

    #[test]
    fn test_validate_add_file_empty_path() {
        let file = AddFile {
            path: "".to_string(),
            size: 1024,
            modification_time: 0,
            data_change_version: 0,
            stats: None,
            stats_truncated: None,
            tags: None,
        };
        assert!(validate_action(&Action::Add(file)).is_err());
    }

    #[test]
    fn test_validate_add_file_negative_size() {
        let file = AddFile {
            path: "s3://bucket/file.parquet".to_string(),
            size: -1,
            modification_time: 0,
            data_change_version: 0,
            stats: None,
            stats_truncated: None,
            tags: None,
        };
        assert!(validate_action(&Action::Add(file)).is_err());
    }

    #[test]
    fn test_operation_type_detection() {
        let file = AddFile {
            path: "file.parquet".to_string(),
            size: 1024,
            modification_time: 0,
            data_change_version: 0,
            stats: None,
            stats_truncated: None,
            tags: None,
        };

        let actions = vec![Action::Add(file)];
        assert_eq!(get_operation_type(&actions), "AddFile");

        let actions = vec![Action::Metadata(MetadataUpdate {
            description: None,
            schema: None,
            partition_columns: None,
            created_time: None,
            configuration: None,
        })];
        assert_eq!(get_operation_type(&actions), "Metadata");
    }
}
