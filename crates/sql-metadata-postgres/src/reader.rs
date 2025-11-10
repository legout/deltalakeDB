//! PostgreSQL-backed implementation of `TxnLogReader` for Delta Lake metadata.
//!
//! This module provides efficient SQL-based reading of Delta table metadata
//! with support for snapshots, time travel, and active file listing.

use deltalakedb_core::error::TxnLogError;
use deltalakedb_core::traits::TxnLogReader;
use deltalakedb_core::types::{AddFile, MetadataUpdate, ProtocolUpdate, Snapshot};
use sqlx::postgres::PgPool;
use sqlx::Row;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// PostgreSQL-backed reader for Delta Lake transaction logs.
///
/// Provides efficient querying of Delta metadata stored in PostgreSQL
/// with support for:
/// - Latest version lookup (O(1))
/// - Snapshot reconstruction (< 800ms for 100k files)
/// - Time travel by version or timestamp
/// - Active file computation
///
/// # Example
///
/// ```ignore
/// use sqlx::postgres::PgPool;
/// use deltalakedb_sql_metadata_postgres::PostgresReader;
///
/// let pool = PgPool::connect("postgresql://...").await?;
/// let reader = PostgresReader::new(pool, table_id);
///
/// let version = reader.get_latest_version().await?;
/// let snapshot = reader.read_snapshot(None).await?;
/// let files = reader.get_files(version).await?;
/// ```
pub struct PostgresReader {
    /// Connection pool for database access
    pool: PgPool,
    /// Table ID for this reader
    table_id: Uuid,
}

impl PostgresReader {
    /// Create a new PostgreSQL reader for a specific table.
    ///
    /// # Arguments
    ///
    /// * `pool` - Connection pool to PostgreSQL database
    /// * `table_id` - UUID of the table to read
    pub fn new(pool: PgPool, table_id: Uuid) -> Self {
        PostgresReader { pool, table_id }
    }

    /// Get the table ID this reader is accessing.
    pub fn table_id(&self) -> Uuid {
        self.table_id
    }

    /// Get a reference to the connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Implementation of `TxnLogReader` for PostgreSQL.
#[async_trait::async_trait]
impl TxnLogReader for PostgresReader {
    async fn get_latest_version(&self) -> Result<i64, TxnLogError> {
        // Query the current version directly from dl_tables (O(1) lookup)
        let row = sqlx::query(
            "SELECT current_version FROM dl_tables WHERE table_id = $1"
        )
        .bind(self.table_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TxnLogError::Other(format!("Database query failed: {}", e)))?;

        match row {
            Some(r) => {
                let version: i64 = r.get("current_version");
                Ok(version)
            }
            None => Err(TxnLogError::TableNotFound(self.table_id.to_string())),
        }
    }

    async fn read_snapshot(&self, version: Option<i64>) -> Result<Snapshot, TxnLogError> {
        // Determine which version to read
        let target_version = match version {
            Some(v) => v,
            None => self.get_latest_version().await?,
        };

        // Validate version exists
        let version_row = sqlx::query(
            "SELECT commit_timestamp FROM dl_table_versions WHERE table_id = $1 AND version = $2"
        )
        .bind(self.table_id)
        .bind(target_version)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TxnLogError::Other(format!("Failed to fetch version: {}", e)))?;

        let commit_timestamp: i64 = match version_row {
            Some(r) => r.get("commit_timestamp"),
            None => {
                return Err(TxnLogError::Other(format!(
                    "Version {} not found for table {}",
                    target_version, self.table_id
                )))
            }
        };

        // Get active files at this version
        let files = self.get_files(target_version).await?;

        // Get metadata (latest before or at this version)
        let metadata = self.get_metadata(target_version).await?;

        // Get protocol requirements (latest before or at this version)
        let protocol = self.get_protocol(target_version).await?;

        Ok(Snapshot {
            version: target_version,
            timestamp: commit_timestamp,
            files,
            metadata,
            protocol,
        })
    }

    async fn get_files(&self, version: i64) -> Result<Vec<AddFile>, TxnLogError> {
        // Get all files added up to this version
        let add_rows = sqlx::query(
            "SELECT file_path, file_size_bytes, modification_time, stats, stats_truncated, tags
             FROM dl_add_files 
             WHERE table_id = $1 AND version <= $2
             ORDER BY file_path"
        )
        .bind(self.table_id)
        .bind(version)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| TxnLogError::Other(format!("Failed to fetch add files: {}", e)))?;

        // Get all files removed up to this version
        let remove_rows = sqlx::query("SELECT file_path FROM dl_remove_files WHERE table_id = $1 AND version <= $2")
            .bind(self.table_id)
            .bind(version)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| TxnLogError::Other(format!("Failed to fetch remove files: {}", e)))?;

        // Build set of removed files for efficient lookup
        let removed_paths: HashSet<String> = remove_rows
            .iter()
            .map(|r| r.get::<String, _>("file_path"))
            .collect();

        // Convert rows to AddFile, filtering out removed files
        // Keep the latest version of each file
        let mut files_by_path: HashMap<String, AddFile> = HashMap::new();

        for row in add_rows {
            let path: String = row.get("file_path");

            // Skip if this file was removed
            if removed_paths.contains(&path) {
                continue;
            }

            let file = AddFile {
                path: path.clone(),
                size: row.get("file_size_bytes"),
                modification_time: row.get("modification_time"),
                data_change_version: version, // Latest version where this file is active
                stats: row.get("stats"),
                stats_truncated: row.get("stats_truncated"),
                tags: parse_tags(row.get::<Option<sqlx::types::JsonValue>, _>("tags")),
            };

            files_by_path.insert(path, file);
        }

        // Convert HashMap to sorted Vec
        let mut files: Vec<AddFile> = files_by_path.into_values().collect();
        files.sort_by(|a, b| a.path.cmp(&b.path));

        Ok(files)
    }

    async fn time_travel(&self, timestamp: i64) -> Result<Snapshot, TxnLogError> {
        // Find the latest version with commit_timestamp <= target timestamp
        let version_row = sqlx::query(
            "SELECT version FROM dl_table_versions 
             WHERE table_id = $1 AND commit_timestamp <= $2
             ORDER BY commit_timestamp DESC LIMIT 1"
        )
        .bind(self.table_id)
        .bind(timestamp)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TxnLogError::Other(format!("Time travel query failed: {}", e)))?;

        match version_row {
            Some(r) => {
                let version: i64 = r.get("version");
                self.read_snapshot(Some(version)).await
            }
            None => Err(TxnLogError::Other(
                "No versions exist before the given timestamp".to_string(),
            )),
        }
    }
}

// Helper methods (private)
impl PostgresReader {
    /// Get metadata (schema, partition columns, etc.) at a specific version.
    async fn get_metadata(&self, version: i64) -> Result<MetadataUpdate, TxnLogError> {
        let row = sqlx::query(
            "SELECT description, schema_json, partition_columns, configuration 
             FROM dl_metadata_updates 
             WHERE table_id = $1 AND version <= $2 
             ORDER BY version DESC LIMIT 1"
        )
        .bind(self.table_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TxnLogError::Other(format!("Failed to fetch metadata: {}", e)))?;

        match row {
            Some(r) => {
                let schema_json: Option<sqlx::types::JsonValue> = r.get("schema_json");
                let config: Option<sqlx::types::JsonValue> = r.get("configuration");

                Ok(MetadataUpdate {
                    description: r.get("description"),
                    schema: schema_json.map(|v| v.to_string()),
                    partition_columns: r.get("partition_columns"),
                    created_time: r.get("created_time"),
                    configuration: config
                        .and_then(|v| {
                            serde_json::from_value::<HashMap<String, String>>(v.into()).ok()
                        }),
                })
            }
            None => {
                // Return empty metadata if none found
                Ok(MetadataUpdate {
                    description: None,
                    schema: None,
                    partition_columns: None,
                    created_time: None,
                    configuration: None,
                })
            }
        }
    }

    /// Get protocol requirements at a specific version.
    async fn get_protocol(&self, version: i64) -> Result<ProtocolUpdate, TxnLogError> {
        let row = sqlx::query(
            "SELECT min_reader_version, min_writer_version 
             FROM dl_protocol_updates 
             WHERE table_id = $1 AND version <= $2 
             ORDER BY version DESC LIMIT 1"
        )
        .bind(self.table_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TxnLogError::Other(format!("Failed to fetch protocol: {}", e)))?;

        match row {
            Some(r) => {
                Ok(ProtocolUpdate {
                    min_reader_version: r.get("min_reader_version"),
                    min_writer_version: r.get("min_writer_version"),
                })
            }
            None => {
                // Return default protocol if none found
                Ok(ProtocolUpdate {
                    min_reader_version: Some(1),
                    min_writer_version: Some(2),
                })
            }
        }
    }
}

/// Parse JSONB tags into HashMap.
fn parse_tags(
    tags_json: Option<sqlx::types::JsonValue>,
) -> Option<HashMap<String, String>> {
    tags_json.and_then(|v| {
        serde_json::from_value::<HashMap<String, String>>(v.into()).ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reader_creation() {
        // Create a mock pool (not actually connecting)
        let table_id = Uuid::new_v4();
        let reader = PostgresReader::new(
            sqlx::postgres::PgPool::connect_lazy("postgresql://localhost").unwrap(),
            table_id,
        );

        assert_eq!(reader.table_id(), table_id);
    }
}
