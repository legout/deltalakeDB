//! SQL-based transaction log reader for Delta Lake metadata.
//!
//! This module implements the `TxnLogReader` trait for reading Delta Lake
//! metadata from SQL databases, providing fast access to table snapshots and
//! supporting time travel queries.

use crate::connection::{DatabaseConnection, DatabaseConfig};
use crate::schema::{DatabaseEngine, SchemaConfig};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deltalakedb_core::{
    error::{TxnLogResult, TxnLogError},
    reader::{TxnLogReader, TableMetadata, Snapshot},
    transaction::TransactionState,
    DeltaAction, AddFile, RemoveFile, Metadata, Protocol, Format,
};
use serde::{Deserialize, Serialize};
use sqlx::{Row, Pool, postgres::PgPool, sqlite::SqlitePool, duckdb::DuckDbPool};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

/// Configuration for SQL transaction log reader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlTxnLogReaderConfig {
    /// Enable query result caching
    pub enable_caching: bool,
    /// Cache TTL in seconds
    pub cache_ttl_seconds: u64,
    /// Maximum number of rows to fetch in a single query
    pub batch_size: usize,
    /// Enable query performance logging
    pub enable_query_logging: bool,
    /// Connection pool size
    pub pool_size: u32,
}

impl Default for SqlTxnLogReaderConfig {
    fn default() -> Self {
        Self {
            enable_caching: true,
            cache_ttl_seconds: 300, // 5 minutes
            batch_size: 1000,
            enable_query_logging: false,
            pool_size: 10,
        }
    }
}

/// SQL-based transaction log reader implementation.
pub struct SqlTxnLogReader {
    /// Database connection pool
    pool: Arc<DatabaseConnection>,
    /// Reader configuration
    config: SqlTxnLogReaderConfig,
    /// Database engine type
    engine: DatabaseEngine,
    /// Table prefix
    table_prefix: String,
}

impl SqlTxnLogReader {
    /// Create a new SQL transaction log reader.
    pub async fn new(
        connection: Arc<DatabaseConnection>,
        config: SqlTxnLogReaderConfig,
    ) -> TxnLogResult<Self> {
        let engine = connection.engine();
        let table_prefix = "dl_".to_string(); // Default prefix

        info!(
            "Creating SQL TxnLogReader for engine: {} with pool size: {}",
            engine.as_str(),
            config.pool_size
        );

        Ok(Self {
            pool: connection,
            config,
            engine,
            table_prefix,
        })
    }

    /// Create a new reader from database configuration.
    pub async fn from_config(
        db_config: &DatabaseConfig,
        reader_config: SqlTxnLogReaderConfig,
    ) -> TxnLogResult<Self> {
        let connection = Arc::new(db_config.connect().await?);
        Self::new(connection, reader_config).await
    }

    /// Get the current version of a table.
    #[instrument(skip(self))]
    pub async fn get_current_version(&self, table_id: &str) -> TxnLogResult<i64> {
        let query = format!(
            "SELECT COALESCE(MAX(version), 0) FROM {}table_versions WHERE table_id = $1",
            self.table_prefix
        );

        let result: Option<i64> = match &*self.pool {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query_scalar(&query)
                    .bind(table_id)
                    .fetch_one(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get current version: {}", e)))?
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query_scalar(&query)
                    .bind(table_id)
                    .fetch_one(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get current version: {}", e)))?
            }
            DatabaseConnection::DuckDb(pool) => {
                sqlx::query_scalar(&query)
                    .bind(table_id)
                    .fetch_one(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get current version: {}", e)))?
            }
        };

        Ok(result.unwrap_or(0))
    }

    /// Get table metadata by table ID.
    #[instrument(skip(self))]
    pub async fn get_table_metadata(&self, table_id: &str) -> TxnLogResult<Option<TableMetadata>> {
        let query = format!(
            r#"
            SELECT 
                t.table_id,
                t.name,
                t.location,
                t.created_at,
                t.protocol_min_reader,
                t.protocol_min_writer,
                t.properties,
                COALESCE(tv.max_version, 0) as current_version
            FROM {}tables t
            LEFT JOIN (
                SELECT table_id, MAX(version) as max_version
                FROM {}table_versions
                GROUP BY table_id
            ) tv ON t.table_id = tv.table_id
            WHERE t.table_id = $1
            "#,
            self.table_prefix, self.table_prefix
        );

        let row: Option<sqlx::types::Json<_>> = match &*self.pool {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query_scalar(&query)
                    .bind(table_id)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get table metadata: {}", e)))?
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query_scalar(&query)
                    .bind(table_id)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get table metadata: {}", e)))?
            }
            DatabaseConnection::DuckDb(pool) => {
                sqlx::query_scalar(&query)
                    .bind(table_id)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get table metadata: {}", e)))?
            }
        };

        if let Some(json_row) = row {
            let metadata: TableMetadataRow = json_row.0;
            Ok(Some(metadata.into()))
        } else {
            Ok(None)
        }
    }

    /// Get active files for a table at a specific version.
    #[instrument(skip(self))]
    pub async fn get_active_files(&self, table_id: &str, version: Option<i64>) -> TxnLogResult<Vec<AddFile>> {
        let target_version = version.unwrap_or_else(|| {
            // Get latest version if not specified
            self.get_current_version(table_id).await.unwrap_or(0)
        });

        let query = format!(
            r#"
            WITH latest_version AS (
                SELECT COALESCE(MAX(version), 0) as max_version
                FROM {}table_versions
                WHERE table_id = $1
            ),
            active_adds AS (
                SELECT 
                    a.path,
                    a.size_bytes,
                    a.partition_values,
                    a.stats,
                    a.data_change,
                    a.modification_time,
                    a.file_tags,
                    a.version as add_version
                FROM {}add_files a
                CROSS JOIN latest_version lv
                WHERE a.table_id = $1 
                    AND a.version <= COALESCE($2, lv.max_version)
                    AND NOT EXISTS (
                        SELECT 1 FROM {}remove_files r
                        WHERE r.table_id = a.table_id
                            AND r.path = a.path
                            AND r.version <= COALESCE($2, lv.max_version)
                            AND r.version >= a.version
                    )
            )
            SELECT 
                path,
                size_bytes,
                partition_values,
                stats,
                data_change,
                modification_time,
                file_tags
            FROM active_adds
            "#,
            self.table_prefix, self.table_prefix, self.table_prefix
        );

        let rows: Vec<AddFileRow> = match &*self.pool {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query_as(&query)
                    .bind(table_id)
                    .bind(target_version)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get active files: {}", e)))?
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query_as(&query)
                    .bind(table_id)
                    .bind(target_version)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get active files: {}", e)))?
            }
            DatabaseConnection::DuckDb(pool) => {
                sqlx::query_as(&query)
                    .bind(table_id)
                    .bind(target_version)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get active files: {}", e)))?
            }
        };

        Ok(rows.into_iter().map(|row| row.into()).collect())
    }

    /// Get table schema at a specific version.
    #[instrument(skip(self))]
    pub async fn get_table_schema(&self, table_id: &str, version: Option<i64>) -> TxnLogResult<Option<Metadata>> {
        let target_version = version.unwrap_or_else(|| {
            self.get_current_version(table_id).await.unwrap_or(0)
        });

        let query = format!(
            r#"
            SELECT schema_json, partition_columns, table_properties, created_time
            FROM {}metadata_updates
            WHERE table_id = $1 AND version = (
                SELECT MAX(version)
                FROM {}metadata_updates
                WHERE table_id = $1 AND version <= $2
            )
            "#,
            self.table_prefix, self.table_prefix
        );

        let row: Option<MetadataRow> = match &*self.pool {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query_as(&query)
                    .bind(table_id)
                    .bind(target_version)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get table schema: {}", e)))?
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query_as(&query)
                    .bind(table_id)
                    .bind(target_version)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get table schema: {}", e)))?
            }
            DatabaseConnection::DuckDb(pool) => {
                sqlx::query_as(&query)
                    .bind(table_id)
                    .bind(target_version)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get table schema: {}", e)))?
            }
        };

        Ok(row.map(|r| r.into()))
    }

    /// Get table protocol at a specific version.
    #[instrument(skip(self))]
    pub async fn get_table_protocol(&self, table_id: &str, version: Option<i64>) -> TxnLogResult<Option<Protocol>> {
        let target_version = version.unwrap_or_else(|| {
            self.get_current_version(table_id).await.unwrap_or(0)
        });

        let query = format!(
            r#"
            SELECT min_reader_version, min_writer_version, reader_features, writer_features
            FROM {}protocol_updates
            WHERE table_id = $1 AND version = (
                SELECT MAX(version)
                FROM {}protocol_updates
                WHERE table_id = $1 AND version <= $2
            )
            "#,
            self.table_prefix, self.table_prefix
        );

        let row: Option<ProtocolRow> = match &*self.pool {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query_as(&query)
                    .bind(table_id)
                    .bind(target_version)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get table protocol: {}", e)))?
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query_as(&query)
                    .bind(table_id)
                    .bind(target_version)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get table protocol: {}", e)))?
            }
            DatabaseConnection::DuckDb(pool) => {
                sqlx::query_as(&query)
                    .bind(table_id)
                    .bind(target_version)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get table protocol: {}", e)))?
            }
        };

        Ok(row.map(|r| r.into()))
    }

    /// Get version history for a table.
    #[instrument(skip(self))]
    pub async fn get_version_history(
        &self,
        table_id: &str,
        limit: Option<usize>,
    ) -> TxnLogResult<Vec<VersionHistoryRow>> {
        let limit_clause = limit.map(|l| format!("LIMIT {}", l)).unwrap_or_default();
        let query = format!(
            r#"
            SELECT 
                version,
                committed_at,
                committer,
                operation,
                operation_params
            FROM {}table_versions
            WHERE table_id = $1
            ORDER BY version DESC
            {}
            "#,
            self.table_prefix, limit_clause
        );

        let rows: Vec<VersionHistoryRow> = match &*self.pool {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query_as(&query)
                    .bind(table_id)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get version history: {}", e)))?
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query_as(&query)
                    .bind(table_id)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get version history: {}", e)))?
            }
            DatabaseConnection::DuckDb(pool) => {
                sqlx::query_as(&query)
                    .bind(table_id)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get version history: {}", e)))?
            }
        };

        Ok(rows)
    }

    /// Discover tables in the database.
    #[instrument(skip(self))]
    pub async fn discover_tables(&self) -> TxnLogResult<Vec<TableMetadata>> {
        let query = format!(
            r#"
            SELECT 
                t.table_id,
                t.name,
                t.location,
                t.created_at,
                t.protocol_min_reader,
                t.protocol_min_writer,
                t.properties,
                COALESCE(tv.max_version, 0) as current_version
            FROM {}tables t
            LEFT JOIN (
                SELECT table_id, MAX(version) as max_version
                FROM {}table_versions
                GROUP BY table_id
            ) tv ON t.table_id = tv.table_id
            ORDER BY t.name
            "#,
            self.table_prefix, self.table_prefix
        );

        let rows: Vec<TableMetadataRow> = match &*self.pool {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query_as(&query)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to discover tables: {}", e)))?
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query_as(&query)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to discover tables: {}", e)))?
            }
            DatabaseConnection::DuckDb(pool) => {
                sqlx::query_as(&query)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to discover tables: {}", e)))?
            }
        };

        Ok(rows.into_iter().map(|row| row.into()).collect())
    }

    /// Get table by name.
    #[instrument(skip(self))]
    pub async fn get_table_by_name(&self, name: &str) -> TxnLogResult<Option<TableMetadata>> {
        let query = format!(
            r#"
            SELECT 
                t.table_id,
                t.name,
                t.location,
                t.created_at,
                t.protocol_min_reader,
                t.protocol_min_writer,
                t.properties,
                COALESCE(tv.max_version, 0) as current_version
            FROM {}tables t
            LEFT JOIN (
                SELECT table_id, MAX(version) as max_version
                FROM {}table_versions
                GROUP BY table_id
            ) tv ON t.table_id = tv.table_id
            WHERE t.name = $1
            "#,
            self.table_prefix, self.table_prefix
        );

        let row: Option<sqlx::types::Json<_>> = match &*self.pool {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query_scalar(&query)
                    .bind(name)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get table by name: {}", e)))?
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query_scalar(&query)
                    .bind(name)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get table by name: {}", e)))?
            }
            DatabaseConnection::DuckDb(pool) => {
                sqlx::query_scalar(&query)
                    .bind(name)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get table by name: {}", e)))?
            }
        };

        if let Some(json_row) = row {
            let metadata: TableMetadataRow = json_row.0;
            Ok(Some(metadata.into()))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl TxnLogReader for SqlTxnLogReader {
    /// Get the latest snapshot for a table.
    async fn get_latest_snapshot(&self, table_id: &str) -> TxnLogResult<Snapshot> {
        debug!("Getting latest snapshot for table: {}", table_id);

        let metadata = self.get_table_metadata(table_id).await?;
        let metadata = metadata.ok_or_else(|| {
            TxnLogError::table_not_found(format!("Table not found: {}", table_id))
        })?;

        let files = self.get_active_files(table_id, None).await?;
        let schema = self.get_table_schema(table_id, None).await?;
        let protocol = self.get_table_protocol(table_id, None).await?;

        Ok(Snapshot {
            version: metadata.current_version,
            metadata,
            files,
            schema,
            protocol,
        })
    }

    /// Get snapshot for a specific version.
    async fn get_snapshot(&self, table_id: &str, version: i64) -> TxnLogResult<Snapshot> {
        debug!("Getting snapshot for table: {} at version: {}", table_id, version);

        let metadata = self.get_table_metadata(table_id).await?;
        let metadata = metadata.ok_or_else(|| {
            TxnLogError::table_not_found(format!("Table not found: {}", table_id))
        })?;

        let files = self.get_active_files(table_id, Some(version)).await?;
        let schema = self.get_table_schema(table_id, Some(version)).await?;
        let protocol = self.get_table_protocol(table_id, Some(version)).await?;

        Ok(Snapshot {
            version,
            metadata,
            files,
            schema,
            protocol,
        })
    }

    /// Get snapshot for a specific timestamp.
    async fn get_snapshot_as_of(&self, table_id: &str, timestamp: DateTime<Utc>) -> TxnLogResult<Snapshot> {
        debug!("Getting snapshot for table: {} as of: {}", table_id, timestamp);

        let query = format!(
            "SELECT MAX(version) FROM {}table_versions WHERE table_id = $1 AND committed_at <= $2",
            self.table_prefix
        );

        let version: Option<i64> = match &*self.pool {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query_scalar(&query)
                    .bind(table_id)
                    .bind(timestamp)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get version as of timestamp: {}", e)))?
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query_scalar(&query)
                    .bind(table_id)
                    .bind(timestamp)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get version as of timestamp: {}", e)))?
            }
            DatabaseConnection::DuckDb(pool) => {
                sqlx::query_scalar(&query)
                    .bind(table_id)
                    .bind(timestamp)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TxnLogError::database(format!("Failed to get version as of timestamp: {}", e)))?
            }
        };

        let version = version.ok_or_else(|| {
            TxnLogError::validation(format!(
                "No version found for table {} at or before {}",
                table_id, timestamp
            ))
        })?;

        self.get_snapshot(table_id, version).await
    }

    /// List all available tables.
    async fn list_tables(&self) -> TxnLogResult<Vec<TableMetadata>> {
        debug!("Listing all tables");
        self.discover_tables().await
    }

    /// Check if a table exists.
    async fn table_exists(&self, table_id: &str) -> TxnLogResult<bool> {
        debug!("Checking if table exists: {}", table_id);
        let metadata = self.get_table_metadata(table_id).await?;
        Ok(metadata.is_some())
    }
}

// Database row structures for SQL queries
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
struct TableMetadataRow {
    table_id: String,
    name: String,
    location: String,
    created_at: DateTime<Utc>,
    protocol_min_reader: i32,
    protocol_min_writer: i32,
    properties: Option<serde_json::Value>,
    current_version: i64,
}

impl From<TableMetadataRow> for TableMetadata {
    fn from(row: TableMetadataRow) -> Self {
        Self {
            table_id: row.table_id,
            name: row.name,
            location: row.location,
            version: row.current_version,
            created_at: row.created_at,
            protocol_min_reader: row.protocol_min_reader,
            protocol_min_writer: row.protocol_min_writer,
            properties: row.properties.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
struct AddFileRow {
    path: String,
    size_bytes: i64,
    partition_values: Option<serde_json::Value>,
    stats: Option<serde_json::Value>,
    data_change: bool,
    modification_time: i64,
    file_tags: Option<serde_json::Value>,
}

impl From<AddFileRow> for AddFile {
    fn from(row: AddFileRow) -> Self {
        Self {
            path: row.path,
            size: row.size_bytes,
            modification_time: row.modification_time,
            data_change: row.data_change,
            partition_values: row.partition_values
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default(),
            stats: row.stats.and_then(|v| serde_json::from_value(v).ok()),
            tags: row.file_tags.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
struct MetadataRow {
    schema_json: serde_json::Value,
    partition_columns: Option<serde_json::Value>,
    table_properties: Option<serde_json::Value>,
    created_time: Option<i64>,
}

impl From<MetadataRow> for Metadata {
    fn from(row: MetadataRow) -> Self {
        Self {
            id: "metadata".to_string(), // Placeholder ID
            format: Format {
                provider: "parquet".to_string(),
                options: Default::default(),
            },
            schema_string: row.schema_json.to_string(),
            partition_columns: row.partition_columns
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default(),
            configuration: row.table_properties
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default(),
            created_time: row.created_time,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
struct ProtocolRow {
    min_reader_version: i32,
    min_writer_version: i32,
    reader_features: Option<serde_json::Value>,
    writer_features: Option<serde_json::Value>,
}

impl From<ProtocolRow> for Protocol {
    fn from(row: ProtocolRow) -> Self {
        Self {
            min_reader_version: row.min_reader_version,
            min_writer_version: row.min_writer_version,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
struct VersionHistoryRow {
    version: i64,
    committed_at: DateTime<Utc>,
    committer: Option<String>,
    operation: Option<String>,
    operation_params: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::DatabaseConfig;
    use tempfile::tempdir;

    async fn create_test_reader() -> SqlTxnLogReader {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let config = DatabaseConfig::new(format!("sqlite:{}", db_path.to_str().unwrap()));
        let connection = config.connect().await.unwrap();
        SqlTxnLogReader::new(Arc::new(connection), SqlTxnLogReaderConfig::default())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_get_current_version_empty() {
        let reader = create_test_reader().await;
        let version = reader.get_current_version("nonexistent").await.unwrap();
        assert_eq!(version, 0);
    }

    #[tokio::test]
    async fn test_table_exists_nonexistent() {
        let reader = create_test_reader().await;
        let exists = reader.table_exists("nonexistent").await.unwrap();
        assert!(!exists);
    }

    #[tokio::test]
    async fn test_discover_tables_empty() {
        let reader = create_test_reader().await;
        let tables = reader.discover_tables().await.unwrap();
        assert!(tables.is_empty());
    }

    #[tokio::test]
    async fn test_reader_config_default() {
        let config = SqlTxnLogReaderConfig::default();
        assert!(config.enable_caching);
        assert_eq!(config.cache_ttl_seconds, 300);
        assert_eq!(config.batch_size, 1000);
        assert!(!config.enable_query_logging);
        assert_eq!(config.pool_size, 10);
    }
}