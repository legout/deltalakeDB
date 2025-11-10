//! PostgreSQL adapter for Delta Lake metadata storage

use crate::{
    SqlResult, DatabaseConfig, PoolStats, MigrationInfo, schema::SchemaManager,
    adapters::{BaseAdapter, BaseTransaction, PoolManager, utils},
};
use crate::traits::{TxnLogReader, TxnLogWriter, DatabaseAdapter, Transaction};
use crate::error::SqlError;

use deltalakedb_core::{Table, Commit, Protocol, Metadata, Action};
use async_trait::async_trait;
use deadpool_postgres::{Config as PoolConfig, Manager, Pool, Runtime, Object};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, Row, PgPool};
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// PostgreSQL adapter implementation
pub struct PostgresAdapter {
    base: BaseAdapter,
    pool: PgPool,
}

/// PostgreSQL transaction implementation
pub struct PostgresTransaction {
    base_tx: BaseTransaction,
    conn: Object,
}

impl PostgresAdapter {
    /// Create a new PostgreSQL adapter
    pub async fn new(config: DatabaseConfig) -> SqlResult<Self> {
        // Parse PostgreSQL connection URL and create pool
        let pool_config = PoolManager::get_pool_config("postgresql");

        let pool = PgPoolOptions::new()
            .max_connections(pool_config.max_size)
            .min_connections(pool_config.min_size)
            .acquire_timeout(Duration::from_secs(config.timeout))
            .idle_timeout(Duration::from_secs(pool_config.idle_timeout))
            .max_lifetime(Duration::from_secs(pool_config.max_lifetime))
            .connect(&config.url)
            .await?;

        let mut adapter = Self {
            base: BaseAdapter::new(config, "postgresql"),
            pool,
        };

        // Initialize schema if needed
        adapter.initialize_schema().await?;

        Ok(adapter)
    }

    /// Get a connection from the pool
    async fn get_connection(&self) -> SqlResult<sqlx::postgres::PgPoolConnection> {
        self.pool
            .acquire()
            .await
            .map_err(|e| SqlError::pool_error(format!("Failed to acquire connection: {}", e)))
    }

    /// Execute a query with timeout
    async fn execute_with_timeout<F, T>(&self, operation: F) -> SqlResult<T>
    where
        F: std::future::Future<Output = SqlResult<T>>,
    {
        let duration = Duration::from_secs(self.base.get_config().timeout);
        timeout(duration, operation)
            .await
            .map_err(|_| SqlError::timeout_error(self.base.get_config().timeout))?
    }

    /// Convert a database row to a Table
    fn row_to_table(row: &sqlx::postgres::PgRow) -> SqlResult<Table> {
        let id: Uuid = row.try_get("id")?;
        let table_path: String = row.try_get("table_path")?;
        let table_name: String = row.try_get("table_name")?;
        let table_uuid: Uuid = row.try_get("table_uuid")?;
        let created_at: DateTime<Utc> = utils::parse_timestamp(&row.try_get::<String, _>("created_at")?)?;
        let updated_at: DateTime<Utc> = utils::parse_timestamp(&row.try_get::<String, _>("updated_at")?)?;

        Ok(Table {
            id,
            table_path,
            table_name,
            table_uuid,
            created_at,
            updated_at,
        })
    }

    /// Convert a database row to a Commit
    fn row_to_commit(row: &sqlx::postgres::PgRow) -> SqlResult<Commit> {
        let id: Uuid = row.try_get("id")?;
        let table_id: Uuid = row.try_get("table_id")?;
        let version: i64 = row.try_get("version")?;
        let timestamp: DateTime<Utc> = utils::parse_timestamp(&row.try_get::<String, _>("timestamp")?)?;
        let operation_type: String = row.try_get("operation_type")?;
        let operation_parameters: Value = row.try_get("operation_parameters")?;
        let commit_info: Value = row.try_get("commit_info")?;

        Ok(Commit {
            id,
            table_id,
            version,
            timestamp,
            operation_type,
            operation_parameters,
            commit_info,
        })
    }

    /// Convert a database row to Protocol
    fn row_to_protocol(row: &sqlx::postgres::PgRow) -> SqlResult<Protocol> {
        let min_reader_version: i32 = row.try_get("min_reader_version")?;
        let min_writer_version: i32 = row.try_get("min_writer_version")?;

        Ok(Protocol {
            min_reader_version,
            min_writer_version,
        })
    }

    /// Convert a database row to Metadata
    fn row_to_metadata(row: &sqlx::postgres::PgRow) -> SqlResult<Metadata> {
        let metadata_json: Value = row.try_get("metadata_json")?;

        // Extract metadata fields from JSON
        let id: String = metadata_json["id"].as_str().unwrap_or("").to_string();
        let name: String = metadata_json["name"].as_str().unwrap_or("").to_string();
        let description: Option<String> = metadata_json["description"].as_str().map(|s| s.to_string());
        let format: String = metadata_json["format"]["provider"].as_str().unwrap_or("parquet").to_string();
        let schema_string: Option<String> = metadata_json["schemaString"].as_str().map(|s| s.to_string());
        let partition_columns: Vec<String> = metadata_json["partitionColumns"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        let configuration: Value = metadata_json["configuration"].clone();
        let created_time: Option<i64> = metadata_json["createdTime"].as_i64();

        Ok(Metadata {
            id,
            name,
            description,
            format,
            schema_string,
            partition_columns,
            configuration,
            created_time,
        })
    }

    /// Convert a database row to Action
    fn row_to_action(row: &sqlx::postgres::PgRow) -> SqlResult<Action> {
        let action_type: String = row.try_get("action_type")?;
        let action_data: Value = row.try_get("action_data")?;
        let file_path: Option<String> = row.try_get("file_path").ok();
        let data_change: bool = row.try_get("data_change").unwrap_or(true);

        // Create appropriate action based on type
        match action_type.as_str() {
            "add" => {
                if let Some(file_path) = file_path {
                    Ok(Action::AddFile {
                        path: file_path,
                        size: action_data["size"].as_u64().unwrap_or(0) as i64,
                        modification_time: action_data["modificationTime"].as_i64().unwrap_or(0),
                        data_change,
                        stats: action_data["stats"].as_str().map(|s| s.to_string()),
                        partition_values: action_data["partitionValues"]
                            .as_object()
                            .map(|obj| obj.iter()
                                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                                .collect()),
                        tags: action_data["tags"]
                            .as_object()
                            .map(|obj| obj.iter()
                                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                                .collect()),
                    })
                } else {
                    Err(SqlError::validation_error("AddFile action missing file path"))
                }
            },
            "remove" => {
                if let Some(file_path) = file_path {
                    Ok(Action::RemoveFile {
                        path: file_path,
                        deletion_timestamp: action_data["deletionTimestamp"].as_i64().unwrap_or(0),
                        data_change,
                        extended_file_metadata: action_data["extendedFileMetadata"].as_bool().unwrap_or(false),
                        partition_values: action_data["partitionValues"]
                            .as_object()
                            .map(|obj| obj.iter()
                                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                                .collect()),
                        tags: action_data["tags"]
                            .as_object()
                            .map(|obj| obj.iter()
                                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                                .collect()),
                    })
                } else {
                    Err(SqlError::validation_error("RemoveFile action missing file path"))
                }
            },
            _ => Ok(Action::Metadata(action_data)),
        }
    }
}

#[async_trait]
impl TxnLogReader for PostgresAdapter {
    async fn read_table(&self, table_id: Uuid) -> SqlResult<Option<Table>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let row = sqlx::query(
                "SELECT id, table_path, table_name, table_uuid, created_at, updated_at
                 FROM delta_tables WHERE id = $1 AND is_active = true"
            )
            .bind(table_id)
            .fetch_optional(&mut *conn)
            .await?;

            Ok(row.map(Self::row_to_table).transpose()?)
        }).await
    }

    async fn read_table_by_path(&self, table_path: &str) -> SqlResult<Option<Table>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let row = sqlx::query(
                "SELECT id, table_path, table_name, table_uuid, created_at, updated_at
                 FROM delta_tables WHERE table_path = $1 AND is_active = true"
            )
            .bind(table_path)
            .fetch_optional(&mut *conn)
            .await?;

            Ok(row.map(Self::row_to_table).transpose()?)
        }).await
    }

    async fn read_protocol(&self, table_id: Uuid) -> SqlResult<Option<Protocol>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let row = sqlx::query(
                "SELECT min_reader_version, min_writer_version
                 FROM delta_protocols WHERE table_id = $1"
            )
            .bind(table_id)
            .fetch_optional(&mut *conn)
            .await?;

            Ok(row.map(Self::row_to_protocol).transpose()?)
        }).await
    }

    async fn read_metadata(&self, table_id: Uuid) -> SqlResult<Option<Metadata>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let row = sqlx::query(
                "SELECT metadata_json FROM delta_metadata WHERE table_id = $1"
            )
            .bind(table_id)
            .fetch_optional(&mut *conn)
            .await?;

            Ok(row.map(Self::row_to_metadata).transpose()?)
        }).await
    }

    async fn read_commit(&self, table_id: Uuid, version: i64) -> SqlResult<Option<Commit>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let row = sqlx::query(
                "SELECT id, table_id, version, timestamp, operation_type, operation_parameters, commit_info
                 FROM delta_commits WHERE table_id = $1 AND version = $2"
            )
            .bind(table_id)
            .bind(version)
            .fetch_optional(&mut *conn)
            .await?;

            Ok(row.map(Self::row_to_commit).transpose()?)
        }).await
    }

    async fn read_commits_range(
        &self,
        table_id: Uuid,
        start_version: Option<i64>,
        end_version: Option<i64>,
        limit: Option<u32>,
    ) -> SqlResult<Vec<Commit>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let mut query = "SELECT id, table_id, version, timestamp, operation_type, operation_parameters, commit_info
                           FROM delta_commits WHERE table_id = $1".to_string();
            let mut params: Vec<Box<dyn sqlx::database::HasArguments<sqlx::Postgres> + Send>> = Vec::new();

            if let Some(start) = start_version {
                query.push_str(" AND version >= $2");
                params.push(Box::new(start));
            }
            if let Some(end) = end_version {
                let param_idx = if start_version.is_some() { 3 } else { 2 };
                query.push_str(&format!(" AND version <= ${}", param_idx));
                params.push(Box::new(end));
            }

            query.push_str(" ORDER BY version DESC");

            if let Some(limit_val) = limit {
                let param_idx = 2 + params.len();
                query.push_str(&format!(" LIMIT {}", limit_val));
            }

            let mut query_builder = sqlx::query(&query);
            query_builder = query_builder.bind(table_id);

            if let Some(start) = start_version {
                query_builder = query_builder.bind(start);
            }
            if let Some(end) = end_version {
                query_builder = query_builder.bind(end);
            }

            let rows = query_builder.fetch_all(&mut *conn).await?;

            let mut commits = Vec::new();
            for row in rows {
                commits.push(Self::row_to_commit(&row)?);
            }

            Ok(commits)
        }).await
    }

    async fn read_latest_commit(&self, table_id: Uuid) -> SqlResult<Option<Commit>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let row = sqlx::query(
                "SELECT id, table_id, version, timestamp, operation_type, operation_parameters, commit_info
                 FROM delta_commits WHERE table_id = $1 ORDER BY version DESC LIMIT 1"
            )
            .bind(table_id)
            .fetch_optional(&mut *conn)
            .await?;

            Ok(row.map(Self::row_to_commit).transpose()?)
        }).await
    }

    async fn read_commit_actions(&self, commit_id: Uuid) -> SqlResult<Vec<Action>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let rows = sqlx::query(
                "SELECT action_type, action_data, file_path, data_change
                 FROM delta_commit_actions WHERE commit_id = $1 ORDER BY id"
            )
            .bind(commit_id)
            .fetch_all(&mut *conn)
            .await?;

            let mut actions = Vec::new();
            for row in rows {
                actions.push(Self::row_to_action(&row)?);
            }

            Ok(actions)
        }).await
    }

    async fn list_tables(&self, limit: Option<u32>, offset: Option<u32>) -> SqlResult<Vec<Table>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let mut query = "SELECT id, table_path, table_name, table_uuid, created_at, updated_at
                           FROM delta_tables WHERE is_active = true ORDER BY table_path".to_string();

            if let Some(offset_val) = offset {
                query.push_str(&format!(" OFFSET {}", offset_val));
            }

            if let Some(limit_val) = limit {
                query.push_str(&format!(" LIMIT {}", limit_val));
            }

            let rows = sqlx::query(&query)
                .fetch_all(&mut *conn)
                .await?;

            let mut tables = Vec::new();
            for row in rows {
                tables.push(Self::row_to_table(&row)?);
            }

            Ok(tables)
        }).await
    }

    async fn table_exists(&self, table_path: &str) -> SqlResult<bool> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM delta_tables WHERE table_path = $1 AND is_active = true"
            )
            .bind(table_path)
            .fetch_one(&mut *conn)
            .await?;

            Ok(count > 0)
        }).await
    }

    async fn get_table_version(&self, table_id: Uuid) -> SqlResult<Option<i64>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let version = sqlx::query_scalar(
                "SELECT MAX(version) FROM delta_commits WHERE table_id = $1"
            )
            .bind(table_id)
            .fetch_optional(&mut *conn)
            .await?;

            Ok(version)
        }).await
    }

    async fn count_commits(&self, table_id: Uuid) -> SqlResult<i64> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM delta_commits WHERE table_id = $1"
            )
            .bind(table_id)
            .fetch_one(&mut *conn)
            .await?;

            Ok(count)
        }).await
    }

    async fn read_table_files(
        &self,
        table_id: Uuid,
        start_version: Option<i64>,
        end_version: Option<i64>,
    ) -> SqlResult<Vec<Value>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let mut query = "SELECT file_path, file_size, modification_time, data_change,
                           file_stats, partition_values, tags, commit_version
                           FROM delta_files WHERE table_id = $1 AND is_active = true".to_string();

            if let Some(start) = start_version {
                query.push_str(" AND commit_version >= $2");
            }
            if let Some(end) = end_version {
                let param_idx = if start_version.is_some() { 3 } else { 2 };
                query.push_str(&format!(" AND commit_version <= ${}", param_idx));
            }

            query.push_str(" ORDER BY commit_version DESC, file_path");

            let mut query_builder = sqlx::query(&query);
            query_builder = query_builder.bind(table_id);

            if let Some(start) = start_version {
                query_builder = query_builder.bind(start);
            }
            if let Some(end) = end_version {
                query_builder = query_builder.bind(end);
            }

            let rows = query_builder.fetch_all(&mut *conn).await?;

            let mut files = Vec::new();
            for row in rows {
                let mut file = serde_json::Map::new();
                file.insert("path".to_string(), Value::String(row.try_get("file_path")?));
                file.insert("size".to_string(), Value::Number(row.try_get::<i64, _>("file_size")?.into()));
                file.insert("modificationTime".to_string(),
                    Value::Number(row.try_get::<i64, _>("modification_time")?.into()));
                file.insert("dataChange".to_string(), Value::Bool(row.try_get("data_change")?));
                file.insert("stats".to_string(), row.try_get("file_stats")?);
                file.insert("partitionValues".to_string(), row.try_get("partition_values")?);
                file.insert("tags".to_string(), row.try_get("tags")?);
                file.insert("commitVersion".to_string(),
                    Value::Number(row.try_get::<i64, _>("commit_version")?.into()));

                files.push(Value::Object(file));
            }

            Ok(files)
        }).await
    }
}

#[async_trait]
impl TxnLogWriter for PostgresAdapter {
    async fn create_table(&self, table: &Table) -> SqlResult<Table> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            // Begin transaction
            let mut tx = conn.begin().await?;

            let mut created_table = table.clone();

            // Insert table
            sqlx::query(
                "INSERT INTO delta_tables (id, table_path, table_name, table_uuid, created_at, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(table.id)
            .bind(&table.table_path)
            .bind(&table.table_name)
            .bind(table.table_uuid)
            .bind(utils::format_timestamp(table.created_at))
            .bind(utils::format_timestamp(table.updated_at))
            .execute(&mut *tx)
            .await?;

            // Update updated_at time
            let now = Utc::now();
            sqlx::query("UPDATE delta_tables SET updated_at = $1 WHERE id = $2")
                .bind(utils::format_timestamp(now))
                .bind(table.id)
                .execute(&mut *tx)
                .await?;

            created_table.updated_at = now;

            // Commit transaction
            tx.commit().await?;

            Ok(created_table)
        }).await
    }

    async fn update_table(&self, table: &Table) -> SqlResult<Table> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let mut updated_table = table.clone();

            let now = Utc::now();
            let result = sqlx::query(
                "UPDATE delta_tables
                 SET table_name = $1, updated_at = $2
                 WHERE id = $3 AND is_active = true"
            )
            .bind(&table.table_name)
            .bind(utils::format_timestamp(now))
            .bind(table.id)
            .execute(&mut *conn)
            .await?;

            if result.rows_affected() == 0 {
                return Err(SqlError::table_not_found(table.id.to_string()));
            }

            updated_table.updated_at = now;
            Ok(updated_table)
        }).await
    }

    async fn delete_table(&self, table_id: Uuid) -> SqlResult<bool> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            // Soft delete by setting is_active = false
            let result = sqlx::query(
                "UPDATE delta_tables
                 SET is_active = false, updated_at = $1
                 WHERE id = $2"
            )
            .bind(utils::format_timestamp(Utc::now()))
            .bind(table_id)
            .execute(&mut *conn)
            .await?;

            Ok(result.rows_affected() > 0)
        }).await
    }

    async fn write_commit(&self, commit: &Commit) -> SqlResult<Commit> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            // Begin transaction
            let mut tx = conn.begin().await?;

            // Insert commit
            let mut created_commit = commit.clone();

            sqlx::query(
                "INSERT INTO delta_commits (id, table_id, version, timestamp, operation_type,
                 operation_parameters, isolation_level, is_blind_append, transaction_id, commit_info, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
            )
            .bind(commit.id)
            .bind(commit.table_id)
            .bind(commit.version)
            .bind(utils::format_timestamp(commit.timestamp))
            .bind(&commit.operation_type)
            .bind(&commit.operation_parameters)
            .bind("Serializable")
            .bind(false) // TODO: Extract from commit info
            .bind(None::<Uuid>) // TODO: Extract from commit info
            .bind(&commit.commit_info)
            .bind(utils::format_timestamp(Utc::now()))
            .execute(&mut *tx)
            .await?;

            // TODO: Insert actions for this commit (would need commit actions data)

            // Commit transaction
            tx.commit().await?;

            Ok(created_commit)
        }).await
    }

    async fn write_commits(&self, commits: &[Commit]) -> SqlResult<Vec<Commit>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            // Begin transaction
            let mut tx = conn.begin().await?;

            let mut created_commits = Vec::new();

            for commit in commits {
                // Insert commit
                sqlx::query(
                    "INSERT INTO delta_commits (id, table_id, version, timestamp, operation_type,
                     operation_parameters, isolation_level, is_blind_append, transaction_id, commit_info, created_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
                )
                .bind(commit.id)
                .bind(commit.table_id)
                .bind(commit.version)
                .bind(utils::format_timestamp(commit.timestamp))
                .bind(&commit.operation_type)
                .bind(&commit.operation_parameters)
                .bind("Serializable")
                .bind(false)
                .bind(None::<Uuid>)
                .bind(&commit.commit_info)
                .bind(utils::format_timestamp(Utc::now()))
                .execute(&mut *tx)
                .await?;

                created_commits.push(commit.clone());
            }

            // TODO: Insert actions for all commits

            // Commit transaction
            tx.commit().await?;

            Ok(created_commits)
        }).await
    }

    async fn update_protocol(&self, table_id: Uuid, protocol: &Protocol) -> SqlResult<()> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            sqlx::query(
                "INSERT INTO delta_protocols (table_id, min_reader_version, min_writer_version, created_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (table_id) DO UPDATE SET
                 min_reader_version = EXCLUDED.min_reader_version,
                 min_writer_version = EXCLUDED.min_writer_version,
                 created_at = EXCLUDED.created_at"
            )
            .bind(table_id)
            .bind(protocol.min_reader_version)
            .bind(protocol.min_writer_version)
            .bind(utils::format_timestamp(Utc::now()))
            .execute(&mut *conn)
            .await?;

            Ok(())
        }).await
    }

    async fn update_metadata(&self, table_id: Uuid, metadata: &Metadata) -> SqlResult<()> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            // Build metadata JSON
            let mut metadata_json = serde_json::Map::new();
            metadata_json.insert("id".to_string(), Value::String(metadata.id.clone()));
            metadata_json.insert("name".to_string(), Value::String(metadata.name.clone()));
            if let Some(desc) = &metadata.description {
                metadata_json.insert("description".to_string(), Value::String(desc.clone()));
            }

            let mut format = serde_json::Map::new();
            format.insert("provider".to_string(), Value::String(metadata.format.clone()));
            metadata_json.insert("format".to_string(), Value::Object(format));

            if let Some(schema) = &metadata.schema_string {
                metadata_json.insert("schemaString".to_string(), Value::String(schema.clone()));
            }

            let partition_columns = serde_json::Value::Array(
                metadata.partition_columns.iter()
                    .map(|col| Value::String(col.clone()))
                    .collect()
            );
            metadata_json.insert("partitionColumns".to_string(), partition_columns);
            metadata_json.insert("configuration".to_string(), metadata.configuration.clone());

            if let Some(created_time) = metadata.created_time {
                metadata_json.insert("createdTime".to_string(), Value::Number(created_time.into()));
            }

            sqlx::query(
                "INSERT INTO delta_metadata (table_id, metadata_json, created_at, configuration, partition_columns)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (table_id) DO UPDATE SET
                 metadata_json = EXCLUDED.metadata_json,
                 configuration = EXCLUDED.configuration,
                 partition_columns = EXCLUDED.partition_columns"
            )
            .bind(table_id)
            .bind(Value::Object(metadata_json))
            .bind(utils::format_timestamp(Utc::now()))
            .bind(&metadata.configuration)
            .bind(Value::Array(
                metadata.partition_columns.iter()
                    .map(|col| Value::String(col.clone()))
                    .collect()
            ))
            .execute(&mut *conn)
            .await?;

            Ok(())
        }).await
    }

    async fn vacuum_commits(&self, table_id: Uuid, keep_last_n: i64) -> SqlResult<i64> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            // Get the version to keep up to
            let cutoff_version: Option<i64> = sqlx::query_scalar(
                "SELECT version FROM delta_commits
                 WHERE table_id = $1
                 ORDER BY version DESC
                 OFFSET $2 LIMIT 1"
            )
            .bind(table_id)
            .bind(keep_last_n as i64 - 1)
            .fetch_optional(&mut *conn)
            .await?;

            if let Some(cutoff) = cutoff_version {
                // Delete commits older than cutoff
                let result = sqlx::query(
                    "DELETE FROM delta_commits WHERE table_id = $1 AND version <= $2"
                )
                .bind(table_id)
                .bind(cutoff)
                .execute(&mut *conn)
                .await?;

                Ok(result.rows_affected() as i64)
            } else {
                Ok(0)
            }
        }).await
    }

    async fn get_next_version(&self, table_id: Uuid) -> SqlResult<i64> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let current_version: Option<i64> = sqlx::query_scalar(
                "SELECT COALESCE(MAX(version), 0) FROM delta_commits WHERE table_id = $1"
            )
            .bind(table_id)
            .fetch_optional(&mut *conn)
            .await?;

            Ok(current_version.unwrap_or(0) + 1)
        }).await
    }

    async fn begin_transaction(&self) -> SqlResult<Box<dyn Transaction>> {
        let mut conn = self.get_connection().await?;
        let tx = conn.begin().await?;

        // Note: This is a simplified implementation
        // A full implementation would need to manage the transaction object properly
        Err(SqlError::generic("Transaction management not fully implemented in PostgreSQL adapter"))
    }

    async fn health_check(&self) -> SqlResult<bool> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let result: i64 = sqlx::query_scalar("SELECT 1")
                .fetch_one(&mut *conn)
                .await?;

            Ok(result == 1)
        }).await
    }
}

#[async_trait]
impl DatabaseAdapter for PostgresAdapter {
    fn database_type(&self) -> &'static str {
        self.base.database_type()
    }

    async fn database_version(&self) -> SqlResult<String> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let version: String = sqlx::query_scalar("SELECT version()")
                .fetch_one(&mut *conn)
                .await?;

            Ok(version)
        }).await
    }

    async fn initialize_schema(&self) -> SqlResult<()> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let schema_sql = SchemaManager::get_schema_sql("postgresql");
            let index_sql = SchemaManager::get_index_sql("postgresql");

            let mut tx = conn.begin().await?;

            // Create tables
            for statement in schema_sql {
                sqlx::query(&statement)
                    .execute(&mut *tx)
                    .await?;
            }

            // Create indexes
            for statement in index_sql {
                sqlx::query(&statement)
                    .execute(&mut *tx)
                    .await?;
            }

            // Insert initial migrations
            let migrations = SchemaManager::get_initial_migrations();
            for (version, name, description) in migrations {
                sqlx::query(
                    "INSERT INTO schema_migrations (version, name, description)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (version) DO NOTHING"
                )
                .bind(version)
                .bind(name)
                .bind(description)
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;

            Ok(())
        }).await
    }

    async fn check_schema_version(&self) -> SqlResult<bool> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let result: Option<i64> = sqlx::query_scalar(
                "SELECT MAX(version) FROM schema_migrations"
            )
            .fetch_optional(&mut *conn)
            .await?;

            Ok(result.unwrap_or(0) >= crate::schema::SCHEMA_VERSION)
        }).await
    }

    async fn migrate_schema(&self) -> SqlResult<()> {
        // For now, just call initialize_schema
        // Future implementations would handle incremental migrations
        self.initialize_schema().await
    }

    async fn create_indexes(&self) -> SqlResult<()> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let index_sql = SchemaManager::get_index_sql("postgresql");

            for statement in index_sql {
                sqlx::query(&statement)
                    .execute(&mut *conn)
                    .await?;
            }

            Ok(())
        }).await
    }

    async fn pool_stats(&self) -> SqlResult<PoolStats> {
        // Get pool statistics from sqlx
        let size = self.pool.size();
        let idle = self.pool.num_idle();

        // Note: sqlx doesn't expose active connection count directly
        // This is an approximation
        let active = size.saturating_sub(idle);

        Ok(PoolStats::new(size, active, idle, size))
    }

    async fn test_connection(&self) -> SqlResult<bool> {
        self.health_check().await
    }

    fn get_config(&self) -> &DatabaseConfig {
        self.base.get_config()
    }

    async fn close(&self) -> SqlResult<()> {
        self.pool.close().await;
        Ok(())
    }

    async fn execute_raw(&self, query: &str, params: &[Value]) -> SqlResult<Vec<HashMap<String, Value>>> {
        self.execute_with_timeout(async {
            let mut conn = self.get_connection().await?;

            let mut query_builder = sqlx::query(query);
            for param in params {
                // Note: This is a simplified parameter binding
                // A full implementation would need to handle different parameter types
                query_builder = query_builder.bind(param.to_string());
            }

            let rows = query_builder.fetch_all(&mut *conn).await?;

            let mut results = Vec::new();
            for row in rows {
                let mut row_map = HashMap::new();
                // Note: This is a simplified row to map conversion
                // A full implementation would need to handle different column types
                for (i, column) in row.columns().iter().enumerate() {
                    if let Ok(value) = row.try_get::<String, _>(i) {
                        row_map.insert(column.name().to_string(), Value::String(value));
                    }
                }
                results.push(row_map);
            }

            Ok(results)
        }).await
    }

    async fn get_optimization_hints(&self) -> SqlResult<Vec<String>> {
        self.base.get_optimization_hints().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_postgres_adapter_creation() {
        // This test would require a real PostgreSQL database
        // For now, just test URL parsing
        let config = DatabaseConfig {
            url: "postgresql://localhost/testdb".to_string(),
            ..Default::default()
        };

        // Note: This will fail without a running PostgreSQL instance
        // In real tests, we'd use testcontainers or similar
        // let result = PostgresAdapter::new(config).await;
        // assert!(result.is_ok());
    }

    #[test]
    fn test_row_conversions() {
        // Note: These tests would require actual database rows
        // In real tests, we'd create mock rows or use test database
    }
}