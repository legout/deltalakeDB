//! DuckDB adapter for Delta Lake metadata storage

use crate::{
    SqlResult, DatabaseConfig, PoolStats, MigrationInfo, schema::SchemaManager,
    adapters::{BaseAdapter, BaseTransaction, PoolManager, utils},
};
use crate::traits::{TxnLogReader, TxnLogWriter, DatabaseAdapter, Transaction};
use crate::error::SqlError;

use deltalakedb_core::{Table, Commit, Protocol, Metadata, Action};
use async_trait::async_trait;
use duckdb::{params, Connection as DuckConnection, Result as DuckResult};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tokio::time::{timeout, Duration};
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// DuckDB adapter implementation
pub struct DuckDBAdapter {
    base: BaseAdapter,
    connection: Arc<Mutex<DuckConnection>>,
}

/// DuckDB transaction implementation
pub struct DuckDBTransaction {
    base_tx: BaseTransaction,
    conn: DuckConnection,
}

impl DuckDBAdapter {
    /// Create a new DuckDB adapter
    pub async fn new(config: DatabaseConfig) -> SqlResult<Self> {
        // Parse DuckDB connection URL
        let path = if config.url.starts_with("duckdb://memory") {
            ":memory:"
        } else if config.url.starts_with("duckdb://") {
            &config.url[9..]
        } else {
            &config.url
        };

        // Create DuckDB connection
        let conn = DuckConnection::open(path)
            .map_err(|e| SqlError::connection_error(format!("Failed to connect to DuckDB: {}", e)))?;

        // Configure DuckDB for optimal performance
        Self::configure_connection(&conn)?;

        let adapter = Self {
            base: BaseAdapter::new(config, "duckdb"),
            connection: Arc::new(Mutex::new(conn)),
        };

        // Initialize schema if needed
        adapter.initialize_schema().await?;

        Ok(adapter)
    }

    /// Configure DuckDB connection settings
    fn configure_connection(conn: &DuckConnection) -> SqlResult<()> {
        // Enable parallel query execution
        conn.execute_batch("PRAGMA threads=4;")
            .map_err(|e| SqlError::config_error(format!("Failed to set threads: {}", e)))?;

        // Set memory limit (500MB)
        conn.execute_batch("PRAGMA memory_limit='500MB';")
            .map_err(|e| SqlError::config_error(format!("Failed to set memory limit: {}", e)))?;

        // Enable progress bar (optional)
        conn.execute_batch("PRAGMA enable_progress_bar=true;")
            .map_err(|e| SqlError::config_error(format!("Failed to enable progress bar: {}", e)))?;

        Ok(())
    }

    /// Get a connection reference
    fn get_connection(&self) -> std::sync::MutexGuard<DuckConnection> {
        self.connection.lock().unwrap()
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

    /// Convert a DuckDB row to a Table
    fn row_to_table(row: &duckdb::Row) -> SqlResult<Table> {
        let id_str: String = row.get(0)?;
        let id: Uuid = Uuid::parse_str(&id_str)
            .map_err(|e| SqlError::UuidError(e))?;

        let table_path: String = row.get(1)?;
        let table_name: String = row.get(2)?;

        let table_uuid_str: String = row.get(3)?;
        let table_uuid: Uuid = Uuid::parse_str(&table_uuid_str)
            .map_err(|e| SqlError::UuidError(e))?;

        let created_at_str: String = row.get(4)?;
        let created_at: DateTime<Utc> = utils::parse_timestamp(&created_at_str)?;

        let updated_at_str: String = row.get(5)?;
        let updated_at: DateTime<Utc> = utils::parse_timestamp(&updated_at_str)?;

        Ok(Table {
            id,
            table_path,
            table_name,
            table_uuid,
            created_at,
            updated_at,
        })
    }

    /// Convert a DuckDB row to a Commit
    fn row_to_commit(row: &duckdb::Row) -> SqlResult<Commit> {
        let id_str: String = row.get(0)?;
        let id: Uuid = Uuid::parse_str(&id_str)
            .map_err(|e| SqlError::UuidError(e))?;

        let table_id_str: String = row.get(1)?;
        let table_id: Uuid = Uuid::parse_str(&table_id_str)
            .map_err(|e| SqlError::UuidError(e))?;

        let version: i64 = row.get(2)?;
        let timestamp_str: String = row.get(3)?;
        let timestamp: DateTime<Utc> = utils::parse_timestamp(&timestamp_str)?;
        let operation_type: String = row.get(4)?;
        let operation_parameters: Value = serde_json::from_str(&row.get::<_, String>(5)?)?;
        let commit_info: Value = serde_json::from_str(&row.get::<_, String>(6)?)?;

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

    /// Convert a DuckDB row to Protocol
    fn row_to_protocol(row: &duckdb::Row) -> SqlResult<Protocol> {
        let min_reader_version: i32 = row.get(0)?;
        let min_writer_version: i32 = row.get(1)?;

        Ok(Protocol {
            min_reader_version,
            min_writer_version,
        })
    }

    /// Convert a DuckDB row to Metadata
    fn row_to_metadata(row: &duckdb::Row) -> SqlResult<Metadata> {
        let metadata_json: Value = serde_json::from_str(&row.get::<_, String>(0)?)?;

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

    /// Convert a DuckDB row to Action
    fn row_to_action(row: &duckdb::Row) -> SqlResult<Action> {
        let action_type: String = row.get(0)?;
        let action_data: Value = serde_json::from_str(&row.get::<_, String>(1)?)?;
        let file_path: Option<String> = row.get(2).ok();
        let data_change: bool = row.get(3).unwrap_or(true);

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
impl TxnLogReader for DuckDBAdapter {
    async fn read_table(&self, table_id: Uuid) -> SqlResult<Option<Table>> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut stmt = conn.prepare(
                "SELECT id, table_path, table_name, table_uuid, created_at, updated_at
                 FROM delta_tables WHERE id = ? AND is_active = true"
            ).map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let mut rows = stmt.query(params![table_id.to_string()])
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_table(row)?))
            } else {
                Ok(None)
            }
        }).await
    }

    async fn read_table_by_path(&self, table_path: &str) -> SqlResult<Option<Table>> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut stmt = conn.prepare(
                "SELECT id, table_path, table_name, table_uuid, created_at, updated_at
                 FROM delta_tables WHERE table_path = ? AND is_active = true"
            ).map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let mut rows = stmt.query(params![table_path])
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_table(row)?))
            } else {
                Ok(None)
            }
        }).await
    }

    async fn read_protocol(&self, table_id: Uuid) -> SqlResult<Option<Protocol>> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut stmt = conn.prepare(
                "SELECT min_reader_version, min_writer_version
                 FROM delta_protocols WHERE table_id = ?"
            ).map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let mut rows = stmt.query(params![table_id.to_string()])
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_protocol(row)?))
            } else {
                Ok(None)
            }
        }).await
    }

    async fn read_metadata(&self, table_id: Uuid) -> SqlResult<Option<Metadata>> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut stmt = conn.prepare(
                "SELECT metadata_json FROM delta_metadata WHERE table_id = ?"
            ).map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let mut rows = stmt.query(params![table_id.to_string()])
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_metadata(row)?))
            } else {
                Ok(None)
            }
        }).await
    }

    async fn read_commit(&self, table_id: Uuid, version: i64) -> SqlResult<Option<Commit>> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut stmt = conn.prepare(
                "SELECT id, table_id, version, timestamp, operation_type, operation_parameters, commit_info
                 FROM delta_commits WHERE table_id = ? AND version = ?"
            ).map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let mut rows = stmt.query(params![table_id.to_string(), version])
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_commit(row)?))
            } else {
                Ok(None)
            }
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
            let conn = self.get_connection();

            let mut query = "SELECT id, table_id, version, timestamp, operation_type, operation_parameters, commit_info
                           FROM delta_commits WHERE table_id = ?".to_string();

            if start_version.is_some() || end_version.is_some() {
                if let Some(start) = start_version {
                    query.push_str(" AND version >= ?");
                }
                if let Some(end) = end_version {
                    query.push_str(" AND version <= ?");
                }
            }

            query.push_str(" ORDER BY version DESC");

            if let Some(limit_val) = limit {
                query.push_str(&format!(" LIMIT {}", limit_val));
            }

            let mut stmt = conn.prepare(&query)
                .map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let mut params = vec![table_id.to_string()];
            if let Some(start) = start_version {
                params.push(start.to_string());
            }
            if let Some(end) = end_version {
                params.push(end.to_string());
            }

            let param_refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| p as &dyn duckdb::ToSql).collect();

            let mut rows = stmt.query(&param_refs[..])
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            let mut commits = Vec::new();
            while let Some(row) = rows.next()? {
                commits.push(Self::row_to_commit(row)?);
            }

            Ok(commits)
        }).await
    }

    async fn read_latest_commit(&self, table_id: Uuid) -> SqlResult<Option<Commit>> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut stmt = conn.prepare(
                "SELECT id, table_id, version, timestamp, operation_type, operation_parameters, commit_info
                 FROM delta_commits WHERE table_id = ? ORDER BY version DESC LIMIT 1"
            ).map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let mut rows = stmt.query(params![table_id.to_string()])
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_commit(row)?))
            } else {
                Ok(None)
            }
        }).await
    }

    async fn read_commit_actions(&self, commit_id: Uuid) -> SqlResult<Vec<Action>> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut stmt = conn.prepare(
                "SELECT action_type, action_data, file_path, data_change
                 FROM delta_commit_actions WHERE commit_id = ? ORDER BY id"
            ).map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let mut rows = stmt.query(params![commit_id.to_string()])
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            let mut actions = Vec::new();
            while let Some(row) = rows.next()? {
                actions.push(Self::row_to_action(row)?);
            }

            Ok(actions)
        }).await
    }

    async fn list_tables(&self, limit: Option<u32>, offset: Option<u32>) -> SqlResult<Vec<Table>> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut query = "SELECT id, table_path, table_name, table_uuid, created_at, updated_at
                           FROM delta_tables WHERE is_active = true ORDER BY table_path".to_string();

            if let Some(offset_val) = offset {
                query.push_str(&format!(" OFFSET {}", offset_val));
            }

            if let Some(limit_val) = limit {
                query.push_str(&format!(" LIMIT {}", limit_val));
            }

            let mut stmt = conn.prepare(&query)
                .map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let mut rows = stmt.query(params![])
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            let mut tables = Vec::new();
            while let Some(row) = rows.next()? {
                tables.push(Self::row_to_table(row)?);
            }

            Ok(tables)
        }).await
    }

    async fn table_exists(&self, table_path: &str) -> SqlResult<bool> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut stmt = conn.prepare(
                "SELECT COUNT(*) FROM delta_tables WHERE table_path = ? AND is_active = true"
            ).map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let count: i64 = stmt.query_row(params![table_path], |row| row.get(0))
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            Ok(count > 0)
        }).await
    }

    async fn get_table_version(&self, table_id: Uuid) -> SqlResult<Option<i64>> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut stmt = conn.prepare(
                "SELECT MAX(version) FROM delta_commits WHERE table_id = ?"
            ).map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let version: Option<i64> = stmt.query_row(params![table_id.to_string()], |row| row.get(0))
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            Ok(version)
        }).await
    }

    async fn count_commits(&self, table_id: Uuid) -> SqlResult<i64> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut stmt = conn.prepare(
                "SELECT COUNT(*) FROM delta_commits WHERE table_id = ?"
            ).map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let count: i64 = stmt.query_row(params![table_id.to_string()], |row| row.get(0))
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

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
            let conn = self.get_connection();

            let mut query = "SELECT file_path, file_size, modification_time, data_change,
                           file_stats, partition_values, tags, commit_version
                           FROM delta_files WHERE table_id = ? AND is_active = true".to_string();

            if start_version.is_some() || end_version.is_some() {
                if let Some(start) = start_version {
                    query.push_str(" AND commit_version >= ?");
                }
                if let Some(end) = end_version {
                    query.push_str(" AND commit_version <= ?");
                }
            }

            query.push_str(" ORDER BY commit_version DESC, file_path");

            let mut stmt = conn.prepare(&query)
                .map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let mut params = vec![table_id.to_string()];
            if let Some(start) = start_version {
                params.push(start.to_string());
            }
            if let Some(end) = end_version {
                params.push(end.to_string());
            }

            let param_refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| p as &dyn duckdb::ToSql).collect();

            let mut rows = stmt.query(&param_refs[..])
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            let mut files = Vec::new();
            while let Some(row) = rows.next()? {
                let mut file = serde_json::Map::new();
                file.insert("path".to_string(), Value::String(row.get(0)?));
                file.insert("size".to_string(), Value::Number(row.get::<_, i64>(1)?.into()));
                file.insert("modificationTime".to_string(), Value::Number(row.get::<_, i64>(2)?.into()));
                file.insert("dataChange".to_string(), Value::Bool(row.get(3).unwrap_or(true)));

                let file_stats: String = row.get(4)?;
                file.insert("stats".to_string(), Value::String(file_stats));

                let partition_values: String = row.get(5)?;
                file.insert("partitionValues".to_string(), Value::String(partition_values));

                let tags: String = row.get(6)?;
                file.insert("tags".to_string(), Value::String(tags));

                file.insert("commitVersion".to_string(), Value::Number(row.get::<_, i64>(7)?.into()));

                files.push(Value::Object(file));
            }

            Ok(files)
        }).await
    }
}

#[async_trait]
impl TxnLogWriter for DuckDBAdapter {
    async fn create_table(&self, table: &Table) -> SqlResult<Table> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            // Begin transaction
            let tx = conn.transaction()
                .map_err(|e| SqlError::transaction_error(format!("Failed to begin transaction: {}", e)))?;

            let mut created_table = table.clone();

            // Insert table
            tx.execute(
                "INSERT INTO delta_tables (id, table_path, table_name, table_uuid, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    table.id.to_string(),
                    table.table_path,
                    table.table_name,
                    table.table_uuid.to_string(),
                    utils::format_timestamp(table.created_at),
                    utils::format_timestamp(table.updated_at)
                ]
            ).map_err(|e| SqlError::query_error(format!("Failed to insert table: {}", e)))?;

            // Update updated_at time
            let now = Utc::now();
            tx.execute(
                "UPDATE delta_tables SET updated_at = ? WHERE id = ?",
                params![utils::format_timestamp(now), table.id.to_string()]
            ).map_err(|e| SqlError::query_error(format!("Failed to update table: {}", e)))?;

            created_table.updated_at = now;

            // Commit transaction
            tx.commit()
                .map_err(|e| SqlError::transaction_error(format!("Failed to commit transaction: {}", e)))?;

            Ok(created_table)
        }).await
    }

    async fn update_table(&self, table: &Table) -> SqlResult<Table> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut updated_table = table.clone();

            let now = Utc::now();
            let rows_affected = conn.execute(
                "UPDATE delta_tables
                 SET table_name = ?, updated_at = ?
                 WHERE id = ? AND is_active = true",
                params![
                    table.table_name,
                    utils::format_timestamp(now),
                    table.id.to_string()
                ]
            ).map_err(|e| SqlError::query_error(format!("Failed to update table: {}", e)))?;

            if rows_affected == 0 {
                return Err(SqlError::table_not_found(table.id.to_string()));
            }

            updated_table.updated_at = now;
            Ok(updated_table)
        }).await
    }

    async fn delete_table(&self, table_id: Uuid) -> SqlResult<bool> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            // Soft delete by setting is_active = false
            let rows_affected = conn.execute(
                "UPDATE delta_tables
                 SET is_active = false, updated_at = ?
                 WHERE id = ?",
                params![utils::format_timestamp(Utc::now()), table_id.to_string()]
            ).map_err(|e| SqlError::query_error(format!("Failed to delete table: {}", e)))?;

            Ok(rows_affected > 0)
        }).await
    }

    async fn write_commit(&self, commit: &Commit) -> SqlResult<Commit> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            // Begin transaction
            let tx = conn.transaction()
                .map_err(|e| SqlError::transaction_error(format!("Failed to begin transaction: {}", e)))?;

            let mut created_commit = commit.clone();

            // Insert commit
            tx.execute(
                "INSERT INTO delta_commits (id, table_id, version, timestamp, operation_type,
                 operation_parameters, isolation_level, is_blind_append, transaction_id, commit_info, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    commit.id.to_string(),
                    commit.table_id.to_string(),
                    commit.version,
                    utils::format_timestamp(commit.timestamp),
                    commit.operation_type,
                    serde_json::to_string(&commit.operation_parameters)?,
                    "Serializable",
                    false,
                    None::<String>, // Transaction ID as NULL for now
                    serde_json::to_string(&commit.commit_info)?,
                    utils::format_timestamp(Utc::now())
                ]
            ).map_err(|e| SqlError::query_error(format!("Failed to insert commit: {}", e)))?;

            // TODO: Insert actions for this commit

            // Commit transaction
            tx.commit()
                .map_err(|e| SqlError::transaction_error(format!("Failed to commit transaction: {}", e)))?;

            Ok(created_commit)
        }).await
    }

    async fn write_commits(&self, commits: &[Commit]) -> SqlResult<Vec<Commit>> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            // Begin transaction
            let tx = conn.transaction()
                .map_err(|e| SqlError::transaction_error(format!("Failed to begin transaction: {}", e)))?;

            let mut created_commits = Vec::new();

            for commit in commits {
                // Insert commit
                tx.execute(
                    "INSERT INTO delta_commits (id, table_id, version, timestamp, operation_type,
                     operation_parameters, isolation_level, is_blind_append, transaction_id, commit_info, created_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    params![
                        commit.id.to_string(),
                        commit.table_id.to_string(),
                        commit.version,
                        utils::format_timestamp(commit.timestamp),
                        commit.operation_type,
                        serde_json::to_string(&commit.operation_parameters)?,
                        "Serializable",
                        false,
                        None::<String>,
                        serde_json::to_string(&commit.commit_info)?,
                        utils::format_timestamp(Utc::now())
                    ]
                ).map_err(|e| SqlError::query_error(format!("Failed to insert commit: {}", e)))?;

                created_commits.push(commit.clone());
            }

            // TODO: Insert actions for all commits

            // Commit transaction
            tx.commit()
                .map_err(|e| SqlError::transaction_error(format!("Failed to commit transaction: {}", e)))?;

            Ok(created_commits)
        }).await
    }

    async fn update_protocol(&self, table_id: Uuid, protocol: &Protocol) -> SqlResult<()> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            conn.execute(
                "INSERT OR REPLACE INTO delta_protocols (table_id, min_reader_version, min_writer_version, created_at)
                 VALUES (?, ?, ?, ?)",
                params![
                    table_id.to_string(),
                    protocol.min_reader_version,
                    protocol.min_writer_version,
                    utils::format_timestamp(Utc::now())
                ]
            ).map_err(|e| SqlError::query_error(format!("Failed to update protocol: {}", e)))?;

            Ok(())
        }).await
    }

    async fn update_metadata(&self, table_id: Uuid, metadata: &Metadata) -> SqlResult<()> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

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

            conn.execute(
                "INSERT OR REPLACE INTO delta_metadata (table_id, metadata_json, created_at, configuration, partition_columns)
                 VALUES (?, ?, ?, ?, ?)",
                params![
                    table_id.to_string(),
                    serde_json::to_string(&Value::Object(metadata_json))?,
                    utils::format_timestamp(Utc::now()),
                    serde_json::to_string(&metadata.configuration)?,
                    serde_json::to_string(&Value::Array(
                        metadata.partition_columns.iter()
                            .map(|col| Value::String(col.clone()))
                            .collect()
                    ))?
                ]
            ).map_err(|e| SqlError::query_error(format!("Failed to update metadata: {}", e)))?;

            Ok(())
        }).await
    }

    async fn vacuum_commits(&self, table_id: Uuid, keep_last_n: i64) -> SqlResult<i64> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            // Get the version to keep up to
            let cutoff_version: Option<i64> = {
                let mut stmt = conn.prepare(
                    "SELECT version FROM delta_commits
                     WHERE table_id = ?
                     ORDER BY version DESC
                     LIMIT 1 OFFSET ?"
                ).map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

                stmt.query_row(params![table_id.to_string(), keep_last_n - 1], |row| row.get(0))
                    .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?
            };

            if let Some(cutoff) = cutoff_version {
                // Delete commits older than cutoff
                let rows_affected = conn.execute(
                    "DELETE FROM delta_commits WHERE table_id = ? AND version <= ?",
                    params![table_id.to_string(), cutoff]
                ).map_err(|e| SqlError::query_error(format!("Failed to delete commits: {}", e)))?;

                Ok(rows_affected as i64)
            } else {
                Ok(0)
            }
        }).await
    }

    async fn get_next_version(&self, table_id: Uuid) -> SqlResult<i64> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut stmt = conn.prepare(
                "SELECT COALESCE(MAX(version), 0) FROM delta_commits WHERE table_id = ?"
            ).map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let current_version: i64 = stmt.query_row(params![table_id.to_string()], |row| row.get(0))
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            Ok(current_version + 1)
        }).await
    }

    async fn begin_transaction(&self) -> SqlResult<Box<dyn Transaction>> {
        // Note: This is a simplified implementation
        // A full implementation would need to manage the transaction object properly
        Err(SqlError::generic("Transaction management not fully implemented in DuckDB adapter"))
    }

    async fn health_check(&self) -> SqlResult<bool> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let result: i64 = conn.query_row("SELECT 1", [], |row| row.get(0))
                .map_err(|e| SqlError::query_error(format!("Health check failed: {}", e)))?;

            Ok(result == 1)
        }).await
    }
}

#[async_trait]
impl DatabaseAdapter for DuckDBAdapter {
    fn database_type(&self) -> &'static str {
        self.base.database_type()
    }

    async fn database_version(&self) -> SqlResult<String> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let version: String = conn.query_row("SELECT version()", [], |row| row.get(0))
                .map_err(|e| SqlError::query_error(format!("Failed to get version: {}", e)))?;

            Ok(version)
        }).await
    }

    async fn initialize_schema(&self) -> SqlResult<()> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let schema_sql = SchemaManager::get_schema_sql("duckdb");
            let index_sql = SchemaManager::get_index_sql("duckdb");

            // Begin transaction
            let tx = conn.transaction()
                .map_err(|e| SqlError::transaction_error(format!("Failed to begin transaction: {}", e)))?;

            // Create tables
            for statement in schema_sql {
                tx.execute(&statement, [])
                    .map_err(|e| SqlError::schema_error(format!("Failed to create table: {}", e)))?;
            }

            // Create indexes
            for statement in index_sql {
                tx.execute(&statement, [])
                    .map_err(|e| SqlError::schema_error(format!("Failed to create index: {}", e)))?;
            }

            // Insert initial migrations
            let migrations = SchemaManager::get_initial_migrations();
            for (version, name, description) in migrations {
                tx.execute(
                    "INSERT OR IGNORE INTO schema_migrations (version, name, description)
                     VALUES (?, ?, ?)",
                    params![version, name, description]
                ).map_err(|e| SqlError::migration_error(format!("Failed to insert migration: {}", e)))?;
            }

            // Commit transaction
            tx.commit()
                .map_err(|e| SqlError::transaction_error(format!("Failed to commit schema initialization: {}", e)))?;

            Ok(())
        }).await
    }

    async fn check_schema_version(&self) -> SqlResult<bool> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            let mut stmt = conn.prepare("SELECT MAX(version) FROM schema_migrations")
                .map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let result: Option<i64> = stmt.query_row([], |row| row.get(0))
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

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
            let conn = self.get_connection();

            let index_sql = SchemaManager::get_index_sql("duckdb");

            for statement in index_sql {
                conn.execute(&statement, [])
                    .map_err(|e| SqlError::schema_error(format!("Failed to create index: {}", e)))?;
            }

            Ok(())
        }).await
    }

    async fn pool_stats(&self) -> SqlResult<PoolStats> {
        // DuckDB doesn't have a connection pool in the traditional sense
        // Use a single connection as the pool
        Ok(PoolStats::new(1, 1, 0, 1))
    }

    async fn test_connection(&self) -> SqlResult<bool> {
        self.health_check().await
    }

    fn get_config(&self) -> &DatabaseConfig {
        self.base.get_config()
    }

    async fn close(&self) -> SqlResult<()> {
        // DuckDB connections are cleaned up when dropped
        Ok(())
    }

    async fn execute_raw(&self, query: &str, params: &[Value]) -> SqlResult<Vec<std::collections::HashMap<String, Value>>> {
        self.execute_with_timeout(async {
            let conn = self.get_connection();

            // Convert params to strings (simplified)
            let param_strings: Vec<String> = params.iter()
                .map(|v| v.to_string())
                .collect();

            let param_refs: Vec<&dyn duckdb::ToSql> = param_strings.iter().map(|p| p as &dyn duckdb::ToSql).collect();

            let mut stmt = conn.prepare(query)
                .map_err(|e| SqlError::query_error(format!("Failed to prepare statement: {}", e)))?;

            let mut rows = stmt.query(&param_refs[..])
                .map_err(|e| SqlError::query_error(format!("Failed to execute query: {}", e)))?;

            let mut results = Vec::new();
            while let Some(row) = rows.next()? {
                let mut row_map = std::collections::HashMap::new();
                // Note: This is a simplified row to map conversion
                // A full implementation would need to handle different column types
                for i in 0..row.as_ref().column_count() {
                    let column_name = row.as_ref().column_name(i);
                    if let Ok(value) = row.get::<_, String>(i) {
                        row_map.insert(column_name.to_string(), Value::String(value));
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
    async fn test_duckdb_adapter_creation() {
        let config = DatabaseConfig {
            url: "duckdb://memory".to_string(),
            ..Default::default()
        };

        let adapter = DuckDBAdapter::new(config).await.unwrap();
        assert_eq!(adapter.database_type(), "duckdb");

        // Test health check
        let healthy = adapter.health_check().await.unwrap();
        assert!(healthy);

        // Test database version
        let version = adapter.database_version().await.unwrap();
        assert!(!version.is_empty());

        // Close adapter
        adapter.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_duckdb_schema_initialization() {
        let config = DatabaseConfig {
            url: "duckdb://memory".to_string(),
            ..Default::default()
        };

        let adapter = DuckDBAdapter::new(config).await.unwrap();

        // Check schema version
        let schema_ok = adapter.check_schema_version().await.unwrap();
        assert!(schema_ok);

        adapter.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_duckdb_table_operations() {
        let config = DatabaseConfig {
            url: "duckdb://memory".to_string(),
            ..Default::default()
        };

        let adapter = DuckDBAdapter::new(config).await.unwrap();

        // Create a test table
        let table = Table {
            id: Uuid::new_v4(),
            table_path: "/test/delta_table".to_string(),
            table_name: "test_table".to_string(),
            table_uuid: Uuid::new_v4(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        // Test create_table
        let created_table = adapter.create_table(&table).await.unwrap();
        assert_eq!(created_table.table_path, table.table_path);
        assert_eq!(created_table.table_name, table.table_name);

        // Test read_table
        let read_table = adapter.read_table(table.id).await.unwrap();
        assert!(read_table.is_some());
        assert_eq!(read_table.unwrap().table_path, table.table_path);

        // Test read_table_by_path
        let read_by_path = adapter.read_table_by_path(&table.table_path).await.unwrap();
        assert!(read_by_path.is_some());
        assert_eq!(read_by_path.unwrap().id, table.id);

        // Test table_exists
        let exists = adapter.table_exists(&table.table_path).await.unwrap();
        assert!(exists);

        // Test list_tables
        let tables = adapter.list_tables(None, None).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].table_path, table.table_path);

        // Test update_table
        let mut updated_table = created_table.clone();
        updated_table.table_name = "updated_table".to_string();
        let table_result = adapter.update_table(&updated_table).await.unwrap();
        assert_eq!(table_result.table_name, "updated_table");

        // Test delete_table
        let deleted = adapter.delete_table(table.id).await.unwrap();
        assert!(deleted);

        // Verify table is soft-deleted
        let exists_after_delete = adapter.table_exists(&table.table_path).await.unwrap();
        assert!(!exists_after_delete);

        adapter.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_duckdb_protocol_and_metadata() {
        let config = DatabaseConfig {
            url: "duckdb://memory".to_string(),
            ..Default::default()
        };

        let adapter = DuckDBAdapter::new(config).await.unwrap();

        let table_id = Uuid::new_v4();

        // Test protocol operations
        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        adapter.update_protocol(table_id, &protocol).await.unwrap();

        let read_protocol = adapter.read_protocol(table_id).await.unwrap();
        assert!(read_protocol.is_some());
        let proto = read_protocol.unwrap();
        assert_eq!(proto.min_reader_version, 1);
        assert_eq!(proto.min_writer_version, 2);

        // Test metadata operations
        let metadata = Metadata {
            id: "test-metadata-id".to_string(),
            name: "test_table".to_string(),
            description: Some("Test table description".to_string()),
            format: "parquet".to_string(),
            schema_string: Some("{\\"type\\":\\"struct\\",\\"fields\\":[]}".to_string()),
            partition_columns: vec!["year".to_string(), "month".to_string()],
            configuration: serde_json::json!({"key": "value"}),
            created_time: Some(1234567890),
        };

        adapter.update_metadata(table_id, &metadata).await.unwrap();

        let read_metadata = adapter.read_metadata(table_id).await.unwrap();
        assert!(read_metadata.is_some());
        let meta = read_metadata.unwrap();
        assert_eq!(meta.id, "test-metadata-id");
        assert_eq!(meta.name, "test_table");
        assert_eq!(meta.description, Some("Test table description".to_string()));
        assert_eq!(meta.format, "parquet");
        assert_eq!(meta.partition_columns, vec!["year".to_string(), "month".to_string()]);
        assert_eq!(meta.created_time, Some(1234567890));

        adapter.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_duckdb_commit_operations() {
        let config = DatabaseConfig {
            url: "duckdb://memory".to_string(),
            ..Default::default()
        };

        let adapter = DuckDBAdapter::new(config).await.unwrap();

        let table_id = Uuid::new_v4();

        // Test get_next_version
        let next_version = adapter.get_next_version(table_id).await.unwrap();
        assert_eq!(next_version, 1);

        // Test write_commit
        let commit = Commit {
            id: Uuid::new_v4(),
            table_id,
            version: 1,
            timestamp: Utc::now(),
            operation_type: "WRITE".to_string(),
            operation_parameters: serde_json::json!({"mode": "Append"}),
            commit_info: serde_json::json!({"timestamp": Utc::now().timestamp_millis()}),
        };

        let written_commit = adapter.write_commit(&commit).await.unwrap();
        assert_eq!(written_commit.version, 1);
        assert_eq!(written_commit.operation_type, "WRITE");

        // Test read_commit
        let read_commit = adapter.read_commit(table_id, 1).await.unwrap();
        assert!(read_commit.is_some());
        assert_eq!(read_commit.unwrap().version, 1);

        // Test read_latest_commit
        let latest_commit = adapter.read_latest_commit(table_id).await.unwrap();
        assert!(latest_commit.is_some());
        assert_eq!(latest_commit.unwrap().version, 1);

        // Test get_table_version
        let table_version = adapter.get_table_version(table_id).await.unwrap();
        assert_eq!(table_version, Some(1));

        // Test count_commits
        let commit_count = adapter.count_commits(table_id).await.unwrap();
        assert_eq!(commit_count, 1);

        adapter.close().await.unwrap();
    }
}