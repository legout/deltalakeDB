//! Postgres-backed `TxnLogReader` implementation.

use chrono::{DateTime, Utc};
use deltalakedb_core::txn_log::{
    ActiveFile, Protocol, TableMetadata, TableSnapshot, TxnLogError, TxnLogReader, Version,
    INITIAL_VERSION,
};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tracing::{trace, warn};
use uuid::Uuid;

/// Connection tuning knobs for the Postgres reader.
#[derive(Debug, Clone)]
pub struct PostgresConnectionOptions {
    /// Maximum number of pooled connections.
    pub max_connections: u32,
    /// Timeout applied when establishing the initial pool.
    pub connect_timeout: Duration,
}

impl Default for PostgresConnectionOptions {
    fn default() -> Self {
        Self {
            max_connections: 5,
            connect_timeout: Duration::from_secs(5),
        }
    }
}

/// `TxnLogReader` implementation backed by Postgres metadata tables.
pub struct PostgresTxnLogReader {
    table_uri: PathBuf,
    table_id: Uuid,
    runtime: Arc<Runtime>,
    pool: PgPool,
}

impl fmt::Debug for PostgresTxnLogReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresTxnLogReader")
            .field("table_uri", &self.table_uri)
            .field("table_id", &self.table_id)
            .finish_non_exhaustive()
    }
}

impl Clone for PostgresTxnLogReader {
    fn clone(&self) -> Self {
        Self {
            table_uri: self.table_uri.clone(),
            table_id: self.table_id,
            runtime: Arc::clone(&self.runtime),
            pool: self.pool.clone(),
        }
    }
}

impl PostgresTxnLogReader {
    /// Creates a reader using default connection options.
    pub fn connect(
        table_uri: impl Into<PathBuf>,
        table_id: Uuid,
        dsn: impl Into<String>,
    ) -> Result<Self, TxnLogError> {
        Self::with_options(
            table_uri,
            table_id,
            dsn,
            PostgresConnectionOptions::default(),
        )
    }

    /// Creates a reader using the provided tuning options.
    pub fn with_options(
        table_uri: impl Into<PathBuf>,
        table_id: Uuid,
        dsn: impl Into<String>,
        options: PostgresConnectionOptions,
    ) -> Result<Self, TxnLogError> {
        let runtime = Arc::new(build_runtime()?);
        let dsn = dsn.into();
        let pool = runtime
            .block_on(
                PgPoolOptions::new()
                    .max_connections(options.max_connections)
                    .acquire_timeout(options.connect_timeout)
                    .connect(&dsn),
            )
            .map_err(map_db_error)?;

        Ok(Self {
            table_uri: table_uri.into(),
            table_id,
            runtime,
            pool,
        })
    }

    fn block_on<F, R>(&self, fut: F) -> R
    where
        F: Future<Output = R>,
    {
        self.runtime.block_on(fut)
    }

    async fn current_version_async(&self) -> Result<Version, TxnLogError> {
        let version = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT current_version FROM dl_table_heads WHERE table_id = $1",
        )
        .bind(self.table_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?;

        Ok(version.flatten().unwrap_or(INITIAL_VERSION))
    }

    async fn resolve_version(&self, version: Option<Version>) -> Result<Version, TxnLogError> {
        match version {
            Some(v) => Ok(v),
            None => self.current_version_async().await,
        }
    }

    async fn snapshot_for_version(&self, version: Version) -> Result<TableSnapshot, TxnLogError> {
        if version < 0 {
            return Err(TxnLogError::EmptyLog);
        }

        let commit = self
            .fetch_commit_metadata(version)
            .await?
            .ok_or(TxnLogError::MissingVersion(version))?;
        let metadata = self.fetch_metadata(version).await?;
        let protocol = self.fetch_protocol(version).await?;
        let properties = metadata.configuration.clone();
        let files = self.fetch_active_files(version).await?;

        Ok(TableSnapshot {
            version,
            timestamp_millis: commit.timestamp_millis,
            metadata,
            protocol,
            properties,
            files,
        })
    }

    async fn fetch_commit_metadata(
        &self,
        version: Version,
    ) -> Result<Option<CommitRow>, TxnLogError> {
        let row = sqlx::query(
            "SELECT committed_at FROM dl_table_versions WHERE table_id = $1 AND version = $2",
        )
        .bind(self.table_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?;

        row.map(|record| record.try_into()).transpose()
    }

    async fn fetch_metadata(&self, version: Version) -> Result<TableMetadata, TxnLogError> {
        let row = sqlx::query(
            r#"
            SELECT schema_json, partition_columns, table_properties
            FROM dl_metadata_updates
            WHERE table_id = $1 AND version <= $2
            ORDER BY version DESC
            LIMIT 1
            "#,
        )
        .bind(self.table_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?;

        let row = row.ok_or(TxnLogError::MissingMetadata)?;
        let schema_json: Value = row.try_get("schema_json").map_err(map_db_error)?;
        let partition_columns: Option<Vec<String>> =
            row.try_get("partition_columns").map_err(map_db_error)?;
        let properties_value: Option<Value> =
            row.try_get("table_properties").map_err(map_db_error)?;
        let configuration = json_object_to_map(properties_value)?;

        Ok(TableMetadata::new(
            schema_json.to_string(),
            partition_columns.unwrap_or_default(),
            configuration,
        ))
    }

    async fn fetch_protocol(&self, version: Version) -> Result<Protocol, TxnLogError> {
        let row = sqlx::query(
            r#"
            SELECT min_reader_version, min_writer_version
            FROM dl_protocol_updates
            WHERE table_id = $1 AND version <= $2
            ORDER BY version DESC
            LIMIT 1
            "#,
        )
        .bind(self.table_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?;

        let row = row.ok_or(TxnLogError::MissingProtocol)?;
        let min_reader: i32 = row.try_get("min_reader_version").map_err(map_db_error)?;
        let min_writer: i32 = row.try_get("min_writer_version").map_err(map_db_error)?;

        Ok(Protocol {
            min_reader_version: min_reader as u32,
            min_writer_version: min_writer as u32,
        })
    }

    async fn fetch_active_files(&self, version: Version) -> Result<Vec<ActiveFile>, TxnLogError> {
        let rows = sqlx::query(
            r#"
            WITH actions AS (
                SELECT path,
                       version,
                       TRUE AS is_add,
                       size_bytes,
                       partition_values,
                       modification_time
                FROM dl_add_files
                WHERE table_id = $1 AND version <= $2
                UNION ALL
                SELECT path,
                       version,
                       FALSE AS is_add,
                       NULL::BIGINT AS size_bytes,
                       NULL::JSONB AS partition_values,
                       NULL::BIGINT AS modification_time
                FROM dl_remove_files
                WHERE table_id = $1 AND version <= $2
            ), ranked AS (
                SELECT path,
                       size_bytes,
                       partition_values,
                       modification_time,
                       is_add,
                       ROW_NUMBER() OVER (PARTITION BY path ORDER BY version DESC) AS rn
                FROM actions
            )
            SELECT path, size_bytes, partition_values, modification_time
            FROM ranked
            WHERE rn = 1 AND is_add = TRUE
            ORDER BY path
            "#,
        )
        .bind(self.table_id)
        .bind(version)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;

        rows.into_iter()
            .map(|row| ActiveFileRow::try_from(row)?.into_active_file())
            .collect()
    }

    /// Builds a snapshot by resolving the timestamp first.
    async fn snapshot_by_timestamp_async(
        &self,
        timestamp_millis: i64,
    ) -> Result<TableSnapshot, TxnLogError> {
        let target = sqlx::query_scalar::<_, Option<i64>>(
            r#"
            WITH version_times AS (
                SELECT version,
                       FLOOR(EXTRACT(EPOCH FROM committed_at) * 1000)::BIGINT AS commit_ms
                FROM dl_table_versions
                WHERE table_id = $1
            )
            SELECT MAX(version) AS version
            FROM version_times
            WHERE commit_ms <= $2
            "#,
        )
        .bind(self.table_id)
        .bind(timestamp_millis)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?
        .flatten()
        .ok_or_else(|| {
            TxnLogError::Invalid("no commits exist at or before the requested timestamp".into())
        })?;

        self.snapshot_for_version(target).await
    }
}

impl TxnLogReader for PostgresTxnLogReader {
    fn table_uri(&self) -> &Path {
        &self.table_uri
    }

    fn current_version(&self) -> Result<Version, TxnLogError> {
        trace!(table_id = %self.table_id, "current_version request");
        self.block_on(self.current_version_async())
    }

    fn snapshot_at_version(&self, version: Option<Version>) -> Result<TableSnapshot, TxnLogError> {
        let version = self.block_on(self.resolve_version(version))?;
        trace!(table_id = %self.table_id, version, "building snapshot by version");
        self.block_on(self.snapshot_for_version(version))
    }

    fn snapshot_by_timestamp(&self, timestamp_millis: i64) -> Result<TableSnapshot, TxnLogError> {
        trace!(table_id = %self.table_id, timestamp_millis, "building snapshot by timestamp");
        self.block_on(self.snapshot_by_timestamp_async(timestamp_millis))
    }
}

fn build_runtime() -> Result<Runtime, TxnLogError> {
    match Builder::new_multi_thread().enable_all().build() {
        Ok(runtime) => Ok(runtime),
        Err(err) => {
            warn!("failed to build multi-threaded runtime, falling back: {err}");
            Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|fallback_err| {
                    TxnLogError::Invalid(format!(
                        "failed to build tokio runtime: {err}; fallback error: {fallback_err}"
                    ))
                })
        }
    }
}

fn map_db_error(err: sqlx::Error) -> TxnLogError {
    TxnLogError::Invalid(format!("postgres query failed: {err}"))
}

fn json_object_to_map(value: Option<Value>) -> Result<HashMap<String, String>, TxnLogError> {
    let mut map = HashMap::new();
    let Some(Value::Object(obj)) = value else {
        return Ok(map);
    };

    for (key, val) in obj {
        map.insert(key, json_value_to_string(val));
    }
    Ok(map)
}

fn json_value_to_string(value: Value) -> String {
    match value {
        Value::String(s) => s,
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[derive(Debug)]
struct CommitRow {
    timestamp_millis: i64,
}

impl TryFrom<sqlx::postgres::PgRow> for CommitRow {
    type Error = TxnLogError;

    fn try_from(row: sqlx::postgres::PgRow) -> Result<Self, Self::Error> {
        let committed_at: DateTime<Utc> = row.try_get("committed_at").map_err(map_db_error)?;
        Ok(Self {
            timestamp_millis: committed_at.timestamp_millis(),
        })
    }
}

#[derive(Debug)]
struct ActiveFileRow {
    path: String,
    size_bytes: Option<i64>,
    partition_values: Option<Value>,
    modification_time: Option<i64>,
}

impl TryFrom<sqlx::postgres::PgRow> for ActiveFileRow {
    type Error = TxnLogError;

    fn try_from(row: sqlx::postgres::PgRow) -> Result<Self, Self::Error> {
        Ok(Self {
            path: row.try_get("path").map_err(map_db_error)?,
            size_bytes: row.try_get("size_bytes").map_err(map_db_error)?,
            partition_values: row.try_get("partition_values").map_err(map_db_error)?,
            modification_time: row.try_get("modification_time").map_err(map_db_error)?,
        })
    }
}

impl ActiveFileRow {
    fn into_active_file(self) -> Result<ActiveFile, TxnLogError> {
        Ok(ActiveFile {
            path: self.path,
            size_bytes: self
                .size_bytes
                .map(|value| {
                    value
                        .try_into()
                        .map_err(|_| TxnLogError::Invalid("size_bytes must be non-negative".into()))
                })
                .transpose()?
                .unwrap_or_default(),
            modification_time: self.modification_time.unwrap_or_default(),
            partition_values: json_object_to_map(self.partition_values)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_json_objects_to_map() {
        let value = serde_json::json!({
            "a": "1",
            "b": 2,
            "c": true,
            "d": null,
        });
        let map = json_object_to_map(Some(value)).expect("map");
        assert_eq!(map.get("a"), Some(&"1".to_string()));
        assert_eq!(map.get("b"), Some(&"2".to_string()));
        assert_eq!(map.get("c"), Some(&"true".to_string()));
        assert_eq!(map.get("d"), Some(&"".to_string()));
    }

    #[test]
    fn empty_json_yields_empty_map() {
        let map = json_object_to_map(None).expect("map");
        assert!(map.is_empty());
    }
}
