//! SQLite-backed implementation of `TxnLogReader`.

use chrono::{DateTime, Utc};
use deltalakedb_core::txn_log::{
    ActiveFile, Protocol, TableMetadata, TableSnapshot, TxnLogError, TxnLogReader, Version,
    INITIAL_VERSION,
};
use serde_json::Value;
use sqlx::{sqlite::SqlitePoolOptions, Row, SqlitePool};
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tracing::trace;
use uuid::Uuid;

/// SQLite transaction log reader.
pub struct SqliteTxnLogReader {
    table_uri: PathBuf,
    table_id: Uuid,
    runtime: Arc<Runtime>,
    pool: SqlitePool,
}

impl SqliteTxnLogReader {
    /// Connects to a SQLite database using the provided DSN (e.g. `sqlite:///tmp/meta.db`).
    pub fn connect(
        table_uri: impl Into<PathBuf>,
        table_id: Uuid,
        dsn: impl AsRef<str>,
    ) -> Result<Self, TxnLogError> {
        let runtime = Arc::new(build_runtime()?);
        let pool = runtime
            .block_on(
                SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect(dsn.as_ref()),
            )
            .map_err(sqlite_err)?;

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
        let version: Option<Option<i64>> =
            sqlx::query_scalar("SELECT MAX(version) FROM dl_table_versions WHERE table_id = ?")
                .bind(self.table_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(sqlite_err)?;
        Ok(version.flatten().unwrap_or(INITIAL_VERSION))
    }

    async fn resolve_version(&self, version: Option<Version>) -> Result<Version, TxnLogError> {
        match version {
            Some(value) => Ok(value),
            None => self.current_version_async().await,
        }
    }

    async fn snapshot_at(&self, version: Version) -> Result<TableSnapshot, TxnLogError> {
        if version < 0 {
            return Err(TxnLogError::EmptyLog);
        }

        let row = sqlx::query(
            "SELECT committed_at FROM dl_table_versions WHERE table_id = ? AND version = ?",
        )
        .bind(self.table_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await
        .map_err(sqlite_err)?
        .ok_or(TxnLogError::MissingVersion(version))?;
        let committed_at: String = row.try_get("committed_at").map_err(sqlite_err)?;
        let committed_at = DateTime::parse_from_rfc3339(&committed_at)
            .map_err(|err| TxnLogError::Invalid(err.to_string()))?
            .with_timezone(&Utc);

        let metadata = self.fetch_metadata(version).await?;
        let properties = metadata.configuration.clone();
        let protocol = self.fetch_protocol(version).await?;
        let files = self.fetch_active_files(version).await?;

        Ok(TableSnapshot {
            version,
            timestamp_millis: committed_at.timestamp_millis(),
            metadata,
            protocol,
            properties,
            files,
        })
    }

    async fn fetch_metadata(&self, version: Version) -> Result<TableMetadata, TxnLogError> {
        let row = sqlx::query(
            r#"
            SELECT schema_json, partition_columns, table_properties
            FROM dl_metadata_updates
            WHERE table_id = ? AND version <= ?
            ORDER BY version DESC
            LIMIT 1
            "#,
        )
        .bind(self.table_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await
        .map_err(sqlite_err)?
        .ok_or(TxnLogError::MissingMetadata)?;

        let schema_json: String = row.try_get("schema_json").map_err(sqlite_err)?;
        let partition_columns: Option<String> =
            row.try_get("partition_columns").map_err(sqlite_err)?;
        let table_properties: Option<String> =
            row.try_get("table_properties").map_err(sqlite_err)?;

        let schema_value: Value = serde_json::from_str(&schema_json)?;
        let partitions = partition_columns
            .map(|value| serde_json::from_str::<Vec<String>>(&value))
            .transpose()?;
        let configuration = table_properties
            .map(|value| serde_json::from_str::<HashMap<String, String>>(&value))
            .transpose()?;

        Ok(TableMetadata::new(
            schema_value.to_string(),
            partitions.unwrap_or_default(),
            configuration.unwrap_or_default(),
        ))
    }

    async fn fetch_protocol(&self, version: Version) -> Result<Protocol, TxnLogError> {
        let row = sqlx::query(
            r#"
            SELECT min_reader_version, min_writer_version
            FROM dl_protocol_updates
            WHERE table_id = ? AND version <= ?
            ORDER BY version DESC
            LIMIT 1
            "#,
        )
        .bind(self.table_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await
        .map_err(sqlite_err)?
        .ok_or(TxnLogError::MissingProtocol)?;

        let min_reader_version: i64 = row.try_get("min_reader_version").map_err(sqlite_err)?;
        let min_writer_version: i64 = row.try_get("min_writer_version").map_err(sqlite_err)?;

        Ok(Protocol {
            min_reader_version: min_reader_version as u32,
            min_writer_version: min_writer_version as u32,
        })
    }

    async fn fetch_active_files(&self, version: Version) -> Result<Vec<ActiveFile>, TxnLogError> {
        let rows = sqlx::query(
            r#"
            WITH actions AS (
                SELECT path,
                       version,
                       1 AS is_add,
                       size_bytes,
                       partition_values,
                       modification_time
                FROM dl_add_files
                WHERE table_id = ? AND version <= ?
                UNION ALL
                SELECT path,
                       version,
                       0 AS is_add,
                       NULL AS size_bytes,
                       NULL AS partition_values,
                       NULL AS modification_time
                FROM dl_remove_files
                WHERE table_id = ? AND version <= ?
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
            WHERE rn = 1 AND is_add = 1
            ORDER BY path
            "#,
        )
        .bind(self.table_id)
        .bind(version)
        .bind(self.table_id)
        .bind(version)
        .fetch_all(&self.pool)
        .await
        .map_err(sqlite_err)?;

        let mut files = Vec::new();
        for row in rows {
            let path: String = row.try_get("path").map_err(sqlite_err)?;
            let size_bytes: Option<i64> = row.try_get("size_bytes").map_err(sqlite_err)?;
            let partition_values: Option<String> =
                row.try_get("partition_values").map_err(sqlite_err)?;
            let modification_time: Option<i64> =
                row.try_get("modification_time").map_err(sqlite_err)?;
            let size_bytes = size_bytes.unwrap_or_default() as u64;
            let partitions = partition_values
                .map(|value| serde_json::from_str::<HashMap<String, String>>(&value))
                .transpose()?;
            files.push(ActiveFile {
                path,
                size_bytes,
                modification_time: modification_time.unwrap_or_default(),
                partition_values: partitions.unwrap_or_default(),
            });
        }

        Ok(files)
    }

    async fn snapshot_by_timestamp_async(
        &self,
        timestamp_millis: i64,
    ) -> Result<TableSnapshot, TxnLogError> {
        let timestamp = DateTime::from_timestamp_millis(timestamp_millis)
            .ok_or_else(|| TxnLogError::Invalid("invalid timestamp".into()))?;
        let iso = timestamp.to_rfc3339();
        let version: Option<Option<i64>> = sqlx::query_scalar(
            r#"
            SELECT MAX(version)
            FROM dl_table_versions
            WHERE table_id = ? AND committed_at <= ?
            "#,
        )
        .bind(self.table_id)
        .bind(iso)
        .fetch_optional(&self.pool)
        .await
        .map_err(sqlite_err)?;

        let target = version.flatten().ok_or_else(|| {
            TxnLogError::Invalid("no commits exist at or before the requested timestamp".into())
        })?;

        self.snapshot_at(target).await
    }
}

impl TxnLogReader for SqliteTxnLogReader {
    fn table_uri(&self) -> &Path {
        &self.table_uri
    }

    fn current_version(&self) -> Result<Version, TxnLogError> {
        self.block_on(self.current_version_async())
    }

    fn snapshot_at_version(&self, version: Option<Version>) -> Result<TableSnapshot, TxnLogError> {
        let version = self.block_on(self.resolve_version(version))?;
        trace!(table_id = %self.table_id, version, "building sqlite snapshot");
        self.block_on(self.snapshot_at(version))
    }

    fn snapshot_by_timestamp(&self, timestamp_millis: i64) -> Result<TableSnapshot, TxnLogError> {
        trace!(table_id = %self.table_id, timestamp_millis, "sqlite snapshot by timestamp");
        self.block_on(self.snapshot_by_timestamp_async(timestamp_millis))
    }
}

fn build_runtime() -> Result<Runtime, TxnLogError> {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| TxnLogError::Invalid(format!("failed to build tokio runtime: {err}")))
}

fn sqlite_err(err: sqlx::Error) -> TxnLogError {
    TxnLogError::Invalid(format!("sqlite query failed: {err}"))
}
