//! DuckDB-backed implementation of `TxnLogReader`.

use chrono::{DateTime, Utc};
use deltalakedb_core::txn_log::{
    ActiveFile, Protocol, TableMetadata, TableSnapshot, TxnLogError, TxnLogReader, Version,
    INITIAL_VERSION,
};
use duckdb::{params, Connection};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// DuckDB transaction log reader.
pub struct DuckdbTxnLogReader {
    table_uri: PathBuf,
    table_id: Uuid,
    conn: Connection,
}

impl DuckdbTxnLogReader {
    /// Connects to a DuckDB catalog at the provided file path.
    pub fn connect(
        table_uri: impl Into<PathBuf>,
        table_id: Uuid,
        db_path: impl Into<PathBuf>,
    ) -> Result<Self, TxnLogError> {
        let db_path = db_path.into();
        let conn = Connection::open(&db_path).map_err(duckdb_err)?;
        Ok(Self {
            table_uri: table_uri.into(),
            table_id,
            conn,
        })
    }

    fn current_version_internal(&self) -> Result<Version, TxnLogError> {
        let mut stmt = self
            .conn
            .prepare("SELECT MAX(version) FROM dl_table_versions WHERE table_id = ?")
            .map_err(duckdb_err)?;
        let mut rows = stmt
            .query(params![self.table_id.to_string()])
            .map_err(duckdb_err)?;
        let value: Option<Option<i64>> = rows
            .next()
            .map_err(duckdb_err)?
            .map(|row| row.get(0))
            .transpose()
            .map_err(duckdb_err)?;
        Ok(value.flatten().unwrap_or(INITIAL_VERSION))
    }

    fn resolve_version(&self, version: Option<Version>) -> Result<Version, TxnLogError> {
        match version {
            Some(value) => Ok(value),
            None => self.current_version_internal(),
        }
    }

    fn snapshot_at(&self, version: Version) -> Result<TableSnapshot, TxnLogError> {
        if version < 0 {
            return Err(TxnLogError::EmptyLog);
        }

        let committed_at = self.fetch_commit_timestamp(version)?;
        let metadata = self.fetch_metadata(version)?;
        let properties = metadata.configuration.clone();
        let protocol = self.fetch_protocol(version)?;
        let files = self.fetch_active_files(version)?;

        Ok(TableSnapshot {
            version,
            timestamp_millis: committed_at.timestamp_millis(),
            metadata,
            protocol,
            properties,
            files,
        })
    }

    fn fetch_commit_timestamp(&self, version: Version) -> Result<DateTime<Utc>, TxnLogError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT strftime(committed_at, '%Y-%m-%dT%H:%M:%S.%fZ') AS committed_at \
                 FROM dl_table_versions WHERE table_id = ? AND version = ?",
            )
            .map_err(duckdb_err)?;
        let mut rows = stmt
            .query(params![self.table_id.to_string(), version])
            .map_err(duckdb_err)?;
        let row = rows
            .next()
            .map_err(duckdb_err)?
            .ok_or(TxnLogError::MissingVersion(version))?;
        let committed_at: String = row.get(0).map_err(duckdb_err)?;
        let parsed = DateTime::parse_from_rfc3339(&committed_at)
            .map_err(|err| TxnLogError::Invalid(err.to_string()))?
            .with_timezone(&Utc);
        Ok(parsed)
    }

    fn fetch_metadata(&self, version: Version) -> Result<TableMetadata, TxnLogError> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                    CAST(schema_json AS VARCHAR) AS schema_json,
                    CASE
                        WHEN partition_columns IS NULL THEN NULL
                        ELSE to_json(partition_columns)
                    END AS partition_columns_json,
                    CASE
                        WHEN table_properties IS NULL THEN NULL
                        ELSE CAST(table_properties AS VARCHAR)
                    END AS table_properties
                FROM dl_metadata_updates
                WHERE table_id = ? AND version <= ?
                ORDER BY version DESC
                LIMIT 1
                "#,
            )
            .map_err(duckdb_err)?;
        let mut rows = stmt
            .query(params![self.table_id.to_string(), version])
            .map_err(duckdb_err)?;
        let row = rows
            .next()
            .map_err(duckdb_err)?
            .ok_or(TxnLogError::MissingMetadata)?;

        let schema_json: String = row.get(0).map_err(duckdb_err)?;
        let partition_columns_json: Option<String> = row.get(1).map_err(duckdb_err)?;
        let table_properties_json: Option<String> = row.get(2).map_err(duckdb_err)?;

        let schema_value: Value = serde_json::from_str(&schema_json)?;
        let partition_columns = partition_columns_json
            .map(|value| serde_json::from_str::<Vec<String>>(&value))
            .transpose()?;
        let configuration = table_properties_json
            .map(|value| serde_json::from_str::<HashMap<String, String>>(&value))
            .transpose()?;

        Ok(TableMetadata::new(
            schema_value.to_string(),
            partition_columns.unwrap_or_default(),
            configuration.unwrap_or_default(),
        ))
    }

    fn fetch_protocol(&self, version: Version) -> Result<Protocol, TxnLogError> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT min_reader_version, min_writer_version
                FROM dl_protocol_updates
                WHERE table_id = ? AND version <= ?
                ORDER BY version DESC
                LIMIT 1
                "#,
            )
            .map_err(duckdb_err)?;
        let mut rows = stmt
            .query(params![self.table_id.to_string(), version])
            .map_err(duckdb_err)?;
        let row = rows
            .next()
            .map_err(duckdb_err)?
            .ok_or(TxnLogError::MissingProtocol)?;
        let min_reader_version: i64 = row.get(0).map_err(duckdb_err)?;
        let min_writer_version: i64 = row.get(1).map_err(duckdb_err)?;

        Ok(Protocol {
            min_reader_version: min_reader_version as u32,
            min_writer_version: min_writer_version as u32,
        })
    }

    fn fetch_active_files(&self, version: Version) -> Result<Vec<ActiveFile>, TxnLogError> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                WITH actions AS (
                SELECT path,
                       version,
                       TRUE AS is_add,
                       size_bytes,
                       CAST(partition_values AS VARCHAR) AS partition_values,
                       modification_time
                FROM dl_add_files
                WHERE table_id = ? AND version <= ?
                UNION ALL
                SELECT path,
                       version,
                       FALSE AS is_add,
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
                SELECT path,
                       size_bytes,
                       partition_values,
                       modification_time
                FROM ranked
                WHERE rn = 1 AND is_add
                ORDER BY path
                "#,
            )
            .map_err(duckdb_err)?;
        let mut rows = stmt
            .query(params![
                self.table_id.to_string(),
                version,
                self.table_id.to_string(),
                version
            ])
            .map_err(duckdb_err)?;

        let mut files = Vec::new();
        while let Some(row) = rows.next().map_err(duckdb_err)? {
            let path: String = row.get(0).map_err(duckdb_err)?;
            let size_bytes: Option<i64> = row.get(1).map_err(duckdb_err)?;
            let partition_values: Option<String> = row.get(2).map_err(duckdb_err)?;
            let modification_time: Option<i64> = row.get(3).map_err(duckdb_err)?;
            let partitions = partition_values
                .map(|value| serde_json::from_str::<HashMap<String, String>>(&value))
                .transpose()?;
            files.push(ActiveFile {
                path,
                size_bytes: size_bytes.unwrap_or_default() as u64,
                modification_time: modification_time.unwrap_or_default(),
                partition_values: partitions.unwrap_or_default(),
            });
        }

        Ok(files)
    }

    fn snapshot_by_timestamp_internal(
        &self,
        timestamp_millis: i64,
    ) -> Result<TableSnapshot, TxnLogError> {
        let timestamp = DateTime::from_timestamp_millis(timestamp_millis)
            .ok_or_else(|| TxnLogError::Invalid("invalid timestamp".into()))?;
        let iso = timestamp.to_rfc3339();
        let mut stmt = self
            .conn
            .prepare(
                "SELECT MAX(version) FROM dl_table_versions \
                 WHERE table_id = ? AND committed_at <= CAST(? AS TIMESTAMP)",
            )
            .map_err(duckdb_err)?;
        let mut rows = stmt
            .query(params![self.table_id.to_string(), iso])
            .map_err(duckdb_err)?;
        let target: Option<Option<i64>> = rows
            .next()
            .map_err(duckdb_err)?
            .map(|row| row.get(0))
            .transpose()
            .map_err(duckdb_err)?;
        let version = target.flatten().ok_or_else(|| {
            TxnLogError::Invalid("no commits exist at or before the requested timestamp".into())
        })?;
        self.snapshot_at(version)
    }
}

impl TxnLogReader for DuckdbTxnLogReader {
    fn table_uri(&self) -> &Path {
        &self.table_uri
    }

    fn current_version(&self) -> Result<Version, TxnLogError> {
        self.current_version_internal()
    }

    fn snapshot_at_version(&self, version: Option<Version>) -> Result<TableSnapshot, TxnLogError> {
        let version = self.resolve_version(version)?;
        self.snapshot_at(version)
    }

    fn snapshot_by_timestamp(&self, timestamp_millis: i64) -> Result<TableSnapshot, TxnLogError> {
        self.snapshot_by_timestamp_internal(timestamp_millis)
    }
}

fn duckdb_err(err: duckdb::Error) -> TxnLogError {
    TxnLogError::Invalid(format!("duckdb query failed: {err}"))
}
