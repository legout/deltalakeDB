use std::collections::HashMap;
use std::time::Instant;

use chrono::{DateTime, Utc};
use deltalakedb_core::txn_log::{RemovedFile, Version};
use deltalakedb_observability as obs;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use tracing::{info_span, trace};
use uuid::Uuid;

use crate::error::MirrorError;
use crate::json::JsonCommitSerializer;
use crate::object_store::ObjectStore;
use deltalakedb_core::delta::{
    AddPayload, CommitInfo, DeltaAction, MetaDataPayload, ProtocolPayload, RemovePayload,
};

/// Result of processing a single pending mirror job.
#[derive(Debug, Clone)]
pub struct MirrorOutcome {
    /// Table identifier.
    pub table_id: Uuid,
    /// Version that was mirrored.
    pub version: Version,
    /// Status after processing.
    pub status: MirrorStatus,
}

/// Final status for a processed job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorStatus {
    /// Payload written successfully.
    Succeeded,
    /// Payload write failed; job recorded as failed for retry.
    Failed,
}

/// Worker that mirrors committed actions to `_delta_log` JSON files.
#[derive(Clone)]
pub struct MirrorRunner<S>
where
    S: ObjectStore + Clone + 'static,
{
    pool: PgPool,
    store: S,
}

impl<S> MirrorRunner<S>
where
    S: ObjectStore + Send + Sync + Clone + 'static,
{
    /// Creates a new runner from the provided pool and object store implementation.
    pub fn new(pool: PgPool, store: S) -> Self {
        Self { pool, store }
    }

    /// Returns a clone of the underlying database pool.
    pub fn pool(&self) -> PgPool {
        self.pool.clone()
    }

    /// Provides a clone of the object store implementation.
    pub fn store(&self) -> S {
        self.store.clone()
    }

    /// Processes at most one pending mirror job.
    pub async fn process_next(&self) -> Result<Option<MirrorOutcome>, MirrorError> {
        let mut tx = self.pool.begin().await?;
        let Some(job) = self.fetch_next_job(&mut tx).await? else {
            return Ok(None);
        };

        let span = info_span!("mirror_job", table_id = %job.table_id, version = job.version);
        let _guard = span.enter();
        let start = Instant::now();
        let commit = self.load_commit(&mut tx, &job).await?;
        let actions = self.build_actions(&commit)?;
        let bytes = JsonCommitSerializer::serialize(&actions)?;
        let digest = hex::encode(Sha256::digest(&bytes));
        let file_name = format!("{:020}.json", job.version);

        let result = {
            let write_span =
                info_span!("object_store_write", table_id = %job.table_id, version = job.version);
            let _write_guard = write_span.enter();
            self.store.put_file(&job.location, &file_name, &bytes).await
        };

        let outcome = match result {
            Ok(()) => {
                self.mark_succeeded(&mut tx, &job, &digest).await?;
                obs::record_mirror_latency(job.table_id, job.version, start.elapsed());
                MirrorOutcome {
                    table_id: job.table_id,
                    version: job.version,
                    status: MirrorStatus::Succeeded,
                }
            }
            Err(err) => {
                let msg = err.to_string();
                self.mark_failed(&mut tx, &job, &msg).await?;
                obs::record_mirror_failure(job.table_id, job.version, &msg);
                MirrorOutcome {
                    table_id: job.table_id,
                    version: job.version,
                    status: MirrorStatus::Failed,
                }
            }
        };

        tx.commit().await?;
        Ok(Some(outcome))
    }

    async fn fetch_next_job(
        &self,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<Option<PendingJob>, MirrorError> {
        let row = sqlx::query(
            r#"
            SELECT ms.table_id, ms.version, t.location
            FROM dl_mirror_status ms
            JOIN dl_tables t ON t.table_id = ms.table_id
            WHERE ms.status IN ('PENDING','FAILED')
              AND NOT EXISTS (
                    SELECT 1 FROM dl_mirror_status blocking
                    WHERE blocking.table_id = ms.table_id
                      AND blocking.version < ms.version
                      AND blocking.status <> 'SUCCEEDED'
                )
            ORDER BY ms.updated_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            "#,
        )
        .fetch_optional(tx.as_mut())
        .await?;

        Ok(row.map(|row| PendingJob {
            table_id: row.get("table_id"),
            version: row.get::<i64, _>("version"),
            location: row.get("location"),
        }))
    }

    async fn load_commit(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        job: &PendingJob,
    ) -> Result<CommitData, MirrorError> {
        let commit_row = sqlx::query(
            r#"
            SELECT committed_at, operation, operation_params
            FROM dl_table_versions
            WHERE table_id = $1 AND version = $2
            "#,
        )
        .bind(job.table_id)
        .bind(job.version)
        .fetch_optional(tx.as_mut())
        .await?;

        let Some(commit_row) = commit_row else {
            return Err(MirrorError::InvalidState(format!(
                "missing dl_table_versions row for table {}/version {}",
                job.table_id, job.version
            )));
        };

        let committed_at: DateTime<Utc> = commit_row.try_get("committed_at")?;
        let operation: String = commit_row.try_get("operation")?;
        let operation_params: Option<Value> = commit_row.try_get("operation_params")?;

        let protocol = sqlx::query(
            "SELECT min_reader_version, min_writer_version FROM dl_protocol_updates WHERE table_id = $1 AND version = $2",
        )
        .bind(job.table_id)
        .bind(job.version)
        .fetch_optional(tx.as_mut())
        .await?;

        let metadata = sqlx::query(
            r#"
            SELECT schema_json, partition_columns, table_properties
            FROM dl_metadata_updates
            WHERE table_id = $1 AND version = $2
            "#,
        )
        .bind(job.table_id)
        .bind(job.version)
        .fetch_optional(tx.as_mut())
        .await?;

        let add_rows = sqlx::query(
            r#"
            SELECT path, size_bytes, partition_values, stats, modification_time, data_change
            FROM dl_add_files
            WHERE table_id = $1 AND version = $2
            ORDER BY path
            "#,
        )
        .bind(job.table_id)
        .bind(job.version)
        .fetch_all(tx.as_mut())
        .await?;

        let remove_rows = sqlx::query(
            r#"
            SELECT path, deletion_timestamp, data_change
            FROM dl_remove_files
            WHERE table_id = $1 AND version = $2
            ORDER BY path
            "#,
        )
        .bind(job.table_id)
        .bind(job.version)
        .fetch_all(tx.as_mut())
        .await?;

        Ok(CommitData {
            committed_at,
            operation,
            operation_params,
            protocol,
            metadata,
            add_rows,
            remove_rows,
        })
    }

    fn build_actions(&self, data: &CommitData) -> Result<Vec<DeltaAction>, MirrorError> {
        let mut actions = Vec::new();
        let timestamp = data.committed_at.timestamp_millis();
        actions.push(DeltaAction::CommitInfo {
            commit_info: CommitInfo::new(
                data.operation.clone(),
                data.operation_params.clone(),
                timestamp,
            ),
        });

        if let Some(row) = &data.protocol {
            let payload = ProtocolPayload {
                min_reader_version: row.get::<i32, _>("min_reader_version") as u32,
                min_writer_version: row.get::<i32, _>("min_writer_version") as u32,
            };
            actions.push(DeltaAction::Protocol { protocol: payload });
        }

        if let Some(row) = &data.metadata {
            let schema_json: Value = row.try_get("schema_json")?;
            let partition_columns: Option<Vec<String>> = row.try_get("partition_columns")?;
            let configuration: Option<Value> = row.try_get("table_properties")?;
            let payload = MetaDataPayload {
                schema_string: schema_json.to_string(),
                partition_columns: partition_columns.unwrap_or_default(),
                configuration: value_to_string_map(configuration)?,
            };
            actions.push(DeltaAction::MetaData { meta_data: payload });
        }

        for row in &data.add_rows {
            let path: String = row.try_get("path")?;
            let size_bytes: i64 = row.try_get("size_bytes")?;
            let partition_values: Option<Value> = row.try_get("partition_values")?;
            let stats: Option<Value> = row.try_get("stats")?;
            let modification_time: Option<i64> = row.try_get("modification_time")?;
            let mut map = HashMap::new();
            for (key, value) in value_to_value_map(partition_values)? {
                map.insert(key, value);
            }
            let payload = AddPayload {
                path,
                size: size_bytes,
                partition_values: map,
                stats,
                modification_time: modification_time
                    .unwrap_or_else(|| data.committed_at.timestamp_millis()),
                data_change: row
                    .try_get::<Option<bool>, _>("data_change")?
                    .unwrap_or(true),
            };
            actions.push(DeltaAction::Add { add: payload });
        }

        for row in &data.remove_rows {
            let removed = RemovedFile {
                path: row.try_get("path")?,
                deletion_timestamp: row.try_get("deletion_timestamp")?,
            };
            let payload = RemovePayload::from_removed(&removed);
            actions.push(DeltaAction::Remove { remove: payload });
        }

        Ok(actions)
    }

    async fn mark_succeeded(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        job: &PendingJob,
        digest: &str,
    ) -> Result<(), MirrorError> {
        sqlx::query(
            r#"
            UPDATE dl_mirror_status
            SET status = 'SUCCEEDED',
                attempts = attempts + 1,
                last_error = NULL,
                digest = $3,
                updated_at = now()
            WHERE table_id = $1 AND version = $2
            "#,
        )
        .bind(job.table_id)
        .bind(job.version)
        .bind(digest)
        .execute(tx.as_mut())
        .await?;
        trace!(table_id = %job.table_id, version = job.version, "mirrored JSON commit");
        Ok(())
    }

    async fn mark_failed(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        job: &PendingJob,
        message: &str,
    ) -> Result<(), MirrorError> {
        sqlx::query(
            r#"
            UPDATE dl_mirror_status
            SET status = 'FAILED',
                attempts = attempts + 1,
                last_error = $3,
                updated_at = now()
            WHERE table_id = $1 AND version = $2
            "#,
        )
        .bind(job.table_id)
        .bind(job.version)
        .bind(truncate_error(message))
        .execute(tx.as_mut())
        .await?;
        trace!(table_id = %job.table_id, version = job.version, "mirror attempt failed" );
        Ok(())
    }
}

fn truncate_error(message: &str) -> String {
    const MAX: usize = 512;
    if message.len() <= MAX {
        message.to_string()
    } else {
        format!("{}…", &message[..MAX])
    }
}

fn value_to_value_map(value: Option<Value>) -> Result<HashMap<String, Value>, MirrorError> {
    let mut map = HashMap::new();
    match value {
        None => Ok(map),
        Some(Value::Object(obj)) => {
            for (key, value) in obj {
                map.insert(key, value);
            }
            Ok(map)
        }
        Some(other) => Err(MirrorError::InvalidState(format!(
            "expected JSON object for partition_values, got {other:?}"
        ))),
    }
}

fn value_to_string_map(value: Option<Value>) -> Result<HashMap<String, String>, MirrorError> {
    let mut map = HashMap::new();
    match value {
        None => Ok(map),
        Some(Value::Object(obj)) => {
            for (key, value) in obj {
                map.insert(key, value_to_string(value));
            }
            Ok(map)
        }
        Some(other) => Err(MirrorError::InvalidState(format!(
            "expected JSON object for table_properties, got {other:?}"
        ))),
    }
}

fn value_to_string(value: Value) -> String {
    match value {
        Value::String(s) => s,
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[derive(Clone)]
struct PendingJob {
    table_id: Uuid,
    version: Version,
    location: String,
}

struct CommitData {
    committed_at: DateTime<Utc>,
    operation: String,
    operation_params: Option<Value>,
    protocol: Option<PgRow>,
    metadata: Option<PgRow>,
    add_rows: Vec<PgRow>,
    remove_rows: Vec<PgRow>,
}
