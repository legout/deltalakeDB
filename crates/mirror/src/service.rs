use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use deltalakedb_catalog::{
    active_files_query, latest_metadata_query, latest_protocol_query, CatalogParam, Dialect,
};
use deltalakedb_core::delta::json_value_to_string;
use deltalakedb_core::txn_log::{ActiveFile, Protocol, TableMetadata};
use deltalakedb_observability as obs;
use serde_json::Value;
use sqlx::{PgPool, Row};
use tokio::time::sleep;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::checkpoint::CheckpointSerializer;
use crate::error::MirrorError;
use crate::worker::{MirrorRunner, MirrorStatus};

/// Severity for lag alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LagSeverity {
    /// Lag exceeded warning threshold.
    Warning,
    /// Lag exceeded critical threshold.
    Critical,
}

/// Lag alert emitted when `_delta_log` mirroring falls behind.
#[derive(Debug, Clone)]
pub struct LagAlert {
    /// Table identifier.
    pub table_id: Uuid,
    /// Version still pending mirroring.
    pub version: i64,
    /// Computed lag in seconds.
    pub lag_seconds: i64,
    /// Alert severity.
    pub severity: LagSeverity,
}

/// Sink that receives lag alerts.
pub trait AlertSink: Send + Sync + 'static {
    /// Emits a new alert.
    fn emit(&self, alert: LagAlert);
}

/// Alert sink that logs via `tracing` macros.
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub struct LoggingAlertSink;

impl AlertSink for LoggingAlertSink {
    fn emit(&self, alert: LagAlert) {
        warn!(
            table_id = %alert.table_id,
            version = alert.version,
            lag_seconds = alert.lag_seconds,
            severity = ?alert.severity,
            "mirror lag alert"
        );
    }
}

/// Background service that continuously reconciles mirror backlog and checks lag.
pub struct MirrorService<S, A>
where
    S: crate::ObjectStore + Send + Sync + Clone + 'static,
    A: AlertSink,
{
    runner: Arc<MirrorRunner<S>>,
    pool: PgPool,
    alert_sink: A,
    warn_threshold_secs: i64,
    crit_threshold_secs: i64,
    poll_interval: Duration,
    checkpoint_interval: i64,
}

impl<S, A> MirrorService<S, A>
where
    S: crate::ObjectStore + Send + Sync + Clone + 'static,
    A: AlertSink,
{
    /// Creates a new service with default thresholds (warn=60s, crit=300s, poll interval 1s).
    pub fn new(runner: MirrorRunner<S>, alert_sink: A) -> Self {
        let pool = runner.pool();
        Self {
            runner: Arc::new(runner),
            pool,
            alert_sink,
            warn_threshold_secs: 60,
            crit_threshold_secs: 300,
            poll_interval: Duration::from_secs(1),
            checkpoint_interval: 10,
        }
    }

    /// Sets the polling interval between reconciliation attempts.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Sets the checkpoint interval in versions (default 10).
    pub fn with_checkpoint_interval(mut self, interval: i64) -> Self {
        self.checkpoint_interval = interval.max(1);
        self
    }

    /// Sets custom lag thresholds (warning, critical).
    pub fn with_lag_thresholds(mut self, warn_secs: i64, crit_secs: i64) -> Self {
        self.warn_threshold_secs = warn_secs;
        self.crit_threshold_secs = crit_secs.max(warn_secs);
        self
    }

    /// Runs reconciliation until cancelled.
    pub async fn run_forever(&self) -> Result<(), MirrorError> {
        loop {
            self.run_once().await?;
            sleep(self.poll_interval).await;
        }
    }

    /// Executes a single reconciliation pass and lag check.
    pub async fn run_once(&self) -> Result<(), MirrorError> {
        if let Some(outcome) = self.runner.process_next().await? {
            match outcome.status {
                MirrorStatus::Succeeded => {
                    debug!(
                        table_id = %outcome.table_id,
                        version = outcome.version,
                        "mirror succeeded"
                    );
                    self.maybe_checkpoint(outcome.table_id, outcome.version)
                        .await?;
                }
                MirrorStatus::Failed => warn!(
                    table_id = %outcome.table_id,
                    version = outcome.version,
                    "mirror failed; will retry"
                ),
            }
        }
        let alerts = self.check_lag().await?;
        for alert in alerts {
            self.alert_sink.emit(alert);
        }
        Ok(())
    }

    async fn maybe_checkpoint(&self, table_id: Uuid, version: i64) -> Result<(), MirrorError> {
        if self.checkpoint_interval <= 0 || version < 0 {
            return Ok(());
        }
        if (version % self.checkpoint_interval) != 0 {
            return Ok(());
        }

        let location: Option<String> =
            sqlx::query_scalar("SELECT location FROM dl_tables WHERE table_id = $1")
                .bind(table_id)
                .fetch_optional(&self.pool)
                .await?;

        let Some(location) = location else {
            return Err(MirrorError::InvalidState(format!(
                "table {table_id} missing location"
            )));
        };

        let snapshot = load_snapshot(&self.pool, table_id, version).await?;
        let bytes = CheckpointSerializer::serialize(
            &snapshot.protocol,
            &snapshot.metadata,
            &snapshot.files,
            &snapshot.properties,
        )?;
        let file_name = format!("{:020}.checkpoint.parquet", version);
        self.runner
            .store()
            .put_file(&location, &file_name, &bytes)
            .await?;
        Ok(())
    }

    /// Checks lag for outstanding mirrors and returns alerts exceeding thresholds.
    pub async fn check_lag(&self) -> Result<Vec<LagAlert>, MirrorError> {
        let rows = sqlx::query(
            r#"
            SELECT ms.table_id,
                   ms.version,
                   tv.committed_at
            FROM dl_mirror_status ms
            JOIN dl_table_versions tv
              ON tv.table_id = ms.table_id AND tv.version = ms.version
            WHERE ms.status IN ('PENDING','FAILED')
            "#,
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| LagRow {
            table_id: row.get("table_id"),
            version: row.get("version"),
            committed_at: row.get("committed_at"),
        })
        .collect::<Vec<_>>();

        obs::set_mirror_backlog(rows.len() as u64);
        let now = Utc::now();
        let mut alerts = Vec::new();
        for row in rows {
            let lag_seconds = (now - row.committed_at).num_seconds();
            let severity = if lag_seconds >= self.crit_threshold_secs {
                Some(LagSeverity::Critical)
            } else if lag_seconds >= self.warn_threshold_secs {
                Some(LagSeverity::Warning)
            } else {
                None
            };
            if let Some(severity) = severity {
                alerts.push(LagAlert {
                    table_id: row.table_id,
                    version: row.version,
                    lag_seconds,
                    severity,
                });
            }
        }
        Ok(alerts)
    }
}

struct LagRow {
    table_id: Uuid,
    version: i64,
    committed_at: DateTime<Utc>,
}

struct SnapshotData {
    metadata: TableMetadata,
    protocol: Protocol,
    files: Vec<ActiveFile>,
    properties: HashMap<String, String>,
}

async fn load_snapshot(
    pool: &PgPool,
    table_id: Uuid,
    version: i64,
) -> Result<SnapshotData, MirrorError> {
    let metadata_q = latest_metadata_query(Dialect::Postgres);
    let mut metadata_query = sqlx::query(metadata_q.sql);
    for p in metadata_q.params {
        metadata_query = match p {
            CatalogParam::TableId => metadata_query.bind(table_id),
            CatalogParam::Version => metadata_query.bind(version),
        };
    }
    let metadata_row = metadata_query.fetch_optional(pool).await?;

    let metadata_row = metadata_row.ok_or_else(|| {
        MirrorError::InvalidState(format!(
            "missing metadata for table {table_id} at version {version}"
        ))
    })?;

    let schema_json: Value = metadata_row.try_get("schema_json")?;
    let partition_columns: Option<Vec<String>> = metadata_row.try_get("partition_columns")?;
    let properties_json: Option<Value> = metadata_row.try_get("table_properties")?;
    let configuration = value_to_string_map(properties_json)?;

    let metadata = TableMetadata::new(
        schema_json.to_string(),
        partition_columns.unwrap_or_default(),
        configuration.clone(),
    );

    let protocol_q = latest_protocol_query(Dialect::Postgres);
    let mut protocol_query = sqlx::query(protocol_q.sql);
    for p in protocol_q.params {
        protocol_query = match p {
            CatalogParam::TableId => protocol_query.bind(table_id),
            CatalogParam::Version => protocol_query.bind(version),
        };
    }
    let protocol_row = protocol_query.fetch_optional(pool).await?;

    let protocol_row = protocol_row.ok_or_else(|| {
        MirrorError::InvalidState(format!(
            "missing protocol for table {table_id} at version {version}"
        ))
    })?;

    let protocol = Protocol {
        min_reader_version: protocol_row.try_get::<i32, _>("min_reader_version")? as u32,
        min_writer_version: protocol_row.try_get::<i32, _>("min_writer_version")? as u32,
    };

    let files = load_active_files(pool, table_id, version).await?;

    Ok(SnapshotData {
        metadata,
        protocol,
        files,
        properties: configuration,
    })
}

async fn load_active_files(
    pool: &PgPool,
    table_id: Uuid,
    version: i64,
) -> Result<Vec<ActiveFile>, MirrorError> {
    let files_q = active_files_query(Dialect::Postgres);
    let mut files_query = sqlx::query(files_q.sql);
    for p in files_q.params {
        files_query = match p {
            CatalogParam::TableId => files_query.bind(table_id),
            CatalogParam::Version => files_query.bind(version),
        };
    }
    let rows = files_query.fetch_all(pool).await?;

    let mut files = Vec::new();
    for row in rows {
        let path: String = row.try_get("path")?;
        let size_bytes: i64 = row.try_get("size_bytes")?;
        let partition_values: Option<Value> = row.try_get("partition_values")?;
        let modification_time: Option<i64> = row.try_get("modification_time")?;
        let partitions = value_to_value_map(partition_values)?
            .into_iter()
            .map(|(k, v)| (k, json_value_to_string(v)))
            .collect();
        files.push(ActiveFile {
            path,
            size_bytes: size_bytes as u64,
            modification_time: modification_time.unwrap_or(0),
            partition_values: partitions,
        });
    }

    Ok(files)
}

fn value_to_string_map(value: Option<Value>) -> Result<HashMap<String, String>, MirrorError> {
    let mut map = HashMap::new();
    match value {
        None => Ok(map),
        Some(Value::Object(obj)) => {
            for (key, value) in obj {
                map.insert(key, json_value_to_string(value));
            }
            Ok(map)
        }
        Some(other) => Err(MirrorError::InvalidState(format!(
            "expected JSON object, got {other:?}"
        ))),
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
            "expected JSON object, got {other:?}"
        ))),
    }
}
