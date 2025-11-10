use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use tokio::time::sleep;
use tracing::{debug, warn};
use uuid::Uuid;

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
        }
    }

    /// Sets the polling interval between reconciliation attempts.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
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
                MirrorStatus::Succeeded => debug!(
                    table_id = %outcome.table_id,
                    version = outcome.version,
                    "mirror succeeded"
                ),
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
