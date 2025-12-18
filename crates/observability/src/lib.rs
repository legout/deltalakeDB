use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::{info, warn};
use uuid::Uuid;

static VERSIONS_COMMITTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static MIRROR_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static MIRROR_BACKLOG: AtomicU64 = AtomicU64::new(0);

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

/// Records metadata/read latency as a structured log entry.
pub fn record_metadata_latency(table_id: Uuid, duration: Duration) {
    info!(
        metric = "metadata_open_latency_ms",
        table_id = %table_id,
        latency_ms = duration_ms(duration)
    );
}

/// Records commit latency and increments the versions committed counter.
pub fn record_commit_latency(table_id: Uuid, duration: Duration, action_count: usize) {
    let total = VERSIONS_COMMITTED_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
    info!(
        metric = "commit_latency_ms",
        table_id = %table_id,
        latency_ms = duration_ms(duration),
        actions = action_count,
        versions_committed_total = total
    );
}

/// Marks a commit failure for observability logs.
pub fn record_commit_failure(table_id: Uuid, reason: &str) {
    warn!(metric = "commit_failure", table_id = %table_id, reason);
}

/// Records mirror latency for a `(table, version)` pair.
pub fn record_mirror_latency(table_id: Uuid, version: i64, duration: Duration) {
    info!(
        metric = "mirror_latency_ms",
        table_id = %table_id,
        version,
        latency_ms = duration_ms(duration)
    );
}

/// Records a mirror failure, incrementing failure totals and logging diagnostics.
pub fn record_mirror_failure(table_id: Uuid, version: i64, error: &str) {
    let total = MIRROR_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
    warn!(
        metric = "mirror_failure",
        table_id = %table_id,
        version,
        error,
        mirror_failures_total = total
    );
}

/// Sets the current mirror backlog gauge and logs the change.
pub fn set_mirror_backlog(backlog: u64) {
    MIRROR_BACKLOG.store(backlog, Ordering::Relaxed);
    info!(metric = "mirror_backlog_gauge", backlog);
}
