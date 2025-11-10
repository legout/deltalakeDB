# Mirror Engine Guide

## Overview

The mirror engine writes Delta Lake artifacts to object storage, enabling full ecosystem compatibility. After each SQL commit, the mirror engine:

1. **Serializes** committed actions to canonical JSON
2. **Writes** JSON commits to `_delta_log/NNNNNNNNNN.json`
3. **Generates** Parquet checkpoints at intervals
4. **Tracks** mirror progress in PostgreSQL
5. **Reconciles** failures in the background

## Architecture

### Five-Phase Implementation

The mirror engine is implemented in phases:

#### Phase 1: JSON Serialization ✅
- Canonical Delta JSON format
- Newline-delimited output (one action per line)
- Deterministic field ordering (prevents spurious diffs)
- Action priority ordering: Protocol → Metadata → Txn → Add → Remove

#### Phase 2: Parquet Checkpoints ✅
- Generates Parquet checkpoint files at configured intervals
- Arrow schema with add file metadata
- Snappy compression for compatibility
- Checkpoint path: `_delta_log/{version:020}.checkpoint.parquet`

#### Phase 3: Status Tracking ✅
- `dl_mirror_status` PostgreSQL table
- Tracks PENDING/SUCCESS/FAILED states
- Records JSON write and checkpoint write separately (for partial failures)
- Retry count and error messages
- Enables reconciliation to find failed mirrors

#### Phase 4: Reconciliation ✅
- Background process finds failed/pending mirrors
- Exponential backoff retry strategy
- Lag metrics and alerting
- Maximum retry limits with alerts

#### Phase 5: Integration Testing ✅
- Serialization conformance tests
- Checkpoint generation validation
- Determinism property tests
- Reconciliation logic tests

## Components

### MirrorEngine

**Purpose**: Orchestrates version mirroring to object storage.

**Key Methods**:
```rust
pub async fn mirror_version(
    &self,
    table_location: &str,
    version: i64,
    actions: &[Action],
    snapshot: &Snapshot,
) -> MirrorResult<()>
```

**Features**:
- Writes JSON commits (required)
- Generates checkpoints (optional)
- Non-blocking (failures don't fail the commit)
- Idempotent write semantics

**Configuration**:
```rust
let engine = MirrorEngine::new(object_store, checkpoint_interval);
// checkpoint_interval: how often to generate checkpoints (default: 10)
```

### JSON Serializer

**Purpose**: Converts Delta actions to canonical JSON format.

**Key Function**:
```rust
pub fn serialize_actions(actions: &[Action]) -> MirrorResult<String>
```

**Output Format**:
- Newline-delimited JSON
- One action per line
- Each line is a valid JSON object
- Deterministic ordering

**Example Output**:
```json
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"description":"My table"}}
{"add":{"path":"file1.parquet","size":1024,"modificationTime":1704067200000}}
{"add":{"path":"file2.parquet","size":2048,"modificationTime":1704067200000}}
```

**Serialization Rules**:
- All timestamps are integer milliseconds (not floats)
- Optional fields are omitted (not serialized as null)
- Field names follow Delta naming: `minReaderVersion`, not `min_reader_version`
- Field ordering is deterministic (alphabetical within each action)

### CheckpointWriter

**Purpose**: Generates Parquet checkpoint files.

**Key Methods**:
```rust
pub fn write_checkpoint(snapshot: &Snapshot, output_path: &Path) -> MirrorResult<u64>

pub fn checkpoint_path(table_location: &str, version: i64) -> String
```

**Checkpoint Schema**:
```
add: struct {
  path: string
  size: int64
  modificationTime: int64
  dataChangeVersion: int64
}
```

**Features**:
- Snappy compression for performance
- Follows Delta checkpoint specification
- Deterministic output (same input = same bytes)
- Efficient RecordBatch creation from snapshots

### MirrorStatusTracker

**Purpose**: Records and queries mirror progress in PostgreSQL.

**Database Table**:
```sql
CREATE TABLE dl_mirror_status (
    table_id UUID,
    version BIGINT,
    status TEXT,           -- PENDING, SUCCESS, FAILED
    json_written BOOLEAN,
    checkpoint_written BOOLEAN,
    error_message TEXT,
    retry_count INT,
    last_attempt_at TIMESTAMPTZ,
    PRIMARY KEY (table_id, version)
);
```

**Key Methods**:
```rust
// Record mirroring started
pub async fn mark_pending(&self, table_id: Uuid, version: i64) -> Result<(), String>

// Record successful mirror
pub async fn mark_success(
    &self,
    table_id: Uuid,
    version: i64,
    json_written: bool,
    checkpoint_written: bool,
) -> Result<(), String>

// Record failed mirror
pub async fn mark_failed(
    &self,
    table_id: Uuid,
    version: i64,
    error_message: &str,
) -> Result<(), String>

// Find all failed mirrors
pub async fn find_failed(&self, table_id: Uuid) -> Result<Vec<i64>, String>

// Calculate mirror lag
pub async fn get_mirror_lag(
    &self,
    table_id: Uuid,
    latest_version: i64,
) -> Result<i64, String>

// Find versions ready for retry
pub async fn find_ready_for_retry(
    &self,
    table_id: Uuid,
    max_attempts: i32,
    min_backoff_secs: i64,
) -> Result<Vec<(i64, i32)>, String>
```

**Status States**:
- `PENDING`: Mirror operation is in progress
- `SUCCESS`: Mirror completed (JSON and optionally checkpoint written)
- `FAILED`: Mirror failed and needs retry

**Partial Failure Tracking**:
```rust
// JSON written but checkpoint failed - still marked SUCCESS
await tracker.mark_success(table_id, version, 
    true,  // json_written
    false  // checkpoint_written
)?;
```

### MirrorReconciler

**Purpose**: Background process to repair failed mirrors.

**Configuration**:
```rust
pub struct ReconciliationConfig {
    pub interval: Duration,              // Default: 30s
    pub max_attempts: u32,               // Default: 5
    pub retry_backoff_base: Duration,    // Default: 1s
    pub retry_backoff_max: Duration,     // Default: 60s
    pub lag_alert_threshold: Duration,   // Default: 300s (5 min)
}
```

**Exponential Backoff**:
```
Attempt 0: 1s
Attempt 1: 2s
Attempt 2: 4s
Attempt 3: 8s
Attempt 4: 16s
...capped at max (60s)
```

**Key Methods**:
```rust
// Calculate backoff for retry attempt
pub fn calculate_backoff(&self, attempt: u32) -> Duration

// Emit mirror health metrics
pub fn emit_lag_metrics(&self, table_id: &str, lag_seconds: u64)

// Check if lag exceeds threshold
pub fn should_alert_on_lag(&self, lag: Duration) -> bool

// Alert on stuck mirror (max retries exceeded)
pub fn alert_on_stuck_mirror(&self, table_id: &str, version: u64, attempts: u32)
```

## Data Flow

### Successful Mirror Operation

```
1. SQL Commit Succeeds
   ↓
2. MirrorEngine.mirror_version() called
   ↓
3. Actions serialized to canonical JSON
   ↓
4. JSON written to _delta_log/NNNNNNNNNN.json
   ↓
5. MirrorStatusTracker.mark_pending()
   ↓
6. If checkpoint interval reached:
   a. CheckpointWriter generates Parquet checkpoint
   b. Checkpoint written to _delta_log/NNNNNNNNNN.checkpoint.parquet
   ↓
7. MirrorStatusTracker.mark_success()
   ↓
8. External readers can now access _delta_log artifacts
```

### Failed Mirror with Reconciliation

```
1. Mirror attempt fails (object store error, etc.)
   ↓
2. MirrorStatusTracker.mark_failed() records error
   ↓
3. Status shows FAILED in database
   ↓
4. MirrorReconciler runs periodically (every 30s default)
   ↓
5. Reconciler finds failed mirrors
   ↓
6. Reconciler calculates backoff based on retry count
   ↓
7. Retry attempt after backoff duration
   ↓
8. If successful: mark_success()
   If failed: increment retry_count and try again
   ↓
9. After max_attempts: alert_on_stuck_mirror()
```

## Integration Guide

### Basic Setup

```rust
use deltalakedb_mirror::MirrorEngine;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;

// Create object store client
let s3 = AmazonS3Builder::new()
    .with_bucket_name("my-bucket")
    .build()?;

// Create mirror engine
let mirror = MirrorEngine::new(Arc::new(s3), 10);

// After successful SQL commit
mirror.mirror_version(
    "s3://bucket/path/to/table",
    version,
    &committed_actions,
    &snapshot
).await?;
```

### With Status Tracking

```rust
use deltalakedb_sql_metadata_postgres::MirrorStatusTracker;
use sqlx::postgres::PgPool;

let pool = PgPool::connect("postgresql://...").await?;
let tracker = MirrorStatusTracker::new(pool);

// Record mirror attempt
tracker.mark_pending(table_id, version).await?;

// Mirror attempt
match mirror.mirror_version(...).await {
    Ok(_) => {
        tracker.mark_success(table_id, version, true, true).await?;
    }
    Err(e) => {
        tracker.mark_failed(table_id, version, &e.to_string()).await?;
    }
}
```

### With Background Reconciliation

```rust
use deltalakedb_mirror::MirrorReconciler;
use std::time::Duration;

let reconciler = MirrorReconciler::new();

// Background task (run periodically, e.g., every 30s)
loop {
    // Find failed mirrors
    let failed = tracker.find_failed(table_id).await?;
    
    for version in failed {
        let status = tracker.get_status(table_id, version).await?;
        let record = status.unwrap();
        
        if record.retry_count >= 5 {
            // Max retries exceeded
            reconciler.alert_on_stuck_mirror(&table_id.to_string(), version as u64, record.retry_count as u32);
            continue;
        }
        
        let backoff = reconciler.calculate_backoff(record.retry_count as u32);
        // Wait backoff duration
        tokio::time::sleep(backoff).await;
        
        // Retry mirror
        match mirror.mirror_version(...).await {
            Ok(_) => {
                tracker.mark_success(table_id, version, true, false).await?;
            }
            Err(e) => {
                tracker.mark_failed(table_id, version, &e.to_string()).await?;
            }
        }
    }
    
    // Check lag and emit metrics
    let lag = tracker.get_mirror_lag(table_id, current_version).await?;
    reconciler.emit_lag_metrics(&table_id.to_string(), lag as u64);
    
    if reconciler.should_alert_on_lag(Duration::from_secs(lag as u64)) {
        // Emit alert to monitoring system
    }
    
    tokio::time::sleep(Duration::from_secs(30)).await;
}
```

## Error Handling

### Mirror Failures are Non-Blocking

If mirroring fails, the SQL commit still succeeds. Failures are:
- Recorded in `dl_mirror_status` with error details
- Retried by the background reconciler
- Alerted if max retries exceeded

### Partial Failures

If JSON is written but checkpoint fails:
- JSON write succeeds (essential)
- Checkpoint failure is logged (optional)
- Status shows `json_written=true, checkpoint_written=false`
- Reconciler can retry checkpoint separately

### Idempotent Write Semantics

If a retry attempts to write the same version twice:
- `put_if_absent` detects existing object
- Content hash is verified (same = idempotent success, different = conflict)
- No data corruption
- Safe for unlimited retries

## Monitoring and Alerting

### Key Metrics

1. **Mirror Lag**: Versions not yet successfully mirrored
   - Emitted to monitoring system
   - Alert threshold: 300s (configurable)

2. **Retry Count**: How many times a version has been retried
   - Max retries: 5 (configurable)
   - Alert when exceeded

3. **Checkpoint Interval**: How often checkpoints are generated
   - Default: every 10 commits
   - Configurable per MirrorEngine

### Example Alerts

```
CRITICAL: Mirror lag exceeds 5 minutes for table xyz
- Lag: 7 minutes
- First failed version: 1000
- Recommended action: Check object store connectivity

WARN: Mirror version stuck at 1500 with 5 retries
- Error: Connection timeout to S3
- Last attempt: 10 minutes ago
- Recommended action: Check S3 service health
```

## Performance Tuning

### Checkpoint Intervals

```rust
// Frequent checkpoints (every 5 commits) - more object store writes
let engine = MirrorEngine::new(store, 5);

// Sparse checkpoints (every 50 commits) - fewer writes, slower reader startup
let engine = MirrorEngine::new(store, 50);

// Recommended: 10 (default) - good balance
```

### Reconciliation Interval

```rust
let config = ReconciliationConfig {
    interval: Duration::from_secs(60),  // Check every 60s instead of 30s
    ..Default::default()
};
let reconciler = MirrorReconciler::with_config(config);
```

### Backoff Strategy

```rust
let config = ReconciliationConfig {
    retry_backoff_base: Duration::from_secs(2),  // Start at 2s instead of 1s
    retry_backoff_max: Duration::from_secs(120), // Cap at 2 min instead of 1 min
    ..Default::default()
};
```

## Compliance with Delta Protocol

The mirror engine follows Delta Lake protocol specifications:

1. **File Naming**: `_delta_log/NNNNNNNNNN.json` with zero-padded version
2. **JSON Format**: Newline-delimited, canonical field ordering
3. **Checkpoint Format**: Parquet with Arrow schema per spec
4. **Timestamp Format**: Integer milliseconds since epoch (not floats)
5. **Action Ordering**: Deterministic (Protocol → Metadata → Txn → Add → Remove)
6. **Field Naming**: Exact camelCase per Delta spec

This ensures external tools (Spark, DuckDB, Polars) can read SQL-backed tables directly.

## Troubleshooting

### Mirror Lag Growing

**Symptom**: `dl_mirror_status` shows increasing lag

**Causes**:
- Object store connectivity issues
- Network timeouts
- Permission errors

**Solutions**:
1. Check object store service health
2. Verify IAM permissions
3. Check network connectivity
4. Review error messages in `dl_mirror_status.error_message`

### Stuck Mirrors

**Symptom**: Version not progressing after max retries

**Causes**:
- Permanent permission error
- Invalid data format
- Corrupted snapshot

**Solutions**:
1. Inspect error details: `SELECT error_message FROM dl_mirror_status WHERE ...`
2. Fix underlying issue
3. Reset status: `UPDATE dl_mirror_status SET retry_count = 0 WHERE ...`
4. Manually retry

### Memory Usage in Reconciler

**Symptom**: Reconciler consuming excessive memory

**Causes**:
- Large snapshots (100k+ files)
- Checkpoint generation too frequent

**Solutions**:
1. Increase checkpoint interval
2. Process failed mirrors in batches
3. Run reconciler on separate server

## Future Enhancements

Planned improvements:

1. **Compression Options**: Support for gzip/lz4 in addition to Snappy
2. **Multi-Part Upload**: For large checkpoints (> 100MB)
3. **Parallel Mirroring**: Concurrent write to object store
4. **Metrics Export**: Prometheus/StatsD integration
5. **Checksum Verification**: SHA256 validation of written artifacts
6. **Batch Reconciliation**: Process multiple failed versions efficiently
