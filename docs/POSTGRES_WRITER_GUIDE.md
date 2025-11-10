# PostgreSQL Writer for Delta Lake Metadata

This guide describes how to use the PostgreSQL writer to safely commit Delta Lake metadata with optimistic concurrency control and atomic guarantees.

## Overview

The `PostgresWriter` implements the `TxnLogWriter` trait, providing safe atomic commits:

- **Optimistic Concurrency Control**: Detects and rejects concurrent writer conflicts
- **Atomic Commits**: All actions in a commit succeed or fail together
- **Version Increment**: Ensures versions increase by exactly 1 per commit
- **Fast Conflicts**: Detects conflicts quickly without retrying partial work
- **Batch Operations**: Efficient insertion of multiple files in single commit

## Basic Usage

### Setup

```rust
use sqlx::postgres::{PgPool, PgPoolOptions};
use deltalakedb_sql_metadata_postgres::PostgresWriter;
use deltalakedb_core::traits::TxnLogWriter;
use deltalakedb_core::types::{Action, AddFile};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create connection pool
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect("postgresql://user:pass@localhost/delta_metadata")
        .await?;

    // Run migrations
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await?;

    // Create writer for a specific table
    let table_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;
    let mut writer = PostgresWriter::new(pool, table_id);

    // Use the writer (see examples below)
    Ok(())
}
```

### Typical Commit Flow

```rust
use deltalakedb_core::traits::TxnLogWriter;
use deltalakedb_core::types::{Action, AddFile};

// 1. Begin commit
let handle = writer.begin_commit().await?;

// 2. Create actions to write
let file1 = AddFile {
    path: "data/file1.parquet".to_string(),
    size: 1024000,
    modification_time: 1704067200000,
    data_change_version: 1,
    stats: None,
    stats_truncated: None,
    tags: None,
};

let file2 = AddFile {
    path: "data/file2.parquet".to_string(),
    size: 2048000,
    modification_time: 1704067200000,
    data_change_version: 1,
    stats: None,
    stats_truncated: None,
    tags: None,
};

// 3. Write actions
let actions = vec![Action::Add(file1), Action::Add(file2)];
writer.write_actions(&handle, actions).await?;

// 4. Finalize commit (atomic operation)
let new_version = writer.finalize_commit(handle).await?;
println!("Committed as version: {}", new_version);
```

## Optimistic Concurrency Control

### How It Works

The writer uses a lock-and-validate pattern:

1. **Begin commit**: Lock table row with `SELECT ... FOR UPDATE`
2. **Validate**: Check `current_version + 1 == expected_version`
3. **Insert**: Add all actions within the same transaction
4. **Commit**: Atomically update `current_version` and commit transaction

If another writer commits between steps 1 and 4, the version check fails with `VersionConflict`.

### Handling Conflicts

```rust
use deltalakedb_core::error::TxnLogError;

match writer.finalize_commit(handle).await {
    Ok(new_version) => {
        println!("Committed as version: {}", new_version);
    }
    Err(TxnLogError::VersionConflict { expected, actual }) => {
        eprintln!("Conflict! Expected {}, but actual is {}", expected, actual);
        eprintln!("Another writer committed first. Retry with fresh reader.");
    }
    Err(e) => eprintln!("Other error: {}", e),
}
```

### Retry Logic

```rust
use std::time::Duration;
use tokio::time::sleep;

async fn commit_with_retry(
    table_id: Uuid,
    pool: &PgPool,
    actions: Vec<Action>,
    max_retries: u32,
) -> Result<i64, Box<dyn std::error::Error>> {
    for attempt in 0..max_retries {
        let mut writer = PostgresWriter::new(pool.clone(), table_id);
        
        // Get reader to check current state
        let reader = PostgresReader::new(pool.clone(), table_id);
        let current_version = reader.get_latest_version().await?;
        
        match writer.begin_commit().await {
            Ok(handle) => {
                writer.write_actions(&handle, actions.clone()).await?;
                
                match writer.finalize_commit(handle).await {
                    Ok(new_version) => return Ok(new_version),
                    Err(TxnLogError::VersionConflict { .. }) => {
                        // Retry with exponential backoff
                        if attempt < max_retries - 1 {
                            let delay = Duration::from_millis(2_u64.pow(attempt));
                            sleep(delay).await;
                            continue;
                        }
                    }
                    Err(e) => return Err(Box::new(e)),
                }
            }
            Err(e) => return Err(Box::new(e)),
        }
    }
    
    Err("Max retries exceeded".into())
}
```

## Action Types

### AddFile

```rust
use deltalakedb_core::types::AddFile;

let file = AddFile {
    path: "s3://bucket/path/to/file.parquet".to_string(),
    size: 1_048_576,  // 1 MB
    modification_time: 1704067200000,  // milliseconds since epoch
    data_change_version: 1,  // version when added
    stats: Some(r#"{"row_count": 1000, "columns": {"id": {"min": 0, "max": 999}}}"#.to_string()),
    stats_truncated: Some(false),
    tags: Some(vec![
        ("encoding".to_string(), "snappy".to_string()),
    ].into_iter().collect()),
};

let actions = vec![Action::Add(file)];
writer.write_actions(&handle, actions).await?;
```

### RemoveFile

```rust
use deltalakedb_core::types::RemoveFile;

let removal = RemoveFile {
    path: "s3://bucket/old/file.parquet".to_string(),
    deletion_timestamp: 1704153600000,
    data_change: true,  // This affects data
    extended_file_metadata: Some(true),
    deletion_vector: None,
};

let actions = vec![Action::Remove(removal)];
writer.write_actions(&handle, actions).await?;
```

### Metadata Update

```rust
use deltalakedb_core::types::MetadataUpdate;

let metadata = MetadataUpdate {
    description: Some("User events table".to_string()),
    schema: Some(r#"{"type":"struct","fields":[{"name":"id","type":"long"}]}"#.to_string()),
    partition_columns: Some(vec!["year".to_string(), "month".to_string(), "day".to_string()]),
    created_time: Some(1704067200000),
    configuration: Some(vec![
        ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
    ].into_iter().collect()),
};

let actions = vec![Action::Metadata(metadata)];
writer.write_actions(&handle, actions).await?;
```

### Protocol Update

```rust
use deltalakedb_core::types::ProtocolUpdate;

let protocol = ProtocolUpdate {
    min_reader_version: Some(1),
    min_writer_version: Some(2),
};

let actions = vec![Action::Protocol(protocol)];
writer.write_actions(&handle, actions).await?;
```

### Transaction Action

```rust
use deltalakedb_core::types::TxnAction;

let txn = TxnAction {
    app_id: "streaming-app-v1".to_string(),
    version: 42,
    timestamp: 1704067200000,
};

let actions = vec![Action::Txn(txn)];
writer.write_actions(&handle, actions).await?;
```

## Batch Operations

### Multiple Files in One Commit

```rust
// Add many files in a single atomic commit
let mut actions = Vec::new();
for i in 0..1000 {
    let file = AddFile {
        path: format!("data/file_{}.parquet", i),
        size: 1024 * (i as i64 + 1),
        modification_time: 1704067200000,
        data_change_version: 1,
        stats: None,
        stats_truncated: None,
        tags: None,
    };
    actions.push(Action::Add(file));
}

let handle = writer.begin_commit().await?;
writer.write_actions(&handle, actions).await?;
let new_version = writer.finalize_commit(handle).await?;
println!("Committed 1000 files as version: {}", new_version);
```

### Mixed Actions

```rust
// Combine different action types
let mut actions = Vec::new();

// Add new files
for i in 0..100 {
    actions.push(Action::Add(AddFile {
        path: format!("data/new_{}.parquet", i),
        size: 1024000,
        modification_time: 1704067200000,
        data_change_version: 1,
        stats: None,
        stats_truncated: None,
        tags: None,
    }));
}

// Remove old files
for i in 0..50 {
    actions.push(Action::Remove(RemoveFile {
        path: format!("data/old_{}.parquet", i),
        deletion_timestamp: 1704153600000,
        data_change: true,
        extended_file_metadata: None,
        deletion_vector: None,
    }));
}

// Update metadata
actions.push(Action::Metadata(MetadataUpdate {
    description: Some("Updated with 100 new files".to_string()),
    schema: None,
    partition_columns: None,
    created_time: None,
    configuration: None,
}));

let handle = writer.begin_commit().await?;
writer.write_actions(&handle, actions).await?;
let new_version = writer.finalize_commit(handle).await?;
```

## Error Handling

### Validation Errors

```rust
// Invalid file path
let bad_file = AddFile {
    path: "".to_string(),  // EMPTY PATH - invalid
    size: 1024,
    modification_time: 0,
    data_change_version: 0,
    stats: None,
    stats_truncated: None,
    tags: None,
};

let actions = vec![Action::Add(bad_file)];
match writer.write_actions(&handle, actions).await {
    Ok(_) => println!("Success"),
    Err(e) => eprintln!("Validation failed: {}", e),  // "Add file path cannot be empty"
}
```

### Negative File Size

```rust
let bad_file = AddFile {
    path: "data/file.parquet".to_string(),
    size: -100,  // NEGATIVE SIZE - invalid
    modification_time: 0,
    data_change_version: 0,
    stats: None,
    stats_truncated: None,
    tags: None,
};

let actions = vec![Action::Add(bad_file)];
match writer.write_actions(&handle, actions).await {
    Ok(_) => println!("Success"),
    Err(e) => eprintln!("Validation failed: {}", e),  // "File size must be non-negative"
}
```

### Invalid Protocol Versions

```rust
let bad_protocol = ProtocolUpdate {
    min_reader_version: Some(0),  // INVALID - must be >= 1
    min_writer_version: Some(2),
};

let actions = vec![Action::Protocol(bad_protocol)];
match writer.write_actions(&handle, actions).await {
    Ok(_) => println!("Success"),
    Err(e) => eprintln!("Validation failed: {}", e),  // "min_reader_version must be >= 1"
}
```

## Performance Tuning

### Commit Performance

Target: <5 seconds for 10,000 files

**Measured on PostgreSQL 12 with local connection:**
- 100 files: ~50ms
- 1,000 files: ~300ms
- 10,000 files: ~2-3 seconds

### Connection Pool Sizing

```rust
let pool = PgPoolOptions::new()
    .max_connections(20)      // Higher for more concurrent writers
    .min_connections(5)
    .acquire_timeout(Duration::from_secs(30))
    .connect(database_url)
    .await?;
```

### Batch Strategies

**For large commits (1000+ files):**

```rust
// Process in batches to reduce memory usage
const BATCH_SIZE: usize = 1000;

let mut writer = PostgresWriter::new(pool, table_id);
let handle = writer.begin_commit().await?;

for chunk in actions.chunks(BATCH_SIZE) {
    writer.write_actions(&handle, chunk.to_vec()).await?;
}

let new_version = writer.finalize_commit(handle).await?;
```

## Testing

### Unit Tests

```bash
cargo test --lib writer
```

### Integration Tests (requires PostgreSQL)

```bash
export TEST_DATABASE_URL=postgresql://user:pass@localhost/delta_test
cargo test -- --ignored --test-threads=1
```

### Concurrent Writer Test

```bash
# Test that concurrent writers detect conflicts
cargo test test_concurrent_writers -- --ignored
```

## Monitoring

### Track Commit Metrics

```rust
use std::time::Instant;

let start = Instant::now();

let handle = writer.begin_commit().await?;
writer.write_actions(&handle, actions.clone()).await?;
let new_version = writer.finalize_commit(handle).await?;

let elapsed = start.elapsed();
println!("Commit time: {:?}", elapsed);
println!("Files per second: {}", actions.len() as f64 / elapsed.as_secs_f64());
```

### Monitor Lock Contention

```sql
-- Check for blocked transactions
SELECT 
    blocked_locks.pid AS blocked_pid,
    blocked_locks.usename AS blocked_user,
    blocking_locks.pid AS blocking_pid,
    blocking_locks.usename AS blocking_user
FROM 
    pg_catalog.pg_locks blocked_locks
    JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid
    JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;
```

## Troubleshooting

### "Commit already in progress"

```
Error: Commit already in progress for this writer
```

**Cause**: Called `begin_commit()` without calling `finalize_commit()` on previous transaction.

**Solution**: Always call `finalize_commit()` or handle errors properly:
```rust
match writer.finalize_commit(handle).await {
    Ok(version) => println!("Committed: {}", version),
    Err(e) => {
        eprintln!("Commit failed: {}", e);
        // Abort the transaction by dropping the writer
        // or reuse the writer for the next commit
    }
}
```

### "No commit in progress"

```
Error: No commit in progress
```

**Cause**: Called `write_actions()` or `finalize_commit()` before `begin_commit()`.

**Solution**: Always start with `begin_commit()`:
```rust
let handle = writer.begin_commit().await?;  // This first!
writer.write_actions(&handle, actions).await?;
writer.finalize_commit(handle).await?;
```

### VersionConflict Errors

```
VersionConflict: expected 5, got 4
```

**Cause**: Another writer committed between your `begin_commit()` and `finalize_commit()`.

**Solution**: Implement retry logic:
```rust
for attempt in 0..max_retries {
    match attempt_commit(&mut writer, actions.clone()).await {
        Ok(version) => return Ok(version),
        Err(TxnLogError::VersionConflict { .. }) => {
            // Retry with backoff
            sleep(Duration::from_millis(2_u64.pow(attempt))).await;
        }
        Err(e) => return Err(e),
    }
}
```

### Transaction Timeout

```
Error: Failed to begin transaction: Timeout
```

**Cause**: Cannot acquire database connection due to pool exhaustion or slow server.

**Solution**: 
1. Increase pool size: `.max_connections(50)`
2. Check database performance: `EXPLAIN ANALYZE` slow queries
3. Increase timeout: `.acquire_timeout(Duration::from_secs(60))`

## Advanced Topics

### Atomic Multi-File Compaction

```rust
// Read current files, delete old ones, write new compacted file
let reader = PostgresReader::new(pool.clone(), table_id);
let snapshot = reader.read_snapshot(None).await?;

let mut actions = Vec::new();

// Remove all existing files
for file in &snapshot.files {
    actions.push(Action::Remove(RemoveFile {
        path: file.path.clone(),
        deletion_timestamp: current_time_ms(),
        data_change: false,  // Not a data change, just compaction
        extended_file_metadata: None,
        deletion_vector: None,
    }));
}

// Add new compacted file
actions.push(Action::Add(AddFile {
    path: "data/compacted/file_v2.parquet".to_string(),
    size: 5_000_000,  // Combined size
    modification_time: current_time_ms(),
    data_change_version: snapshot.version + 1,
    stats: Some(r#"{"row_count": 50000}"#.to_string()),
    stats_truncated: None,
    tags: None,
}));

// Atomic commit - all or nothing
let mut writer = PostgresWriter::new(pool, table_id);
let handle = writer.begin_commit().await?;
writer.write_actions(&handle, actions).await?;
writer.finalize_commit(handle).await?;
```

### Transaction Tracking

```rust
// Track streaming application progress
let txn = TxnAction {
    app_id: "kafka-consumer".to_string(),
    version: 42,
    timestamp: 1704067200000,
};

let mut actions = vec![
    Action::Add(new_file),
    Action::Txn(txn),
];

let handle = writer.begin_commit().await?;
writer.write_actions(&handle, actions).await?;
let new_version = writer.finalize_commit(handle).await?;
```

## See Also

- [PostgreSQL Schema Design](./POSTGRES_SCHEMA_DESIGN.md)
- [PostgreSQL Reader Guide](./POSTGRES_READER_GUIDE.md)
- [Delta Lake Specification](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [sqlx Documentation](https://github.com/launchbadge/sqlx)
