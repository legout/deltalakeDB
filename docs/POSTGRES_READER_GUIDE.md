# PostgreSQL Reader for Delta Lake Metadata

This guide describes how to use the PostgreSQL reader to efficiently query Delta Lake metadata from PostgreSQL.

## Overview

The `PostgresReader` implements the `TxnLogReader` trait, providing fast, SQL-based access to Delta table metadata:

- **Fast version lookup**: <1ms for latest version
- **Efficient snapshots**: ~800ms (p95) for 100k files
- **Time travel support**: By version or timestamp
- **Active file computation**: Smart adds/removes logic
- **Connection pooling**: Reuses connections efficiently

## Basic Usage

### Setup

```rust
use sqlx::postgres::{PgPool, PgPoolOptions};
use deltalakedb_sql_metadata_postgres::PostgresReader;
use deltalakedb_core::traits::TxnLogReader;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create connection pool
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect("postgresql://user:pass@localhost/delta_metadata")
        .await?;

    // Run migrations (sets up schema)
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await?;

    // Create reader for a specific table
    let table_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;
    let reader = PostgresReader::new(pool, table_id);

    // Use the reader
    let version = reader.get_latest_version().await?;
    println!("Latest version: {}", version);

    Ok(())
}
```

### Common Operations

#### Get Latest Version

```rust
let version = reader.get_latest_version().await?;
println!("Current version: {}", version);
```

**Performance**: <1ms (O(1) lookup via primary key)

#### Read Current Snapshot

```rust
// Get latest snapshot
let snapshot = reader.read_snapshot(None).await?;
println!("Version: {}", snapshot.version);
println!("Files: {}", snapshot.files.len());
println!("Schema: {}", snapshot.metadata.schema.unwrap_or_default());
```

**Performance**: ~100-800ms depending on number of files

#### Read Specific Version

```rust
// Read snapshot at version 5
let snapshot = reader.read_snapshot(Some(5)).await?;
println!("Files at version 5: {}", snapshot.files.len());
```

#### List Active Files

```rust
let version = reader.get_latest_version().await?;
let files = reader.get_files(version).await?;

for file in files {
    println!("Path: {}, Size: {} bytes", file.path, file.size);
}
```

**Performance**: 
- Small tables (< 10k files): ~50ms
- Medium tables (10k-100k files): ~200-500ms
- Large tables (100k+ files): ~800ms (p95)

#### Time Travel by Version

```rust
// Get table state as it was at version 10
let snapshot = reader.read_snapshot(Some(10)).await?;
```

#### Time Travel by Timestamp

```rust
use std::time::{SystemTime, UNIX_EPOCH};

// Get table state as it was 1 hour ago
let now = SystemTime::now()
    .duration_since(UNIX_EPOCH)?
    .as_millis() as i64;
let one_hour_ago = now - 3600 * 1000;

let snapshot = reader.time_travel(one_hour_ago).await?;
println!("Files at that time: {}", snapshot.files.len());
```

## Connection Pooling

### Configuration

```rust
let pool = PgPoolOptions::new()
    .max_connections(20)           // Connection pool size
    .min_connections(5)             // Minimum idle connections
    .acquire_timeout(Duration::from_secs(30))
    .idle_timeout(Duration::from_secs(600))
    .max_lifetime(Duration::from_secs(3600))
    .connect("postgresql://user:pass@host/db")
    .await?;
```

### Best Practices

1. **Reuse pools**: Create one pool per application, not per query
2. **Size appropriately**: `max_connections` = (number of readers × 2)
3. **Monitor**: Check `pool.num_idle()` and `pool.size()`

```rust
// Monitor pool health
println!("Pool size: {}", pool.size());
println!("Idle connections: {}", pool.num_idle());
```

## Query Performance Optimization

### Query Times (100k files, local PostgreSQL)

| Operation | Typical | P95 | Index Used |
|-----------|---------|-----|-----------|
| Latest version | <1ms | <1ms | PK lookup |
| Get version | ~5ms | ~10ms | `(table_id, version DESC)` |
| Active files | ~300ms | ~800ms | Multiple indexes |
| Time travel version | ~100ms | ~200ms | Timestamp index |

### Optimization Techniques

#### 1. Use Specific Versions When Known

```rust
// If you know the version, specify it
let snapshot = reader.read_snapshot(Some(42)).await?;

// Instead of always getting latest
let version = reader.get_latest_version().await?;
let snapshot = reader.read_snapshot(Some(version)).await?;
```

#### 2. Batch Operations

```rust
// Read multiple snapshots in parallel
use futures::future::join_all;

let versions = vec![10, 20, 30, 40, 50];
let snapshots = join_all(
    versions.iter()
        .map(|&v| reader.read_snapshot(Some(v)))
).await;
```

#### 3. Filter Files After Read

```rust
let snapshot = reader.read_snapshot(None).await?;

// Filter to specific partition
let filtered: Vec<_> = snapshot.files
    .into_iter()
    .filter(|f| f.path.contains("2024/01"))
    .collect();
```

## Error Handling

### Common Errors

**TableNotFound**
```rust
match reader.get_latest_version().await {
    Ok(v) => println!("Version: {}", v),
    Err(TxnLogError::TableNotFound(table_id)) => {
        eprintln!("Table not found: {}", table_id);
    }
    Err(e) => eprintln!("Other error: {}", e),
}
```

**No versions before timestamp**
```rust
match reader.time_travel(timestamp).await {
    Ok(snap) => println!("Version: {}", snap.version),
    Err(e) => eprintln!("Time travel failed: {}", e),
}
```

### Retry Logic

```rust
use std::time::Duration;
use tokio::time::sleep;

async fn read_with_retry(
    reader: &PostgresReader,
    max_retries: u32,
) -> Result<i64, Box<dyn std::error::Error>> {
    for attempt in 0..max_retries {
        match reader.get_latest_version().await {
            Ok(version) => return Ok(version),
            Err(e) => {
                if attempt < max_retries - 1 {
                    let delay = Duration::from_millis(2_u64.pow(attempt));
                    eprintln!("Attempt {} failed, retrying in {:?}: {}", attempt + 1, delay, e);
                    sleep(delay).await;
                } else {
                    return Err(Box::new(e));
                }
            }
        }
    }
    unreachable!()
}
```

## Advanced Usage

### Snapshot Iteration

```rust
let snapshot = reader.read_snapshot(None).await?;

// Iterate through files
for file in &snapshot.files {
    println!("File: {}", file.path);
    println!("  Size: {} bytes", file.size);
    
    if let Some(stats) = &file.stats {
        println!("  Stats: {}", stats);
    }
}

// Statistics
println!("Total files: {}", snapshot.files.len());
let total_size: i64 = snapshot.files.iter().map(|f| f.size).sum();
println!("Total size: {} bytes", total_size);
```

### Schema Inspection

```rust
let snapshot = reader.read_snapshot(None).await?;

if let Some(schema_json) = &snapshot.metadata.schema {
    println!("Schema: {}", schema_json);
}

if let Some(partitions) = &snapshot.metadata.partition_columns {
    println!("Partitioned by: {:?}", partitions);
}

println!("Protocol version: {}", 
    snapshot.protocol.min_reader_version.unwrap_or(1));
```

### Multiple Readers

```rust
use tokio::task;

// Create multiple readers for different tables
let readers: Vec<_> = table_ids
    .into_iter()
    .map(|id| PostgresReader::new(pool.clone(), id))
    .collect();

// Query concurrently
let handles: Vec<_> = readers
    .into_iter()
    .map(|reader| {
        task::spawn(async move {
            reader.get_latest_version().await
        })
    })
    .collect();

let results = futures::future::join_all(handles).await;
```

## Testing

### Unit Tests

```bash
cargo test --lib
```

### Integration Tests (requires PostgreSQL)

```bash
# Set test database URL
export TEST_DATABASE_URL=postgresql://user:pass@localhost/delta_test

# Run integration tests
cargo test -- --ignored --test-threads=1
```

### Performance Benchmarks

```bash
# Run with logging to see query times
RUST_LOG=debug cargo test --test performance_tests -- --ignored
```

## Troubleshooting

### Connection Timeouts

**Problem**: `Failed to acquire connection in 30s`

**Solution**:
```rust
let pool = PgPoolOptions::new()
    .acquire_timeout(Duration::from_secs(60))  // Increase timeout
    .connect(database_url)
    .await?;
```

### Out of Memory

**Problem**: Reading 100k+ files causes OOM

**Solution**: Batch reads or use pagination:
```rust
const BATCH_SIZE: i64 = 10000;

// Process in batches
for offset in (0..total_files).step_by(BATCH_SIZE as usize) {
    let batch = read_files_batch(offset, BATCH_SIZE).await?;
    process_batch(batch);
}
```

### Slow Queries

**Problem**: `read_snapshot()` takes > 1 second

**Solution**:
1. Check PostgreSQL indexes: `\d dl_add_files`
2. Analyze query plan: `EXPLAIN ANALYZE SELECT ...`
3. Run `VACUUM ANALYZE` on tables
4. Increase `work_mem` in PostgreSQL config

```sql
-- Increase work_mem for this connection
SET work_mem = '256MB';

-- Then run your query
SELECT * FROM dl_add_files WHERE table_id = ... LIMIT 1;
```

## Performance Tuning

### Database Configuration

For optimal performance on 100k+ files:

```sql
-- In postgresql.conf
shared_buffers = 256MB          # 25% of RAM
effective_cache_size = 1GB      # 50-75% of RAM
work_mem = 64MB                 # For large sorts
maintenance_work_mem = 128MB    # For maintenance

-- Restart PostgreSQL
sudo systemctl restart postgresql
```

### Index Maintenance

```sql
-- Vacuum and analyze
VACUUM ANALYZE dl_add_files;
VACUUM ANALYZE dl_table_versions;
VACUUM ANALYZE dl_remove_files;

-- Reindex if fragmented
REINDEX INDEX idx_dl_add_files_lookup;
REINDEX INDEX idx_dl_table_versions_latest;
```

## Monitoring

### Connection Pool Monitoring

```rust
async fn monitor_pool(pool: &PgPool) {
    loop {
        println!("Pool - Size: {}, Idle: {}", 
            pool.size(), 
            pool.num_idle());
        
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}
```

### Query Logging

```rust
// Enable sqlx query logging
env_logger::builder()
    .filter(Some("sqlx"), log::LevelFilter::Debug)
    .init();

// Now sqlx will log all queries
let version = reader.get_latest_version().await?;
```

## Examples

### Complete Application

```rust
use sqlx::postgres::PgPoolOptions;
use deltalakedb_sql_metadata_postgres::PostgresReader;
use deltalakedb_core::traits::TxnLogReader;
use uuid::Uuid;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    // Create pool
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(30))
        .connect("postgresql://user:pass@localhost/delta_metadata")
        .await?;

    // Run migrations
    sqlx::migrate!("./migrations").run(&pool).await?;

    // Create reader
    let table_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;
    let reader = PostgresReader::new(pool, table_id);

    // Get and display table info
    let version = reader.get_latest_version().await?;
    println!("Table version: {}", version);

    let snapshot = reader.read_snapshot(None).await?;
    println!("Files: {}", snapshot.files.len());
    println!("Schema: {}", snapshot.metadata.schema.unwrap_or_default());

    Ok(())
}
```

## See Also

- [PostgreSQL Schema Design](./POSTGRES_SCHEMA_DESIGN.md)
- [Delta Lake Specification](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [sqlx Documentation](https://github.com/launchbadge/sqlx)
