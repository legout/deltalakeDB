# SQL Adapters Performance Tuning Guide

This guide provides comprehensive performance optimization strategies for Delta Lake SQL adapters.

## Overview

The SQL adapters are designed for high-performance metadata operations, but optimal performance requires proper configuration and tuning based on your specific workload and database choice.

## Database-Specific Optimizations

### PostgreSQL Performance Tuning

#### Connection Pool Configuration

```rust
let config = DatabaseConfig {
    url: "postgresql://user:password@localhost/deltalake".to_string(),
    pool_size: 20,              // Optimal for most workloads
    timeout: 30,
    ssl_enabled: true,
    max_connections: Some(50),  // Allow bursts
    min_connections: Some(5),   // Maintain warm connections
    idle_timeout: Some(600),    // 10 minutes
    max_lifetime: Some(1800),   // 30 minutes
};
```

#### PostgreSQL Configuration Settings

```sql
-- Memory settings
shared_buffers = 256MB                 -- 25% of RAM
effective_cache_size = 1GB             -- 75% of RAM
work_mem = 4MB                         -- Per connection
maintenance_work_mem = 64MB

-- WAL settings for high write throughput
wal_buffers = 16MB
checkpoint_completion_target = 0.9
wal_writer_delay = 200ms

-- Parallel query settings
max_parallel_workers_per_gather = 4
max_parallel_workers = 8
parallel_tuple_cost = 0.1
parallel_setup_cost = 1000.0

-- Autovacuum tuning for Delta tables
autovacuum = on
autovacuum_max_workers = 3
autovacuum_naptime = 1min
```

#### Indexing Strategy

```sql
-- Primary indexes (created automatically)
CREATE INDEX CONCURRENTLY idx_delta_tables_table_path
ON delta_tables(table_path);

CREATE INDEX CONCURRENTLY idx_delta_commits_table_version
ON delta_commits(table_id, version DESC);

-- JSONB indexes for metadata queries
CREATE INDEX CONCURRENTLY idx_delta_metadata_config_gin
ON delta_metadata USING GIN(configuration);

CREATE INDEX CONCURRENTLY idx_delta_commit_actions_params_gin
ON delta_commit_actions USING GIN(operation_parameters);

-- Partial indexes for common query patterns
CREATE INDEX CONCURRENTLY idx_active_files_by_table
ON delta_files(table_id, commit_id)
WHERE is_add = true AND deleted_at IS NULL;

-- Time-based indexes for time travel queries
CREATE INDEX CONCURRENTLY idx_commits_timestamp
ON delta_commits(timestamp DESC);
```

#### Query Optimization

```rust
// Use prepared statements for repeated queries
let mut stmt = conn.prepare(
    "SELECT * FROM delta_commits
     WHERE table_id = $1 AND version >= $2
     ORDER BY version ASC LIMIT $3"
).await?;

// Batch operations for better performance
let batch_size = 1000;
for chunk in commits.chunks(batch_size) {
    adapter.write_commits(chunk).await?;
}
```

### SQLite Performance Tuning

#### Connection Configuration

```rust
let config = DatabaseConfig {
    url: "sqlite:///path/to/database.db".to_string(),
    pool_size: 1,              // SQLite typically uses single connection
    timeout: 30,
    ssl_enabled: false,
    read_only: false,
};
```

#### SQLite PRAGMA Settings

```sql
-- Enable WAL mode for better concurrency
PRAGMA journal_mode = WAL;

-- Optimize for Delta workloads
PRAGMA synchronous = NORMAL;        -- Balance between safety and performance
PRAGMA cache_size = 10000;          -- 10MB cache
PRAGMA temp_store = MEMORY;         -- Store temporary tables in memory
PRAGMA mmap_size = 268435456;       -- 256MB memory-mapped I/O

-- Optimize for large metadata
PRAGMA page_size = 4096;            -- 4KB pages
PRAGMA locking_mode = NORMAL;       -- Normal locking mode
PRAGMA wal_autocheckpoint = 1000;   -- Checkpoint every 1000 pages

-- Query optimization
PRAGMA optimize;                    -- Automatic query optimization
```

#### SQLite-Specific Indexes

```sql
-- Covering indexes for common queries
CREATE INDEX idx_commits_table_version_covering
ON delta_commits(table_id, version DESC)
INCLUDE (timestamp, operation_type);

-- Composite indexes for file queries
CREATE INDEX idx_files_table_commit_add
ON delta_files(table_id, commit_id, is_add);

-- Partial indexes for active files
CREATE INDEX idx_active_files
ON delta_files(table_id, path)
WHERE is_add = 1 AND deleted_at IS NULL;
```

#### Performance Tips

```rust
// Use transactions for batch operations
let mut tx = conn.begin().await?;

// Batch inserts for better performance
for commit in commits {
    tx.execute(insert_commit_sql, &commit).await?;
}

tx.commit().await?;

// Use WAL mode for concurrent reads during writes
// Enable memory-mapped I/O for large databases
// Regular VACUUM operations for maintenance
```

### DuckDB Performance Tuning

#### Connection Configuration

```rust
let config = DatabaseConfig {
    url: "duckdb:///path/to/database.duckdb".to_string(),
    pool_size: 10,             // DuckDB supports multiple connections
    timeout: 30,
    ssl_enabled: false,
};
```

#### DuckDB Configuration

```sql
-- Memory settings
SET memory_limit='1GB';
SET temp_directory='./temp';

-- Parallel execution settings
SET threads=4;
SET enable_profiling=false;
SET enable_progress_bar=false;

-- WAL settings
SET wal_autocheckpoint=1000000;

-- Optimization settings
SET enable_optimizer=true;
SET force_parallelism=true;
SET preserve_insertion_order=false;
```

#### DuckDB-Specific Optimizations

```sql
-- Create materialized views for frequent queries
CREATE MATERIALIZED VIEW active_files AS
SELECT f.* FROM delta_files f
JOIN delta_commits c ON f.commit_id = c.id
WHERE f.is_add = true
  AND c.operation_type = 'WRITE'
  AND f.deleted_at IS NULL;

-- Use columnar storage benefits
SELECT table_id, COUNT(*), SUM(size)
FROM delta_files
WHERE is_add = true
GROUP BY table_id;

-- Leverage DuckDB's analytical capabilities
SELECT
    DATE_TRUNC('day', timestamp) as day,
    COUNT(*) as commits,
    COUNT(DISTINCT table_id) as tables
FROM delta_commits
WHERE timestamp >= NOW() - INTERVAL '30 days'
GROUP BY day
ORDER BY day;
```

## General Performance Strategies

### Connection Pool Optimization

#### Pool Sizing Guidelines

```rust
// For OLTP workloads (many small transactions)
let oltp_config = DatabaseConfig {
    pool_size: num_cpus() * 2,
    min_connections: num_cpus(),
    max_connections: num_cpus() * 3,
    // ...
};

// For analytical workloads (few large queries)
let olap_config = DatabaseConfig {
    pool_size: num_cpus(),
    min_connections: 2,
    max_connections: num_cpus() * 2,
    // ...
};

// For mixed workloads
let mixed_config = DatabaseConfig {
    pool_size: num_cpus() * 1.5,
    min_connections: num_cpus() / 2,
    max_connections: num_cpus() * 2.5,
    // ...
};
```

#### Pool Monitoring

```rust
// Monitor pool utilization
let stats = adapter.pool_stats().await?;

if stats.utilization_percent > 90.0 {
    eprintln!("High pool utilization: {:.1}%", stats.utilization_percent);
    // Consider increasing pool size
}

if stats.average_wait_time > Duration::from_millis(100) {
    eprintln!("High wait time: {:?}", stats.average_wait_time);
    // Consider optimizing queries or increasing pool size
}
```

### Query Optimization

#### Batch Operations

```rust
// Good: Batch commit writes
let batch_size = 1000;
for chunk in commits.chunks(batch_size) {
    let count = adapter.write_commits(chunk).await?;
    println!("Wrote {} commits", count);
}

// Bad: Individual commit writes
for commit in commits {
    adapter.write_commit(&commit).await?;  // Inefficient
}
```

#### Query Patterns

```rust
// Good: Use indexed columns in WHERE clauses
let commits = adapter.read_commits_range(
    table_id,
    Some(start_version),  // Use version range
    Some(end_version),
    Some(1000)            // Limit results
).await?;

// Bad: Full table scans
let all_commits = adapter
    .execute("SELECT * FROM delta_commits")
    .await?;  // Inefficient for large tables
```

#### Prepared Statements

```rust
// Cache prepared statements for repeated queries
struct PreparedQueries {
    get_latest_commit: Statement,
    get_commit_range: Statement,
    get_active_files: Statement,
}

impl PreparedQueries {
    async fn new(conn: &PgConnection) -> SqlResult<Self> {
        Ok(Self {
            get_latest_commit: conn.prepare(
                "SELECT * FROM delta_commits
                 WHERE table_id = $1
                 ORDER BY version DESC LIMIT 1"
            ).await?,
            get_commit_range: conn.prepare(
                "SELECT * FROM delta_commits
                 WHERE table_id = $1 AND version BETWEEN $2 AND $3
                 ORDER BY version ASC"
            ).await?,
            get_active_files: conn.prepare(
                "SELECT f.* FROM delta_files f
                 JOIN delta_commits c ON f.commit_id = c.id
                 WHERE f.table_id = $1 AND f.is_add = true
                 AND c.version = (SELECT MAX(version) FROM delta_commits WHERE table_id = $1)"
            ).await?,
        })
    }
}
```

### Memory Usage Optimization

#### Connection Memory

```rust
// Limit connection memory usage
let config = DatabaseConfig {
    max_connections: Some(20),     // Limit total connections
    idle_timeout: Some(300),       // Close idle connections quickly
    max_lifetime: Some(900),       // Limit connection lifetime
    // ...
};
```

#### Query Result Streaming

```rust
// Use streaming for large result sets
let mut stream = adapter.query_stream(
    "SELECT * FROM delta_commits WHERE table_id = $1",
    &[&table_id]
).await?;

let mut count = 0;
while let Some(row) = stream.next().await {
    let commit: Commit = row?;
    count += 1;

    if count >= 10000 {
        break;  // Prevent memory exhaustion
    }
}
```

### Caching Strategies

#### Application-Level Caching

```rust
use std::collections::LruCache;
use tokio::sync::RwLock;

struct CachedTable {
    table_id: Uuid,
    latest_version: AtomicI64,
    cache: RwLock<LruCache<i64, Commit>>,
}

impl CachedTable {
    async fn get_commit(&self, version: i64, adapter: &dyn DatabaseAdapter) -> SqlResult<Option<Commit>> {
        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(commit) = cache.get(&version) {
                return Ok(Some(commit.clone()));
            }
        }

        // Load from database
        let commit = adapter.read_commit(self.table_id, version).await?;

        // Update cache
        if let Some(ref commit) = commit {
            let mut cache = self.cache.write().await;
            cache.put(version, commit.clone());
        }

        commit
    }
}
```

#### Materialized Views

```sql
-- PostgreSQL materialized view for active files
CREATE MATERIALIZED VIEW active_files AS
SELECT DISTINCT ON (f.path)
    f.table_id,
    f.path,
    f.size,
    f.modification_time,
    c.version as latest_version
FROM delta_files f
JOIN delta_commits c ON f.commit_id = c.id
WHERE f.is_add = true
ORDER BY f.path, c.version DESC;

-- Refresh strategy
CREATE OR REPLACE FUNCTION refresh_active_files()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY active_files;
END;
$$ LANGUAGE plpgsql;
```

### Monitoring and Profiling

#### Performance Metrics

```rust
use std::time::Instant;

struct PerformanceTracker {
    query_count: AtomicU64,
    total_time: AtomicU64,
    slow_queries: AtomicU64,
}

impl PerformanceTracker {
    fn track_query<F, T>(&self, operation: F) -> T
    where
        F: FnOnce() -> T
    {
        let start = Instant::now();
        let result = operation();
        let duration = start.elapsed();

        self.query_count.fetch_add(1, Ordering::Relaxed);
        self.total_time.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);

        if duration > Duration::from_secs(1) {
            self.slow_queries.fetch_add(1, Ordering::Relaxed);
            eprintln!("Slow query detected: {:?}", duration);
        }

        result
    }
}
```

#### Query Plan Analysis

```sql
-- PostgreSQL query analysis
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT * FROM delta_commits
WHERE table_id = $1 AND version >= $2;

-- SQLite query analysis
EXPLAIN QUERY PLAN
SELECT * FROM delta_commits
WHERE table_id = ? AND version >= ?;

-- DuckDB query analysis
EXPLAIN ANALYZE
SELECT * FROM delta_commits
WHERE table_id = ? AND version >= ?;
```

## Performance Testing

### Benchmark Suite

```rust
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_commit_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("write_single_commit", |b| {
        b.iter(|| {
            rt.block_on(async {
                let commit = create_test_commit();
                adapter.write_commit(&commit).await
            })
        })
    });

    c.bench_function("write_batch_commits", |b| {
        b.iter(|| {
            rt.block_on(async {
                let commits = create_test_commits(100);
                adapter.write_commits(&commits).await
            })
        })
    });
}

criterion_group!(benches, bench_commit_writes);
criterion_main!(benches);
```

### Load Testing

```rust
async fn load_test_concurrent_writes() {
    let adapter = create_adapter().await;
    let num_writers = 10;
    let writes_per_writer = 1000;

    let mut handles = Vec::new();

    for writer_id in 0..num_writers {
        let adapter_clone = adapter.clone();
        let handle = tokio::spawn(async move {
            for i in 0..writes_per_writer {
                let commit = create_test_commit_with_id(writer_id, i);
                adapter_clone.write_commit(&commit).await?;
            }
            Ok::<(), SqlError>(())
        });
        handles.push(handle);
    }

    // Wait for all writers to complete
    for handle in handles {
        handle.await??;
    }
}
```

## Troubleshooting Performance Issues

### Common Issues and Solutions

#### High Connection Latency

```
Problem: Slow connection establishment
Solution: Use connection pooling, enable SSL session caching
```

#### Memory Usage Growth

```
Problem: Memory usage increases over time
Solution: Implement connection lifetime limits, monitor memory usage
```

#### Slow Query Performance

```
Problem: Queries become slower as data grows
Solution: Add appropriate indexes, analyze query plans, consider partitioning
```

#### Lock Contention

```
Problem: High write contention on tables
Solution: Use batch operations, implement retry logic, consider read replicas
```

### Performance Debugging Checklist

- [ ] Check connection pool utilization
- [ ] Analyze slow query logs
- [ ] Verify index usage
- [ ] Monitor memory usage
- [ ] Check database-specific metrics
- [ ] Review application-level caching
- [ ] Test with realistic data volumes
- [ ] Benchmark under realistic load patterns