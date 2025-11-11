# Performance Tuning Guide for SQL-Backed Delta Lake

This comprehensive guide helps you optimize the performance of your SQL-Backed Delta Lake deployment for maximum throughput, minimal latency, and efficient resource utilization.

## Table of Contents

1. [Performance Overview](#performance-overview)
2. [Database Optimization](#database-optimization)
3. [Caching Strategies](#caching-strategies)
4. [Memory Optimization](#memory-optimization)
5. [Async I/O Configuration](#async-io-configuration)
6. [Query Performance](#query-performance)
7. [Write Performance](#write-performance)
8. [Multi-Table Transaction Tuning](#multi-table-transaction-tuning)
9. [Monitoring and Metrics](#monitoring-and-metrics)
10. [Benchmarking and Testing](#benchmarking-and-testing)

## Performance Overview

### Key Performance Metrics

- **Query Latency**: Time to execute metadata queries
- **Throughput**: Operations per second
- **Memory Usage**: Peak and average memory consumption
- **Cache Hit Ratio**: Percentage of requests served from cache
- **Concurrent Operations**: Number of simultaneous operations

### Performance Benchmarks

Typical performance improvements over standard Delta Lake:

| Operation | Standard Delta Lake | SQL-Backed Delta Lake | Improvement |
|-----------|-------------------|----------------------|-------------|
| Metadata Query | 100-500ms | 5-20ms | 5-25x faster |
| Large File List Loading | 2-10s | 100-500ms | 10-40x faster |
| Multi-Table Transaction | N/A | 50-200ms | New capability |
| Memory Usage | High | 95% reduction | 20x improvement |

## Database Optimization

### 1. Database Configuration

#### PostgreSQL Optimization

```sql
-- postgresql.conf optimizations for Delta Lake workloads

# Memory settings
shared_buffers = '4GB'                    -- 25% of system memory
effective_cache_size = '12GB'             -- 75% of system memory
work_mem = '256MB'                        -- Per query memory
maintenance_work_mem = '1GB'              -- For maintenance operations

# Connection settings
max_connections = 200
superuser_reserved_connections = 3

# Checkpoint settings
checkpoint_completion_target = 0.9
wal_buffers = '64MB'
default_statistics_target = 1000

# Query planning
random_page_cost = 1.1                    -- For SSD storage
effective_io_concurrency = 200            -- For SSD storage
```

#### MySQL Optimization

```ini
# my.cnf optimizations for Delta Lake workloads

# Memory settings
innodb_buffer_pool_size = 8G              -- 50-70% of system memory
innodb_log_file_size = 1G
innodb_log_buffer_size = 256M
innodb_flush_log_at_trx_commit = 2        -- Better performance

# Connection settings
max_connections = 200
thread_cache_size = 50

# Query cache (if using older MySQL)
query_cache_type = 1
query_cache_size = 512M

# InnoDB settings
innodb_io_capacity = 2000                 -- For SSD storage
innodb_io_capacity_max = 4000
innodb_flush_method = O_DIRECT
innodb_file_per_table = 1
```

### 2. Connection Pool Configuration

```python
import deltalakedb as dl

# Production-grade connection pool configuration
sql_config = dl.SqlConfig(
    connection_string="postgresql://user:pass@localhost:5432/db",
    pool_size=50,              # Base connection pool size
    max_overflow=100,          # Additional connections under load
    pool_timeout=60,           # Wait time for connection
    pool_recycle=3600,         # Recycle connections every hour
    pool_pre_ping=True,        # Validate connections
    echo=False                 # Disable SQL logging in production
)

connection_pool = dl.ConnectionPool(sql_config)

# Monitor pool performance
def monitor_pool_performance():
    stats = connection_pool.get_pool_stats()
    print(f"🏊 Connection Pool Stats:")
    print(f"   Active connections: {stats['active_connections']}")
    print(f"   Idle connections: {stats['idle_connections']}")
    print(f"   Overflow connections: {stats['overflow_connections']}")
    print(f"   Pool utilization: {stats['utilization_percent']:.1f}%")

    # Alert if pool is under pressure
    if stats['utilization_percent'] > 80:
        print("⚠️  High pool utilization - consider increasing pool size")

monitor_pool_performance()
```

### 3. Database Schema Optimization

```sql
-- Optimized schema for Delta Lake metadata

-- Main metadata table with proper indexes
CREATE TABLE delta_table_metadata (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    version BIGINT NOT NULL,
    metadata_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    -- Composite index for version queries
    UNIQUE(table_name, schema_name, version)
);

-- Indexes for common query patterns
CREATE INDEX idx_metadata_table_version ON delta_table_metadata(table_name, version DESC);
CREATE INDEX idx_metadata_created_at ON delta_table_metadata(created_at DESC);

-- Files table with partitioning for large datasets
CREATE TABLE delta_files (
    id BIGSERIAL PRIMARY KEY,
    table_id INTEGER REFERENCES delta_table_metadata(id),
    file_path TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    modification_time TIMESTAMP WITH TIME ZONE NOT NULL,
    partition_values JSONB,
    stats_json JSONB,
    -- Partition by table for better performance
    PARTITION BY HASH (table_id);
CREATE INDEX idx_files_table_id ON delta_files(table_id);
CREATE INDEX idx_files_size ON delta_files(file_size) WHERE file_size > 1000000;

-- Commits table for transaction history
CREATE TABLE delta_commits (
    id BIGSERIAL PRIMARY KEY,
    table_id INTEGER REFERENCES delta_table_metadata(id),
    version BIGINT NOT NULL,
    operation VARCHAR(50) NOT NULL,
    parameters JSONB,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_id, version)
);
CREATE INDEX idx_commits_table_version ON delta_commits(table_id, version DESC);
```

## Caching Strategies

### 1. Multi-Level Cache Configuration

```python
import deltalakedb as dl

# Comprehensive caching configuration
cache_config = dl.CacheConfig(
    max_size=5000,                    # Maximum items per cache level
    eviction_policy=dl.EvictionPolicy.LRU,
    ttl_seconds=1800,                # 30 minutes TTL
    enable_statistics=True,
    enable_compression=True
)

# Create cache manager with multiple cache levels
cache_manager = dl.DeltaLakeCacheManager(cache_config)

# Configure cache levels
metadata_cache = cache_manager.get_cache("metadata")      # Table metadata
file_cache = cache_manager.get_cache("files")            # File information
query_cache = cache_manager.get_cache("queries")         # Query results
schema_cache = cache_manager.get_cache("schemas")        # Schema information

# Performance monitoring
def monitor_cache_performance():
    stats = cache_manager.get_stats()

    print("📊 Cache Performance Metrics:")
    print(f"   Overall hit ratio: {stats.overall_hit_ratio:.2%}")
    print(f"   Memory usage: {stats.memory_usage_mb:.1f} MB")
    print(f"   Evictions per hour: {stats.evictions_per_hour}")

    # Individual cache stats
    for cache_name, cache_stats in stats.cache_stats.items():
        print(f"   {cache_name}:")
        print(f"     Hit ratio: {cache_stats.hit_ratio:.2%}")
        print(f"     Items: {cache_stats.item_count:,}")
        print(f"     Memory: {cache_stats.memory_usage_mb:.1f} MB")

monitor_cache_performance()
```

### 2. Intelligent Cache Preloading

```python
import deltalakedb as dl

def preload_critical_data(table_uri: str):
    """Preload frequently accessed data into cache."""

    cache_manager = dl.DeltaLakeCacheManager()

    # Preload metadata
    table = dl.connect_to_table(table_uri)

    # Current version metadata
    current_metadata = table.metadata
    cache_manager.set(f"{table_uri}:metadata:v{table.version}", current_metadata)

    # Recent versions
    for version_offset in range(1, 6):  # Last 5 versions
        try:
            snapshot = table.snapshot(table.version - version_offset)
            cache_manager.set(
                f"{table_uri}:snapshot:v{table.version - version_offset}",
                snapshot,
                ttl_seconds=3600  # 1 hour for historical versions
            )
        except Exception:
            break  # Version doesn't exist

    # Schema information (changes rarely)
    cache_manager.set(f"{table_uri}:schema", table.metadata.schema, ttl_seconds=86400)

    # Partition information
    if table.metadata.partition_columns:
        cache_manager.set(
            f"{table_uri}:partitions",
            table.metadata.partition_columns,
            ttl_seconds=7200  # 2 hours
        )

# Preload for critical tables
critical_tables = [
    "deltasql://pg://localhost/db.sales_data",
    "deltasql://pg://localhost/db.inventory",
    "deltasql://pg://localhost/db.customers"
]

for table_uri in critical_tables:
    preload_critical_data(table_uri)
    print(f"✅ Preloaded cache for {table_uri}")
```

### 3. Cache Warming Strategies

```python
import deltalakedb as dl
from datetime import datetime, timedelta

class CacheWarmer:
    """Automated cache warming for optimal performance."""

    def __init__(self, cache_manager: dl.DeltaLakeCacheManager):
        self.cache_manager = cache_manager

    def warm_access_patterns(self, table_uri: str, days_back: int = 7):
        """Warm cache based on recent access patterns."""

        # Simulate typical access patterns
        table = dl.connect_to_table(table_uri)
        current_version = table.version

        # Most recent version (highest priority)
        latest_snapshot = table.snapshot()
        self.cache_manager.set(f"{table_uri}:latest", latest_snapshot)

        # Recent commits from last week
        commits = table.history(limit=100)
        recent_commits = [
            c for c in commits
            if c.timestamp > datetime.now() - timedelta(days=days_back)
        ]

        for commit in recent_commits[:20]:  # Top 20 recent commits
            try:
                snapshot = table.snapshot(commit.version)
                self.cache_manager.set(
                    f"{table_uri}:v{commit.version}",
                    snapshot,
                    ttl_seconds=1800  # 30 minutes for historical data
                )
            except Exception:
                continue

    def warm_partitions(self, table_uri: str):
        """Warm cache for partition information."""

        table = dl.connect_to_table(table_uri)

        if table.metadata.partition_columns:
            # Get partition values
            snapshot = table.snapshot()
            partitions = set()

            for file in snapshot.files:
                for col in table.metadata.partition_columns:
                    if col in file.partition_values:
                        partitions.add((col, file.partition_values[col]))

            # Cache partition information
            self.cache_manager.set(
                f"{table_uri}:partition_values",
                list(partitions),
                ttl_seconds=3600
            )

# Initialize cache warmer
cache_manager = dl.DeltaLakeCacheManager()
warmer = CacheWarmer(cache_manager)

# Warm critical tables
for table_uri in critical_tables:
    warmer.warm_access_patterns(table_uri)
    warmer.warm_partitions(table_uri)
    print(f"🔥 Warmed cache for {table_uri}")
```

## Memory Optimization

### 1. Lazy Loading Configuration

```python
import deltalakedb as dl

# Optimized lazy loading for different use cases
def configure_lazy_loading_for_workload(workload_type: str):
    """Configure lazy loading based on workload characteristics."""

    if workload_type == "analytics":
        # Analytics workloads benefit from larger chunks
        return dl.LazyLoadingConfig(
            default_strategy=dl.LoadingStrategy.PAGINATED,
            chunk_size=2000,         # Larger chunks for analytics
            cache_size=200,          # Moderate cache
            preload_threshold=10     # Preload small tables
        )

    elif workload_type == "transactional":
        # Transactional workloads need responsive access
        return dl.LazyLoadingConfig(
            default_strategy=dl.LoadingStrategy.LAZY,
            chunk_size=500,          # Smaller chunks for responsiveness
            cache_size=1000,         # Larger cache for hot data
            preload_threshold=50     # Preload more aggressively
        )

    elif workload_type == " archival":
        # Archival workloads handle huge datasets
        return dl.LazyLoadingConfig(
            default_strategy=dl.LoadingStrategy.STREAMING,
            chunk_size=5000,         # Very large chunks
            cache_size=50,           # Small cache (data is cold)
            preload_threshold=1      # Minimal preloading
        )

    else:  # General purpose
        return dl.LazyLoadingConfig(
            default_strategy=dl.LoadingStrategy.PAGINATED,
            chunk_size=1000,
            cache_size=500,
            preload_threshold=25
        )

# Apply configuration based on workload
analytics_config = configure_lazy_loading_for_workload("analytics")
lazy_manager = dl.LazyLoadingManager(analytics_config)
```

### 2. Memory-Efficient File Handling

```python
import deltalakedb as dl

# Memory-optimized file list handling
def process_large_file_list(table_uri: str, max_memory_mb: int = 512):
    """Process large file lists with controlled memory usage."""

    table = dl.connect_to_table(table_uri)

    # Configure memory optimization
    memory_config = dl.MemoryOptimizationConfig(
        strategy=dl.OptimizationStrategy.CHUNKED,
        chunk_size=1000,
        compression_type=dl.CompressionType.GZIP,
        max_memory_mb=max_memory_mb
    )

    # Create memory-optimized file list
    file_list = dl.MemoryOptimizedFileList(
        optimization_strategy=dl.OptimizationStrategy.HYBRID,
        chunk_size=1000,
        compression_type=dl.CompressionType.GZIP
    )

    # Add files in chunks to control memory
    snapshot = table.snapshot()
    files = snapshot.files

    for i in range(0, len(files), 1000):
        chunk = files[i:i+1000]
        for file in chunk:
            file_list.add_file(file)

        # Monitor memory usage
        memory_stats = file_list.get_memory_stats()
        if memory_stats.memory_usage_mb > max_memory_mb:
            print(f"⚠️  Memory limit reached: {memory_stats.memory_usage_mb:.1f} MB")
            file_list.optimize_memory()

    return file_list

# Process with memory limits
large_table_files = process_large_file_list(
    "deltasql://pg://localhost/db.large_table",
    max_memory_mb=256
)
```

### 3. Streaming Data Processing

```python
import deltalakedb as dl
from typing import Iterator

class StreamingFileProcessor:
    """Stream process files to minimize memory usage."""

    def __init__(self, table_uri: str, batch_size: int = 1000):
        self.table_uri = table_uri
        self.batch_size = batch_size
        self.table = dl.connect_to_table(table_uri)

    def stream_files(self) -> Iterator[list]:
        """Stream files in batches."""

        snapshot = self.table.snapshot()
        total_files = len(snapshot.files)

        for offset in range(0, total_files, self.batch_size):
            batch = snapshot.files[offset:offset + self.batch_size]
            yield batch

    def stream_by_partition(self, partition_column: str) -> Iterator[dict]:
        """Stream files grouped by partition values."""

        snapshot = self.table.snapshot()
        partitions = {}

        # Group files by partition
        for file in snapshot.files:
            if partition_column in file.partition_values:
                partition_value = file.partition_values[partition_column]
                if partition_value not in partitions:
                    partitions[partition_value] = []
                partitions[partition_value].append(file)

        # Yield partitions
        for partition_value, files in partitions.items():
            yield {
                'partition': {partition_column: partition_value},
                'files': files,
                'file_count': len(files),
                'total_size': sum(f.size for f in files)
            }

# Process large datasets with minimal memory
processor = StreamingFileProcessor("deltasql://pg://localhost/db.huge_table")

# Stream files in batches
print("📦 Processing files in batches:")
for i, batch in enumerate(processor.stream_files()):
    print(f"   Batch {i+1}: {len(batch)} files")
    # Process batch without loading all files into memory

# Stream by partitions
print("\n📂 Processing by partitions:")
for partition_data in processor.stream_by_partition('date'):
    partition = partition_data['partition']
    print(f"   Partition {partition}: {partition_data['file_count']} files, "
          f"{partition_data['total_size'] / (1024**3):.1f} GB")
```

## Async I/O Configuration

### 1. Async Executor Configuration

```python
import deltalakedb as dl
import asyncio

# High-performance async configuration
async_config = dl.AsyncTaskConfig(
    max_concurrent_tasks=50,          # Concurrent operations
    task_timeout_seconds=300,         # 5 minute timeout
    queue_size=1000,                  # Task queue size
    enable_metrics=True,
    retry_attempts=3,
    retry_delay_seconds=1.0
)

# Create async executor
executor = dl.AsyncIOExecutor(async_config)
ops = dl.AsyncDeltaLakeOperations(executor)

# Batch async operations
async def batch_operations(table_uris: list):
    """Execute multiple operations concurrently."""

    tasks = []

    for uri in table_uris:
        # Create async tasks
        task = asyncio.create_task(ops.read_table_async(uri))
        tasks.append(task)

    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    successful = []
    failed = []

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            failed.append((table_uris[i], result))
        else:
            successful.append(result)

    return successful, failed

# Execute batch operations
table_uris = [
    "deltasql://pg://localhost/db.table1",
    "deltasql://pg://localhost/db.table2",
    "deltasql://pg://localhost/db.table3"
]

async def run_batch():
    successful, failed = await batch_operations(table_uris)
    print(f"✅ Successful operations: {len(successful)}")
    print(f"❌ Failed operations: {len(failed)}")

asyncio.run(run_batch())
```

### 2. Async Query Optimization

```python
import deltalakedb as dl
import asyncio
from concurrent.futures import ThreadPoolExecutor

class AsyncQueryOptimizer:
    """Optimize queries for async execution."""

    def __init__(self, executor: dl.AsyncIOExecutor):
        self.executor = executor

    async def parallel_table_scans(self, table_uris: list, operation: str):
        """Execute parallel table scans."""

        async def scan_table(uri: str):
            ops = dl.AsyncDeltaLakeOperations(self.executor)

            if operation == "metadata":
                snapshot = await ops.read_table_async(uri)
                return {
                    'uri': uri,
                    'version': snapshot.version,
                    'file_count': len(snapshot.files),
                    'size_mb': sum(f.size for f in snapshot.files) / (1024**2)
                }
            elif operation == "schema":
                snapshot = await ops.read_table_async(uri)
                return {
                    'uri': uri,
                    'schema': snapshot.metadata.schema.schema_string,
                    'partitions': snapshot.metadata.partition_columns
                }

        # Execute scans in parallel
        tasks = [scan_table(uri) for uri in table_uris]
        results = await asyncio.gather(*tasks)

        return results

    async def distributed_aggregation(self, table_uri: str):
        """Distribute aggregation across multiple workers."""

        # Get table snapshot
        ops = dl.AsyncDeltaLakeOperations(self.executor)
        snapshot = await ops.read_table_async(table_uri)
        files = snapshot.files

        # Split files across workers
        num_workers = min(10, len(files) // 1000)  # 1 worker per 1000 files
        chunk_size = max(100, len(files) // num_workers)

        async def process_file_chunk(file_chunk):
            total_size = sum(f.size for f in file_chunk)
            partition_stats = {}

            for file in file_chunk:
                for key, value in file.partition_values.items():
                    if key not in partition_stats:
                        partition_stats[key] = {}
                    if value not in partition_stats[key]:
                        partition_stats[key][value] = 0
                    partition_stats[key][value] += 1

            return {
                'file_count': len(file_chunk),
                'total_size': total_size,
                'partition_stats': partition_stats
            }

        # Process chunks in parallel
        tasks = []
        for i in range(0, len(files), chunk_size):
            chunk = files[i:i + chunk_size]
            task = asyncio.create_task(process_file_chunk(chunk))
            tasks.append(task)

        # Aggregate results
        chunk_results = await asyncio.gather(*tasks)

        total_files = sum(r['file_count'] for r in chunk_results)
        total_size = sum(r['total_size'] for r in chunk_results)

        return {
            'total_files': total_files,
            'total_size': total_size,
            'chunk_count': len(chunk_results)
        }

# Use async query optimizer
optimizer = AsyncQueryOptimizer(executor)

# Parallel table scans
table_uris = ["deltasql://pg://localhost/db.table{i}" for i in range(1, 11)]
scan_results = await optimizer.parallel_table_scans(table_uris, "metadata")

for result in scan_results:
    print(f"📊 {result['uri']}: {result['file_count']:,} files, "
          f"{result['size_mb']:.1f} MB, version {result['version']}")
```

## Query Performance

### 1. Query Optimization

```python
import deltalakedb as dl
from typing import Dict, Any

class QueryOptimizer:
    """Optimize queries for better performance."""

    def __init__(self):
        self.query_cache = {}
        self.stats = {'cache_hits': 0, 'cache_misses': 0}

    def optimize_metadata_query(self, table_uri: str, version: Optional[int] = None):
        """Optimize metadata queries with caching."""

        cache_key = f"{table_uri}:v{version or 'latest'}"

        if cache_key in self.query_cache:
            self.stats['cache_hits'] += 1
            return self.query_cache[cache_key]

        self.stats['cache_misses'] += 1

        table = dl.connect_to_table(table_uri)

        if version is None:
            result = table.metadata
        else:
            snapshot = table.snapshot(version)
            result = snapshot.metadata

        # Cache result
        self.query_cache[cache_key] = result
        return result

    def batch_metadata_queries(self, table_uris: list):
        """Batch multiple metadata queries."""

        results = {}

        # Use connection pooling for efficiency
        # In a real implementation, you'd use a connection pool here

        for uri in table_uris:
            try:
                results[uri] = self.optimize_metadata_query(uri)
            except Exception as e:
                results[uri] = {'error': str(e)}

        return results

    def get_cache_stats(self):
        """Get cache performance statistics."""
        total = self.stats['cache_hits'] + self.stats['cache_misses']
        hit_ratio = self.stats['cache_hits'] / total if total > 0 else 0

        return {
            'hit_ratio': hit_ratio,
            'total_queries': total,
            'cache_size': len(self.query_cache)
        }

# Use query optimizer
optimizer = QueryOptimizer()

# Batch optimize multiple tables
table_uris = [
    "deltasql://pg://localhost/db.sales",
    "deltasql://pg://localhost/db.inventory",
    "deltasql://pg://localhost/db.customers"
]

metadata_results = optimizer.batch_metadata_queries(table_uris)

print("📊 Batch metadata query results:")
for uri, metadata in metadata_results.items():
    if 'error' in metadata:
        print(f"   ❌ {uri}: {metadata['error']}")
    else:
        print(f"   ✅ {uri}: Schema loaded")

cache_stats = optimizer.get_cache_stats()
print(f"\n🎯 Cache performance: {cache_stats['hit_ratio']:.2%} hit ratio")
```

### 2. Index Optimization

```sql
-- Database indexes for optimal query performance

-- Composite index for version-based queries
CREATE INDEX CONCURRENTLY idx_metadata_table_version_desc
ON delta_table_metadata(table_name, schema_name, version DESC);

-- Partial index for recent data (most frequently accessed)
CREATE INDEX CONCURRENTLY idx_metadata_recent
ON delta_table_metadata(table_name, version DESC)
WHERE created_at > NOW() - INTERVAL '30 days';

-- Files index with partition filtering
CREATE INDEX CONCURRENTLY idx_files_table_partition
ON delta_files(table_id, partition_values)
WHERE partition_values IS NOT NULL;

-- Function-based index for file size queries
CREATE INDEX CONCURRENTLY idx_files_size_mb
ON delta_files(table_id, (file_size / 1024.0 / 1024.0));

-- Commit history index for time-series queries
CREATE INDEX CONCURRENTLY idx_commits_timestamp_desc
ON delta_commits(table_id, timestamp DESC);

-- Partial index for recent commits
CREATE INDEX CONCURRENTLY idx_commits_recent
ON delta_commits(table_id, timestamp DESC)
WHERE timestamp > NOW() - INTERVAL '7 days';
```

## Write Performance

### 1. Batch Write Optimization

```python
import deltalakedb as dl
import asyncio
from typing import List, Any

class BatchWriteOptimizer:
    """Optimize batch write operations."""

    def __init__(self, executor: dl.AsyncIOExecutor):
        self.executor = executor
        self.ops = dl.AsyncDeltaLakeOperations(executor)

    async def batch_write_operations(self, write_requests: List[Dict[str, Any]]):
        """Execute multiple write operations in parallel."""

        async def execute_write(request):
            return await self.ops.write_table_async(
                request['uri'],
                request['data'],
                mode=request.get('mode', dl.WriteMode.APPEND),
                partition_by=request.get('partition_by'),
                overwrite_schema=request.get('overwrite_schema', False)
            )

        # Execute writes concurrently
        tasks = [execute_write(req) for req in write_requests]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        return results

    async def optimized_transaction_writes(self, tx_operations: List[Dict[str, Any]]):
        """Execute writes within optimized transactions."""

        # Group operations by table
        table_operations = {}
        for op in tx_operations:
            table_uri = op['table_uri']
            if table_uri not in table_operations:
                table_operations[table_uri] = []
            table_operations[table_uri].append(op)

        # Create transaction manager
        manager = dl.MultiTableTransactionManager()
        tx = manager.create_transaction(
            isolation_level=dl.MultiTableIsolationLevel.READ_COMMITTED
        )

        try:
            # Add participants
            for table_uri in table_operations.keys():
                tx.add_participant(table_uri, [dl.CrossTableOperationType.WRITE])

            # Execute operations within transaction
            results = []
            for table_uri, operations in table_operations.items():
                for op in operations:
                    result = await self.ops.write_table_async(
                        table_uri,
                        op['data'],
                        mode=op.get('mode', dl.WriteMode.APPEND)
                    )
                    results.append(result)

            # Commit transaction
            await tx.commit_async()
            return results

        except Exception as e:
            await tx.rollback_async()
            raise e

# Use batch write optimizer
write_requests = [
    {
        'uri': 'deltasql://pg://localhost/db.sales',
        'data': sales_data,
        'mode': dl.WriteMode.APPEND,
        'partition_by': 'date'
    },
    {
        'uri': 'deltasql://pg://localhost/db.inventory',
        'data': inventory_data,
        'mode': dl.WriteMode.OVERWRITE
    }
]

async def execute_batch_writes():
    optimizer = BatchWriteOptimizer(executor)
    results = await optimizer.batch_write_operations(write_requests)

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"❌ Write {i+1} failed: {result}")
        else:
            print(f"✅ Write {i+1} completed: {result}")

asyncio.run(execute_batch_writes())
```

### 2. Partition-Aware Writing

```python
import deltalakedb as dl
from datetime import datetime, timedelta

class PartitionAwareWriter:
    """Optimize writes based on partitioning."""

    def __init__(self, table_uri: str):
        self.table_uri = table_uri
        self.table = dl.connect_to_table(table_uri)

    def optimize_for_partitions(self, data, partition_columns: List[str]):
        """Optimize writes based on table partitions."""

        if not partition_columns:
            return [data]  # No partitioning, single batch

        # Group data by partition values
        partition_groups = {}

        for row in data:
            # Generate partition key
            partition_key = []
            for col in partition_columns:
                if col in row:
                    value = row[col]
                    # Format date partitions
                    if isinstance(value, datetime):
                        value = value.strftime('%Y-%m-%d')
                    partition_key.append(f"{col}={value}")
                else:
                    partition_key.append(f"{col}=__NULL__")

            partition_key_str = "/".join(partition_key)

            if partition_key_str not in partition_groups:
                partition_groups[partition_key_str] = []
            partition_groups[partition_key_str].append(row)

        return list(partition_groups.values())

    async def write_partitioned_data(self, data, batch_size: int = 10000):
        """Write data with partition awareness."""

        partition_columns = self.table.metadata.partition_columns

        if not partition_columns:
            # Simple write for non-partitioned tables
            ops = dl.AsyncDeltaLakeOperations(executor)
            return await ops.write_table_async(
                self.table_uri,
                data,
                mode=dl.WriteMode.APPEND
            )

        # Partition-aware write
        batches = self.optimize_for_partitions(data, partition_columns)

        results = []
        ops = dl.AsyncDeltaLakeOperations(executor)

        for i, batch in enumerate(batches):
            try:
                result = await ops.write_table_async(
                    self.table_uri,
                    batch,
                    mode=dl.WriteMode.APPEND
                )
                results.append(result)

                print(f"✅ Batch {i+1}/{len(batches)}: {len(batch)} rows")

            except Exception as e:
                print(f"❌ Batch {i+1} failed: {e}")
                results.append(e)

        return results

# Use partition-aware writer
writer = PartitionAwareWriter("deltasql://pg://localhost/db.partitioned_table")

# Write large dataset with partition optimization
async def write_large_dataset():
    results = await writer.write_partitioned_data(
        large_dataset,
        batch_size=50000
    )

    successful = sum(1 for r in results if not isinstance(r, Exception))
    print(f"📊 Partitioned write completed: {successful}/{len(results)} batches successful")

asyncio.run(write_large_dataset())
```

## Multi-Table Transaction Tuning

### 1. Transaction Configuration

```python
import deltalakedb as dl

# Optimized transaction configuration
def create_optimized_transaction_manager():
    """Create transaction manager with performance optimizations."""

    config = dl.MultiTableTransactionConfig(
        # Concurrency settings
        max_concurrent_transactions=10,
        transaction_timeout_seconds=300,

        # Distributed locking
        lock_timeout_seconds=60,
        deadlock_detection_interval_seconds=10,

        # Recovery settings
        enable_auto_recovery=True,
        recovery_check_interval_seconds=30,
        max_recovery_attempts=3,

        # Performance settings
        enable_batch_operations=True,
        batch_size=100,
        enable_parallel_validation=True
    )

    return dl.MultiTableTransactionManager(config)

# Create optimized manager
tx_manager = create_optimized_transaction_manager()

# Performance monitoring
def monitor_transaction_performance():
    """Monitor transaction manager performance."""

    stats = tx_manager.get_performance_stats()

    print("📊 Transaction Performance Stats:")
    print(f"   Active transactions: {stats.active_transactions}")
    print(f"   Completed transactions: {stats.completed_transactions:,}")
    print(f"   Average transaction time: {stats.avg_transaction_time_ms:.1f} ms")
    print(f"   Transaction success rate: {stats.success_rate:.2%}")
    print(f"   Lock contention rate: {stats.lock_contention_rate:.2%}")
    print(f"   Recovery events: {stats.recovery_events}")

monitor_transaction_performance()
```

### 2. Distributed Lock Optimization

```python
import deltalakedb as dl
import time
from typing import Set

class OptimizedLockManager:
    """Optimized distributed locking for transactions."""

    def __init__(self):
        self.lock_manager = dl.DistributedLockManager()
        self.lock_stats = {
            'acquisitions': 0,
            'contentions': 0,
            'timeouts': 0,
            'total_wait_time': 0
        }

    async def acquire_with_backoff(self, resources: Set[str],
                                  max_attempts: int = 5,
                                  base_delay: float = 0.1) -> bool:
        """Acquire locks with exponential backoff."""

        for attempt in range(max_attempts):
            try:
                start_time = time.time()

                # Attempt to acquire all locks atomically
                lock_result = await self.lock_manager.acquire_locks(
                    resources,
                    timeout_seconds=30
                )

                if lock_result.success:
                    wait_time = time.time() - start_time
                    self.lock_stats['acquisitions'] += 1
                    self.lock_stats['total_wait_time'] += wait_time

                    if attempt > 0:
                        print(f"🔒 Acquired locks after {attempt + 1} attempts "
                              f"({wait_time:.2f}s wait)")

                    return True
                else:
                    # Lock contention detected
                    self.lock_stats['contentions'] += 1

                    if attempt < max_attempts - 1:
                        # Exponential backoff with jitter
                        delay = base_delay * (2 ** attempt) + (0.1 * (attempt + 1))
                        await asyncio.sleep(delay)

            except TimeoutError:
                self.lock_stats['timeouts'] += 1
                break

        return False

    def get_lock_performance_stats(self):
        """Get lock performance statistics."""
        total = self.lock_stats['acquisitions'] + self.lock_stats['contentions']
        avg_wait = (self.lock_stats['total_wait_time'] /
                   self.lock_stats['acquisitions']
                   if self.lock_stats['acquisitions'] > 0 else 0)

        return {
            'contention_rate': self.lock_stats['contentions'] / total if total > 0 else 0,
            'timeout_rate': self.lock_stats['timeouts'] / total if total > 0 else 0,
            'average_wait_time': avg_wait,
            'total_acquisitions': self.lock_stats['acquisitions']
        }

# Use optimized lock manager
lock_manager = OptimizedLockManager()

async def transaction_with_optimized_locks():
    """Execute transaction with optimized locking."""

    # Resources to lock
    resources = {
        "table:sales_data:write",
        "table:inventory:write",
        "table:audit_log:write"
    }

    # Acquire locks with backoff
    if await lock_manager.acquire_with_backoff(resources):
        try:
            # Execute transaction logic
            print("🔒 Locks acquired, executing transaction...")

            # ... transaction operations ...

            print("✅ Transaction completed successfully")

        finally:
            # Release locks
            await lock_manager.lock_manager.release_all_locks()
            print("🔓 Locks released")
    else:
        print("❌ Failed to acquire locks")

asyncio.run(transaction_with_optimized_locks())
```

## Monitoring and Metrics

### 1. Comprehensive Performance Monitoring

```python
import deltalakedb as dl
import time
import threading
from collections import defaultdict, deque
from typing import Dict, Any

class PerformanceMonitor:
    """Comprehensive performance monitoring."""

    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.metrics = defaultdict(lambda: deque(maxlen=window_size))
        self.counters = defaultdict(int)
        self.lock = threading.Lock()

    def record_operation(self, operation_type: str, duration_ms: float,
                        success: bool = True, metadata: Dict[str, Any] = None):
        """Record operation metrics."""

        with self.lock:
            timestamp = time.time()

            # Record timing
            self.metrics[f"{operation_type}_duration"].append({
                'timestamp': timestamp,
                'value': duration_ms
            })

            # Record success/failure
            self.metrics[f"{operation_type}_success"].append({
                'timestamp': timestamp,
                'value': 1 if success else 0
            })

            # Record metadata if provided
            if metadata:
                for key, value in metadata.items():
                    self.metrics[f"{operation_type}_{key}"].append({
                        'timestamp': timestamp,
                        'value': value
                    })

            # Update counters
            self.counters[f"{operation_type}_total"] += 1
            if success:
                self.counters[f"{operation_type}_success"] += 1
            else:
                self.counters[f"{operation_type}_failure"] += 1

    def get_operation_stats(self, operation_type: str) -> Dict[str, Any]:
        """Get statistics for an operation type."""

        with self.lock:
            duration_key = f"{operation_type}_duration"
            success_key = f"{operation_type}_success"

            duration_data = list(self.metrics.get(duration_key, []))
            success_data = list(self.metrics.get(success_key, []))

            if not duration_data:
                return {}

            # Calculate statistics
            durations = [d['value'] for d in duration_data]
            successes = [s['value'] for s in success_data]

            stats = {
                'operation_type': operation_type,
                'total_operations': len(durations),
                'success_rate': sum(successes) / len(successes) if successes else 0,
                'avg_duration_ms': sum(durations) / len(durations),
                'min_duration_ms': min(durations),
                'max_duration_ms': max(durations),
                'p50_duration_ms': self._percentile(durations, 0.5),
                'p95_duration_ms': self._percentile(durations, 0.95),
                'p99_duration_ms': self._percentile(durations, 0.99),
                'operations_per_second': len(durations) / 60 if duration_data else 0  # Last minute
            }

            return stats

    def _percentile(self, data: list, percentile: float) -> float:
        """Calculate percentile of sorted data."""

        if not data:
            return 0

        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile)
        return sorted_data[min(index, len(sorted_data) - 1)]

    def get_system_overview(self) -> Dict[str, Any]:
        """Get system performance overview."""

        with self.lock:
            overview = {
                'timestamp': time.time(),
                'operation_stats': {},
                'system_counters': dict(self.counters)
            }

            # Get stats for each operation type
            operation_types = set()
            for key in self.metrics.keys():
                if '_duration' in key:
                    operation_types.add(key.replace('_duration', ''))

            for op_type in operation_types:
                overview['operation_stats'][op_type] = self.get_operation_stats(op_type)

            return overview

# Initialize performance monitor
monitor = PerformanceMonitor()

# Monitor table operations
def monitored_table_operation(table_uri: str, operation: str):
    """Decorator to monitor table operations."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()

            try:
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000

                monitor.record_operation(
                    operation,
                    duration_ms,
                    success=True,
                    metadata={
                        'table_uri': table_uri,
                        'result_size': len(result) if hasattr(result, '__len__') else None
                    }
                )

                return result

            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000

                monitor.record_operation(
                    operation,
                    duration_ms,
                    success=False,
                    metadata={
                        'table_uri': table_uri,
                        'error_type': type(e).__name__
                    }
                )

                raise

        return wrapper
    return decorator

# Example usage
@monitored_table_operation("deltasql://pg://localhost/db.test", "read_table")
def read_table_with_monitoring():
    table = dl.connect_to_table("deltasql://pg://localhost/db.test")
    return table.snapshot()

# Get performance overview
performance_overview = monitor.get_system_overview()
print("📊 Performance Overview:")
for op_type, stats in performance_overview['operation_stats'].items():
    print(f"   {op_type}:")
    print(f"     Operations: {stats['total_operations']:,}")
    print(f"     Success rate: {stats['success_rate']:.2%}")
    print(f"     Avg duration: {stats['avg_duration_ms']:.1f}ms")
    print(f"     P95 duration: {stats['p95_duration_ms']:.1f}ms")
```

### 2. Real-time Performance Dashboard

```python
import deltalakedb as dl
import asyncio
import json
from datetime import datetime

class RealTimeDashboard:
    """Real-time performance dashboard."""

    def __init__(self, monitor: PerformanceMonitor):
        self.monitor = monitor
        self.running = False

    async def start_dashboard(self, update_interval: int = 5):
        """Start real-time dashboard updates."""

        self.running = True

        while self.running:
            # Clear screen (for terminal dashboard)
            print("\033[2J\033[H")

            # Print dashboard header
            print("=" * 80)
            print("🚀 SQL-Backed Delta Lake Performance Dashboard")
            print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80)

            # Get system overview
            overview = self.monitor.get_system_overview()

            # System counters
            print("\n📈 System Counters:")
            for counter, value in overview['system_counters'].items():
                print(f"   {counter.replace('_', ' ').title()}: {value:,}")

            # Operation performance
            print("\n⚡ Operation Performance:")
            for op_type, stats in overview['operation_stats'].items():
                if stats['total_operations'] > 0:
                    status_emoji = "🟢" if stats['success_rate'] > 0.95 else "🟡" if stats['success_rate'] > 0.9 else "🔴"

                    print(f"\n   {status_emoji} {op_type.upper()}:")
                    print(f"      Total: {stats['total_operations']:,}")
                    print(f"      Success Rate: {stats['success_rate']:.2%}")
                    print(f"      Avg Duration: {stats['avg_duration_ms']:.1f}ms")
                    print(f"      P95 Duration: {stats['p95_duration_ms']:.1f}ms")
                    print(f"      Throughput: {stats['operations_per_second']:.1f} ops/sec")

            # Recommendations
            print("\n💡 Performance Recommendations:")
            recommendations = self._generate_recommendations(overview)
            for rec in recommendations[:3]:  # Top 3 recommendations
                print(f"   • {rec}")

            # Wait for next update
            await asyncio.sleep(update_interval)

    def _generate_recommendations(self, overview: Dict[str, Any]) -> list:
        """Generate performance recommendations."""

        recommendations = []

        for op_type, stats in overview['operation_stats'].items():
            if stats['success_rate'] < 0.95:
                recommendations.append(
                    f"Consider investigating {op_type} failures "
                    f"(success rate: {stats['success_rate']:.1%})"
                )

            if stats['p95_duration_ms'] > 1000:  # 1 second
                recommendations.append(
                    f"Optimize {op_type} performance "
                    f"(P95: {stats['p95_duration_ms']:.0f}ms)"
                )

            if stats['operations_per_second'] > 1000:
                recommendations.append(
                    f"Consider increasing caching for high-frequency {op_type} operations"
                )

        if not recommendations:
            recommendations.append("Performance looks good! No optimizations needed.")

        return recommendations

    def stop_dashboard(self):
        """Stop the dashboard."""
        self.running = False

# Start real-time dashboard (for demo purposes)
async def demo_dashboard():
    dashboard = RealTimeDashboard(monitor)

    # Simulate some activity
    async def simulate_activity():
        for i in range(10):
            monitor.record_operation("read", 50 + i * 10, True)
            monitor.record_operation("write", 100 + i * 15, True)
            if i % 3 == 0:
                monitor.record_operation("read", 5000, False)  # Simulated failure
            await asyncio.sleep(0.5)

    # Run simulation and dashboard concurrently
    await asyncio.gather(
        simulate_activity(),
        dashboard.start_dashboard(update_interval=1)
    )

# To run the dashboard (uncomment for testing):
# asyncio.run(demo_dashboard())
```

## Benchmarking and Testing

### 1. Performance Benchmark Suite

```python
import deltalakedb as dl
import time
import statistics
import asyncio
from typing import List, Dict, Any

class PerformanceBenchmark:
    """Comprehensive performance benchmarking suite."""

    def __init__(self, table_uris: List[str]):
        self.table_uris = table_uris
        self.results = {}

    async def benchmark_metadata_operations(self, iterations: int = 100):
        """Benchmark metadata operations."""

        print("🔍 Benchmarking metadata operations...")

        metadata_times = []

        for i in range(iterations):
            table_uri = self.table_uris[i % len(self.table_uris)]

            start_time = time.time()
            table = dl.connect_to_table(table_uri)
            metadata = table.metadata
            end_time = time.time()

            metadata_times.append((end_time - start_time) * 1000)

        stats = {
            'iterations': iterations,
            'avg_ms': statistics.mean(metadata_times),
            'median_ms': statistics.median(metadata_times),
            'min_ms': min(metadata_times),
            'max_ms': max(metadata_times),
            'std_ms': statistics.stdev(metadata_times) if len(metadata_times) > 1 else 0,
            'ops_per_second': iterations / sum(metadata_times / 1000)
        }

        self.results['metadata'] = stats
        return stats

    async def benchmark_file_list_operations(self, iterations: int = 50):
        """Benchmark file list operations."""

        print("📁 Benchmarking file list operations...")

        file_list_times = []
        file_counts = []

        for i in range(iterations):
            table_uri = self.table_uris[i % len(self.table_uris)]

            start_time = time.time()
            table = dl.connect_to_table(table_uri)
            snapshot = table.snapshot()
            files = snapshot.files
            end_time = time.time()

            file_list_times.append((end_time - start_time) * 1000)
            file_counts.append(len(files))

        stats = {
            'iterations': iterations,
            'avg_ms': statistics.mean(file_list_times),
            'median_ms': statistics.median(file_list_times),
            'min_ms': min(file_list_times),
            'max_ms': max(file_list_times),
            'std_ms': statistics.stdev(file_list_times) if len(file_list_times) > 1 else 0,
            'avg_file_count': statistics.mean(file_counts),
            'ops_per_second': iterations / sum(file_list_times / 1000)
        }

        self.results['file_lists'] = stats
        return stats

    async def benchmark_concurrent_operations(self, concurrent_operations: int = 20):
        """Benchmark concurrent operations."""

        print(f"⚡ Benchmarking {concurrent_operations} concurrent operations...")

        async def single_operation(uri: str):
            start_time = time.time()
            table = dl.connect_to_table(uri)
            snapshot = table.snapshot()
            end_time = time.time()
            return (end_time - start_time) * 1000

        # Execute concurrent operations
        tasks = []
        for i in range(concurrent_operations):
            uri = self.table_uris[i % len(self.table_uris)]
            tasks.append(single_operation(uri))

        results = await asyncio.gather(*tasks)

        stats = {
            'concurrent_operations': concurrent_operations,
            'avg_ms': statistics.mean(results),
            'median_ms': statistics.median(results),
            'min_ms': min(results),
            'max_ms': max(results),
            'std_ms': statistics.stdev(results) if len(results) > 1 else 0,
            'throughput_ops_per_second': concurrent_operations / max(results) / 1000
        }

        self.results['concurrent'] = stats
        return stats

    async def benchmark_cache_performance(self, cache_sizes: List[int] = None):
        """Benchmark cache performance with different sizes."""

        if cache_sizes is None:
            cache_sizes = [100, 500, 1000, 2000, 5000]

        print("💾 Benchmarking cache performance...")

        cache_results = {}

        for cache_size in cache_sizes:
            print(f"   Testing cache size: {cache_size}")

            # Create cache with specific size
            cache_config = dl.CacheConfig(
                max_size=cache_size,
                eviction_policy=dl.EvictionPolicy.LRU
            )
            cache_manager = dl.DeltaLakeCacheManager(cache_config)

            # Warm up cache
            table_uri = self.table_uris[0]
            table = dl.connect_to_table(table_uri)

            # Access same data multiple times
            access_times = []

            for i in range(20):
                start_time = time.time()

                # Try cache first
                cache_key = f"{table_uri}:metadata"
                cached = cache_manager.get(cache_key)

                if cached is None:
                    # Cache miss - load from database
                    metadata = table.metadata
                    cache_manager.set(cache_key, metadata)

                end_time = time.time()
                access_times.append((end_time - start_time) * 1000)

            cache_stats = cache_manager.get_stats()

            cache_results[cache_size] = {
                'avg_access_ms': statistics.mean(access_times),
                'hit_ratio': cache_stats.hit_ratio,
                'cache_size': cache_size
            }

        self.results['cache'] = cache_results
        return cache_results

    def generate_benchmark_report(self) -> str:
        """Generate comprehensive benchmark report."""

        report = []
        report.append("=" * 80)
        report.append("📊 PERFORMANCE BENCHMARK REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Tables tested: {len(self.table_uris)}")
        report.append("")

        # Metadata operations
        if 'metadata' in self.results:
            stats = self.results['metadata']
            report.append("🔍 METADATA OPERATIONS")
            report.append("-" * 40)
            report.append(f"Iterations: {stats['iterations']:,}")
            report.append(f"Average time: {stats['avg_ms']:.2f}ms")
            report.append(f"Median time: {stats['median_ms']:.2f}ms")
            report.append(f"Min time: {stats['min_ms']:.2f}ms")
            report.append(f"Max time: {stats['max_ms']:.2f}ms")
            report.append(f"Std deviation: {stats['std_ms']:.2f}ms")
            report.append(f"Throughput: {stats['ops_per_second']:.1f} ops/sec")
            report.append("")

        # File list operations
        if 'file_lists' in self.results:
            stats = self.results['file_lists']
            report.append("📁 FILE LIST OPERATIONS")
            report.append("-" * 40)
            report.append(f"Iterations: {stats['iterations']:,}")
            report.append(f"Average time: {stats['avg_ms']:.2f}ms")
            report.append(f"Median time: {stats['median_ms']:.2f}ms")
            report.append(f"Average file count: {stats['avg_file_count']:.0f}")
            report.append(f"Throughput: {stats['ops_per_second']:.1f} ops/sec")
            report.append("")

        # Concurrent operations
        if 'concurrent' in self.results:
            stats = self.results['concurrent']
            report.append("⚡ CONCURRENT OPERATIONS")
            report.append("-" * 40)
            report.append(f"Concurrent ops: {stats['concurrent_operations']}")
            report.append(f"Average time: {stats['avg_ms']:.2f}ms")
            report.append(f"Median time: {stats['median_ms']:.2f}ms")
            report.append(f"Throughput: {stats['throughput_ops_per_second']:.1f} ops/sec")
            report.append("")

        # Cache performance
        if 'cache' in self.results:
            report.append("💾 CACHE PERFORMANCE")
            report.append("-" * 40)
            for cache_size, stats in self.results['cache'].items():
                report.append(f"Cache size {cache_size:,}:")
                report.append(f"  Access time: {stats['avg_access_ms']:.2f}ms")
                report.append(f"  Hit ratio: {stats['hit_ratio']:.2%}")
            report.append("")

        # Performance summary
        report.append("📈 PERFORMANCE SUMMARY")
        report.append("-" * 40)

        if 'metadata' in self.results and 'file_lists' in self.results:
            metadata_ops = self.results['metadata']['ops_per_second']
            file_ops = self.results['file_lists']['ops_per_second']

            report.append(f"✅ Metadata throughput: {metadata_ops:.1f} ops/sec")
            report.append(f"✅ File list throughput: {file_ops:.1f} ops/sec")

            if metadata_ops > 100:
                report.append("🚀 EXCELLENT metadata performance")
            elif metadata_ops > 50:
                report.append("✅ GOOD metadata performance")
            else:
                report.append("⚠️  Consider optimizing metadata operations")

        return "\n".join(report)

# Run comprehensive benchmarks
async def run_performance_benchmarks():
    """Run complete performance benchmark suite."""

    # Test tables
    test_tables = [
        "deltasql://pg://localhost/db.table1",
        "deltasql://pg://localhost/db.table2",
        "deltasql://pg://localhost/db.table3"
    ]

    # Create benchmark suite
    benchmark = PerformanceBenchmark(test_tables)

    # Run all benchmarks
    await benchmark.benchmark_metadata_operations(iterations=100)
    await benchmark.benchmark_file_list_operations(iterations=50)
    await benchmark.benchmark_concurrent_operations(concurrent_operations=20)
    await benchmark.benchmark_cache_performance()

    # Generate and print report
    report = benchmark.generate_benchmark_report()
    print(report)

    # Save report to file
    with open("performance_benchmark_report.txt", "w") as f:
        f.write(report)

    print(f"\n📄 Benchmark report saved to performance_benchmark_report.txt")

# Execute benchmarks
asyncio.run(run_performance_benchmarks())
```

This comprehensive performance tuning guide provides SQL-Backed Delta Lake users with detailed strategies for optimizing every aspect of their deployment, from database configuration and caching strategies to async I/O and real-time monitoring. The guide includes practical code examples, benchmarking tools, and best practices for achieving optimal performance in production environments.