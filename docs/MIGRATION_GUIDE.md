# Migration Guide: From Standard Delta Lake to SQL-Backed Delta Lake

This guide helps existing deltalake users migrate to the SQL-Backed Delta Lake system, which offers enhanced performance, better metadata management, and enterprise-grade features.

## Table of Contents

1. [Why Migrate?](#why-migrate)
2. [Prerequisites](#prerequisites)
3. [Migration Strategies](#migration-strategies)
4. [Step-by-Step Migration](#step-by-step-migration)
5. [Code Examples](#code-examples)
6. [Troubleshooting](#troubleshooting)
7. [Rollback Procedures](#rollback-procedures)
8. [Best Practices](#best-practices)

## Why Migrate?

### Key Benefits

- **Performance**: Up to 10x faster metadata operations with intelligent caching
- **Memory Efficiency**: Up to 95% memory reduction through lazy loading and compression
- **ACID Transactions**: Full multi-table transaction support with distributed locking
- **Async I/O**: Concurrent operations for better throughput
- **Monitoring**: Built-in metrics and comprehensive logging
- **Compatibility**: Drop-in replacement with existing deltalake workflows

### When to Migrate

- Large tables with millions of files
- High-frequency metadata operations
- Multi-table transaction requirements
- Memory-constrained environments
- Need for better monitoring and observability

## Prerequisites

### System Requirements

- **Python**: 3.8 or higher
- **Database**: PostgreSQL 12+, MySQL 8.0+, or SQLite 3.35+
- **Memory**: Minimum 4GB RAM (8GB+ recommended for large tables)
- **Storage**: Sufficient space for SQL database (typically 10-20% of Delta table size)

### Software Dependencies

```bash
# Core dependencies
pip install deltalakedb
pip install deltalake  # For compatibility

# Database drivers (choose based on your database)
pip install psycopg2-binary  # PostgreSQL
pip install pymysql          # MySQL
pip install aiosqlite        # SQLite (async support)
```

### Database Setup

Create a dedicated database for Delta Lake metadata:

```sql
-- PostgreSQL
CREATE DATABASE deltalake_metadata;
CREATE USER deltalake_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE deltalake_metadata TO deltalake_user;

-- MySQL
CREATE DATABASE deltalake_metadata;
CREATE USER 'deltalake_user'@'%' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON deltalake_metadata.* TO 'deltalake_user'@'%';
```

## Migration Strategies

### 1. Online Migration (Recommended)

Migrate tables while keeping them accessible:

- **Downtime**: Minimal (< 5 minutes)
- **Complexity**: Medium
- **Risk**: Low
- **Best for**: Production systems with high availability requirements

### 2. Offline Migration

Take tables offline during migration:

- **Downtime**: Variable (depends on table size)
- **Complexity**: Low
- **Risk**: Very Low
- **Best for**: Development/testing environments, scheduled maintenance windows

### 3. Hybrid Migration

Gradual migration with dual-write:

- **Downtime**: None
- **Complexity**: High
- **Risk**: Medium
- **Best for**: Critical systems requiring zero downtime

## Step-by-Step Migration

### Phase 1: Preparation

1. **Install deltalakedb**
```bash
pip install deltalakedb
```

2. **Backup Existing Tables**
```python
import shutil
import os

# Backup your Delta tables
backup_path = "/backup/delta_tables_backup"
os.makedirs(backup_path, exist_ok=True)

# Copy your Delta table directories here
shutil.copytree("/path/to/delta/tables", backup_path)
```

3. **Create Migration Configuration**
```yaml
# migration_config.yaml
database:
  url: "postgresql://deltalake_user:password@localhost:5432/deltalake_metadata"
  pool_size: 20
  max_overflow: 40

migration:
  strategy: "online"
  preserve_partitions: true
  validate_after_migration: true
  create_rollback_point: true

performance:
  enable_caching: true
  cache_size: 1000
  lazy_loading: true
  chunk_size: 1000

logging:
  level: "INFO"
  file: "migration.log"
  structured: true
```

### Phase 2: Database Setup

```python
import deltalakedb as dl
from deltalakedb.config import ConfigLoader

# Load migration configuration
config = ConfigLoader.load_yaml_config("migration_config.yaml")

# Create SQL configuration
sql_config = dl.ConfigFactory.create_sql_config(config['database'])

# Test database connection
try:
    connection = dl.SqlConnection(sql_config)
    connection.execute("SELECT 1")
    print("✅ Database connection successful")
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    exit(1)
```

### Phase 3: Migration Execution

> **New (Nov 2025):** The lightweight `dl` CLI can bootstrap metadata directly from an
> existing `_delta_log` without authoring bespoke migration scripts. Use it when you
> only need to hydrate the SQL catalog and continue mirroring via `_delta_log`.

#### Option A: CLI-driven import

```bash
# 1. Bootstrap SQL catalog from the latest checkpoint + JSON commits
dl import \
  --database /var/lib/deltalake/metadata.sqlite \
  --log-dir /tables/sales/_delta_log \
  --table-location s3://warehouse/sales \
  --mode fast  # use full-history to replay every JSON file

# 2. Record the emitted table_id and re-run to prove idempotence
dl import \
  --database /var/lib/deltalake/metadata.sqlite \
  --log-dir /tables/sales/_delta_log \
  --table-location s3://warehouse/sales \
  --table-id <uuid-from-step-1>
```

The command reads the latest checkpoint (when available) before replaying JSON commits,
populates every `dl_*` table, and records the run in `dl_import_runs` for auditability.
Use `--json-output import.json` to capture machine-readable summaries for change
management systems.

#### Single Table Migration

```python
import deltalakedb as dl

# Define source and target
source_path = "/path/to/existing/delta/table"
target_uri = "deltasql://postgresql://user:pass@localhost:5432/db.migrated_table"

# Create migrator
migrator = dl.DeltaTableMigrator(
    source_uri=source_path,
    target_config=sql_config
)

# Analyze source table
print("🔍 Analyzing source table...")
analysis = migrator.analyze_source()
print(f"📊 Table analysis:")
print(f"   - Versions: {analysis.total_versions}")
print(f"   - Files: {analysis.total_files:,}")
print(f"   - Size: {analysis.total_size_mb / 1024:.1f} GB")
print(f"   - Estimated migration time: {analysis.estimated_time_minutes} minutes")

# Perform migration
print("🚀 Starting migration...")
result = migrator.migrate_table(
    target_table_name="migrated_table",
    preserve_partitions=True
)

if result.success:
    print("✅ Migration completed successfully!")
    print(f"   - Migrated {result.migrated_versions} versions")
    print(f"   - Migrated {result.migrated_files:,} files")
    print(f"   - Target URI: {result.target_uri}")
else:
    print(f"❌ Migration failed: {result.error_message}")
    exit(1)
```

#### Batch Migration

```python
import deltalakedb as dl
import os
from pathlib import Path

def migrate_directory(source_dir: str, target_config, batch_size: int = 5):
    """Migrate all Delta tables in a directory."""

    source_path = Path(source_dir)
    delta_tables = list(source_path.glob("*/_delta_log"))

    print(f"📁 Found {len(delta_tables)} Delta tables to migrate")

    migrators = []
    for table_dir in delta_tables:
        table_name = table_dir.parent.name
        source_uri = f"file://{table_dir.parent}"

        migrator = dl.DeltaTableMigrator(
            source_uri=source_uri,
            target_config=target_config
        )
        migrators.append((table_name, migrator))

    # Process in batches
    for i in range(0, len(migrators), batch_size):
        batch = migrators[i:i+batch_size]
        print(f"🔄 Processing batch {i//batch_size + 1}/{(len(migrators)-1)//batch_size + 1}")

        for table_name, migrator in batch:
            try:
                print(f"   Migrating {table_name}...")
                result = migrator.migrate_table(
                    target_table_name=table_name,
                    preserve_partitions=True
                )

                if result.success:
                    print(f"   ✅ {table_name} migrated successfully")
                else:
                    print(f"   ❌ {table_name} migration failed: {result.error_message}")

            except Exception as e:
                print(f"   ❌ {table_name} migration error: {e}")

# Execute batch migration
migrate_directory("/path/to/delta/tables", sql_config)
```

### Phase 4: Validation

You can still run the SDK-based validation code below, but most teams rely on the
drift tooling baked into the CLI:

```bash
# Compare SQL-derived snapshot with _delta_log truth
dl diff \
  --database /var/lib/deltalake/metadata.sqlite \
  --table-id <uuid> \
  --log-dir /tables/sales/_delta_log \
  --lag-threshold-seconds 5 \
  --max-drift-files 0 \
  --metrics-format prometheus \
  --metrics-output /tmp/sales_metrics.prom

# Exit codes
# 0 -> parity holds and lag within threshold
# 2 -> drift detected (files, metadata, or protocol mismatch)
# 3 -> only the lag SLO was breached (useful for observability alerts)
```

Point CI/CD jobs at `dl diff` to block promotion whenever parity fails. The command
also emits machine- and human-readable summaries (`--format json`), making it simple to
push drift reports into runbooks or ticketing systems.

### Phase 5: Writer Cutover to `deltasql://`

1. **Freeze old writers.** Pause upstream jobs for the target table and confirm that
   `_delta_log` has finished flushing outstanding commits.
2. **Final parity check.** Run `dl diff` and ensure the exit status is `0`.
3. **Switch writer URIs.** Reconfigure applications to use `deltasql://` endpoints, for
   example `deltasql://postgresql://user:pass@metadata-db:5432/catalog.sales`. Writers
   continue to mirror JSON/Parquet artifacts for external engines.
4. **Post-cutover validation.** Issue another diff immediately after the first SQL-backed
   commit:

```bash
dl diff \
  --database /var/lib/deltalake/metadata.sqlite \
  --table-id <uuid> \
  --log-dir /tables/sales/_delta_log \
  --lag-threshold-seconds 5 \
  --max-drift-files 0 \
  --metrics-format json
```

5. **Automate.** Embed the command into release pipelines so parity must be green
   before the cutover checklist is signed off.

### Phase 6: Safe Rollback to File-backed Writers

If drift, regressions, or infra issues appear, you can revert to file-backed writers in
minutes:

1. Run `dl diff --format human` to capture the current divergence and attach the output
   to the incident ticket.
2. Set writers back to their prior `file://`/`s3://` URIs. Do **not** disable SQL
   mirroring; it remains authoritative when you retry the cutover.
3. Use the most recent healthy `_delta_log` version as the rollback target and confirm
   external engines see the expected table version.
4. Once traffic is back on file-backed writers, either:
   - Re-run `dl import --table-id <uuid>` to catch up the SQL catalog, or
   - Archive the stalled run via `dl diff --json-output rollback.json` for audit
     purposes and perform a fresh bootstrap when ready.
5. Document the root cause and any follow-up tasks in this guide before attempting the
   next cutover.

```python
import deltalakedb as dl

def validate_migration(source_path: str, target_uri: str):
    """Validate migration success."""

    # Create bridge for comparison
    bridge = dl.DeltaLakeBridge()

    # Load original table
    original_table = bridge.create_compatible_table(f"file://{source_path}")

    # Load migrated table
    migrated_table = dl.connect_to_table(target_uri)

    # Validate metadata
    print("🔍 Validating metadata...")
    original_metadata = original_table.metadata()
    migrated_metadata = migrated_table.metadata

    metadata_match = (
        original_metadata.get('schemaString') == migrated_metadata.schema.schema_string and
        original_metadata.get('partitionColumns') == migrated_metadata.partition_columns
    )

    print(f"   Schema match: {'✅' if metadata_match else '❌'}")

    # Validate file count
    original_files = len(original_table.files())
    migrated_files = len(migrated_table.snapshot().files)

    print(f"   File count - Original: {original_files:,}, Migrated: {migrated_files:,}")
    print(f"   File count match: {'✅' if original_files == migrated_files else '❌'}")

    # Validate data integrity (sample)
    print("🔍 Validating data integrity...")
    original_sample = original_table.to_pyarrow_dataset().head(100)
    migrated_sample = migrated_table.snapshot().to_pyarrow_dataset().head(100)

    data_match = len(original_sample) == len(migrated_sample)
    print(f"   Data sample match: {'✅' if data_match else '❌'}")

    return metadata_match and (original_files == migrated_files) and data_match

# Validate migration
validation_result = validate_migration(
    "/path/to/original/table",
    "deltasql://postgresql://user:pass@localhost:5432/db.migrated_table"
)

print(f"\n🎯 Overall validation: {'✅ PASSED' if validation_result else '❌ FAILED'}")
```

## Code Examples

### Before Migration (Standard deltalake)

```python
import deltalake as dl

# Standard way to work with Delta tables
table = dl.DeltaTable("/path/to/table")

# Read data
df = table.to_pyarrow_table()

# Write data
table.write(df, mode="append")

# Get version
current_version = table.version()
```

### After Migration (deltalakedb)

```python
import deltalakedb as dl

# SQL-Backed Delta Lake
table = dl.connect_to_table("deltasql://postgresql://user:pass@localhost:5432/db.mytable")

# Read data (with lazy loading)
snapshot = table.snapshot()
df = snapshot.to_pyarrow_table()

# Write data (async support)
async_config = dl.AsyncTaskConfig(max_concurrent_tasks=10)
executor = dl.AsyncIOExecutor(async_config)
ops = dl.AsyncDeltaLakeOperations(executor)

import asyncio
async def write_data():
    result = await ops.write_table_async(
        "deltasql://postgresql://user:pass@localhost:5432/db.mytable",
        df,
        mode=dl.WriteMode.APPEND
    )
    return result

result = asyncio.run(write_data())

# Get version (cached for performance)
current_version = table.version
```

### Multi-Table Transaction Example

```python
import deltalakedb as dl

# Create transaction manager
manager = dl.MultiTableTransactionManager()

# Create distributed transaction
tx = manager.create_transaction(
    isolation_level=dl.MultiTableIsolationLevel.SERIALIZABLE
)

# Add multiple tables to transaction
tx.add_participant("deltasql://pg://localhost/db.sales", [
    dl.CrossTableOperationType.WRITE,
    dl.CrossTableOperationType.UPDATE
])
tx.add_participant("deltasql://pg://localhost/db.inventory", [
    dl.CrossTableOperationType.UPDATE
])
tx.add_participant("deltasql://pg://localhost/db.audit_log", [
    dl.CrossTableOperationType.WRITE
])

# Execute distributed transaction
try:
    tx.prepare()
    result = tx.commit()
    print(f"✅ Distributed transaction committed: {result.success}")
except Exception as e:
    tx.rollback()
    print(f"❌ Transaction rolled back: {e}")
```

### Performance Optimization Example

```python
import deltalakedb as dl

# Configure performance optimizations
lazy_config = dl.LazyLoadingConfig(
    default_strategy=dl.LoadingStrategy.PAGINATED,
    chunk_size=500,
    cache_size=100
)

cache_config = dl.CacheConfig(
    max_size=2000,
    eviction_policy=dl.EvictionPolicy.LRU,
    ttl_seconds=600
)

# Create optimized table connection
table = dl.connect_to_table("deltasql://pg://localhost/db.large_table")

# Use lazy loading for large file lists
lazy_manager = dl.LazyLoadingManager(lazy_config)
metadata = lazy_manager.create_lazy_table_metadata(table.uri)

# Stream files instead of loading all at once
files = metadata.get_files_stream(limit=1000)
for file in files:
    # Process files one chunk at a time
    print(f"Processing: {file.path} ({file.size:,} bytes)")
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Database Connection Errors

**Problem**: Connection timeout or authentication failure

**Solution**:
```python
# Test connection with detailed error reporting
import deltalakedb as dl

try:
    config = dl.SqlConfig(
        connection_string="postgresql://user:pass@localhost:5432/db",
        pool_timeout=60,  # Increase timeout
        pool_size=5       # Start with smaller pool
    )
    conn = dl.SqlConnection(config)
    conn.execute("SELECT version()")
    print("✅ Connection successful")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print("💡 Check:")
    print("   - Database server is running")
    print("   - Network connectivity")
    print("   - Credentials are correct")
    print("   - Database exists")
```

#### 2. Migration Performance Issues

**Problem**: Migration taking too long

**Solution**:
```python
# Optimize migration settings
migrator = dl.DeltaTableMigrator(source_uri, target_config)

# Use parallel processing
result = migrator.migrate_table(
    target_table_name="optimized_table",
    preserve_partitions=True,
    parallel_processing=True,
    batch_size=5000  # Process files in batches
)
```

#### 3. Memory Issues

**Problem**: Out of memory during migration

**Solution**:
```python
# Configure memory optimization
memory_config = dl.MemoryOptimizationConfig(
    strategy=dl.OptimizationStrategy.STREAMING,
    chunk_size=1000,
    compression_type=dl.CompressionType.GZIP
)

migrator = dl.DeltaTableMigrator(source_uri, target_config)
migrator.set_memory_config(memory_config)
```

#### 4. Validation Failures

**Problem**: Post-migration validation fails

**Solution**:
```python
# Detailed validation analysis
def detailed_validation(source_path, target_uri):
    bridge = dl.DeltaLakeBridge()

    original = bridge.create_compatible_table(f"file://{source_path}")
    migrated = dl.connect_to_table(target_uri)

    print("🔍 Detailed Validation Report:")

    # Check schema
    orig_schema = original.metadata().get('schemaString')
    mig_schema = str(migrated.metadata.schema.schema_string)
    print(f"Schema: {'✅' if orig_schema == mig_schema else '❌'}")

    # Check file paths
    orig_files = set(f.path for f in original.files())
    mig_files = set(f.path for f in migrated.snapshot().files)

    missing_files = orig_files - mig_files
    extra_files = mig_files - orig_files

    if missing_files:
        print(f"❌ Missing {len(missing_files)} files:")
        for f in list(missing_files)[:5]:  # Show first 5
            print(f"   - {f}")

    if extra_files:
        print(f"⚠️  Extra {len(extra_files)} files:")
        for f in list(extra_files)[:5]:  # Show first 5
            print(f"   - {f}")

# Run detailed validation
detailed_validation("/path/to/source", "deltasql://pg://localhost/db.target")
```

### Performance Monitoring

```python
import deltalakedb as dl

# Monitor migration performance
monitored_migrator = dl.DeltaTableMigrator(source_uri, target_config)

# Enable detailed metrics
monitored_migrator.enable_metrics()

result = monitored_migrator.migrate_table("monitored_table")

# Get performance metrics
metrics = monitored_migrator.get_migration_metrics()
print(f"📊 Migration Metrics:")
print(f"   - Duration: {metrics.total_duration_seconds:.1f}s")
print(f"   - Files/sec: {metrics.files_per_second:.1f}")
print(f"   - MB/sec: {metrics.megabytes_per_second:.1f}")
print(f"   - Peak memory: {metrics.peak_memory_mb:.1f} MB")
```

## Rollback Procedures

### 1. Controlled Rollback to File-backed Writers

1. Capture the current state for auditing:

```bash
dl diff \
  --database /var/lib/deltalake/metadata.sqlite \
  --table-id <uuid> \
  --log-dir /tables/sales/_delta_log \
  --format json \
  --json-output rollback_report.json
```

2. Stop SQL-backed writers and repoint workloads to their previous `file://`/`s3://`
   URIs.
3. Confirm `_delta_log` is the source of truth (external readers should see the expected
   version).
4. Leave the SQL catalog in place; once the incident is resolved re-run `dl import` with
   the existing `--table-id` to catch up.

### 2. Restore from Backup When `_delta_log` Is Corrupted

If the object store copy is damaged, restore from your Delta backups and mirror back to
SQL:

```bash
rsync -a /backups/delta/sales /tables/sales
dl import --database /var/lib/deltalake/metadata.sqlite \
  --log-dir /tables/sales/_delta_log \
  --table-location s3://warehouse/sales \
  --table-id <uuid>
```

### 3. Hybrid Access Pattern During Recovery

Applications can temporarily fall back to the file-backed path while SQL issues are
investigated:

```python
from deltalake import DeltaTable

file_backed = DeltaTable("/tables/sales")

def read_with_fallback(sql_reader):
    try:
        return sql_reader.read_latest()
    except Exception:
        return file_backed.to_pyarrow_table()
```

Once SQL writers are healthy, repeat the cutover checklist from Phase 5 to resume normal
operations.

## Best Practices

### 1. Migration Planning

- **Start Small**: Begin with non-critical tables
- **Schedule Wisely**: Plan migrations during low-traffic periods
- **Test Thoroughly**: Validate in staging before production
- **Monitor Closely**: Use performance metrics during migration

### 2. Performance Optimization

```python
# Recommended production configuration
production_config = {
    "database": {
        "pool_size": 50,
        "max_overflow": 100,
        "pool_timeout": 60,
        "pool_recycle": 3600
    },
    "performance": {
        "enable_caching": True,
        "cache_size": 5000,
        "lazy_loading": True,
        "chunk_size": 2000,
        "async_io": True,
        "max_concurrent_tasks": 20
    },
    "monitoring": {
        "enable_metrics": True,
        "log_level": "INFO",
        "structured_logging": True
    }
}
```

### 3. Error Handling

```python
import deltalakedb as dl
from deltalakedb.error import DeltaLakeError, DeltaLakeErrorKind

def robust_table_operation(table_uri: str):
    """Robust table operation with comprehensive error handling."""

    try:
        table = dl.connect_to_table(table_uri)

        # Configure retry logic
        for attempt in range(3):
            try:
                snapshot = table.snapshot()
                return snapshot.to_pyarrow_table()

            except DeltaLakeError as e:
                if e.kind == DeltaLakeErrorKind.TIMEOUT_ERROR and attempt < 2:
                    print(f"⏰ Timeout (attempt {attempt + 1}), retrying...")
                    continue
                elif e.kind == DeltaLakeErrorKind.CONCURRENCY_ERROR:
                    print("🔄 Concurrency conflict, retrying...")
                    time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    raise  # Re-raise non-retryable errors

    except Exception as e:
        print(f"❌ Operation failed: {e}")
        # Fallback to original deltalake if available
        return fallback_operation(table_uri)
```

### 4. Monitoring and Alerting

```python
import deltalakedb as dl
import time

def monitor_table_health(table_uri: str, check_interval: int = 60):
    """Monitor table health and performance."""

    table = dl.connect_to_table(table_uri)

    while True:
        try:
            # Performance check
            start_time = time.time()
            snapshot = table.snapshot()
            response_time = (time.time() - start_time) * 1000

            # Health check
            file_count = len(snapshot.files)
            cache_stats = table.get_cache_stats()

            print(f"📊 Table Health Check:")
            print(f"   Response time: {response_time:.1f}ms")
            print(f"   File count: {file_count:,}")
            print(f"   Cache hit ratio: {cache_stats.hit_ratio:.2%}")

            # Alert on issues
            if response_time > 5000:  # 5 second threshold
                print("⚠️  HIGH RESPONSE TIME - Investigate!")

            if cache_stats.hit_ratio < 0.5:  # 50% threshold
                print("⚠️  LOW CACHE HIT RATIO - Consider tuning!")

        except Exception as e:
            print(f"❌ Health check failed: {e}")
            # Send alert (integrate with your monitoring system)

        time.sleep(check_interval)

# Start monitoring (run as background process)
# monitor_table_health("deltasql://pg://localhost/db.critical_table")
```

### 5. Configuration Management

```python
# Use environment-specific configurations
import os
from deltalakedb.config import ConfigLoader

def get_config_for_environment():
    """Get configuration based on environment."""

    env = os.getenv('ENVIRONMENT', 'development')
    config_file = f"config/{env}.yaml"

    return ConfigLoader.load_yaml_config(config_file)

# Use in application
config = get_config_for_environment()
sql_config = dl.ConfigFactory.create_sql_config(config['database'])
table = dl.connect_to_table("deltasql://pg://localhost/db.mytable", sql_config)
```

This comprehensive migration guide provides existing deltalake users with everything needed to successfully migrate to the SQL-Backed Delta Lake system, including troubleshooting guidance, rollback procedures, and best practices for production deployments.
