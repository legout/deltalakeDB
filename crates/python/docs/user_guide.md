# DeltaLake DB Python User Guide

## Table of Contents

1. [Getting Started](#getting-started)
2. [Quick Start](#quick-start)
3. [Core Concepts](#core-concepts)
4. [Working with Tables](#working-with-tables)
5. [Data Operations](#data-operations)
6. [Multi-table Transactions](#multi-table-transactions)
7. [Performance Optimization](#performance-optimization)
8. [Configuration](#configuration)
9. [Migration](#migration)
10. [Logging and Monitoring](#logging-and-monitoring)
11. [Best Practices](#best-practices)
12. [Troubleshooting](#troubleshooting)

## Getting Started

### Prerequisites

- Python 3.8 or higher
- A supported SQL database (PostgreSQL, MySQL, SQLite, SQL Server)
- Familiarity with Delta Lake concepts

### Installation

```bash
# Install from PyPI
pip install deltalakedb-python

# Install with optional dependencies
pip install deltalakedb-python[yaml,toml,async]

# Install development version
pip install deltalakedb-python[dev]
```

### Database Setup

#### PostgreSQL
```sql
CREATE DATABASE delta_db;
CREATE USER delta_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE delta_db TO delta_user;
```

#### MySQL
```sql
CREATE DATABASE delta_db;
CREATE USER 'delta_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON delta_db.* TO 'delta_user'@'localhost';
FLUSH PRIVILEGES;
```

## Quick Start

### Basic Table Operations

```python
import deltalakedb
from deltalakedb import Table, SqlConfig

# Create configuration
config = SqlConfig(
    database_type="postgresql",
    connection_string="postgresql://delta_user:your_password@localhost/delta_db"
)

# Connect to existing table or create new one
table = Table("delta+sql://postgresql://delta_user:your_password@localhost/delta_db/my_table")

# Get table information
snapshot = table.get_version()
print(f"Table version: {snapshot.version}")
print(f"Created: {snapshot.timestamp}")

# List files
files = table.get_files()
print(f"Number of files: {len(files)}")

# Get commit history
history = table.history(limit=5)
for commit in history:
    print(f"Version {commit.version}: {commit.operation}")
```

### Writing Data

```python
import pandas as pd
from deltalakedb.write_operations import WriteMode

# Create sample data
data = pd.DataFrame({
    'id': range(1, 1001),
    'name': [f'item_{i}' for i in range(1, 1001)],
    'value': [i * 2 for i in range(1, 1001)],
    'category': ['A', 'B', 'C'] * 333 + ['A']
})

# Write data
writer = deltalakedb.write_operations.DeltaWriter(table)
result = writer.write(
    data,
    mode=WriteMode.APPEND,
    partition_by=['category']
)

print(f"Wrote {result.files_added} files")
print(f"Added {result.rows_added} rows")
```

## Core Concepts

### Delta SQL URI

DeltaLake DB uses special URIs to connect to SQL-backed Delta tables:

```
delta+sql://[connection_string]/[table_name]?[options]
```

Examples:
```python
# PostgreSQL
uri = "delta+sql://postgresql://user:pass@localhost/delta_db/my_table"

# MySQL
uri = "delta+sql://mysql://user:pass@localhost:3306/delta_db/my_table"

# SQLite
uri = "delta+sql://sqlite:///path/to/database.db/my_table"

# With options
uri = "delta+sql://postgresql://user:pass@localhost/delta_db/my_table?table_prefix=delta_&batch_size=1000"
```

### Table Versions and Snapshots

Delta tables are versioned, and each version represents a snapshot of the table at a point in time:

```python
# Get latest version
latest = table.get_version()

# Get specific version
v10 = table.get_version(10)

# Compare versions
print(f"Latest version: {latest.version}")
print(f"Version 10 files: {len(v10.files)}")
```

### Schemas

Delta tables have strongly typed schemas:

```python
# Get table schema
metadata = table.get_metadata()
schema = metadata.schema

# Print schema
for field in schema.fields:
    print(f"{field.name}: {field.data_type} (nullable: {field.nullable})")

# Check if column exists
if schema.get_field('id'):
    print("ID column exists")

# Get partition columns
print(f"Partition columns: {schema.partition_columns}")
```

## Working with Tables

### Creating Tables

```python
from deltalakedb.types import TableSchema, SchemaField, DeltaDataType

# Define schema
schema = TableSchema([
    SchemaField("id", DeltaDataType.primitive("long"), nullable=False),
    SchemaField("name", DeltaDataType.primitive("string")),
    SchemaField("created_at", DeltaDataType.primitive("timestamp")),
    SchemaField("metadata", DeltaDataType.struct([
        SchemaField("source", DeltaDataType.primitive("string")),
        SchemaField("version", DeltaDataType.primitive("integer"))
    ]))
])

# Create table
new_table = deltalakedb.create_table(
    "delta+sql://postgresql://user:pass@localhost/delta_db/new_table",
    schema,
    config
)
```

### Listing Tables

```python
# List all tables
tables = deltalakedb.list_tables(config)
print("Available tables:")
for table_name in tables:
    print(f"  - {table_name}")
```

### Table History

```python
# Get commit history
history = table.history(limit=10)

for commit in history:
    print(f"Version {commit.version}:")
    print(f"  Operation: {commit.operation}")
    print(f"  User: {commit.user_id}")
    print(f"  Timestamp: {commit.timestamp}")
    print(f"  Actions: {len(commit.actions)}")
    print()
```

## Data Operations

### Reading Data

```python
# Using DeltaTable compatibility
from deltalake import DeltaTable

# Load table
dt = deltalakedb.load_table("delta+sql://postgresql://user:pass@localhost/delta_db/my_table")

# Read as pandas DataFrame
df = dt.to_pandas()
print(f"Data shape: {df.shape}")

# Read specific version
df_v5 = dt.as_version(5).to_pandas()

# Read with filters
df_filtered = dt.to_pandas(filters=[("id", ">", 100)])
```

### Writing Data

```python
# Different write modes
writer = deltalakedb.write_operations.DeltaWriter(table)

# Append data
writer.write(new_data, mode=WriteMode.APPEND)

# Overwrite all data
writer.write(new_data, mode=WriteMode.OVERWRITE)

# Overwrite with partition pruning
writer.write(
    new_data,
    mode=WriteMode.OVERWRITE_DYNAMIC,
    partition_by=['category']
)

# Ignore if exists
writer.write(new_data, mode=WriteMode.IGNORE)
```

### Batch Operations

```python
# Write large datasets in batches
import itertools

def data_generator():
    # Generate data in chunks
    for i in range(0, 100000, 1000):
        yield pd.DataFrame({
            'id': range(i, i + 1000),
            'value': [j * 2 for j in range(i, i + 1000)]
        })

writer = deltalakedb.write_operations.DeltaWriter(table)
results = writer.write_batch(data_generator())

for result in results:
    print(f"Batch result: {result.rows_added} rows added")
```

## Multi-table Transactions

### Basic Transactions

```python
from deltalakedb.multi_table_transactions import create_transaction_context

# Create transaction
transaction = create_transaction_context(
    tables=[table1, table2, table3],
    timeout_seconds=60
)

# Add operations
transaction.add_participant(table1, [
    {"operation": "write", "data": data1}
])

transaction.add_participant(table2, [
    {"operation": "delete", "condition": "status = 'inactive'"}
])

transaction.add_participant(table3, [
    {"operation": "update", "condition": "category = 'A'", "updates": {"flag": True}}
])

# Execute transaction
try:
    result = transaction.commit()
    print(f"Transaction committed: {result.success}")
    print(f"Tables affected: {result.affected_tables}")
except Exception as e:
    print(f"Transaction failed: {e}")
    transaction.rollback()
```

### Transaction Context Manager

```python
from deltalakedb.multi_table_transactions import execute_in_transaction

def business_operation(transaction):
    """Business logic executed within transaction."""
    # Read from table1
    snapshot1 = table1.get_version()

    # Process data
    processed_data = process_data(snapshot1.data)

    # Write to table2
    transaction.add_participant(table2, [
        {"operation": "write", "data": processed_data}
    ])

    # Update table3
    transaction.add_participant(table3, [
        {"operation": "update", "condition": "processed = false", "updates": {"processed": True}}
    ])

    return "Business operation completed"

# Execute with automatic commit/rollback
try:
    result = execute_in_transaction([table1, table2, table3], business_operation)
    print(f"Success: {result.message}")
except Exception as e:
    print(f"Transaction failed: {e}")
```

### Isolation Levels

```python
from deltalakedb.multi_table_transactions import MultiTableIsolationLevel

# Different isolation levels
serializable = create_transaction_context(
    tables=[table1, table2],
    isolation_level=MultiTableIsolationLevel.SERIALIZABLE
)

repeatable_read = create_transaction_context(
    tables=[table1, table2],
    isolation_level=MultiTableIsolationLevel.REPEATABLE_READ
)

read_committed = create_transaction_context(
    tables=[table1, table2],
    isolation_level=MultiTableIsolationLevel.READ_COMMITTED
)
```

## Performance Optimization

### Lazy Loading

```python
from deltalakedb.lazy_loading import LazyLoadingConfig, LoadingStrategy

# Configure lazy loading for large tables
lazy_config = LazyLoadingConfig(
    strategy=LoadingStrategy.PAGINATED,
    chunk_size=1000,
    cache_size=100,
    prefetch_count=5
)

# Use with table
lazy_manager = deltalakedb.lazy_loading.create_lazy_loading_manager(lazy_config)
metadata = lazy_manager.load_table_metadata(table)

# Access data as needed
for chunk in metadata.get_files_chunked():
    process_files(chunk)
```

### Caching

```python
from deltalakedb.caching import DeltaLakeCacheManager

# Setup cache manager
cache_manager = deltalakedb.caching.create_deltalake_cache_manager()

# Cache frequently accessed data
metadata = table.get_metadata()
cache_manager.cache_table_metadata(table, metadata)

# Get cached data
cached_metadata = cache_manager.get(f"table:{table.uri}:metadata")
if cached_metadata:
    print("Using cached metadata")

# Monitor cache performance
stats = cache_manager.get_stats()
print(f"Cache hit ratio: {stats.hit_ratio:.2%}")
```

### Memory Optimization

```python
from deltalakedb.memory_optimization import MemoryOptimizedFileList, OptimizationStrategy

# Handle large file lists
file_list = MemoryOptimizedFileList(
    strategy=OptimizationStrategy.HYBRID
)

# Add files
file_list.add_files(table.get_files())

# Process in chunks
chunk_size = 1000
total_files = file_list.get_file_count()

for i in range(0, total_files, chunk_size):
    files_chunk = file_list.get_files(offset=i, limit=chunk_size)
    process_files(files_chunk)

# Monitor memory usage
memory_stats = file_list.get_memory_usage()
print(f"Memory usage: {memory_stats.current_mb:.2f} MB")
```

### Async Operations

```python
import asyncio
from deltalakedb.async_io import AsyncIOExecutor

async def async_table_operations():
    # Create async executor
    executor = deltalakedb.async_io.create_async_executor()

    # Execute queries concurrently
    tasks = [
        executor.execute_query_async("SELECT COUNT(*) FROM table1"),
        executor.execute_query_async("SELECT COUNT(*) FROM table2"),
        executor.execute_query_async("SELECT * FROM table3 WHERE status = 'active'")
    ]

    results = await asyncio.gather(*tasks)

    for i, result in enumerate(results):
        print(f"Query {i+1}: {len(result)} rows")

    # Batch operations
    batch_ops = [
        {"query": "UPDATE table1 SET status = 'processed' WHERE id = %s", "params": [1]},
        {"query": "UPDATE table2 SET status = 'processed' WHERE id = %s", "params": [2]},
    ]

    batch_results = await executor.batch_execute_async(batch_ops)
    print(f"Batch results: {batch_results}")

# Run async operations
asyncio.run(async_table_operations())
```

## Configuration

### Configuration Files

#### YAML Configuration
```yaml
# config.yaml
database:
  type: postgresql
  connection_string: "postgresql://user:pass@localhost/delta_db"
  table_prefix: "delta_"

connection:
  timeout: 30
  max_connections: 10
  retry_attempts: 3

performance:
  batch_size: 1000
  cache_size: 100
  lazy_loading: true
  async_operations: true

logging:
  level: INFO
  format: structured
  file: "deltalake.log"
```

#### TOML Configuration
```toml
[database]
type = "postgresql"
connection_string = "postgresql://user:pass@localhost/delta_db"
table_prefix = "delta_"

[connection]
timeout = 30
max_connections = 10
retry_attempts = 3

[performance]
batch_size = 1000
cache_size = 100
lazy_loading = true
async_operations = true

[logging]
level = "INFO"
format = "structured"
file = "deltalake.log"
```

### Loading Configuration

```python
from deltalakedb.pydantic_models import ConfigLoader

# Load from YAML
config = ConfigLoader.load_yaml_config("config.yaml")

# Load from TOML
config = ConfigLoader.load_toml_config("config.toml")

# Validate configuration
errors = ConfigLoader.validate_config_file("config.yaml")
if errors:
    for error in errors:
        print(f"Config error: {error}")

# Generate sample configurations
ConfigLoader.generate_sample_configs("./configs")
```

### Environment Variables

```python
import os
from deltalakedb import SqlConfig

# Use environment variables
config = SqlConfig(
    database_type=os.getenv("DELTA_DB_TYPE", "postgresql"),
    connection_string=os.getenv("DELTA_DB_CONNECTION"),
    table_prefix=os.getenv("DELTA_TABLE_PREFIX", "delta_"),
    connection_timeout=int(os.getenv("DELTA_DB_TIMEOUT", "30"))
)
```

## Migration

### Migrating from File-based Delta Tables

```python
from deltalakedb.migration import DeltaTableMigrator, MigrationStrategy

# Create migrator
migrator = DeltaTableMigrator(
    source_uri="/path/to/delta/table",
    target_config=config
)

# Analyze migration
status = migrator.analyze_migration()
print(f"Source files: {status.source_file_count}")
print(f"Estimated size: {status.estimated_size_mb} MB")
print(f"Estimated time: {status.estimated_time_minutes} minutes")

# Execute migration
result = migrator.execute_migration(
    strategy=MigrationStrategy.INCREMENTAL,
    batch_size=1000
)

print(f"Migration completed: {result.success}")
print(f"Files migrated: {result.files_migrated}")
print(f"Rows migrated: {result.rows_migrated}")
```

### Migration Strategies

```python
# Full migration - copies all data at once
full_result = migrator.execute_migration(
    strategy=MigrationStrategy.FULL,
    batch_size=5000
)

# Incremental migration - processes in batches
incremental_result = migrator.execute_migration(
    strategy=MigrationStrategy.INCREMENTAL,
    batch_size=1000,
    resume_from_last_checkpoint=True
)

# Streaming migration - processes data as it's read
streaming_result = migrator.execute_migration(
    strategy=MigrationStrategy.STREAMING,
    batch_size=100
)
```

### Validation and Rollback

```python
# Validate migration
validation_errors = migrator.validate_migration()
if validation_errors:
    print("Migration validation failed:")
    for error in validation_errors:
        print(f"  - {error}")
else:
    print("Migration validation passed")

# Rollback if needed
try:
    # Test the migrated table
    test_operations()
except Exception as e:
    print(f"Migration test failed: {e}")
    print("Rolling back migration...")
    migrator.rollback_migration()
```

## Logging and Monitoring

### Setting Up Logging

```python
from deltalakedb.logging import setup_logging, DeltaLogger

# Setup structured logging
setup_logging(
    level="INFO",
    format="structured",
    file_path="deltalake.log",
    include_performance=True
)

# Create logger
logger = DeltaLogger("my_app")

# Log operations
logger.info("Starting table operation", table_name="users", operation="read")
logger.log_operation("write", "orders", row_count=1000, duration=2.5)
logger.log_performance("query", 0.15, rows_returned=500, query_type="select")
```

### Monitoring Performance

```python
# Monitor cache performance
cache_stats = cache_manager.get_stats()
print(f"Cache performance:")
print(f"  Hit ratio: {cache_stats.hit_ratio:.2%}")
print(f"  Miss ratio: {cache_stats.miss_ratio:.2%}")
print(f"  Size: {cache_stats.current_size}")
print(f"  Evictions: {cache_stats.eviction_count}")

# Monitor memory usage
memory_stats = file_list.get_memory_usage()
print(f"Memory performance:")
print(f"  Current usage: {memory_stats.current_mb:.2f} MB")
print(f"  Peak usage: {memory_stats.peak_mb:.2f} MB")
print(f"  Optimization savings: {memory_stats.optimization_savings_mb:.2f} MB")

# Monitor async operations
async_stats = async_executor.get_stats()
print(f"Async performance:")
print(f"  Tasks completed: {async_stats.completed_tasks}")
print(f"  Average duration: {async_stats.average_duration:.2f}s")
print(f"  Concurrency: {async_stats.current_concurrency}")
```

## Best Practices

### Performance

1. **Use appropriate batch sizes** for your data volume
2. **Enable caching** for frequently accessed metadata
3. **Use lazy loading** for large tables
4. **Leverage async operations** for I/O-bound workloads
5. **Partition data** appropriately for your query patterns

### Reliability

1. **Use transactions** for multi-table operations
2. **Implement proper error handling** and retry logic
3. **Monitor memory usage** and optimize for large datasets
4. **Validate data** before and after operations
5. **Use appropriate isolation levels** for transactions

### Security

1. **Use environment variables** for sensitive configuration
2. **Implement proper access controls** on the database
3. **Validate all inputs** and use parameterized queries
4. **Audit operations** using the logging framework
5. **Use connection pooling** to manage database connections

### Configuration

1. **Externalize configuration** using YAML/TOML files
2. **Validate configuration** at startup
3. **Use different configs** for development, staging, and production
4. **Document configuration options** for your team
5. **Version control** configuration files (excluding secrets)

## Troubleshooting

### Common Issues

#### Connection Problems

```python
# Test connection
try:
    table = Table("delta+sql://postgresql://user:pass@localhost/delta_db/test")
    snapshot = table.get_version()
    print("Connection successful")
except Exception as e:
    print(f"Connection failed: {e}")

    # Check configuration
    if "connection" in str(e).lower():
        print("Check connection string and database availability")
    elif "authentication" in str(e).lower():
        print("Check credentials and permissions")
```

#### Memory Issues

```python
# Monitor memory usage
import psutil
import os

process = psutil.Process(os.getpid())
memory_mb = process.memory_info().rss / 1024 / 1024
print(f"Current memory usage: {memory_mb:.2f} MB")

# Enable memory optimization if needed
if memory_mb > 1000:  # 1GB
    print("High memory usage detected, enabling optimization")
    file_list.optimize()
```

#### Performance Issues

```python
# Profile operations
import time

start_time = time.time()
snapshot = table.get_version()
end_time = time.time()

print(f"Operation took {end_time - start_time:.2f} seconds")

# Check cache effectiveness
cache_stats = cache_manager.get_stats()
if cache_stats.hit_ratio < 0.5:
    print("Low cache hit ratio, consider adjusting cache strategy")
```

### Debug Mode

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
deltalakedb.logging.setup_logging(level="DEBUG")

# Enable detailed performance logging
deltalakedb.logging.setup_logging(
    level="DEBUG",
    include_performance=True,
    include_sql=True
)
```

### Getting Help

1. **Check the logs** for detailed error information
2. **Validate configuration** using the provided utilities
3. **Use the testing framework** to isolate issues
4. **Monitor system resources** during operations
5. **Consult the API documentation** for proper usage

This user guide provides comprehensive coverage of DeltaLake DB Python functionality, helping users get the most out of the library while following best practices for performance, reliability, and maintainability.