# DeltaLakeDB Python API

This document provides a comprehensive guide to using the DeltaLakeDB Python API to access Delta Lake tables backed by SQL metadata.

## Installation

```bash
pip install -e .
```

This requires Rust and Cargo to build the pyo3 bindings. For binary wheels (once available):

```bash
pip install delkalakedb
```

## Quick Start

### Connect to a Database

Create a connection using a DeltaSQL URI:

```python
from delkalakedb import DeltaSQL

# Postgres
conn = DeltaSQL("deltasql://postgres://user:pass@localhost/mydb?table=my_table")

# SQLite (file-based)
conn = DeltaSQL("deltasql://sqlite:///path/to/db.sqlite?table=my_table")

# DuckDB
conn = DeltaSQL("deltasql://duckdb:///path/to/db.duckdb?table=my_table")
```

### Read a Table Snapshot

```python
# Open table
table = conn.open_table("my_table")

# Get current snapshot
snapshot = table.snapshot()

print(f"Version: {snapshot.version}")
print(f"Files: {len(snapshot.files)}")
print(f"Schema: {snapshot.metadata.schema_json}")
print(f"Partitions: {snapshot.metadata.partition_columns}")

# Iterate over active files
for file in snapshot.files:
    print(f"  {file.path} ({file.size} bytes)")
```

### Time Travel: Read Historical Snapshots

```python
# By version
table_v5 = table.version(5)
snapshot_v5 = table_v5.snapshot()
print(f"Version 5 snapshot: {snapshot_v5.version}")

# By timestamp (ISO 8601 format)
table_past = table.at_timestamp("2024-12-18T10:00:00Z")
snapshot_past = table_past.snapshot()
print(f"Snapshot at 10:00 UTC: {snapshot_past.version}")

# Chain operations
files_at_version_3 = table.version(3).snapshot().files
```

## Advanced Usage

### Single-Table Transactions

Write data to a Delta table using a transaction:

```python
# Begin a transaction
txn = conn.begin_transaction("my_table")

# Add files to the transaction
txn.add_file(
    path="/path/to/data/new_file.parquet",
    size=1024,
    modification_time=1702905600000  # milliseconds since epoch
)
txn.add_file(
    path="/path/to/data/another_file.parquet",
    size=2048,
    modification_time=1702905600000
)

# Remove files if needed
txn.remove_file(path="/path/to/data/old_file.parquet")

# Update metadata
from delkalakedb import TableMetadata, Protocol

metadata = TableMetadata(
    schema_json='{"type":"struct","fields":[...]}',  # Arrow schema as JSON
    partition_columns=["year", "month"],
    configuration='{"key":"value"}'
)
txn.set_metadata(metadata)

# Commit the transaction (atomically persists all changes)
new_version = txn.commit()
print(f"Committed as version {new_version}")
```

### Multi-Table Transactions

Atomically update multiple Delta tables in a single database transaction:

```python
# Create a transaction builder
builder = conn.transaction_builder()

# Stage actions for table A
builder.add_table_actions(
    table_id="table_a",
    actions=[
        'add_file("/data/new_a.parquet")',
        'remove_file("/data/old_a.parquet")',
    ]
)

# Stage actions for table B
builder.add_table_actions(
    table_id="table_b",
    actions=[
        'add_file("/data/new_b.parquet")',
    ]
)

# Verify staged state
print(f"Staged tables: {builder.staged_tables()}")

# Commit atomically (all or nothing)
try:
    result = builder.commit()
    print(f"Committed versions: {result}")
    # result = {"table_a": 42, "table_b": 15}
except ConcurrencyError as e:
    print(f"Conflict: {e}")
    # One or more tables had version conflicts; entire transaction rolled back
```

## Error Handling

### ConcurrencyError

Raised when a write operation encounters a version conflict (another writer already updated the table):

```python
from delkalakedb import ConcurrencyError

try:
    new_version = txn.commit()
except ConcurrencyError as e:
    print(f"Version conflict: {e}")
    # Re-read the current snapshot and retry
    current = table.snapshot()
```

### ConnectionError

Raised when database connection fails:

```python
from delkalakedb import ConnectionError

try:
    table = conn.open_table("my_table")
except ConnectionError as e:
    print(f"Database connection failed: {e}")
```

### ValidationError

Raised on invalid input (e.g., malformed URI, invalid table name):

```python
from delkalakedb import ValidationError

try:
    conn = DeltaSQL("invalid://uri")
except ValidationError as e:
    print(f"Invalid URI: {e}")
```

## Type Hints and IDE Support

All types are fully type-hinted for use with `mypy` and IDE autocomplete:

```python
# mypy understands types automatically
snapshot: Snapshot = table.snapshot()
version: int = snapshot.version
files: List[ActiveFile] = snapshot.files

for file in files:
    path: str = file.path
    size: int = file.size
```

Validate your code with mypy:

```bash
mypy --strict your_script.py
```

## Observability

### Logging

DeltaLakeDB emits structured logs for all operations:

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Perform operations
table = conn.open_table("my_table")
snapshot = table.snapshot()  # Logs: table open, snapshot read latency
```

### Metrics

Metrics are automatically collected and can be exported via OpenTelemetry:

```python
# (Future: OpenTelemetry integration)
# from delkalakedb import MetricsExporter
# exporter = MetricsExporter("prometheus://localhost:8000")
```

## Example: Data Pipeline

Here's a complete example of a data ingestion pipeline:

```python
from delkalakedb import DeltaSQL, ConcurrencyError, Protocol, TableMetadata
from datetime import datetime
import time

# Connect to the metadata database
conn = DeltaSQL("deltasql://postgres://user:pass@localhost/mydb?table=events")
table = conn.open_table("events")

# Get current state
current = table.snapshot()
print(f"Current version: {current.version}")
print(f"Files: {len(current.files)}")

# Process new data
new_files = [
    "/data/events/2024-12-18/batch_1.parquet",
    "/data/events/2024-12-18/batch_2.parquet",
]

# Write new data
max_retries = 3
for attempt in range(max_retries):
    try:
        txn = conn.begin_transaction("events")
        
        for file_path in new_files:
            txn.add_file(
                path=file_path,
                size=1024 * 1024,  # 1MB (placeholder)
                modification_time=int(datetime.now().timestamp() * 1000)
            )
        
        new_version = txn.commit()
        print(f"✓ Successfully committed version {new_version}")
        break
        
    except ConcurrencyError:
        if attempt < max_retries - 1:
            print(f"Conflict (attempt {attempt + 1}/{max_retries}), retrying...")
            time.sleep(1)
        else:
            print(f"✗ Failed after {max_retries} attempts")
            raise

# Verify
updated = table.snapshot()
print(f"New version: {updated.version}")
print(f"New file count: {len(updated.files)}")
```

## Performance Tips

1. **Reuse connections**: Create one `DeltaSQL` instance and reuse it across operations
2. **Batch writes**: Use multi-table transactions to write multiple tables efficiently
3. **Cache snapshots**: If you need multiple snapshots at the same version, cache the result
4. **Time-travel sparingly**: Querying old versions may be slower on large tables

## Migration from Pure Delta Lake

If you have an existing Delta table, bootstrap the SQL metadata using the migration CLI:

```bash
python -m delkalakedb import /path/to/delta/table \
  --dsn "postgresql://user:pass@localhost/mydb" \
  --schema public \
  --table my_table
```

This reads the `_delta_log/` directory and populates the SQL metadata. Your existing readers and writers continue to work.

## API Reference

### DeltaSQL

Main connection class for accessing Delta tables via SQL metadata.

**Methods**:
- `open_table(name: str) -> Table` - Open a table for reading
- `list_tables(schema: Optional[str] = None) -> List[str]` - List tables in catalog
- `begin_transaction(table_name: str) -> Transaction` - Start a single-table transaction
- `transaction_builder() -> TransactionBuilder` - Start a multi-table transaction builder

### Table

Represents a Delta table at a specific version.

**Methods**:
- `snapshot() -> Snapshot` - Get current snapshot
- `version(v: int) -> Table` - Time-travel to version
- `at_timestamp(ts: str) -> Table` - Time-travel to timestamp

**Properties**:
- `current_version: int` - Current version number
- `location: str` - Table location (S3 path, etc.)

### Snapshot

Immutable view of a table at a specific version.

**Properties**:
- `version: int` - Version number
- `timestamp_millis: int` - Commit timestamp
- `files: List[ActiveFile]` - Active data files
- `protocol: Protocol` - Protocol version constraints
- `metadata: TableMetadata` - Schema, partitions, properties

### Transaction

Single-table write transaction.

**Methods**:
- `add_file(path, size, modification_time)` - Add a file
- `remove_file(path)` - Remove a file
- `set_metadata(metadata)` - Update table metadata
- `commit() -> int` - Commit atomically

### TransactionBuilder

Multi-table transaction builder.

**Methods**:
- `add_table_actions(table_id, actions)` - Stage actions for a table
- `staged_tables() -> List[str]` - List staged tables
- `commit() -> Dict[str, int]` - Commit all tables atomically

### Error Types

- `ConcurrencyError` - Version conflict during commit
- `ConnectionError` - Database connection failure
- `ValidationError` - Invalid input
- `NotFoundError` - Resource not found

## FAQ

**Q: What happens if my code crashes between snapshot read and commit?**
A: Snapshots are read-only. Your transaction isn't started until you call `commit()`. If you crash before that, no changes are persisted.

**Q: Can I use this with my existing Delta readers?**
A: Yes! External Delta readers use `_delta_log/`, which is mirrored automatically by DeltaLakeDB.

**Q: What if two processes write the same table?**
A: The second writer gets a `ConcurrencyError`. Retry with a fresh snapshot.

**Q: Is async/await supported?**
A: Not yet, but it's on the roadmap (Phase 5+).

## Contributing

Issues and PRs are welcome at https://github.com/your-org/deltalakeDB

## License

MIT OR Apache-2.0
