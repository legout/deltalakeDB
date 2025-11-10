# DeltaLakeDB Python Bindings

Python bindings for SQL-Backed Delta Lake metadata system, providing seamless integration with the Python data ecosystem while maintaining compatibility with existing deltalake workflows.

## Features

- **SQL-Backed Metadata**: Store Delta Lake metadata in SQL databases for better performance and reliability
- **URI Scheme Support**: Use the `deltasql://` scheme for easy database connections
- **Connection Management**: Built-in connection pooling and transaction support
- **Type Safety**: Full type system integration with Python type hints
- **Compatibility Layer**: Seamless compatibility with existing deltalake workflows
- **Error Handling**: Comprehensive error handling with helpful suggestions
- **CLI Utilities**: Command-line tools for common operations

## Quick Start

### Installation

```bash
pip install deltalakedb
```

### Basic Usage

```python
import deltalakedb as dl

# Connect to a Delta table
table, conn = dl.connect("deltasql://postgres://localhost:5432/mydb#my_table")

# Create a new table
table = dl.create(
    "deltasql://postgres://localhost:5432/mydb",
    name="my_table",
    description="My first Delta table",
    partition_columns=["date", "country"]
)

# List all tables
tables = dl.list("deltasql://postgres://localhost:5432/mydb")
for table in tables:
    print(f"Table: {table.name}, Version: {table.version}")

# Work with connections
conn = dl.SqlConnection("deltasql://sqlite:///my_database.db")
print(f"Connected to {conn.database_type} database")

# Parse and validate URIs
uri = dl.DeltaSqlUri.parse("deltasql://postgres://localhost:5432/mydb#my_table")
print(f"Database type: {uri.database_type}")
print(f"Table name: {uri.table_name}")
```

## URI Scheme

The `deltasql://` URI scheme provides a unified way to connect to different databases:

```
deltasql://[database_type]://[connection_string]/[table_name]?[parameters]
```

Examples:

- PostgreSQL: `deltasql://postgres://localhost:5432/mydb#my_table`
- MySQL: `deltasql://mysql://localhost:3306/mydb#my_table`
- SQLite: `deltasql://sqlite:///path/to/database.db#my_table`
- DuckDB: `deltasql://duckdb:///path/to/database.duckdb#my_table`

## Configuration

### SQL Configuration

```python
config = dl.SqlConfig(
    database_type="postgres",
    pool_size=10,
    timeout=30,
    ssl_mode="require"
)

conn = dl.SqlConnection("deltasql://postgres://localhost/mydb", config)
```

### Connection Pooling

```python
pool = dl.ConnectionPool(max_size=20)
conn = pool.get_connection("deltasql://postgres://localhost/mydb")
```

### Transactions

```python
tx = dl.TransactionContext(connection)
tx.begin()
try:
    # Perform operations
    conn.create_table("new_table", ...)
    tx.commit()
except Exception:
    tx.rollback()
```

## Type System

DeltaLakeDB provides a rich type system for defining table schemas:

```python
from deltalakedb import DeltaDataType, SchemaField, TableSchema

# Define a schema
fields = [
    SchemaField("id", DeltaDataType.Primitive("long"), nullable=False),
    SchemaField("name", DeltaDataType.Primitive("string"), nullable=True),
    SchemaField("data", DeltaDataType.Array(DeltaDataType.Primitive("double")), nullable=True),
]

schema = TableSchema(
    fields=fields,
    partition_columns=["date"],
    schema_id="schema-123"
)
```

## Error Handling

DeltaLakeDB provides detailed error information with suggestions:

```python
try:
    conn = dl.SqlConnection("deltasql://invalid://uri")
except dl.DeltaLakeError as e:
    print(f"Error: {e.message}")
    print(f"Kind: {e.kind}")
    print(f"Suggestions: {e.get_suggestions()}")
```

## CLI Utilities

Command-line tools for common operations:

```python
from deltalakedb import CliUtils

utils = CliUtils()

# Parse URI
result = utils.parse_uri("deltasql://postgres://localhost/mydb")
print(result.message)
print(result.data)

# List database types
result = utils.list_database_types()
print(result.data["database_types"])

# Quick connect
result = dl.quick_connect("deltasql://sqlite:///my.db")
if result.success:
    print("Connected successfully!")
```

## Compatibility

DeltaLakeDB provides a compatibility layer for existing deltalake workflows:

```python
from deltalakedb import DeltaLakeBridge

bridge = DeltaLakeBridge()
bridge.initialize(connection)

# Check feature support
compat = DeltaLakeCompatibility()
if compat.is_feature_supported("merge"):
    # Use merge functionality
    pass
```

## Supported Databases

- PostgreSQL (>= 9.6)
- MySQL (>= 8.0)
- SQLite (>= 3.24)
- DuckDB (>= 0.8)
- Microsoft SQL Server (>= 2019)
- Oracle Database (>= 19c)
- Snowflake
- Google BigQuery
- Amazon Redshift

## Development

### Building from Source

```bash
# Clone the repository
git clone https://github.com/your-org/deltalakeDB.git
cd deltalakeDB/crates/python

# Install in development mode
pip install -e .

# Or build manually
maturin develop
```

### Running Tests

```bash
# Run Python tests
pytest tests/

# Run Rust tests
cargo test
```

## License

This project is licensed under either of:

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
  https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  https://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## Support

- Documentation: https://deltalakedb.readthedocs.io
- Issues: https://github.com/your-org/deltalakeDB/issues
- Discussions: https://github.com/your-org/deltalakeDB/discussions