# deltalakedb

SQL-backed metadata plane for Delta Lake.

> **Status:** Early scaffolding. See `SQL_Backed_Delta_Metadata_PRD.md` for the product requirements document.

## Overview

`deltalakedb` provides a SQL-backed metadata plane for Delta Lake while preserving full Delta compatibility by mirroring commits to `_delta_log/` for external engines. It improves metadata read latency and enables multi-table ACID semantics using a relational database as the source of truth.

## Features

- **SQL Metadata Reader**: Read Delta Lake metadata from PostgreSQL, SQLite, or DuckDB
- **Time Travel Support**: Query historical table states and versions
- **Multi-Table ACID**: Atomic transactions across multiple Delta tables
- **Performance Optimized**: Materialized views and performance indexes
- **Protocol Compliant**: Full Delta Lake protocol compatibility
- **Easy Migration**: Drop-in replacement for file-based readers

## Quick Start

### SQL Metadata Reader

```rust
use deltalakedb_sql::{connection::DatabaseConfig, reader::SqlTxnLogReader};
use deltalakedb_core::reader::TxnLogReader;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create database configuration
    let config = DatabaseConfig::sqlite_memory();
    
    // Create reader
    let reader = SqlTxnLogReader::new(config, "my_table".to_string())?;
    
    // Read current version
    let version = reader.get_version().await?;
    println!("Current version: {}", version);
    
    // Get active files
    let files = reader.get_active_files().await?;
    println!("Active files: {}", files.len());
    
    Ok(())
}
```

### Multi-Table Transactions

```rust
use deltalakedb_sql::{multi_table::MultiTableWriter, connection::DatabaseConfig};
use deltalakedb_core::writer::TxnLogWriterExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = DatabaseConfig::postgres("localhost", 5432, "db", "user", "pass")?;
    let connection = config.connect().await?;
    let writer = MultiTableWriter::new(connection, None, Default::default());
    
    let mut tx = writer.begin_transaction();
    tx.add_files("table1".to_string(), 0, vec![])?;
    tx.add_files("table2".to_string(), 0, vec![])?;
    
    let result = writer.commit_transaction(tx).await?;
    Ok(())
}
```

## Development

This project uses:
- Rust for the core (extends `delta-rs`) with a Cargo workspace.
- Python 3.12 for bindings/UX, managed with `uv`.
- See `pyproject.toml` for Python tooling configuration.

### Quickstart

```bash
# Install dependencies and set up the dev environment
uv sync --extra dev

# Run Python tooling
uv run ruff check .
uv run black --check .
uv run pytest

# Build Rust workspace
cargo build
cargo test
```

### Examples

```bash
# Run SQL metadata reader example
cargo run --example sql_metadata_reader -p deltalakedb-sql

# Run multi-table transactions example
cargo run --example multi_table_transactions -p deltalakedb-sql

# Run benchmarks
cargo run --example performance_benchmark -p deltalakedb-sql --features=benchmarks
```

## Local Testing

For integration testing with Postgres and multiple S3-compatible object stores, use the provided Docker Compose setup:

```bash
# Start the test environment
./scripts/test-env.sh start

# Stop the test environment
./scripts/test-env.sh stop

# Check status
./scripts/test-env.sh status
```

**Endpoints and credentials**

### Database
- Postgres: `localhost:5432` (user: `postgres`, password: `postgres`, database: `deltalakedb`)

### S3-Compatible Object Stores
- **MinIO**: `http://localhost:9000` (access key: `minioadmin`, secret key: `minioadmin`)
  - Console: `http://localhost:9001`
- **SeaweedFS S3**: `http://localhost:8333` (access key: `seaweedfsadmin`, secret key: `seaweedfsadmin`)
  - Master: `http://localhost:9333`
- **RustFS**: `http://localhost:9100` (access key: `rustfsadmin`, secret key: `rustfsadmin`)
  - Console: `http://localhost:9101`
- **Garage**: `http://localhost:3900` (access key: created via Garage CLI, secret key: created via Garage CLI)
  - Admin API: `http://localhost:3903` (admin token: `garageadmin`)

You can customize these by copying `.env.example` to `.env` and modifying the values.

## License

TBD (to be added in a future change).