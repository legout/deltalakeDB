# deltalakedb-sql

SQL adapters for Delta Lake metadata storage with support for PostgreSQL, SQLite, and DuckDB.

## Overview

The `deltalakedb-sql` crate provides a unified interface for storing Delta Lake metadata in SQL databases. It implements the core transaction log reading and writing capabilities required for SQL-backed Delta Lake operations.

## Features

- **Multi-database support**: PostgreSQL, SQLite, and DuckDB
- **Async/await support**: Built on tokio and async-trait
- **Connection pooling**: Optimized connection management
- **Type safety**: Compile-time query checking with sqlx
- **Schema migrations**: Automatic schema versioning and updates
- **Performance optimized**: Database-specific optimizations and indexing
- **Comprehensive testing**: Full test suite with integration tests

## Architecture

The crate is organized into several key components:

### Core Traits

- **`TxnLogReader`**: Reading Delta metadata from SQL databases
- **`TxnLogWriter`**: Writing Delta metadata to SQL databases
- **`DatabaseAdapter`**: Database-specific operations and management

### Adapters

- **`PostgresAdapter`**: Production-ready PostgreSQL implementation
- **`SQLiteAdapter`**: Embedded SQLite for development and testing
- **`DuckDBAdapter`**: Analytical DuckDB for data warehousing workloads

### Components

- **`SchemaManager`**: Database schema definitions and migrations
- **`AdapterFactory`**: Factory for creating appropriate adapters
- **`Error handling`**: Comprehensive error types and management

## Quick Start

### Basic Usage

```rust
use deltalakedb_sql::{AdapterFactory, DatabaseConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure database connection
    let config = DatabaseConfig {
        url: "sqlite::memory:".to_string(),
        pool_size: 10,
        timeout: 30,
        ssl_enabled: false,
    };

    // Create adapter
    let adapter = AdapterFactory::create_adapter(config).await?;

    // Check database health
    let healthy = adapter.health_check().await?;
    println!("Database healthy: {}", healthy);

    Ok(())
}
```

### PostgreSQL Example

```rust
let config = DatabaseConfig {
    url: "postgresql://user:password@localhost/deltalake".to_string(),
    pool_size: 20,
    timeout: 30,
    ssl_enabled: true,
};

let adapter = AdapterFactory::create_adapter(config).await?;
```

### SQLite Example

```rust
let config = DatabaseConfig {
    url: "sqlite:///path/to/database.db".to_string(),
    pool_size: 1, // SQLite typically uses single connection
    timeout: 30,
    ssl_enabled: false,
};

let adapter = AdapterFactory::create_adapter(config).await?;
```

### DuckDB Example

```rust
let config = DatabaseConfig {
    url: "duckdb:///path/to/database.duckdb".to_string(),
    pool_size: 5,
    timeout: 30,
    ssl_enabled: false,
};

let adapter = AdapterFactory::create_adapter(config).await?;
```

## Database Operations

### Table Management

```rust
use deltalakedb_core::Table;

let table = Table {
    id: uuid::Uuid::new_v4(),
    table_path: "/path/to/delta/table".to_string(),
    table_name: "my_table".to_string(),
    table_uuid: uuid::Uuid::new_v4(),
    created_at: chrono::Utc::now(),
    updated_at: chrono::Utc::now(),
};

// Create table
let created = adapter.create_table(&table).await?;

// Read table
let read_table = adapter.read_table(table.id).await?;
assert!(read_table.is_some());

// List tables
let tables = adapter.list_tables(None, None).await?;

// Update table
let mut updated_table = created.clone();
updated_table.table_name = "updated_name".to_string();
let table_result = adapter.update_table(&updated_table).await?;

// Delete table (soft delete)
let deleted = adapter.delete_table(table.id).await?;
```

### Commit Management

```rust
use deltalakedb_core::Commit;
use serde_json::json;

let commit = Commit {
    id: uuid::Uuid::new_v4(),
    table_id: table.id,
    version: 1,
    timestamp: chrono::Utc::now(),
    operation_type: "WRITE".to_string(),
    operation_parameters: json!({"mode": "Append"}),
    commit_info: json!({"timestamp": chrono::Utc::now().timestamp_millis()}),
};

// Write single commit
let written = adapter.write_commit(&commit).await?;

// Write multiple commits
let commits = vec![commit1, commit2, commit3];
let written_commits = adapter.write_commits(&commits).await?;

// Read commits
let latest = adapter.read_latest_commit(table.id).await?;
let range = adapter.read_commits_range(table_id, Some(1), Some(10), Some(100)).await?;
let count = adapter.count_commits(table.id).await?;
```

### Metadata Management

```rust
use deltalakedb_core::{Protocol, Metadata};

// Update protocol
let protocol = Protocol {
    min_reader_version: 1,
    min_writer_version: 2,
};
adapter.update_protocol(table_id, &protocol).await?;

// Update metadata
let metadata = Metadata {
    id: "metadata_id".to_string(),
    name: "my_table".to_string(),
    description: Some("My Delta table".to_string()),
    format: "parquet".to_string(),
    schema_string: Some(schema_json),
    partition_columns: vec!["year".to_string(), "month".to_string()],
    configuration: json!({"delta.autoOptimize.optimizeWrite": "true"}),
    created_time: Some(chrono::Utc::now().timestamp_millis()),
};
adapter.update_metadata(table_id, &metadata).await?;
```

## Database Schema

The adapter creates a normalized schema for Delta Lake metadata:

### Core Tables

- **`delta_tables`**: Table metadata and configuration
- **`delta_protocols`**: Delta protocol versions
- **`delta_metadata`**: Table metadata in JSON format
- **`delta_commits`**: Transaction log commits
- **`delta_commit_actions`**: Individual actions within commits
- **`delta_files`**: File-level metadata for fast queries
- **`schema_migrations`**: Schema version tracking

### Indexes

Comprehensive indexing is created for optimal query performance:

- Table path and ID indexes
- Commit version and timestamp indexes
- File path and commit version indexes
- Action type indexes

## Performance Optimizations

### PostgreSQL

- Connection pooling with `deadpool-postgres`
- JSONB for efficient metadata storage
- GIN indexes for JSONB columns
- Proper connection configuration
- Query optimization hints

### SQLite

- WAL mode for better concurrency
- Memory-mapped I/O
- Custom SQLite functions
- Connection pooling
- Query optimization with `PRAGMA` settings

### DuckDB

- Parallel query execution
- Columnar storage optimization
- Memory limits and configuration
- Materialized views for frequent queries
- Analytical query optimization

## Configuration

### Database Configuration

```rust
pub struct DatabaseConfig {
    pub url: String,           // Database connection URL
    pub pool_size: u32,        // Connection pool size
    pub timeout: u64,          // Connection timeout (seconds)
    pub ssl_enabled: bool,     // Enable SSL (PostgreSQL only)
}
```

### Pool Configuration

Each adapter uses optimal pool settings:

```rust
// PostgreSQL
max_size: 20
min_size: 5
idle_timeout: 600s
max_lifetime: 1800s

// SQLite
max_size: 1 (typically)
min_size: 1
idle_timeout: none
max_lifetime: none

// DuckDB
max_size: 10
min_size: 2
idle_timeout: 300s
max_lifetime: 900s
```

## Testing

### Running Tests

```bash
# Run all tests
cargo test

# Run specific adapter tests
cargo test --features postgres
cargo test --features sqlite
cargo test --features duckdb

# Run integration tests
cargo test --test integration_tests

# Run performance tests
cargo test --test performance_tests
```

### Test Environment Variables

```bash
# PostgreSQL tests
export POSTGRES_URL="postgresql://user:password@localhost/testdb"

# SQLite tests (optional - uses in-memory by default)
export SQLITE_PATH="/path/to/test.db"

# DuckDB tests (optional - uses in-memory by default)
export DUCKDB_PATH="/path/to/test.duckdb"
```

### Test Macros

The crate provides testing macros for easy cross-database testing:

```rust
use crate::{test_all_adapters, test_sqlite_only, test_duckdb_only};

// Test across all available adapters
test_all_adapters!(my_test, |adapter| async move {
    // Test code here
});

// Test SQLite only
test_sqlite_only!(sqlite_specific_test, |adapter| async move {
    // SQLite-specific test code
});
```

## Error Handling

The crate provides comprehensive error handling:

```rust
use deltalakedb_sql::{SqlError, SqlResult};

fn handle_errors() -> SqlResult<()> {
    // Your operations here
    Ok(())
}

// Error types:
// - ConnectionError: Database connection issues
// - QueryError: SQL query execution problems
// - TransactionError: Transaction failures
// - SchemaError: Schema-related issues
// - ValidationError: Data validation problems
// - DatabaseSpecific: Database-specific errors
```

## Migration and Versioning

The schema includes a migration system:

```rust
// Check current schema version
let is_current = adapter.check_schema_version().await?;

// Run migrations (idempotent)
adapter.migrate_schema().await?;

// Get database version
let version = adapter.database_version().await?;
```

## Monitoring and Observability

### Connection Pool Statistics

```rust
let stats = adapter.pool_stats().await?;

println!("Total connections: {}", stats.total_connections);
println!("Active connections: {}", stats.active_connections);
println!("Idle connections: {}", stats.idle_connections);
println!("Utilization: {:.1}%", stats.utilization_percent);
```

### Health Checks

```rust
// Basic health check
let is_healthy = adapter.health_check().await?;

// Full connection test
let can_connect = adapter.test_connection().await?;
```

### Optimization Hints

```rust
let hints = adapter.get_optimization_hints().await?;
for hint in hints {
    println!("Optimization hint: {}", hint);
}
```

## Production Deployment

### PostgreSQL Production Setup

```rust
let config = DatabaseConfig {
    url: "postgresql://user:password@prod-db:5432/deltalake?sslmode=require".to_string(),
    pool_size: 50,
    timeout: 60,
    ssl_enabled: true,
};
```

### Connection Pooling

- Use appropriate pool sizes based on workload
- Monitor pool utilization
- Set appropriate timeouts
- Consider read replicas for query scaling

### Performance Tuning

- Monitor query performance
- Use database-specific optimization hints
- Consider materialized views for frequent queries
- Implement proper indexing strategy
- Monitor memory usage and connection patterns

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Run the full test suite
5. Submit a pull request

## License

This project is licensed under the MIT OR Apache-2.0 license.