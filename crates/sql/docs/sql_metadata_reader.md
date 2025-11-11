# SQL Metadata Reader Documentation

## Overview

The SQL metadata reader provides Delta Lake transaction log access through relational databases. This enables efficient metadata queries, time travel operations, and integration with existing database infrastructure.

## Supported Databases

- **PostgreSQL**: Production-ready with full feature support including materialized views
- **SQLite**: Lightweight option for development and testing
- **DuckDB**: Analytical database optimized for query performance

## Quick Start

### Basic Usage

```rust
use deltalakedb_sql::{connection::DatabaseConfig, reader::SqlTxnLogReader};
use deltalakedb_core::reader::TxnLogReader;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create database configuration
    let config = DatabaseConfig::sqlite_memory();
    
    // Initialize database and create reader
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

### Database-Specific Configuration

#### PostgreSQL

```rust
let config = DatabaseConfig::postgres("localhost", 5432, "deltalakedb", "user", "password")
    .ssl_mode(SslMode::Require)
    .max_connections(20)
    .connect_timeout(30);
```

#### SQLite

```rust
// In-memory
let config = DatabaseConfig::sqlite_memory();

// File-based
let config = DatabaseConfig::sqlite_file("/path/to/database.db");
```

#### DuckDB

```rust
// In-memory
let config = DatabaseConfig::duckdb_memory();

// File-based
let config = DatabaseConfig::duckdb("/path/to/database.duckdb");
```

## Core Features

### 1. Transaction Log Reading

The SQL reader implements the `TxnLogReader` trait, providing standard Delta Lake operations:

```rust
// Get current table version
let version = reader.get_version().await?;

// Get table metadata
let metadata = reader.get_table_metadata().await?;

// Get table schema
let schema = reader.get_schema().await?;

// Get protocol information
let protocol = reader.get_protocol().await?;

// Get active files
let files = reader.get_active_files().await?;
```

### 2. Time Travel

Query historical table states:

```rust
// Get version at specific point
let version = reader.get_version_at(10).await?;

// Get active files at specific version
let files = reader.get_active_files_at(10).await?;

// Get schema at specific version
let schema = reader.get_schema_at(10).await?;
```

### 3. Version History

Access complete version history:

```rust
let history = reader.get_version_history().await?;
for version_info in history {
    println!("Version {}: {} by {}", 
        version_info.version, 
        version_info.operation, 
        version_info.committer);
}
```

### 4. Table Discovery

List all Delta tables in the database:

```rust
let tables = reader.list_tables().await?;
for table in tables {
    println!("Table: {} at {}", table.name, table.location);
}
```

## Schema Management

### Automatic Schema Initialization

```rust
let manager = config.connect_and_initialize_schema().await?;
```

This automatically:
- Creates all required tables
- Applies migrations
- Sets up indexes
- Validates schema integrity

### Manual Schema Management

```rust
use deltalakedb_sql::schema::{SchemaManager, SchemaConfig};

let manager = config.connect().await?;
let engine = manager.connection().engine();
let schema_config = SchemaConfig::default();
let schema_manager = SchemaManager::new(engine, schema_config);

// Initialize schema
let pool = manager.connection().as_any_pool()?;
schema_manager.initialize_schema(&pool).await?;

// Apply migrations
schema_manager.migrate_to_version(&pool, 3).await?;

// Validate schema
let is_valid = schema_manager.validate_schema(&pool).await?;
```

## Performance Optimization

### Connection Pooling

```rust
let config = DatabaseConfig::postgres("localhost", 5432, "db", "user", "pass")
    .max_connections(50)        // Maximum connections
    .min_connections(5)         // Minimum connections
    .connect_timeout(30)        // Connection timeout
    .idle_timeout(600);         // Idle timeout
```

### Materialized Views (PostgreSQL)

Enable materialized views for faster active file queries:

```rust
let schema_config = SchemaConfig {
    enable_materialized_views: true,
    ..Default::default()
};

// Refresh materialized views
manager.refresh_views().await?;
```

### Performance Indexes

The system automatically creates indexes for:
- Version lookups
- File path searches
- Time travel queries
- Partition pruning

## Error Handling

The SQL reader provides comprehensive error handling:

```rust
match reader.get_version().await {
    Ok(version) => println!("Version: {}", version),
    Err(e) => {
        eprintln!("Error reading version: {}", e);
        // Handle specific error types
        if e.to_string().contains("table does not exist") {
            // Table doesn't exist
        } else if e.to_string().contains("connection") {
            // Connection error
        }
    }
}
```

## Testing

### Unit Tests

```bash
cargo test -p deltalakedb-sql
```

### Integration Tests

```bash
# SQLite and DuckDB
cargo test -p deltalakedb-sql sql_reader_tests

# PostgreSQL (requires POSTGRES_TEST_URL)
POSTGRES_TEST_URL="postgres://user:pass@localhost/db" cargo test -p deltalakedb-sql sql_reader_tests
```

### Benchmarks

```bash
cargo run --example sql_metadata_reader --features=benchmarks
```

### Conformance Tests

```bash
cargo test -p deltalakedb-sql conformance_tests
```

## Migration from File-Based Reader

### Before (File-based)

```rust
use deltalakedb_core::reader::FileTxnLogReader;

let reader = FileTxnLogReader::new("/path/to/delta-table")?;
let version = reader.get_version().await?;
```

### After (SQL-based)

```rust
use deltalakedb_sql::{connection::DatabaseConfig, reader::SqlTxnLogReader};

let config = DatabaseConfig::postgres("localhost", 5432, "db", "user", "pass")?;
let reader = SqlTxnLogReader::new(config, "table_name".to_string())?;
let version = reader.get_version().await?;
```

## Advanced Usage

### Custom Connection Options

```rust
let config = DatabaseConfig::postgres("localhost", 5432, "db", "user", "pass")
    .option("application_name".to_string(), "my_app".to_string())
    .option("search_path".to_string(), "public,delta".to_string());
```

### Health Monitoring

```rust
// Health check
manager.health_check().await?;

// Pool statistics
let stats = manager.pool_stats();
println!("Pool: {}/{} connections active", stats.active, stats.size);

// Schema validation
let pool = manager.connection().as_any_pool()?;
let is_valid = manager.schema_manager().validate_schema(&pool).await?;
```

### Concurrent Operations

```rust
use std::sync::Arc;

let reader = Arc::new(SqlTxnLogReader::new(config, "table".to_string())?);

// Spawn multiple concurrent reads
let mut handles = vec![];
for i in 0..10 {
    let reader_clone = reader.clone();
    let handle = tokio::spawn(async move {
        let version = reader_clone.get_version().await?;
        let files = reader_clone.get_active_files().await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((i, version, files.len()))
    });
    handles.push(handle);
}

// Collect results
for handle in handles {
    let (i, version, file_count) = handle.await??;
    println!("Task {}: version={}, files={}", i, version, file_count);
}
```

## Best Practices

### 1. Connection Management

- Use appropriate pool sizes for your workload
- Set connection timeouts to prevent hanging
- Monitor pool statistics in production

### 2. Schema Management

- Always use `connect_and_initialize_schema()` for new databases
- Run schema validation in production deployments
- Test migrations before applying to production

### 3. Performance

- Enable materialized views for read-heavy workloads (PostgreSQL)
- Use DuckDB for analytical workloads
- Monitor query performance and adjust indexes as needed

### 4. Error Handling

- Implement proper error handling for network issues
- Use connection retries for transient failures
- Log errors for debugging purposes

### 5. Security

- Use SSL for PostgreSQL connections in production
- Store database credentials securely
- Apply principle of least privilege to database users

## Troubleshooting

### Common Issues

#### Connection Errors

```
Error: connection to server failed
```

**Solution**: Check database URL, network connectivity, and credentials.

#### Schema Errors

```
Error: table "dl_tables" does not exist
```

**Solution**: Use `connect_and_initialize_schema()` or run schema initialization manually.

#### Performance Issues

**Symptoms**: Slow queries, high latency

**Solutions**:
- Check connection pool configuration
- Verify indexes are created
- Consider materialized views (PostgreSQL)
- Use DuckDB for analytical workloads

#### Memory Issues

**Symptoms**: High memory usage

**Solutions**:
- Reduce connection pool size
- Use file-based databases for large datasets
- Monitor memory usage in production

### Debug Mode

Enable debug logging:

```rust
use tracing_subscriber;

tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .init();
```

## API Reference

### DatabaseConfig

Configuration for database connections.

#### Methods

- `new(url: String) -> Self`: Create new configuration
- `sqlite_memory() -> Self`: In-memory SQLite
- `sqlite_file(path: &str) -> Self`: File-based SQLite
- `postgres(host, port, database, user, password) -> Self`: PostgreSQL
- `duckdb(path: &str) -> Self`: File-based DuckDB
- `duckdb_memory() -> Self`: In-memory DuckDB
- `ssl_mode(mode: SslMode) -> Self`: Set SSL mode
- `max_connections(max: u32) -> Self`: Set max connections
- `connect_timeout(secs: u64) -> Self`: Set connection timeout
- `option(key: String, value: String) -> Self`: Add custom option

#### Connection Methods

- `connect() -> Result<Arc<DatabaseConnection>>`: Connect to database
- `connect_and_initialize_schema() -> Result<DatabaseConnectionManager>`: Connect and initialize schema

### SqlTxnLogReader

SQL-based implementation of Delta Lake transaction log reader.

#### Constructor

- `new(config: DatabaseConfig, table_name: String) -> Result<Self>`

#### TxnLogReader Implementation

- `get_version() -> Result<i64>`: Get current version
- `get_table_metadata() -> Result<TableMetadata>`: Get table metadata
- `get_schema() -> Result<String>`: Get current schema
- `get_schema_at(version: i64) -> Result<String>`: Get schema at version
- `get_protocol() -> Result<Protocol>`: Get protocol info
- `get_active_files() -> Result<Vec<AddFile>>`: Get active files
- `get_active_files_at(version: i64) -> Result<Vec<AddFile>>`: Get files at version
- `get_version_at(version: i64) -> Result<i64>`: Get version at timestamp
- `get_version_history() -> Result<Vec<VersionInfo>>`: Get version history
- `list_tables() -> Result<Vec<TableInfo>>`: List all tables

### DatabaseConnectionManager

Enhanced connection management with schema support.

#### Methods

- `connection() -> &Arc<DatabaseConnection>`: Get connection
- `schema_manager() -> &SchemaManager`: Get schema manager
- `engine() -> DatabaseEngine`: Get database engine
- `health_check() -> Result<()>`: Perform health check
- `pool_stats() -> ConnectionPoolStats`: Get pool statistics
- `refresh_views() -> Result<()>`: Refresh materialized views

### SchemaManager

Database schema management and migrations.

#### Methods

- `initialize_schema(pool) -> Result<()>`: Initialize complete schema
- `migrate_to_version(pool, version) -> Result<()>`: Apply migrations
- `validate_schema(pool) -> Result<bool>`: Validate schema integrity
- `refresh_materialized_views(pool) -> Result<()>`: Refresh views (PostgreSQL)

## Examples

See the `examples/` directory for complete working examples:

- `sql_metadata_reader.rs`: Basic usage examples
- `performance_benchmark.rs`: Performance testing
- `conformance_tests.rs`: Protocol compliance testing

## Contributing

When contributing to the SQL metadata reader:

1. Add tests for new functionality
2. Update documentation
3. Ensure conformance tests pass
4. Test with all supported databases
5. Update examples if applicable

## License

This project is licensed under the MIT or Apache-2.0 license.