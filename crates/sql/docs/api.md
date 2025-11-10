# SQL Adapters API Documentation

This document provides comprehensive API documentation for the deltalakedb-sql crate.

## Core Traits

### TxnLogReader

Trait for reading Delta Lake transaction logs from SQL databases.

```rust
#[async_trait]
pub trait TxnLogReader: Send + Sync {
    /// Read a specific commit by table ID and version
    async fn read_commit(&self, table_id: Uuid, version: i64) -> SqlResult<Option<Commit>>;

    /// Read the latest commit for a table
    async fn read_latest_commit(&self, table_id: Uuid) -> SqlResult<Option<Commit>>;

    /// Read commits within a version range
    async fn read_commits_range(
        &self,
        table_id: Uuid,
        start_version: Option<i64>,
        end_version: Option<i64>,
        limit: Option<i64>
    ) -> SqlResult<Vec<Commit>>;

    /// Count total commits for a table
    async fn count_commits(&self, table_id: Uuid) -> SqlResult<i64>;

    /// Read files for a specific commit
    async fn read_commit_files(&self, commit_id: Uuid) -> SqlResult<Vec<File>>;

    /// Read active files for a table (latest version)
    async fn read_active_files(&self, table_id: Uuid) -> SqlResult<Vec<File>>;
}
```

#### Parameters

- `table_id: Uuid` - Unique identifier for the Delta table
- `version: i64` - Commit version number (starting from 0)
- `start_version: Option<i64>` - Start of version range (inclusive)
- `end_version: Option<i64>` - End of version range (inclusive)
- `limit: Option<i64>` - Maximum number of commits to return
- `commit_id: Uuid` - Unique identifier for a commit

#### Returns

- `SqlResult<Option<Commit>>` - Single commit or None if not found
- `SqlResult<Vec<Commit>>` - Vector of commits (may be empty)
- `SqlResult<i64>` - Count of commits
- `SqlResult<Vec<File>>` - Vector of files associated with commits

#### Errors

Returns `SqlError` for:
- Database connection issues
- Query execution failures
- Invalid table ID or version
- Permission errors

### TxnLogWriter

Trait for writing Delta Lake transaction logs to SQL databases.

```rust
#[async_trait]
pub trait TxnLogWriter: Send + Sync {
    /// Write a single commit
    async fn write_commit(&self, commit: &Commit) -> SqlResult<bool>;

    /// Write multiple commits in a batch
    async fn write_commits(&self, commits: &[Commit]) -> SqlResult<i64>;

    /// Update table protocol
    async fn update_protocol(&self, table_id: Uuid, protocol: &Protocol) -> SqlResult<()>;

    /// Update table metadata
    async fn update_metadata(&self, table_id: Uuid, metadata: &Metadata) -> SqlResult<()>;

    /// Add files to a table
    async fn add_files(&self, table_id: Uuid, files: &[File]) -> SqlResult<i64>;

    /// Remove files from a table
    async fn remove_files(&self, table_id: Uuid, file_paths: &[String]) -> SqlResult<i64>;
}
```

#### Parameters

- `commit: &Commit` - Commit data to write
- `commits: &[Commit]` - Slice of commits for batch operations
- `table_id: Uuid` - Target table ID
- `protocol: &Protocol` - Protocol version information
- `metadata: &Metadata` - Table metadata
- `files: &[File]` - Files to add/remove
- `file_paths: &[String]` - Paths of files to remove

#### Returns

- `SqlResult<bool>` - Success flag for single commit
- `SqlResult<i64>` - Number of affected rows
- `SqlResult<()>` - Unit type for successful operations

#### Errors

Returns `SqlError` for:
- Constraint violations (duplicate versions, etc.)
- Transaction conflicts
- Database write failures
- Invalid data formats

### DatabaseAdapter

Main trait combining all database operations.

```rust
#[async_trait]
pub trait DatabaseAdapter: TxnLogReader + TxnLogWriter + Send + Sync {
    /// Initialize database schema
    async fn initialize_schema(&self) -> SqlResult<()>;

    /// Check if schema is up to date
    async fn check_schema_version(&self) -> SqlResult<bool>;

    /// Run database migrations
    async fn migrate_schema(&self) -> SqlResult<()>;

    /// Health check for the database
    async fn health_check(&self) -> SqlResult<bool>;

    /// Test database connection
    async fn test_connection(&self) -> SqlResult<bool>;

    /// Get database version information
    async fn database_version(&self) -> SqlResult<String>;

    /// Get optimization hints for the database
    async fn get_optimization_hints(&self) -> SqlResult<Vec<String>>;

    /// Get connection pool statistics
    async fn pool_stats(&self) -> SqlResult<PoolStats>;
}
```

## Database Management

### Table Operations

```rust
// Create a new table
async fn create_table(&self, table: &Table) -> SqlResult<Table>;

// Read table by ID
async fn read_table(&self, table_id: Uuid) -> SqlResult<Option<Table>>;

// Update table information
async fn update_table(&self, table: &Table) -> SqlResult<Table>;

// Delete table (soft delete)
async fn delete_table(&self, table_id: Uuid) -> SqlResult<bool>;

// List tables with pagination
async fn list_tables(
    &self,
    offset: Option<i64>,
    limit: Option<i64>
) -> SqlResult<Vec<Table>>;

// Search tables by name pattern
async fn search_tables(
    &self,
    name_pattern: &str,
    offset: Option<i64>,
    limit: Option<i64>
) -> SqlResult<Vec<Table>>;
```

### File Management

```rust
// Add files to a table
async fn add_files(&self, table_id: Uuid, files: &[File]) -> SqlResult<i64>;

// Remove files from a table
async fn remove_files(&self, table_id: Uuid, file_paths: &[String]) -> SqlResult<i64>;

// Get files for a specific commit
async fn get_commit_files(&self, commit_id: Uuid) -> SqlResult<Vec<File>>;

// Get active files for a table
async fn get_active_files(&self, table_id: Uuid) -> SqlResult<Vec<File>>;

// Get file statistics
async fn get_file_stats(&self, table_id: Uuid) -> SqlResult<FileStats>;
```

### Commit Operations

```rust
// Write a single commit
async fn write_commit(&self, commit: &Commit) -> SqlResult<bool>;

// Write multiple commits in batch
async fn write_commits(&self, commits: &[Commit]) -> SqlResult<i64>;

// Read commits by version range
async fn read_commits_range(
    &self,
    table_id: Uuid,
    start_version: Option<i64>,
    end_version: Option<i64>,
    limit: Option<i64>
) -> SqlResult<Vec<Commit>>;

// Get latest commit
async fn get_latest_commit(&self, table_id: Uuid) -> SqlResult<Option<Commit>>;

// Count commits
async fn count_commits(&self, table_id: Uuid) -> SqlResult<i64>;
```

## Data Types

### Table

```rust
pub struct Table {
    pub id: Uuid,                    // Unique table identifier
    pub table_path: String,          // Path to Delta table
    pub table_name: String,          // Human-readable name
    pub table_uuid: Uuid,            // Delta table UUID
    pub created_at: DateTime<Utc>,   // Creation timestamp
    pub updated_at: DateTime<Utc>,   // Last update timestamp
    pub deleted_at: Option<DateTime<Utc>>, // Soft delete timestamp
    pub description: Option<String>, // Optional description
    pub configuration: Value,        // Table configuration JSON
}
```

### Commit

```rust
pub struct Commit {
    pub id: Uuid,                    // Unique commit identifier
    pub table_id: Uuid,              // Parent table ID
    pub version: i64,                // Commit version
    pub timestamp: DateTime<Utc>,    // Commit timestamp
    pub operation_type: String,      // Type of operation (WRITE, DELETE, etc.)
    pub operation_parameters: Value, // Operation parameters
    pub commit_info: Value,          // Additional commit information
    pub checksum: Option<String>,    // Optional commit checksum
    pub in_progress: bool,           // Whether commit is in progress
}
```

### File

```rust
pub struct File {
    pub id: Uuid,                    // Unique file identifier
    pub table_id: Uuid,              // Parent table ID
    pub commit_id: Uuid,             // Parent commit ID
    pub path: String,                // File path
    pub size: i64,                   // File size in bytes
    pub modification_time: i64,      // Modification timestamp
    pub is_add: bool,                // True for add, false for remove
    pub data_change: bool,           // Whether this changes data
    pub stats: Option<Value>,        // File statistics JSON
    pub tags: Option<Value>,         // File tags
    pub partition_values: Value,     // Partition column values
}
```

### Protocol

```rust
pub struct Protocol {
    pub min_reader_version: i32,     // Minimum reader version
    pub min_writer_version: i32,     // Minimum writer version
    pub reader_features: Option<Vec<String>>, // Optional reader features
    pub writer_features: Option<Vec<String>>, // Optional writer features
}
```

### Metadata

```rust
pub struct Metadata {
    pub id: String,                  // Metadata ID
    pub name: String,                // Table name
    pub description: Option<String>, // Description
    pub format: String,              // File format
    pub schema_string: Option<String>, // Schema JSON
    pub partition_columns: Vec<String>, // Partition column names
    pub created_time: Option<i64>,   // Creation time
    pub configuration: Value,        // Configuration JSON
}
```

## Error Handling

### SqlError Enum

```rust
pub enum SqlError {
    ConnectionError(String),         // Database connection issues
    QueryError(String),             // SQL query execution problems
    TransactionError(String),       // Transaction failures
    SchemaError(String),            // Schema-related issues
    ValidationError(String),        // Data validation problems
    DatabaseSpecific(String),       // Database-specific errors
    NotFound(String),               // Resource not found
    Conflict(String),               // Data conflicts
    Timeout(String),                // Operation timeouts
    Internal(String),               // Internal errors
}
```

### SqlResult Type

```rust
pub type SqlResult<T> = Result<T, SqlError>;
```

## Connection Pooling

### PoolStats

```rust
pub struct PoolStats {
    pub total_connections: u32,      // Total connections in pool
    pub active_connections: u32,     // Currently active connections
    pub idle_connections: u32,       // Idle connections
    pub utilization_percent: f64,    // Pool utilization (0-100)
    pub average_wait_time: Duration, // Average wait time for connections
    pub max_wait_time: Duration,     // Maximum wait time observed
}
```

### Database Configuration

```rust
pub struct DatabaseConfig {
    pub url: String,                // Database connection URL
    pub pool_size: u32,             // Connection pool size
    pub timeout: u64,               // Connection timeout (seconds)
    pub ssl_enabled: bool,          // Enable SSL (PostgreSQL only)
    pub read_only: bool,            // Read-only mode
    pub max_connections: Option<u32>, // Max connections (overrides pool_size)
    pub min_connections: Option<u32>, // Min connections
    pub connection_timeout: Option<u64>, // Connection timeout
    pub idle_timeout: Option<u64>,     // Idle timeout
    pub max_lifetime: Option<u64>,     // Max connection lifetime
}
```

## Optimization Hints

### PostgreSQL Hints

- Use `EXPLAIN ANALYZE` to analyze query plans
- Consider `BRIN` indexes for timestamp columns
- Use `JSONB` for metadata with GIN indexes
- Enable `parallel query` for large scans
- Configure `effective_cache_size` appropriately

### SQLite Hints

- Enable `WAL mode` for better concurrency
- Use `PRAGMA journal_mode = WAL`
- Set `PRAGMA synchronous = NORMAL` for balance
- Consider `PRAGMA temp_store = MEMORY`
- Use `VACUUM` regularly for maintenance

### DuckDB Hints

- Set `memory_limit` appropriately
- Use `PRAGMA threads` for parallel execution
- Consider `checkpoint_threshold` for WAL files
- Enable `force_parallelism` for small queries
- Use `parallelism` settings based on CPU cores

## Monitoring and Debugging

### Health Monitoring

```rust
// Basic health check
let is_healthy = adapter.health_check().await?;

// Connection test
let can_connect = adapter.test_connection().await?;

// Pool statistics
let stats = adapter.pool_stats().await?;

// Database version
let version = adapter.database_version().await?;
```

### Performance Monitoring

```rust
// Query optimization hints
let hints = adapter.get_optimization_hints().await?;
for hint in hints {
    println!("Hint: {}", hint);
}

// Connection utilization
if stats.utilization_percent > 80.0 {
    eprintln!("Warning: High connection pool utilization");
}
```

### Debugging

```rust
match error {
    SqlError::ConnectionError(msg) => {
        eprintln!("Connection failed: {}", msg);
    }
    SqlError::QueryError(msg) => {
        eprintln!("Query failed: {}", msg);
        // Consider checking SQL syntax or table existence
    }
    SqlError::TransactionError(msg) => {
        eprintln!("Transaction failed: {}", msg);
        // May need to retry the operation
    }
    _ => {
        eprintln!("Unexpected error: {}", error);
    }
}
```