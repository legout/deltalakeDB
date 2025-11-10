# SQL Adapters Usage Examples

This document provides comprehensive examples of using the Delta Lake SQL adapters for various scenarios and database types.

## Quick Start Examples

### Basic Setup

```rust
use deltalakedb_sql::{AdapterFactory, DatabaseConfig, DatabaseAdapter};
use deltalakedb_core::{Table, Commit};
use uuid::Uuid;
use chrono::Utc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create database configuration
    let config = DatabaseConfig {
        url: "sqlite::memory:".to_string(),
        pool_size: 10,
        timeout: 30,
        ssl_enabled: false,
    };

    // Create adapter
    let adapter = AdapterFactory::create_adapter(config).await?;

    // Initialize schema
    adapter.initialize_schema().await?;

    println!("Database initialized successfully");
    Ok(())
}
```

### Creating and Managing Tables

```rust
async fn example_table_management(adapter: &dyn DatabaseAdapter) -> Result<(), SqlError> {
    // Create a new table
    let table = Table {
        id: Uuid::new_v4(),
        table_path: "/data/my_delta_table".to_string(),
        table_name: "my_table".to_string(),
        table_uuid: Uuid::new_v4(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        deleted_at: None,
        description: Some("My first Delta table".to_string()),
        configuration: serde_json::json!({
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true"
        }),
    };

    // Create table in database
    let created_table = adapter.create_table(&table).await?;
    println!("Created table: {} ({})", created_table.table_name, created_table.id);

    // Read table by ID
    let found_table = adapter.read_table(created_table.id).await?;
    assert!(found_table.is_some());
    println!("Found table: {}", found_table.unwrap().table_name);

    // List all tables
    let tables = adapter.list_tables(None, Some(10)).await?;
    println!("Found {} tables", tables.len());

    // Search tables by name pattern
    let search_results = adapter.search_tables("my_%", None, Some(5)).await?;
    println!("Found {} matching tables", search_results.len());

    // Update table
    let mut updated_table = created_table;
    updated_table.description = Some("Updated description".to_string());
    updated_table.updated_at = Utc::now();

    let updated_result = adapter.update_table(&updated_table).await?;
    println!("Updated table: {}", updated_result.table_name);

    Ok(())
}
```

## Database-Specific Examples

### PostgreSQL Examples

#### Production PostgreSQL Setup

```rust
use deltalakedb_sql::adapters::PostgresAdapter;

async fn postgres_production_example() -> Result<(), SqlError> {
    let config = DatabaseConfig {
        url: "postgresql://deltalake_user:secure_password@prod-db.example.com:5432/deltalake_db?sslmode=require".to_string(),
        pool_size: 50,                    // Large pool for production
        timeout: 60,                      // Longer timeout for production
        ssl_enabled: true,
        max_connections: Some(100),       // Allow bursts
        min_connections: Some(10),        // Keep warm connections
        idle_timeout: Some(600),          // 10 minutes
        max_lifetime: Some(1800),         // 30 minutes
    };

    let adapter = PostgresAdapter::new(config).await?;
    adapter.initialize_schema().await?;

    // Health check
    let is_healthy = adapter.health_check().await?;
    if !is_healthy {
        return Err(SqlError::ConnectionError("Database health check failed".to_string()));
    }

    println!("PostgreSQL adapter ready for production use");
    Ok(())
}
```

#### PostgreSQL with Connection Pooling

```rust
use deadpool_postgres::{Config, Pool};
use deltalakedb_sql::adapters::PostgresAdapter;

async fn postgres_with_pooling() -> Result<(), SqlError> {
    let mut pg_config = Config::new();
    pg_config.host = Some("localhost".to_string());
    pg_config.port = Some(5432);
    pg_config.dbname = Some("deltalake".to_string());
    pg_config.user = Some("postgres".to_string());
    pg_config.password = Some("password".to_string());
    pg_config.max_pool_size = Some(20);

    let pool = pg_config.create_pool(tokio_postgres::NoTls)?;
    let adapter = PostgresAdapter::with_pool(pool).await?;

    // Use the adapter with automatic connection pooling
    let tables = adapter.list_tables(None, None).await?;
    println!("Found {} tables using pooled connections", tables.len());

    Ok(())
}
```

#### PostgreSQL JSONB Operations

```rust
async fn postgres_jsonb_example(adapter: &dyn DatabaseAdapter) -> Result<(), SqlError> {
    use deltalakedb_core::Metadata;
    use serde_json::json;

    // Create complex metadata
    let metadata = Metadata {
        id: "metadata_001".to_string(),
        name: "sales_data".to_string(),
        description: Some("Sales transactions data".to_string()),
        format: "parquet".to_string(),
        schema_string: Some(json!({
            "type": "struct",
            "fields": [
                {"name": "order_id", "type": "long", "nullable": false},
                {"name": "customer_id", "type": "long", "nullable": false},
                {"name": "order_date", "type": "date", "nullable": false},
                {"name": "amount", "type": "decimal(10,2)", "nullable": false},
                {"name": "status", "type": "string", "nullable": true}
            ]
        }).to_string()),
        partition_columns: vec!["order_date".to_string(), "status".to_string()],
        configuration: json!({
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "delta.checkpoint.writeStatsAsJson": "true",
            "delta.checkpoint.writeStatsAsStruct": "false"
        }),
        created_time: Some(Utc::now().timestamp_millis()),
    };

    let table_id = Uuid::new_v4();
    adapter.update_metadata(table_id, &metadata).await?;

    // Query with JSONB operators (PostgreSQL specific)
    let conn = adapter.get_connection().await?;
    let rows = conn.query(
        "SELECT * FROM delta_metadata
         WHERE configuration->>'delta.autoOptimize.optimizeWrite' = 'true'",
        &[]
    ).await?;

    println!("Found {} tables with auto-optimization enabled", rows.len());
    Ok(())
}
```

### SQLite Examples

#### In-Memory SQLite for Testing

```rust
async fn sqlite_in_memory_example() -> Result<(), SqlError> {
    let config = DatabaseConfig {
        url: "sqlite::memory:".to_string(),
        pool_size: 1,              // SQLite typically uses single connection
        timeout: 30,
        ssl_enabled: false,
    };

    let adapter = AdapterFactory::create_adapter(config).await?;
    adapter.initialize_schema().await?;

    // Create test data
    let table = create_test_table();
    let created = adapter.create_table(&table).await?;

    // Verify table creation
    let found = adapter.read_table(created.id).await?;
    assert!(found.is_some());

    println!("In-memory SQLite test passed");
    Ok(())
}
```

#### SQLite with WAL Mode for Concurrency

```rust
async fn sqlite_wal_mode_example() -> Result<(), SqlError> {
    let config = DatabaseConfig {
        url: "sqlite:///path/to/deltalake.db".to_string(),
        pool_size: 1,
        timeout: 30,
        ssl_enabled: false,
    };

    let adapter = AdapterFactory::create_adapter(config).await?;

    // Initialize schema with WAL mode
    adapter.initialize_schema().await?;

    // Enable WAL mode for better concurrency
    let conn = adapter.get_connection().await?;
    conn.execute("PRAGMA journal_mode = WAL", &[]).await?;
    conn.execute("PRAGMA synchronous = NORMAL", &[]).await?;
    conn.execute("PRAGMA cache_size = 10000", &[]).await?;
    conn.execute("PRAGMA temp_store = MEMORY", &[]).await?;

    // Test concurrent reads during writes
    let table_id = Uuid::new_v4();
    let write_handle = tokio::spawn({
        let adapter = adapter.clone();
        async move {
            for i in 0..100 {
                let commit = create_test_commit(table_id, i);
                adapter.write_commit(&commit).await?;
            }
            Ok::<(), SqlError>(())
        }
    });

    let read_handle = tokio::spawn({
        let adapter = adapter.clone();
        async move {
            for _ in 0..100 {
                let commits = adapter.read_commits_range(table_id, None, None, Some(10)).await?;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<(), SqlError>(())
        }
    });

    // Wait for both operations to complete
    write_handle.await??;
    read_handle.await??;

    println!("SQLite WAL mode concurrency test passed");
    Ok(())
}
```

#### SQLite Backup and Restore

```rust
async fn sqlite_backup_example() -> Result<(), SqlError> {
    let source_config = DatabaseConfig {
        url: "sqlite:///source.db".to_string(),
        pool_size: 1,
        timeout: 30,
        ssl_enabled: false,
    };

    let backup_config = DatabaseConfig {
        url: "sqlite:///backup.db".to_string(),
        pool_size: 1,
        timeout: 30,
        ssl_enabled: false,
    };

    let source_adapter = AdapterFactory::create_adapter(source_config).await?;
    let backup_adapter = AdapterFactory::create_adapter(backup_config).await?;

    // Initialize backup database
    backup_adapter.initialize_schema().await?;

    // Copy all tables
    let tables = source_adapter.list_tables(None, None).await?;
    for table in tables {
        let backup_table = Table {
            id: Uuid::new_v4(),  // Generate new ID for backup
            table_path: format!("backup_{}", table.table_path),
            table_name: format!("backup_{}", table.table_name),
            table_uuid: table.table_uuid,
            created_at: table.created_at,
            updated_at: table.updated_at,
            deleted_at: table.deleted_at,
            description: table.description,
            configuration: table.configuration,
        };
        backup_adapter.create_table(&backup_table).await?;
    }

    // Copy all commits
    for table in &tables {
        let commits = source_adapter.read_commits_range(table.id, None, None, None).await?;
        if !commits.is_empty() {
            backup_adapter.write_commits(&commits).await?;
        }
    }

    println!("SQLite backup completed successfully");
    Ok(())
}
```

### DuckDB Examples

#### DuckDB for Analytical Queries

```rust
async fn duckdb_analytical_example() -> Result<(), SqlError> {
    let config = DatabaseConfig {
        url: "duckdb:///analytics.duckdb".to_string(),
        pool_size: 10,             // DuckDB supports multiple connections
        timeout: 30,
        ssl_enabled: false,
    };

    let adapter = AdapterFactory::create_adapter(config).await?;
    adapter.initialize_schema().await?;

    // Configure DuckDB for analytics
    let conn = adapter.get_connection().await?;
    conn.execute("SET memory_limit='2GB'", &[]).await?;
    conn.execute("SET threads=4", &[]).await?;
    conn.execute("SET enable_progress_bar=false", &[]).await?;

    // Load some sample data
    let table_id = create_sample_table(&adapter).await?;

    // Perform analytical queries
    let rows = conn.query(
        "SELECT
            DATE_TRUNC('day', timestamp) as day,
            COUNT(*) as commit_count,
            COUNT(DISTINCT table_id) as active_tables,
            SUM(CASE WHEN operation_type = 'WRITE' THEN 1 ELSE 0 END) as write_ops,
            SUM(CASE WHEN operation_type = 'DELETE' THEN 1 ELSE 0 END) as delete_ops
         FROM delta_commits
         WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
         GROUP BY day
         ORDER BY day DESC",
        &[]
    ).await?;

    println!("Found {} days of commit activity", rows.len());
    for row in rows {
        let day: String = row.get(0);
        let commits: i64 = row.get(1);
        let tables: i64 = row.get(2);
        let writes: i64 = row.get(3);
        let deletes: i64 = row.get(4);

        println!("{}: {} commits, {} tables, {} writes, {} deletes",
                day, commits, tables, writes, deletes);
    }

    Ok(())
}
```

#### DuckDB Materialized Views

```rust
async fn duckdb_materialized_views_example() -> Result<(), SqlError> {
    let adapter = create_duckdb_adapter().await?;
    let conn = adapter.get_connection().await?;

    // Create materialized view for active files
    conn.execute(
        "CREATE MATERIALIZED VIEW active_files AS
         SELECT
             f.table_id,
             f.path,
             f.size,
             f.modification_time,
             c.version as latest_version,
             c.timestamp as commit_timestamp
         FROM delta_files f
         JOIN delta_commits c ON f.commit_id = c.id
         WHERE f.is_add = true
           AND c.operation_type = 'WRITE'",
        &[]
    ).await?;

    // Create materialized view for table statistics
    conn.execute(
        "CREATE MATERIALIZED VIEW table_stats AS
         SELECT
             t.id as table_id,
             t.table_name,
             COUNT(DISTINCT c.version) as total_versions,
             COUNT(DISTINCT f.path) as total_files,
             SUM(CASE WHEN f.is_add = true THEN f.size ELSE 0 END) as total_size,
             MAX(c.timestamp) as last_commit_time
         FROM delta_tables t
         LEFT JOIN delta_commits c ON t.id = c.table_id
         LEFT JOIN delta_files f ON t.id = f.table_id
         WHERE t.deleted_at IS NULL
         GROUP BY t.id, t.table_name",
        &[]
    ).await?;

    // Query the materialized views (much faster than original queries)
    let stats = conn.query(
        "SELECT table_name, total_versions, total_files, total_size, last_commit_time
         FROM table_stats
         WHERE total_files > 0
         ORDER BY total_size DESC
         LIMIT 10",
        &[]
    ).await?;

    println!("Top 10 tables by size:");
    for row in stats {
        let name: String = row.get(0);
        let versions: i64 = row.get(1);
        let files: i64 = row.get(2);
        let size: i64 = row.get(3);
        let last_commit: String = row.get(4);

        println!("{}: {} versions, {} files, {} bytes, last commit: {}",
                name, versions, files, size, last_commit);
    }

    Ok(())
}
```

## Advanced Usage Patterns

### Batch Operations

```rust
async fn batch_commit_operations(adapter: &dyn DatabaseAdapter) -> Result<(), SqlError> {
    let table_id = Uuid::new_v4();
    let mut commits = Vec::new();

    // Generate many commits
    for i in 0..10000 {
        let commit = Commit {
            id: Uuid::new_v4(),
            table_id,
            version: i,
            timestamp: Utc::now(),
            operation_type: "WRITE".to_string(),
            operation_parameters: serde_json::json!({"mode": "Append"}),
            commit_info: serde_json::json!({"job_id": "batch_job_001", "attempt": i}),
            checksum: None,
            in_progress: false,
        };
        commits.push(commit);
    }

    // Write in batches for better performance
    let batch_size = 1000;
    let mut total_written = 0;

    for chunk in commits.chunks(batch_size) {
        let written = adapter.write_commits(chunk).await?;
        total_written += written;
        println!("Written {} commits (total: {})", written, total_written);
    }

    // Verify all commits were written
    let count = adapter.count_commits(table_id).await?;
    assert_eq!(count, 10000);

    println!("Batch commit test passed: {} commits written", count);
    Ok(())
}
```

### Time Travel Queries

```rust
async fn time_travel_example(adapter: &dyn DatabaseAdapter) -> Result<(), SqlError> {
    let table_id = Uuid::new_v4();
    let now = Utc::now();

    // Create initial commit
    let v1_commit = Commit {
        id: Uuid::new_v4(),
        table_id,
        version: 1,
        timestamp: now - Duration::days(10),
        operation_type: "WRITE".to_string(),
        operation_parameters: serde_json::json!({"mode": "Append"}),
        commit_info: serde_json::json!({"user": "alice"}),
        checksum: None,
        in_progress: false,
    };
    adapter.write_commit(&v1_commit).await?;

    // Add more commits over time
    for i in 2..=5 {
        let commit = Commit {
            id: Uuid::new_v4(),
            table_id,
            version: i,
            timestamp: now - Duration::days(10 - i),
            operation_type: "WRITE".to_string(),
            operation_parameters: serde_json::json!({"mode": "Append"}),
            commit_info: serde_json::json!({"user": if i % 2 == 0 { "bob" } else { "alice" }}),
            checksum: None,
            in_progress: false,
        };
        adapter.write_commit(&commit).await?;
    }

    // Query table as of specific version
    let v3_commits = adapter.read_commits_range(table_id, Some(1), Some(3), None).await?;
    println!("Commits up to version 3: {}", v3_commits.len());

    // Query commits in date range
    let start_time = now - Duration::days(5);
    let end_time = now - Duration::days(2);

    let conn = adapter.get_connection().await?;
    let commits_in_range = conn.query(
        "SELECT * FROM delta_commits
         WHERE table_id = $1 AND timestamp BETWEEN $2 AND $3
         ORDER BY version ASC",
        &[&table_id, &start_time, &end_time]
    ).await?;

    println!("Commits between day 5 and day 2 ago: {}", commits_in_range.len());

    Ok(())
}
```

### Multi-Table Transactions

```rust
async fn multi_table_transaction_example() -> Result<(), SqlError> {
    let adapter = AdapterFactory::create_adapter(create_test_config()).await?;

    // Create multiple tables
    let users_table = create_table("users", "User data");
    let orders_table = create_table("orders", "Order data");
    let products_table = create_table("products", "Product data");

    let users = adapter.create_table(&users_table).await?;
    let orders = adapter.create_table(&orders_table).await?;
    let products = adapter.create_table(&products_table).await?;

    // Begin a multi-table transaction
    let mut tx = adapter.begin_transaction().await?;

    try {
        // Add commits to multiple tables
        let user_commit = create_commit(users.id, 1, "CREATE_USERS");
        let order_commit = create_commit(orders.id, 1, "CREATE_ORDERS");
        let product_commit = create_commit(products.id, 1, "CREATE_PRODUCTS");

        tx.write_commit(&user_commit).await?;
        tx.write_commit(&order_commit).await?;
        tx.write_commit(&product_commit).await?;

        // Add some data commits
        for i in 1..=100 {
            let user_data_commit = create_commit(users.id, i + 1, "ADD_USER");
            let order_data_commit = create_commit(orders.id, i + 1, "ADD_ORDER");

            tx.write_commit(&user_data_commit).await?;
            tx.write_commit(&order_data_commit).await?;
        }

        // Commit all changes atomically
        tx.commit().await?;
        println!("Multi-table transaction committed successfully");

    } catch (e) {
        // Rollback on error
        tx.rollback().await?;
        return Err(e);
    }

    Ok(())
}
```

### Error Handling and Retry Logic

```rust
use tokio::time::{sleep, Duration};
use backoff::{ExponentialBackoff, future::retry};

async fn robust_commit_writing(adapter: &dyn DatabaseAdapter, commits: Vec<Commit>) -> Result<(), SqlError> {
    let operation = || async {
        let mut total_written = 0;
        let batch_size = 1000;

        for chunk in commits.chunks(batch_size) {
            match adapter.write_commits(chunk).await {
                Ok(written) => total_written += written,
                Err(SqlError::TransactionError(msg)) => {
                    eprintln!("Transaction error, will retry: {}", msg);
                    return Err(backoff::Error::transient(SqlError::TransactionError(msg)));
                }
                Err(SqlError::ConnectionError(msg)) => {
                    eprintln!("Connection error, will retry: {}", msg);
                    return Err(backoff::Error::transient(SqlError::ConnectionError(msg)));
                }
                Err(e) => {
                    eprintln!("Permanent error, won't retry: {}", e);
                    return Err(backoff::Error::permanent(e));
                }
            }
        }

        if total_written != commits.len() {
            return Err(backoff::Error::permanent(
                SqlError::ValidationError(format!(
                    "Expected to write {} commits, but wrote {}",
                    commits.len(),
                    total_written
                ))
            ));
        }

        Ok(())
    };

    // Use exponential backoff for retries
    let backoff = ExponentialBackoff {
        max_elapsed_time: Some(Duration::from_secs(300)), // 5 minutes max
        max_interval: Duration::from_secs(30),
        initial_interval: Duration::from_millis(100),
        multiplier: 2.0,
        randomization_factor: 0.1,
        ..Default::default()
    };

    retry(backoff, operation).await
}
```

### Connection Health Monitoring

```rust
async fn health_monitoring_example() -> Result<(), SqlError> {
    let adapter = create_production_adapter().await?;

    // Continuous health monitoring
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Health check
                let is_healthy = match adapter.health_check().await {
                    Ok(healthy) => healthy,
                    Err(e) => {
                        eprintln!("Health check failed: {}", e);
                        false
                    }
                };

                if !is_healthy {
                    eprintln!("Database is unhealthy, attempting recovery...");

                    // Attempt recovery
                    match adapter.test_connection().await {
                        Ok(true) => println!("Database recovery successful"),
                        Ok(false) => eprintln!("Database still unhealthy"),
                        Err(e) => eprintln!("Recovery attempt failed: {}", e),
                    }
                }

                // Pool statistics
                if let Ok(stats) = adapter.pool_stats().await {
                    println!("Pool stats: {} total, {} active, {:.1}% utilization",
                            stats.total_connections,
                            stats.active_connections,
                            stats.utilization_percent);

                    // Alert if pool utilization is high
                    if stats.utilization_percent > 80.0 {
                        eprintln!("WARNING: High connection pool utilization");
                    }
                }
            }

            // Handle graceful shutdown
            _ = tokio::signal::ctrl_c() => {
                println!("Shutting down health monitor");
                break;
            }
        }
    }

    Ok(())
}
```

## Testing Examples

### Unit Testing with Mock Adapters

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use deltalakedb_sql::testing::MockAdapter;

    #[tokio::test]
    async fn test_table_crud_operations() {
        let mock_adapter = MockAdapter::new();

        // Create test table
        let table = create_test_table();
        let created = mock_adapter.create_table(&table).await.unwrap();
        assert_eq!(created.table_name, table.table_name);

        // Read table
        let found = mock_adapter.read_table(created.id).await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().table_name, table.table_name);

        // List tables
        let tables = mock_adapter.list_tables(None, None).await.unwrap();
        assert_eq!(tables.len(), 1);
    }

    #[tokio::test]
    async fn test_commit_operations() {
        let mock_adapter = MockAdapter::new();
        let table_id = Uuid::new_v4();

        // Create test commits
        let commits: Vec<Commit> = (0..100)
            .map(|i| create_test_commit(table_id, i))
            .collect();

        // Write commits
        let written = mock_adapter.write_commits(&commits).await.unwrap();
        assert_eq!(written, 100);

        // Read commits
        let read_commits = mock_adapter
            .read_commits_range(table_id, None, None, None)
            .await
            .unwrap();
        assert_eq!(read_commits.len(), 100);

        // Count commits
        let count = mock_adapter.count_commits(table_id).await.unwrap();
        assert_eq!(count, 100);
    }
}
```

### Integration Testing with Real Databases

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    use docker_test::{Container, Image};

    async fn test_with_postgres() -> Result<(), SqlError> {
        // Start PostgreSQL container
        let container = Container::new(
            Image::from("postgres:15")
                .with_env_var("POSTGRES_DB", "testdb")
                .with_env_var("POSTGRES_USER", "testuser")
                .with_env_var("POSTGRES_PASSWORD", "testpass")
                .with_port_mapping(5432)
        ).await?;

        let pg_url = format!(
            "postgresql://testuser:testpass@localhost:{}/testdb",
            container.port_mapping(5432)
        );

        let config = DatabaseConfig {
            url: pg_url,
            pool_size: 5,
            timeout: 30,
            ssl_enabled: false,
        };

        let adapter = AdapterFactory::create_adapter(config).await?;
        adapter.initialize_schema().await?;

        // Run integration tests
        test_full_workflow(&adapter).await?;

        // Cleanup
        container.stop().await?;
        Ok(())
    }

    async fn test_full_workflow(adapter: &dyn DatabaseAdapter) -> Result<(), SqlError> {
        // Create table
        let table = create_test_table();
        let created = adapter.create_table(&table).await?;

        // Add commits
        for i in 1..=10 {
            let commit = create_commit(created.id, i, "WRITE");
            adapter.write_commit(&commit).await?;
        }

        // Verify data
        let commits = adapter
            .read_commits_range(created.id, Some(1), Some(10), None)
            .await?;
        assert_eq!(commits.len(), 10);

        Ok(())
    }
}
```

These examples provide comprehensive coverage of using the SQL adapters in various scenarios, from basic operations to advanced patterns and testing strategies.