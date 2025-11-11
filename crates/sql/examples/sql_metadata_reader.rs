//! Example: SQL-based Delta Lake metadata reading
//! 
//! This example demonstrates how to use the SQL backend for reading Delta Lake metadata
//! from various database engines (PostgreSQL, SQLite, DuckDB).

use deltalakedb_sql::{
    connection::{DatabaseConfig, DatabaseConnectionManager},
    reader::SqlTxnLogReader,
    schema::{DatabaseEngine, SchemaConfig},
};
use deltalakedb_core::reader::TxnLogReader;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    println!("=== Delta Lake SQL Metadata Reader Example ===\n");
    
    // Example 1: SQLite in-memory database
    println!("1. SQLite In-Memory Example:");
    sqlite_example().await?;
    
    println!("\n" + "=".repeat(50).as_str() + "\n");
    
    // Example 2: DuckDB file-based database
    println!("2. DuckDB File-Based Example:");
    duckdb_example().await?;
    
    println!("\n" + "=".repeat(50).as_str() + "\n");
    
    // Example 3: PostgreSQL (if available)
    if std::env::var("POSTGRES_URL").is_ok() {
        println!("3. PostgreSQL Example:");
        postgres_example().await?;
    } else {
        println!("3. PostgreSQL Example: Skipped (set POSTGRES_URL environment variable)");
    }
    
    println!("\n=== All examples completed successfully! ===");
    
    Ok(())
}

/// Example using SQLite in-memory database.
async fn sqlite_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create SQLite configuration
    let config = DatabaseConfig::sqlite_memory();
    
    // Initialize database with schema
    let manager = config.connect_and_initialize_schema().await?;
    
    // Create reader
    let reader = SqlTxnLogReader::new(config, "example_table".to_string())?;
    
    // Insert sample data
    insert_sample_data(&manager, "example_table").await?;
    
    // Demonstrate basic operations
    println!("  Current version: {}", reader.get_version().await?);
    
    let metadata = reader.get_table_metadata().await?;
    println!("  Table name: {}", metadata.name);
    println!("  Table location: {}", metadata.location);
    
    let schema = reader.get_schema().await?;
    println!("  Table schema: {}", schema);
    
    let active_files = reader.get_active_files().await?;
    println!("  Active files count: {}", active_files.len());
    
    // Demonstrate time travel
    println!("  Time travel to version 0:");
    let version_0 = reader.get_version_at(0).await?;
    let files_v0 = reader.get_active_files_at(0).await?;
    println!("    Version: {}, Files: {}", version_0, files_v0.len());
    
    Ok(())
}

/// Example using DuckDB file-based database.
async fn duckdb_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create DuckDB configuration
    let config = DatabaseConfig::duckdb("/tmp/example_duckdb.duckdb");
    
    // Initialize database with schema
    let manager = config.connect_and_initialize_schema().await?;
    
    // Create reader
    let reader = SqlTxnLogReader::new(config, "duckdb_table".to_string())?;
    
    // Insert sample data
    insert_sample_data(&manager, "duckdb_table").await?;
    
    // Demonstrate operations
    println!("  Current version: {}", reader.get_version().await?);
    
    let history = reader.get_version_history().await?;
    println!("  Version history:");
    for version_info in &history {
        println!("    Version {}: {} at {}", 
            version_info.version, 
            version_info.operation, 
            version_info.timestamp);
    }
    
    // List all tables
    let tables = reader.list_tables().await?;
    println!("  Available tables:");
    for table in tables {
        println!("    - {} ({})", table.name, table.location);
    }
    
    Ok(())
}

/// Example using PostgreSQL database.
async fn postgres_example() -> Result<(), Box<dyn std::error::Error>> {
    let postgres_url = std::env::var("POSTGRES_URL")?;
    
    // Create PostgreSQL configuration with SSL
    let config = DatabaseConfig::new(postgres_url)
        .ssl_mode(deltalakedb_sql::connection::SslMode::Require)
        .max_connections(20)
        .connect_timeout(30);
    
    // Initialize database with schema
    let manager = config.connect_and_initialize_schema().await?;
    
    // Create reader
    let reader = SqlTxnLogReader::new(config, "postgres_table".to_string())?;
    
    // Insert sample data
    insert_sample_data(&manager, "postgres_table").await?;
    
    // Demonstrate operations
    println!("  Current version: {}", reader.get_version().await?);
    
    let protocol = reader.get_protocol().await?;
    println!("  Protocol version: reader={}, writer={}", 
        protocol.min_reader_version, 
        protocol.min_writer_version);
    
    // Health check
    manager.health_check().await?;
    println!("  Database health check: PASSED");
    
    // Pool statistics
    let stats = manager.pool_stats();
    println!("  Connection pool: {} total, {} idle, {} active", 
        stats.size, stats.idle, stats.active);
    
    Ok(())
}

/// Insert sample data for demonstration.
async fn insert_sample_data(
    manager: &DatabaseConnectionManager, 
    table_name: &str
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = manager.connection().as_any_pool()?;
    
    // Insert table
    let table_id = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO dl_tables (table_id, name, location, metadata) 
         VALUES (?, ?, ?, ?)"
    )
    .bind(&table_id)
    .bind(table_name)
    .bind(format!("/tmp/{}", table_name))
    .bind(r#"{"format": "parquet", "partitionColumns": ["year", "month"]}"#)
    .execute(pool)
    .await?;
    
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as i64;
    
    // Insert versions
    for version in 0..=2 {
        sqlx::query(
            "INSERT INTO dl_table_versions (table_id, version, committed_at, committer, operation) 
             VALUES (?, ?, ?, ?, ?)"
        )
        .bind(&table_id)
        .bind(version)
        .bind(now + (version as i64 * 1000))
        .bind("example_user")
        .bind("WRITE")
        .execute(pool)
        .await?;
        
        // Insert files for each version
        for file_idx in 0..=2 {
            sqlx::query(
                "INSERT INTO dl_add_files (table_id, version, path, size, modification_time, data_change, partition_values) 
                 VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(&table_id)
            .bind(version)
            .bind(format!("/tmp/{}/year=2023/month={:02}/part-{:05}-{:04}.parquet", 
                table_name, (version % 12) + 1, version, file_idx))
            .bind(1024 * 1024 * (file_idx + 1))
            .bind(now + (version as i64 * 1000))
            .bind(true)
            .bind(format!(r#"{{"year": "2023", "month": "{:02}"}}"#, (version % 12) + 1))
            .execute(pool)
            .await?;
        }
    }
    
    // Insert metadata
    sqlx::query(
        "INSERT INTO dl_metadata_updates (table_id, version, schema_string, configuration) 
         VALUES (?, ?, ?, ?)"
    )
    .bind(&table_id)
    .bind(0)
    .bind(r#"{"type": "struct", "fields": [
        {"name": "id", "type": "long", "nullable": false},
        {"name": "year", "type": "integer", "nullable": false},
        {"name": "month", "type": "integer", "nullable": false},
        {"name": "data", "type": "string", "nullable": true}
    ]}"#)
    .bind(r#"{"delta.enableDeletionVectors": "false", "delta.autoOptimize.optimizeWrite": "true"}"#)
    .execute(pool)
    .await?;
    
    // Insert protocol
    sqlx::query(
        "INSERT INTO dl_protocol_updates (table_id, version, min_reader_version, min_writer_version) 
         VALUES (?, ?, ?, ?)"
    )
    .bind(&table_id)
    .bind(0)
    .bind(1)
    .bind(2)
    .execute(pool)
    .await?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_sqlite_example() {
        // This test ensures the example code works correctly
        let result = sqlite_example().await;
        assert!(result.is_ok(), "SQLite example should work: {:?}", result);
    }
    
    #[tokio::test]
    async fn test_duckdb_example() {
        let result = duckdb_example().await;
        assert!(result.is_ok(), "DuckDB example should work: {:?}", result);
    }
}