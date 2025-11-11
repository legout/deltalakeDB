//! Integration tests for SQL-based Delta Lake metadata reader.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_test;

use crate::connection::{DatabaseConfig, DatabaseConnectionManager};
use crate::reader::SqlTxnLogReader;
use crate::schema::{DatabaseEngine, SchemaGenerator};
use deltalakedb_core::reader::TxnLogReader;

/// Test setup helper that creates a test database with sample data.
async fn setup_test_database(engine: DatabaseEngine) -> Result<DatabaseConnectionManager, Box<dyn std::error::Error + Send + Sync>> {
    let config = match engine {
        DatabaseEngine::Sqlite => DatabaseConfig::sqlite_memory(),
        DatabaseEngine::DuckDB => DatabaseConfig::duckdb_memory(),
        DatabaseEngine::Postgres => {
            // Skip PostgreSQL tests if no connection string is provided
            let pg_url = std::env::var("POSTGRES_TEST_URL").unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/deltalakedb_test".to_string());
            DatabaseConfig::new(pg_url)
        }
    };

    let manager = config.connect_and_initialize_schema().await?;
    
    // Insert test data
    insert_test_data(&manager).await?;
    
    Ok(manager)
}

/// Insert sample Delta Lake metadata for testing.
async fn insert_test_data(manager: &DatabaseConnectionManager) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let pool = manager.connection().as_any_pool()?;
    
    // Insert a test table
    let table_id = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO dl_tables (table_id, name, location, metadata) 
         VALUES (?, ?, ?, ?)"
    )
    .bind(&table_id)
    .bind("test_table")
    .bind("/tmp/test_table")
    .bind(r#"{"format": "parquet", "partitionColumns": []}"#)
    .execute(pool)
    .await?;
    
    // Insert table versions
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    
    for version in 0..=3 {
        sqlx::query(
            "INSERT INTO dl_table_versions (table_id, version, committed_at, committer, operation) 
             VALUES (?, ?, ?, ?, ?)"
        )
        .bind(&table_id)
        .bind(version)
        .bind(now + (version as i64 * 1000))
        .bind("test_committer")
        .bind("WRITE")
        .execute(pool)
        .await?;
    }
    
    // Insert add files for each version
    for version in 0..=3 {
        for file_idx in 0..=2 {
            sqlx::query(
                "INSERT INTO dl_add_files (table_id, version, path, size, modification_time, data_change, partition_values) 
                 VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(&table_id)
            .bind(version)
            .bind(format!("/tmp/test_table/part-00000-{:04}-{}.parquet", version, file_idx))
            .bind(1024 * (file_idx + 1))
            .bind(now + (version as i64 * 1000))
            .bind(true)
            .bind(r#"{}"#)
            .execute(pool)
            .await?;
        }
    }
    
    // Insert some remove files to test active file filtering
    sqlx::query(
        "INSERT INTO dl_remove_files (table_id, version, path, deletion_timestamp, data_change) 
         VALUES (?, ?, ?, ?, ?)"
    )
    .bind(&table_id)
    .bind(2)
    .bind("/tmp/test_table/part-00000-0001-1.parquet")
    .bind(now + 2000)
    .bind(true)
    .execute(pool)
    .await?;
    
    // Insert metadata updates
    sqlx::query(
        "INSERT INTO dl_metadata_updates (table_id, version, schema_string, configuration) 
         VALUES (?, ?, ?, ?)"
    )
    .bind(&table_id)
    .bind(0)
    .bind(r#"{"type": "struct", "fields": [{"name": "id", "type": "long", "nullable": false}]}"#)
    .bind(r#"{"delta.enableDeletionVectors": "false"}"#)
    .execute(pool)
    .await?;
    
    // Insert protocol updates
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

#[tokio::test]
async fn test_sqlite_reader_basic_operations() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manager = setup_test_database(DatabaseEngine::Sqlite).await?;
    let config = DatabaseConfig::sqlite_memory();
    let reader = SqlTxnLogReader::new(config, "test_table".to_string())?;
    
    // Test getting current version
    let version = reader.get_version().await?;
    assert_eq!(version, 3);
    
    // Test getting table metadata
    let metadata = reader.get_table_metadata().await?;
    assert_eq!(metadata.name, "test_table");
    assert_eq!(metadata.location, "/tmp/test_table");
    
    // Test getting schema
    let schema = reader.get_schema().await?;
    assert!(schema.contains("id"));
    
    // Test getting protocol
    let protocol = reader.get_protocol().await?;
    assert_eq!(protocol.min_reader_version, 1);
    assert_eq!(protocol.min_writer_version, 2);
    
    Ok(())
}

#[tokio::test]
async fn test_duckdb_reader_basic_operations() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manager = setup_test_database(DatabaseEngine::DuckDB).await?;
    let config = DatabaseConfig::duckdb_memory();
    let reader = SqlTxnLogReader::new(config, "test_table".to_string())?;
    
    // Test getting current version
    let version = reader.get_version().await?;
    assert_eq!(version, 3);
    
    // Test getting active files
    let active_files = reader.get_active_files().await?;
    assert!(!active_files.is_empty());
    
    // Should have 3 versions * 3 files per version - 1 removed file = 8 active files
    assert_eq!(active_files.len(), 8);
    
    Ok(())
}

#[tokio::test]
async fn test_time_travel_queries() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manager = setup_test_database(DatabaseEngine::Sqlite).await?;
    let config = DatabaseConfig::sqlite_memory();
    let reader = SqlTxnLogReader::new(config, "test_table".to_string())?;
    
    // Test time travel to specific versions
    for target_version in 0..=3 {
        let version = reader.get_version_at(target_version).await?;
        assert_eq!(version, target_version);
        
        let active_files = reader.get_active_files_at(target_version).await?;
        let expected_files = (target_version + 1) * 3; // Each version adds 3 files
        assert_eq!(active_files.len(), expected_files as usize);
    }
    
    Ok(())
}

#[tokio::test]
async fn test_version_history() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manager = setup_test_database(DatabaseEngine::Sqlite).await?;
    let config = DatabaseConfig::sqlite_memory();
    let reader = SqlTxnLogReader::new(config, "test_table".to_string())?;
    
    let history = reader.get_version_history().await?;
    assert_eq!(history.len(), 4); // Versions 0, 1, 2, 3
    
    for (i, version_info) in history.iter().enumerate() {
        assert_eq!(version_info.version, i as i64);
        assert_eq!(version_info.operation, "WRITE");
        assert_eq!(version_info.committer, "test_committer");
    }
    
    Ok(())
}

#[tokio::test]
async fn test_table_discovery() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manager = setup_test_database(DatabaseEngine::Sqlite).await?;
    let config = DatabaseConfig::sqlite_memory();
    let reader = SqlTxnLogReader::new(config, "test_table".to_string())?;
    
    let tables = reader.list_tables().await?;
    assert!(!tables.is_empty());
    assert!(tables.iter().any(|t| t.name == "test_table"));
    
    Ok(())
}

#[tokio::test]
async fn test_schema_validation() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manager = setup_test_database(DatabaseEngine::Sqlite).await?;
    
    // Test schema validation
    let pool = manager.connection().as_any_pool()?;
    let is_valid = manager.schema_manager().validate_schema(&pool).await?;
    assert!(is_valid);
    
    // Test migration status
    let current_version = manager.schema_manager().get_current_version(&pool).await?;
    assert!(current_version.is_some());
    assert!(current_version.unwrap() > 0);
    
    Ok(())
}

#[tokio::test]
async fn test_connection_health_check() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manager = setup_test_database(DatabaseEngine::Sqlite).await?;
    
    // Test health check
    manager.health_check().await?;
    
    // Test pool stats
    let stats = manager.pool_stats();
    assert!(stats.size > 0);
    
    Ok(())
}

#[tokio::test]
async fn test_error_handling() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Test with non-existent table
    let config = DatabaseConfig::sqlite_memory();
    let reader = SqlTxnLogReader::new(config, "non_existent_table".to_string())?;
    
    // Should return error for non-existent table
    let result = reader.get_version().await;
    assert!(result.is_err());
    
    // Test with invalid database URL
    let invalid_config = DatabaseConfig::new("invalid://url".to_string());
    let invalid_reader = SqlTxnLogReader::new(invalid_config, "test_table".to_string())?;
    
    let result = invalid_reader.get_version().await;
    assert!(result.is_err());
    
    Ok(())
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;
    
    #[tokio::test]
    async fn test_query_performance() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let manager = setup_test_database(DatabaseEngine::Sqlite).await?;
        let config = DatabaseConfig::sqlite_memory();
        let reader = SqlTxnLogReader::new(config, "test_table".to_string())?;
        
        // Measure query performance
        let start = Instant::now();
        let _version = reader.get_version().await?;
        let version_time = start.elapsed();
        
        let start = Instant::now();
        let _active_files = reader.get_active_files().await?;
        let files_time = start.elapsed();
        
        let start = Instant::now();
        let _history = reader.get_version_history().await?;
        let history_time = start.elapsed();
        
        // Basic performance assertions (these values may need adjustment)
        assert!(version_time.as_millis() < 100, "Version query too slow: {:?}", version_time);
        assert!(files_time.as_millis() < 100, "Active files query too slow: {:?}", files_time);
        assert!(history_time.as_millis() < 100, "History query too slow: {:?}", history_time);
        
        println!("Performance metrics:");
        println!("  Version query: {:?}", version_time);
        println!("  Active files query: {:?}", files_time);
        println!("  History query: {:?}", history_time);
        
        Ok(())
    }
}

#[cfg(test)]
mod concurrency_tests {
    use super::*;
    use tokio::task::JoinSet;
    
    #[tokio::test]
    async fn test_concurrent_reads() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let manager = setup_test_database(DatabaseEngine::Sqlite).await?;
        let config = DatabaseConfig::sqlite_memory();
        let reader = std::sync::Arc::new(SqlTxnLogReader::new(config, "test_table".to_string())?);
        
        let mut tasks = JoinSet::new();
        
        // Spawn multiple concurrent read tasks
        for i in 0..10 {
            let reader_clone = reader.clone();
            tasks.spawn(async move {
                let version = reader_clone.get_version().await.unwrap();
                let files = reader_clone.get_active_files().await.unwrap();
                (i, version, files.len())
            });
        }
        
        // Collect results
        let mut results = Vec::new();
        while let Some(result) = tasks.join_next().await {
            results.push(result??);
        }
        
        // Verify all tasks got consistent results
        for (i, version, file_count) in results {
            assert_eq!(version, 3, "Task {} got wrong version", i);
            assert_eq!(file_count, 8, "Task {} got wrong file count", i);
        }
        
        Ok(())
    }
}