//! Comprehensive test suite for SQL adapters

use crate::{AdapterFactory, DatabaseConfig};
use deltalakedb_core::{Table, Commit, Protocol, Metadata, Action};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
use chrono::Utc;
use tempfile::TempDir;
use serde_json::json;

/// Test configuration for different database types
pub struct TestConfig {
    pub postgres_url: Option<String>,
    pub sqlite_file: Option<TempDir>,
    pub duckdb_file: Option<TempDir>,
}

impl TestConfig {
    /// Create a new test configuration
    pub fn new() -> Self {
        Self {
            postgres_url: std::env::var("POSTGRES_URL").ok(),
            sqlite_file: None,
            duckdb_file: None,
        }
    }

    /// Create a SQLite database config
    pub fn sqlite_config(&self) -> Option<DatabaseConfig> {
        let db_path = match &self.sqlite_file {
            Some(temp_dir) => temp_dir.path().join("test.db"),
            None => return None,
        };

        Some(DatabaseConfig {
            url: format!("sqlite://{}", db_path.display()),
            pool_size: 1,
            timeout: 30,
            ssl_enabled: false,
        })
    }

    /// Create a DuckDB database config
    pub fn duckdb_config(&self) -> Option<DatabaseConfig> {
        let db_path = match &self.duckdb_file {
            Some(temp_dir) => temp_dir.path().join("test.duckdb"),
            None => return None,
        };

        Some(DatabaseConfig {
            url: format!("duckdb://{}", db_path.display()),
            pool_size: 1,
            timeout: 30,
            ssl_enabled: false,
        })
    }

    /// Create a PostgreSQL database config
    pub fn postgres_config(&self) -> Option<DatabaseConfig> {
        self.postgres_url.as_ref().map(|url| DatabaseConfig {
            url: url.clone(),
            pool_size: 5,
            timeout: 30,
            ssl_enabled: true,
        })
    }

    /// Create an in-memory database config (SQLite)
    pub fn memory_config() -> DatabaseConfig {
        DatabaseConfig {
            url: "sqlite::memory:".to_string(),
            pool_size: 1,
            timeout: 30,
            ssl_enabled: false,
        }
    }

    /// Create an in-memory DuckDB config
    pub fn duckdb_memory_config() -> DatabaseConfig {
        DatabaseConfig {
            url: "duckdb://memory".to_string(),
            pool_size: 1,
            timeout: 30,
            ssl_enabled: false,
        }
    }
}

/// Test utility functions
pub mod utils {
    use super::*;

    /// Create a test table
    pub fn create_test_table(path: &str) -> Table {
        Table {
            id: Uuid::new_v4(),
            table_path: path.to_string(),
            table_name: path.split('/').last().unwrap_or("test").to_string(),
            table_uuid: Uuid::new_v4(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// Create a test protocol
    pub fn create_test_protocol() -> Protocol {
        Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        }
    }

    /// Create test metadata
    pub fn create_test_metadata(name: &str) -> Metadata {
        Metadata {
            id: format!("test-{}-metadata", name),
            name: name.to_string(),
            description: Some(format!("Test table: {}", name)),
            format: "parquet".to_string(),
            schema_string: Some(
                json!({
                    "type": "struct",
                    "fields": [
                        {"name": "id", "type": "long", "nullable": false},
                        {"name": "name", "type": "string", "nullable": true}
                    ]
                }).to_string()
            ),
            partition_columns: vec!["year".to_string(), "month".to_string()],
            configuration: json!({
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true"
            }),
            created_time: Some(Utc::now().timestamp_millis()),
        }
    }

    /// Create a test commit
    pub fn create_test_commit(table_id: Uuid, version: i64, operation: &str) -> Commit {
        Commit {
            id: Uuid::new_v4(),
            table_id,
            version,
            timestamp: Utc::now(),
            operation_type: operation.to_string(),
            operation_parameters: json!({
                "mode": "Append",
                "partitionBy": ["year", "month"]
            }),
            commit_info: json!({
                "timestamp": Utc::now().timestamp_millis(),
                "operationId": Uuid::new_v4().to_string(),
                "operationParameters": json!({"mode": "Append"})
            }),
        }
    }

    /// Create test AddFile action
    pub fn create_test_add_file_action() -> Action {
        Action::AddFile {
            path: "path/to/file.parquet".to_string(),
            size: 1024,
            modification_time: Utc::now().timestamp_millis(),
            data_change: true,
            stats: Some("{\"numRecords\": 100, \"minValues\": {}, \"maxValues\": {}}".to_string()),
            partition_values: Some(HashMap::from([
                ("year".to_string(), "2023".to_string()),
                ("month".to_string(), "12".to_string())
            ])),
            tags: Some(HashMap::from([
                ("format".to_string(), "parquet".to_string()),
                ("compression".to_string(), "snappy".to_string())
            ])),
        }
    }

    /// Create test RemoveFile action
    pub fn create_test_remove_file_action() -> Action {
        Action::RemoveFile {
            path: "path/to/old_file.parquet".to_string(),
            deletion_timestamp: Utc::now().timestamp_millis(),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(HashMap::from([
                ("year".to_string(), "2023".to_string()),
                ("month".to_string(), "11".to_string())
            ])),
            tags: Some(HashMap::from([
                ("format".to_string(), "parquet".to_string()),
                ("reason".to_string(), "updated".to_string())
            ])),
        }
    }

    /// Create test Metadata action
    pub fn create_test_metadata_action() -> Action {
        Action::Metadata(json!({
            "id": "test-metadata",
            "name": "test_table",
            "format": {"provider": "parquet"},
            "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
            "partitionColumns": ["year", "month"]
        }))
    }

    /// Wait for async operation with timeout
    pub async fn wait_with_timeout<F, T>(
        future: F,
        timeout_secs: u64,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        F: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>,
    {
        tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            future
        ).await.map_err(|_| "Operation timed out")?
    }

    /// Compare two tables for equality (ignoring timestamps)
    pub fn tables_equal_ignoring_timestamps(a: &Table, b: &Table) -> bool {
        a.id == b.id &&
        a.table_path == b.table_path &&
        a.table_name == b.table_name &&
        a.table_uuid == b.table_uuid
    }

    /// Compare two commits for equality (ignoring timestamps)
    pub fn commits_equal_ignoring_timestamps(a: &Commit, b: &Commit) -> bool {
        a.id == b.id &&
        a.table_id == b.table_id &&
        a.version == b.version &&
        a.operation_type == b.operation_type &&
        a.operation_parameters == b.operation_parameters
    }
}

/// Integration test macros for running tests across all supported databases
#[macro_export]
macro_rules! test_all_adapters {
    ($test_name:ident, $test_body:expr) => {
        #[tokio::test]
        async fn $test_name() {
            let test_config = crate::tests::TestConfig::new();

            // Test SQLite if available
            if let Some(config) = test_config.sqlite_config() {
                let adapter = crate::AdapterFactory::create_adapter(config).await.unwrap();
                println!("Testing SQLite adapter");
                $test_body(adapter).await;
            }

            // Test DuckDB if available
            if let Some(config) = test_config.duckdb_config() {
                let adapter = crate::AdapterFactory::create_adapter(config).await.unwrap();
                println!("Testing DuckDB adapter");
                $test_body(adapter).await;
            }

            // Test in-memory SQLite
            {
                let adapter = crate::AdapterFactory::create_adapter(crate::tests::TestConfig::memory_config()).await.unwrap();
                println!("Testing in-memory SQLite adapter");
                $test_body(adapter).await;
            }

            // Test in-memory DuckDB
            {
                let adapter = crate::AdapterFactory::create_adapter(crate::tests::TestConfig::duckdb_memory_config()).await.unwrap();
                println!("Testing in-memory DuckDB adapter");
                $test_body(adapter).await;
            }

            // Test PostgreSQL if available
            if let Some(config) = test_config.postgres_config() {
                let adapter = crate::AdapterFactory::create_adapter(config).await.unwrap();
                println!("Testing PostgreSQL adapter");
                $test_body(adapter).await;
            }
        }
    };
}

#[macro_export]
macro_rules! test_sqlite_only {
    ($test_name:ident, $test_body:expr) => {
        #[tokio::test]
        async fn $test_name() {
            let config = crate::tests::TestConfig::memory_config();
            let adapter = crate::AdapterFactory::create_adapter(config).await.unwrap();
            $test_body(adapter).await;
        }
    };
}

#[macro_export]
macro_rules! test_duckdb_only {
    ($test_name:ident, $test_body:expr) => {
        #[tokio::test]
        async fn $test_name() {
            let config = crate::tests::TestConfig::duckdb_memory_config();
            let adapter = crate::AdapterFactory::create_adapter(config).await.unwrap();
            $test_body(adapter).await;
        }
    };
}

/// Performance testing utilities
pub mod performance {
    use super::*;
    use std::time::Instant;

    /// Performance metrics for operations
    #[derive(Debug, Clone)]
    pub struct PerformanceMetrics {
        pub operation_name: String,
        pub duration_ms: u64,
        pub records_affected: u64,
        pub success: bool,
    }

    impl PerformanceMetrics {
        pub fn new(operation_name: &str, duration_ms: u64, records_affected: u64) -> Self {
            Self {
                operation_name: operation_name.to_string(),
                duration_ms,
                records_affected,
                success: true,
            }
        }

        pub fn failed(operation_name: &str, duration_ms: u64) -> Self {
            Self {
                operation_name: operation_name.to_string(),
                duration_ms,
                records_affected: 0,
                success: false,
            }
        }

        pub fn throughput(&self) -> f64 {
            if self.duration_ms > 0 {
                (self.records_affected as f64) / (self.duration_ms as f64 / 1000.0)
            } else {
                0.0
            }
        }
    }

    /// Measure performance of an operation
    pub async fn measure<F, Fut, T>(operation_name: &str, operation: F) -> (T, PerformanceMetrics)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let start = Instant::now();
        let result = operation().await;
        let duration = start.elapsed();

        let metrics = PerformanceMetrics::new(operation_name, duration.as_millis() as u64, 1);
        (result, metrics)
    }

    /// Measure performance of a bulk operation
    pub async fn measure_bulk<F, Fut, T>(
        operation_name: &str,
        record_count: u64,
        operation: F,
    ) -> (T, PerformanceMetrics)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let start = Instant::now();
        let result = operation().await;
        let duration = start.elapsed();

        let metrics = PerformanceMetrics::new(operation_name, duration.as_millis() as u64, record_count);
        (result, metrics)
    }
}

/// Concurrency testing utilities
pub mod concurrency {
    use super::*;
    use std::sync::Arc;
    use tokio::task::JoinSet;

    /// Run multiple concurrent operations
    pub async fn run_concurrent<F, Fut, T>(
        operations: Vec<F>,
    ) -> Vec<Result<T, Box<dyn std::error::Error + Send + Sync>>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
        T: Send + 'static,
    {
        let mut set = JoinSet::new();

        for operation in operations {
            set.spawn(operation());
        }

        let mut results = Vec::new();
        while let Some(result) = set.join_next().await {
            match result {
                Ok(task_result) => results.push(task_result),
                Err(e) => results.push(Err(Box::new(e))),
            }
        }

        results
    }

    /// Test concurrent table creation
    pub async fn test_concurrent_table_creation(
        adapter: Arc<dyn crate::traits::DatabaseAdapter>,
        table_count: usize,
    ) -> Vec<Result<Table, Box<dyn std::error::Error + Send + Sync>>> {
        let operations: Vec<_> = (0..table_count)
            .map(|i| {
                let adapter = adapter.clone();
                move || {
                    let table = crate::tests::utils::create_test_table(&format!("/test/concurrent_table_{}", i));
                    async move {
                        adapter.create_table(&table).await
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                    }
                }
            })
            .collect();

        run_concurrent(operations).await
    }

    /// Test concurrent commit writing
    pub async fn test_concurrent_commit_writing(
        adapter: Arc<dyn crate::traits::DatabaseAdapter>,
        table_id: Uuid,
        commit_count: usize,
    ) -> Vec<Result<Commit, Box<dyn std::error::Error + Send + Sync>>> {
        let operations: Vec<_> = (0..commit_count)
            .map(|i| {
                let adapter = adapter.clone();
                move || {
                    let commit = crate::tests::utils::create_test_commit(table_id, (i + 1) as i64, "WRITE");
                    async move {
                        adapter.write_commit(&commit).await
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                    }
                }
            })
            .collect();

        run_concurrent(operations).await
    }
}

#[cfg(test)]
mod test_macros {
    use super::*;
    use crate::AdapterFactory;

    test_sqlite_only!(test_macros_work, |adapter| async move {
        assert_eq!(adapter.database_type(), "sqlite");
    });

    test_duckdb_only!(test_duckdb_macros_work, |adapter| async move {
        assert_eq!(adapter.database_type(), "duckdb");
    });
}