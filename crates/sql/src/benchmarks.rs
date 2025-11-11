//! Performance benchmarks for SQL-based Delta Lake metadata reader.

use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::time::sleep;

use crate::connection::{DatabaseConfig, DatabaseConnectionManager};
use crate::reader::SqlTxnLogReader;
use crate::schema::{DatabaseEngine, SchemaGenerator};
use deltalakedb_core::reader::TxnLogReader;

/// Benchmark configuration.
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Number of versions to generate
    pub num_versions: usize,
    /// Number of files per version
    pub files_per_version: usize,
    /// Number of warmup iterations
    pub warmup_iterations: usize,
    /// Number of measurement iterations
    pub measurement_iterations: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            num_versions: 100,
            files_per_version: 50,
            warmup_iterations: 3,
            measurement_iterations: 10,
        }
    }
}

/// Benchmark results.
#[derive(Debug, Clone)]
pub struct BenchmarkResults {
    /// Average query time in milliseconds
    pub avg_time_ms: f64,
    /// Minimum query time in milliseconds
    pub min_time_ms: f64,
    /// Maximum query time in milliseconds
    pub max_time_ms: f64,
    /// Standard deviation in milliseconds
    pub std_dev_ms: f64,
    /// Total queries executed
    pub total_queries: usize,
}

/// Performance benchmark suite.
pub struct PerformanceBenchmark {
    config: BenchmarkConfig,
}

impl PerformanceBenchmark {
    /// Create a new benchmark suite.
    pub fn new(config: BenchmarkConfig) -> Self {
        Self { config }
    }
    
    /// Run all benchmarks for all database engines.
    pub async fn run_all_benchmarks(&self) -> Result<HashMap<String, HashMap<String, BenchmarkResults>>, Box<dyn std::error::Error + Send + Sync>> {
        let mut all_results = HashMap::new();
        
        // Test SQLite
        if let Ok(results) = self.run_engine_benchmarks(DatabaseEngine::Sqlite).await {
            all_results.insert("sqlite".to_string(), results);
        }
        
        // Test DuckDB
        if let Ok(results) = self.run_engine_benchmarks(DatabaseEngine::DuckDB).await {
            all_results.insert("duckdb".to_string(), results);
        }
        
        // Test PostgreSQL if available
        if std::env::var("POSTGRES_TEST_URL").is_ok() {
            if let Ok(results) = self.run_engine_benchmarks(DatabaseEngine::Postgres).await {
                all_results.insert("postgres".to_string(), results);
            }
        }
        
        Ok(all_results)
    }
    
    /// Run benchmarks for a specific database engine.
    async fn run_engine_benchmarks(&self, engine: DatabaseEngine) -> Result<HashMap<String, BenchmarkResults>, Box<dyn std::error::Error + Send + Sync>> {
        let manager = self.setup_benchmark_database(engine).await?;
        let config = self.get_config_for_engine(engine);
        let reader = SqlTxnLogReader::new(config, "benchmark_table".to_string())?;
        
        let mut results = HashMap::new();
        
        // Benchmark version queries
        results.insert("get_version".to_string(), 
            self.benchmark_operation("get_version", &reader, |r| async move { r.get_version().await.map(|_| ()) }).await?);
        
        // Benchmark active files queries
        results.insert("get_active_files".to_string(),
            self.benchmark_operation("get_active_files", &reader, |r| async move { r.get_active_files().await.map(|_| ()) }).await?);
        
        // Benchmark schema queries
        results.insert("get_schema".to_string(),
            self.benchmark_operation("get_schema", &reader, |r| async move { r.get_schema().await.map(|_| ()) }).await?);
        
        // Benchmark version history queries
        results.insert("get_version_history".to_string(),
            self.benchmark_operation("get_version_history", &reader, |r| async move { r.get_version_history().await.map(|_| ()) }).await?);
        
        // Benchmark time travel queries
        results.insert("get_version_at".to_string(),
            self.benchmark_operation("get_version_at", &reader, |r| async move { r.get_version_at(50).await.map(|_| ()) }).await?);
        
        Ok(results)
    }
    
    /// Setup a benchmark database with test data.
    async fn setup_benchmark_database(&self, engine: DatabaseEngine) -> Result<DatabaseConnectionManager, Box<dyn std::error::Error + Send + Sync>> {
        let config = match engine {
            DatabaseEngine::Sqlite => DatabaseConfig::sqlite_memory(),
            DatabaseEngine::DuckDB => DatabaseConfig::duckdb_memory(),
            DatabaseEngine::Postgres => {
                let pg_url = std::env::var("POSTGRES_TEST_URL")
                    .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/deltalakedb_bench".to_string());
                DatabaseConfig::new(pg_url)
            }
        };

        let manager = config.connect_and_initialize_schema().await?;
        
        // Insert benchmark data
        self.insert_benchmark_data(&manager, engine).await?;
        
        Ok(manager)
    }
    
    /// Insert large amount of test data for benchmarking.
    async fn insert_benchmark_data(&self, manager: &DatabaseConnectionManager, engine: DatabaseEngine) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = manager.connection().as_any_pool()?;
        
        println!("Inserting {} versions with {} files each...", self.config.num_versions, self.config.files_per_version);
        
        // Insert test table
        let table_id = uuid::Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO dl_tables (table_id, name, location, metadata) 
             VALUES (?, ?, ?, ?)"
        )
        .bind(&table_id)
        .bind("benchmark_table")
        .bind("/tmp/benchmark_table")
        .bind(r#"{"format": "parquet", "partitionColumns": ["year", "month"]}"#)
        .execute(pool)
        .await?;
        
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as i64;
        
        // Insert versions and files in batches for better performance
        let batch_size = 10;
        for batch_start in (0..self.config.num_versions).step_by(batch_size) {
            let batch_end = (batch_start + batch_size).min(self.config.num_versions);
            
            // Start transaction for batch
            let mut tx = pool.begin().await?;
            
            for version in batch_start..batch_end {
                // Insert version
                sqlx::query(
                    "INSERT INTO dl_table_versions (table_id, version, committed_at, committer, operation) 
                     VALUES (?, ?, ?, ?, ?)"
                )
                .bind(&table_id)
                .bind(version as i64)
                .bind(now + (version as i64 * 1000))
                .bind("benchmark_committer")
                .bind("WRITE")
                .execute(&mut *tx)
                .await?;
                
                // Insert files for this version
                for file_idx in 0..self.config.files_per_version {
                    sqlx::query(
                        "INSERT INTO dl_add_files (table_id, version, path, size, modification_time, data_change, partition_values) 
                         VALUES (?, ?, ?, ?, ?, ?, ?)"
                    )
                    .bind(&table_id)
                    .bind(version as i64)
                    .bind(format!("/tmp/benchmark_table/year=2023/month={:02}/part-{:05}-{:04}.parquet", 
                        (version % 12) + 1, version, file_idx))
                    .bind(1024 * 1024 * (file_idx + 1)) // 1MB to 50MB files
                    .bind(now + (version as i64 * 1000))
                    .bind(true)
                    .bind(format!(r#"{{"year": "2023", "month": "{:02}"}}"#, (version % 12) + 1))
                    .execute(&mut *tx)
                    .await?;
                }
                
                // Insert some remove files to simulate realistic scenarios
                if version > 10 && version % 5 == 0 {
                    let files_to_remove = self.config.files_per_version / 4;
                    for file_idx in 0..files_to_remove {
                        sqlx::query(
                            "INSERT INTO dl_remove_files (table_id, version, path, deletion_timestamp, data_change) 
                             VALUES (?, ?, ?, ?, ?)"
                        )
                        .bind(&table_id)
                        .bind(version as i64)
                        .bind(format!("/tmp/benchmark_table/year=2023/month={:02}/part-{:05}-{:04}.parquet", 
                            ((version - 10) % 12) + 1, version - 10, file_idx))
                        .bind(now + (version as i64 * 1000))
                        .bind(true)
                        .execute(&mut *tx)
                        .await?;
                    }
                }
            }
            
            tx.commit().await?;
            
            if batch_start % 50 == 0 {
                println!("  Inserted {} versions...", batch_end);
            }
        }
        
        // Insert metadata and protocol
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
        
        println!("Benchmark data insertion complete.");
        Ok(())
    }
    
    /// Get database configuration for the specified engine.
    fn get_config_for_engine(&self, engine: DatabaseEngine) -> DatabaseConfig {
        match engine {
            DatabaseEngine::Sqlite => DatabaseConfig::sqlite_memory(),
            DatabaseEngine::DuckDB => DatabaseConfig::duckdb_memory(),
            DatabaseEngine::Postgres => {
                let pg_url = std::env::var("POSTGRES_TEST_URL")
                    .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/deltalakedb_bench".to_string());
                DatabaseConfig::new(pg_url)
            }
        }
    }
    
    /// Benchmark a specific operation.
    async fn benchmark_operation<F, Fut>(&self, operation_name: &str, reader: &SqlTxnLogReader, operation: F) -> Result<BenchmarkResults, Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn(&SqlTxnLogReader) -> Fut,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    {
        println!("Benchmarking {}...", operation_name);
        
        // Warmup iterations
        for _ in 0..self.config.warmup_iterations {
            let _ = operation(reader).await;
            // Small delay between warmup runs
            sleep(Duration::from_millis(10)).await;
        }
        
        // Measurement iterations
        let mut times = Vec::new();
        for _ in 0..self.config.measurement_iterations {
            let start = Instant::now();
            let _ = operation(reader).await?;
            let elapsed = start.elapsed();
            times.push(elapsed.as_millis() as f64);
            
            // Small delay between measurements
            sleep(Duration::from_millis(10)).await;
        }
        
        // Calculate statistics
        let avg_time = times.iter().sum::<f64>() / times.len() as f64;
        let min_time = times.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_time = times.iter().fold(0.0, |a, &b| a.max(b));
        
        let variance = times.iter()
            .map(|&x| (x - avg_time).powi(2))
            .sum::<f64>() / times.len() as f64;
        let std_dev = variance.sqrt();
        
        Ok(BenchmarkResults {
            avg_time_ms: avg_time,
            min_time_ms: min_time,
            max_time_ms: max_time,
            std_dev_ms: std_dev,
            total_queries: self.config.measurement_iterations,
        })
    }
    
    /// Print benchmark results in a formatted table.
    pub fn print_results(&self, results: &HashMap<String, HashMap<String, BenchmarkResults>>) {
        println!("\n=== Performance Benchmark Results ===\n");
        
        for (engine, engine_results) in results {
            println!("Engine: {}", engine.to_uppercase());
            println!("{:<20} {:<12} {:<12} {:<12} {:<12} {:<10}", 
                "Operation", "Avg (ms)", "Min (ms)", "Max (ms)", "Std Dev", "Queries");
            println!("{}", "-".repeat(80));
            
            for (operation, result) in engine_results {
                println!("{:<20} {:<12.2} {:<12.2} {:<12.2} {:<12.2} {:<10}",
                    operation,
                    result.avg_time_ms,
                    result.min_time_ms,
                    result.max_time_ms,
                    result.std_dev_ms,
                    result.total_queries
                );
            }
            println!();
        }
    }
}

/// Run the full benchmark suite.
pub async fn run_benchmarks() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = BenchmarkConfig::default();
    let benchmark = PerformanceBenchmark::new(config);
    
    let results = benchmark.run_all_benchmarks().await?;
    benchmark.print_results(&results);
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_small_benchmark() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = BenchmarkConfig {
            num_versions: 10,
            files_per_version: 5,
            warmup_iterations: 1,
            measurement_iterations: 3,
        };
        
        let benchmark = PerformanceBenchmark::new(config);
        let results = benchmark.run_engine_benchmarks(DatabaseEngine::Sqlite).await?;
        
        assert!(!results.is_empty());
        assert!(results.contains_key("get_version"));
        
        let version_result = &results["get_version"];
        assert!(version_result.avg_time_ms >= 0.0);
        assert!(version_result.total_queries > 0);
        
        Ok(())
    }
}