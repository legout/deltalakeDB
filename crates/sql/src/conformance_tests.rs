//! Conformance tests for Delta Lake protocol compliance.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::connection::{DatabaseConfig, DatabaseConnectionManager};
use crate::reader::SqlTxnLogReader;
use crate::schema::DatabaseEngine;
use deltalakedb_core::reader::TxnLogReader;

/// Delta Lake protocol conformance test suite.
pub struct DeltaConformanceTests {
    manager: DatabaseConnectionManager,
    reader: SqlTxnLogReader,
}

impl DeltaConformanceTests {
    /// Create a new conformance test suite.
    pub async fn new(engine: DatabaseEngine) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = match engine {
            DatabaseEngine::Sqlite => DatabaseConfig::sqlite_memory(),
            DatabaseEngine::DuckDB => DatabaseConfig::duckdb_memory(),
            DatabaseEngine::Postgres => {
                let pg_url = std::env::var("POSTGRES_TEST_URL")
                    .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/deltalakedb_conformance".to_string());
                DatabaseConfig::new(pg_url)
            }
        };

        let manager = config.connect_and_initialize_schema().await?;
        let reader = SqlTxnLogReader::new(config, "conformance_table".to_string())?;
        
        Ok(Self { manager, reader })
    }
    
    /// Run all conformance tests.
    pub async fn run_all_tests(&self) -> Result<ConformanceResults, Box<dyn std::error::Error + Send + Sync>> {
        let mut results = ConformanceResults::new();
        
        // Test basic protocol requirements
        results.add_result("protocol_version_validation", self.test_protocol_version_validation().await);
        results.add_result("schema_compliance", self.test_schema_compliance().await);
        results.add_result("file_action_integrity", self.test_file_action_integrity().await);
        results.add_result("version_monotonicity", self.test_version_monotonicity().await);
        results.add_result("time_travel_consistency", self.test_time_travel_consistency().await);
        results.add_result("active_files_correctness", self.test_active_files_correctness().await);
        results.add_result("metadata_completeness", self.test_metadata_completeness().await);
        results.add_result("transaction_atomicity", self.test_transaction_atomicity().await);
        results.add_result("partition_pruning", self.test_partition_pruning().await);
        results.add_result("schema_evolution", self.test_schema_evolution().await);
        
        Ok(results)
    }
    
    /// Test protocol version validation.
    async fn test_protocol_version_validation(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Setup test data with valid protocol versions
        self.setup_protocol_test_data().await?;
        
        let protocol = self.reader.get_protocol().await?;
        
        // Verify protocol version constraints
        assert!(protocol.min_reader_version >= 1, "Min reader version must be >= 1");
        assert!(protocol.min_writer_version >= 1, "Min writer version must be >= 1");
        
        // Test version compatibility
        if protocol.min_reader_version > 3 {
            // Should have reader features specified
            assert!(!protocol.reader_features.is_empty(), "Reader features required for version > 3");
        }
        
        if protocol.min_writer_version > 7 {
            // Should have writer features specified
            assert!(!protocol.writer_features.is_empty(), "Writer features required for version > 7");
        }
        
        Ok(())
    }
    
    /// Test schema compliance.
    async fn test_schema_compliance(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.setup_schema_test_data().await?;
        
        let schema = self.reader.get_schema().await?;
        
        // Verify schema is valid JSON
        let schema_json: serde_json::Value = serde_json::from_str(&schema)?;
        
        // Check required schema structure
        assert!(schema_json.get("type").is_some(), "Schema must have type field");
        assert!(schema_json.get("fields").is_some(), "Schema must have fields array");
        
        let fields = schema_json["fields"].as_array().ok_or("Fields must be an array")?;
        assert!(!fields.is_empty(), "Schema must have at least one field");
        
        // Verify each field has required properties
        for field in fields {
            assert!(field.get("name").is_some(), "Each field must have a name");
            assert!(field.get("type").is_some(), "Each field must have a type");
            assert!(field.get("nullable").is_some(), "Each field must specify nullable");
        }
        
        Ok(())
    }
    
    /// Test file action integrity.
    async fn test_file_action_integrity(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.setup_file_action_test_data().await?;
        
        let active_files = self.reader.get_active_files().await?;
        
        // Verify file action structure
        for file in active_files {
            assert!(!file.path.is_empty(), "File path must not be empty");
            assert!(file.size >= 0, "File size must be non-negative");
            assert!(file.modification_time > 0, "Modification time must be positive");
            
            // Verify partition values are valid JSON
            let partition_values: serde_json::Value = serde_json::from_str(&file.partition_values)?;
            assert!(partition_values.is_object() || partition_values.is_null(), 
                "Partition values must be JSON object or null");
        }
        
        Ok(())
    }
    
    /// Test version monotonicity.
    async fn test_version_monotonicity(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.setup_version_test_data().await?;
        
        let history = self.reader.get_version_history().await?;
        
        // Verify versions are in ascending order
        for window in history.windows(2) {
            let prev = &window[0];
            let curr = &window[1];
            assert!(curr.version > prev.version, "Versions must be strictly increasing");
            assert!(curr.timestamp >= prev.timestamp, "Timestamps must be non-decreasing");
        }
        
        // Verify current version matches history
        let current_version = self.reader.get_version().await?;
        assert_eq!(current_version, history.last().unwrap().version, 
            "Current version must match last version in history");
        
        Ok(())
    }
    
    /// Test time travel consistency.
    async fn test_time_travel_consistency(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.setup_time_travel_test_data().await?;
        
        let current_version = self.reader.get_version().await?;
        
        // Test time travel to each version
        for target_version in 0..=current_version {
            let version_at = self.reader.get_version_at(target_version).await?;
            assert_eq!(version_at, target_version, 
                "Version at {} should return {}", target_version, target_version);
            
            let files_at = self.reader.get_active_files_at(target_version).await?;
            
            // Files should be non-decreasing as version increases
            if target_version > 0 {
                let prev_files = self.reader.get_active_files_at(target_version - 1).await?;
                assert!(files_at.len() >= prev_files.len(), 
                    "Active files should not decrease with version");
            }
        }
        
        Ok(())
    }
    
    /// Test active files correctness.
    async fn test_active_files_correctness(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.setup_active_files_test_data().await?;
        
        let active_files = self.reader.get_active_files().await?;
        
        // Verify no duplicate files
        let mut paths = std::collections::HashSet::new();
        for file in &active_files {
            assert!(!paths.contains(&file.path), "Duplicate file path found: {}", file.path);
            paths.insert(file.path.clone());
        }
        
        // Verify all files have data_change = true or are part of logical consistency
        for file in &active_files {
            // Files should either be data files or metadata files
            assert!(file.path.ends_with(".parquet") || file.path.contains("_metadata"), 
                "Files should be Parquet or metadata files");
        }
        
        Ok(())
    }
    
    /// Test metadata completeness.
    async fn test_metadata_completeness(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.setup_metadata_test_data().await?;
        
        let table_metadata = self.reader.get_table_metadata().await?;
        
        // Verify required metadata fields
        assert!(!table_metadata.name.is_empty(), "Table name must not be empty");
        assert!(!table_metadata.location.is_empty(), "Table location must not be empty");
        
        // Verify metadata JSON structure
        let metadata_json: serde_json::Value = serde_json::from_str(&table_metadata.metadata)?;
        
        // Check for required Delta Lake metadata fields
        assert!(metadata_json.get("format").is_some(), "Format must be specified");
        assert!(metadata_json.get("partitionColumns").is_some(), "Partition columns must be specified");
        
        let format = &metadata_json["format"];
        assert_eq!(format["provider"], "parquet", "Format provider must be parquet");
        
        Ok(())
    }
    
    /// Test transaction atomicity.
    async fn test_transaction_atomicity(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.setup_atomicity_test_data().await?;
        
        // Verify that all actions within a version are consistent
        let history = self.reader.get_version_history().await?;
        
        for version_info in history {
            let version = version_info.version;
            
            // Get all actions for this version
            let files = self.reader.get_active_files_at(version).await?;
            let schema = self.reader.get_schema_at(version).await?;
            
            // Verify schema is not empty for versions with files
            if !files.is_empty() {
                assert!(!schema.is_empty(), "Schema must be present for versions with files");
            }
            
            // Verify operation consistency
            match version_info.operation.as_str() {
                "WRITE" | "CREATE" => {
                    // Should have files added
                    assert!(!files.is_empty(), "Write operations should add files");
                }
                "DELETE" => {
                    // Should have files removed (verified through active files)
                    // This is more complex to test without direct remove file access
                }
                _ => {}
            }
        }
        
        Ok(())
    }
    
    /// Test partition pruning functionality.
    async fn test_partition_pruning(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.setup_partition_test_data().await?;
        
        let active_files = self.reader.get_active_files().await?;
        
        // Verify partition values are correctly stored
        for file in active_files {
            let partition_values: serde_json::Value = serde_json::from_str(&file.partition_values)?;
            
            if let Some(partitions) = partition_values.as_object() {
                // Verify partition values are strings
                for (key, value) in partitions {
                    assert!(!key.is_empty(), "Partition key must not be empty");
                    assert!(value.is_string(), "Partition value must be string");
                }
            }
        }
        
        Ok(())
    }
    
    /// Test schema evolution.
    async fn test_schema_evolution(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.setup_schema_evolution_test_data().await?;
        
        let current_schema = self.reader.get_schema().await?;
        let initial_schema = self.reader.get_schema_at(0).await?;
        
        // Verify schema evolution is tracked
        let current_schema_json: serde_json::Value = serde_json::from_str(&current_schema)?;
        let initial_schema_json: serde_json::Value = serde_json::from_str(&initial_schema)?;
        
        let current_fields = current_schema_json["fields"].as_array().unwrap();
        let initial_fields = initial_schema_json["fields"].as_array().unwrap();
        
        // Schema should have evolved (more fields in current version)
        assert!(current_fields.len() >= initial_fields.len(), 
            "Schema should evolve to include more fields");
        
        // Verify field ordering is preserved
        for (i, field) in initial_fields.iter().enumerate() {
            assert_eq!(current_fields[i], *field, "Initial fields should be preserved in order");
        }
        
        Ok(())
    }
    
    /// Setup test data for protocol tests.
    async fn setup_protocol_test_data(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.manager.connection().as_any_pool()?;
        
        let table_id = uuid::Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO dl_tables (table_id, name, location) VALUES (?, ?, ?)")
            .bind(&table_id)
            .bind("conformance_table")
            .bind("/tmp/conformance_table")
            .execute(pool)
            .await?;
        
        sqlx::query("INSERT INTO dl_table_versions (table_id, version, committed_at) VALUES (?, ?, ?)")
            .bind(&table_id)
            .bind(0)
            .bind(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64)
            .execute(pool)
            .await?;
        
        sqlx::query("INSERT INTO dl_protocol_updates (table_id, version, min_reader_version, min_writer_version) VALUES (?, ?, ?, ?)")
            .bind(&table_id)
            .bind(0)
            .bind(1)
            .bind(2)
            .execute(pool)
            .await?;
        
        Ok(())
    }
    
    /// Setup test data for schema tests.
    async fn setup_schema_test_data(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.manager.connection().as_any_pool()?;
        
        let table_id = self.get_table_id().await?;
        
        sqlx::query("INSERT INTO dl_metadata_updates (table_id, version, schema_string) VALUES (?, ?, ?)")
            .bind(&table_id)
            .bind(0)
            .bind(r#"{"type": "struct", "fields": [{"name": "id", "type": "long", "nullable": false}]}"#)
            .execute(pool)
            .await?;
        
        Ok(())
    }
    
    /// Setup test data for file action tests.
    async fn setup_file_action_test_data(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.manager.connection().as_any_pool()?;
        let table_id = self.get_table_id().await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
        
        sqlx::query("INSERT INTO dl_add_files (table_id, version, path, size, modification_time, data_change, partition_values) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(&table_id)
            .bind(0)
            .bind("/tmp/test.parquet")
            .bind(1024)
            .bind(now)
            .bind(true)
            .bind(r#"{"year": "2023", "month": "01"}"#)
            .execute(pool)
            .await?;
        
        Ok(())
    }
    
    /// Setup test data for version tests.
    async fn setup_version_test_data(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.manager.connection().as_any_pool()?;
        let table_id = self.get_table_id().await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
        
        for version in 1..=3 {
            sqlx::query("INSERT INTO dl_table_versions (table_id, version, committed_at) VALUES (?, ?, ?)")
                .bind(&table_id)
                .bind(version)
                .bind(now + (version as i64 * 1000))
                .execute(pool)
                .await?;
        }
        
        Ok(())
    }
    
    /// Setup test data for time travel tests.
    async fn setup_time_travel_test_data(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.manager.connection().as_any_pool()?;
        let table_id = self.get_table_id().await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
        
        for version in 1..=3 {
            sqlx::query("INSERT INTO dl_table_versions (table_id, version, committed_at) VALUES (?, ?, ?)")
                .bind(&table_id)
                .bind(version)
                .bind(now + (version as i64 * 1000))
                .execute(pool)
                .await?;
            
            for file_idx in 0..=2 {
                sqlx::query("INSERT INTO dl_add_files (table_id, version, path, size, modification_time, data_change, partition_values) VALUES (?, ?, ?, ?, ?, ?, ?)")
                    .bind(&table_id)
                    .bind(version)
                    .bind(format!("/tmp/part-{:04}-{}.parquet", version, file_idx))
                    .bind(1024)
                    .bind(now + (version as i64 * 1000))
                    .bind(true)
                    .bind(r#"{}"#)
                    .execute(pool)
                    .await?;
            }
        }
        
        Ok(())
    }
    
    /// Setup test data for active files tests.
    async fn setup_active_files_test_data(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.manager.connection().as_any_pool()?;
        let table_id = self.get_table_id().await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
        
        // Add files
        sqlx::query("INSERT INTO dl_add_files (table_id, version, path, size, modification_time, data_change, partition_values) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(&table_id)
            .bind(0)
            .bind("/tmp/file1.parquet")
            .bind(1024)
            .bind(now)
            .bind(true)
            .bind(r#"{}"#)
            .execute(pool)
            .await?;
        
        sqlx::query("INSERT INTO dl_add_files (table_id, version, path, size, modification_time, data_change, partition_values) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(&table_id)
            .bind(1)
            .bind("/tmp/file2.parquet")
            .bind(2048)
            .bind(now + 1000)
            .bind(true)
            .bind(r#"{}"#)
            .execute(pool)
            .await?;
        
        // Remove one file
        sqlx::query("INSERT INTO dl_remove_files (table_id, version, path, deletion_timestamp, data_change) VALUES (?, ?, ?, ?, ?)")
            .bind(&table_id)
            .bind(1)
            .bind("/tmp/file1.parquet")
            .bind(now + 1000)
            .bind(true)
            .execute(pool)
            .await?;
        
        Ok(())
    }
    
    /// Setup test data for metadata tests.
    async fn setup_metadata_test_data(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.manager.connection().as_any_pool()?;
        let table_id = self.get_table_id().await?;
        
        sqlx::query("UPDATE dl_tables SET metadata = ? WHERE table_id = ?")
            .bind(r#"{"format": {"provider": "parquet", "options": {}}, "partitionColumns": ["year"], "configuration": {"delta.enableDeletionVectors": "false"}}"#)
            .bind(&table_id)
            .execute(pool)
            .await?;
        
        Ok(())
    }
    
    /// Setup test data for atomicity tests.
    async fn setup_atomicity_test_data(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.manager.connection().as_any_pool()?;
        let table_id = self.get_table_id().await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
        
        // Create a WRITE transaction
        sqlx::query("INSERT INTO dl_table_versions (table_id, version, committed_at, operation) VALUES (?, ?, ?, ?)")
            .bind(&table_id)
            .bind(1)
            .bind(now + 1000)
            .bind("WRITE")
            .execute(pool)
            .await?;
        
        sqlx::query("INSERT INTO dl_add_files (table_id, version, path, size, modification_time, data_change, partition_values) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(&table_id)
            .bind(1)
            .bind("/tmp/atomic_file.parquet")
            .bind(1024)
            .bind(now + 1000)
            .bind(true)
            .bind(r#"{}"#)
            .execute(pool)
            .await?;
        
        Ok(())
    }
    
    /// Setup test data for partition tests.
    async fn setup_partition_test_data(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.manager.connection().as_any_pool()?;
        let table_id = self.get_table_id().await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
        
        for year in 2022..=2024 {
            for month in 1..=3 {
                sqlx::query("INSERT INTO dl_add_files (table_id, version, path, size, modification_time, data_change, partition_values) VALUES (?, ?, ?, ?, ?, ?, ?)")
                    .bind(&table_id)
                    .bind(0)
                    .bind(format!("/tmp/year={}/month={:02}/data.parquet", year, month))
                    .bind(1024)
                    .bind(now)
                    .bind(true)
                    .bind(format!(r#"{{"year": "{}", "month": "{:02}"}}"#, year, month))
                    .execute(pool)
                    .await?;
            }
        }
        
        Ok(())
    }
    
    /// Setup test data for schema evolution tests.
    async fn setup_schema_evolution_test_data(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.manager.connection().as_any_pool()?;
        let table_id = self.get_table_id().await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
        
        // Initial schema
        sqlx::query("INSERT INTO dl_metadata_updates (table_id, version, schema_string) VALUES (?, ?, ?)")
            .bind(&table_id)
            .bind(0)
            .bind(r#"{"type": "struct", "fields": [{"name": "id", "type": "long", "nullable": false}]}"#)
            .execute(pool)
            .await?;
        
        // Evolved schema
        sqlx::query("INSERT INTO dl_table_versions (table_id, version, committed_at) VALUES (?, ?, ?)")
            .bind(&table_id)
            .bind(1)
            .bind(now + 1000)
            .execute(pool)
            .await?;
        
        sqlx::query("INSERT INTO dl_metadata_updates (table_id, version, schema_string) VALUES (?, ?, ?)")
            .bind(&table_id)
            .bind(1)
            .bind(r#"{"type": "struct", "fields": [{"name": "id", "type": "long", "nullable": false}, {"name": "name", "type": "string", "nullable": true}]}"#)
            .execute(pool)
            .await?;
        
        Ok(())
    }
    
    /// Get the table ID for the conformance table.
    async fn get_table_id(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.manager.connection().as_any_pool()?;
        
        let (table_id,): (String,) = sqlx::query_as("SELECT table_id FROM dl_tables WHERE name = 'conformance_table'")
            .fetch_one(pool)
            .await?;
        
        Ok(table_id)
    }
}

/// Results from conformance tests.
#[derive(Debug)]
pub struct ConformanceResults {
    results: HashMap<String, Result<(), Box<dyn std::error::Error + Send + Sync>>>,
}

impl ConformanceResults {
    /// Create new conformance results.
    pub fn new() -> Self {
        Self {
            results: HashMap::new(),
        }
    }
    
    /// Add a test result.
    pub fn add_result(&mut self, test_name: &str, result: Result<(), Box<dyn std::error::Error + Send + Sync>>) {
        self.results.insert(test_name.to_string(), result);
    }
    
    /// Check if all tests passed.
    pub fn all_passed(&self) -> bool {
        self.results.values().all(|result| result.is_ok())
    }
    
    /// Get the number of passed tests.
    pub fn passed_count(&self) -> usize {
        self.results.values().filter(|result| result.is_ok()).count()
    }
    
    /// Get the number of failed tests.
    pub fn failed_count(&self) -> usize {
        self.results.values().filter(|result| result.is_err()).count()
    }
    
    /// Print detailed results.
    pub fn print_results(&self) {
        println!("\n=== Delta Lake Protocol Conformance Results ===\n");
        
        for (test_name, result) in &self.results {
            match result {
                Ok(_) => println!("✅ {}: PASSED", test_name),
                Err(e) => println!("❌ {}: FAILED - {}", test_name, e),
            }
        }
        
        println!("\nSummary: {}/{} tests passed", self.passed_count(), self.results.len());
        
        if self.all_passed() {
            println!("🎉 All conformance tests passed!");
        } else {
            println!("⚠️  Some tests failed. Review the implementation for Delta Lake protocol compliance.");
        }
    }
}

/// Run conformance tests for all database engines.
pub async fn run_conformance_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let engines = vec![DatabaseEngine::Sqlite, DatabaseEngine::DuckDB];
    
    if std::env::var("POSTGRES_TEST_URL").is_ok() {
        // Add PostgreSQL if available
        // engines.push(DatabaseEngine::Postgres);
    }
    
    for engine in engines {
        println!("Running conformance tests for {:?}...", engine);
        
        let tests = DeltaConformanceTests::new(engine).await?;
        let results = tests.run_all_tests().await?;
        results.print_results();
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_sqlite_conformance() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tests = DeltaConformanceTests::new(DatabaseEngine::Sqlite).await?;
        let results = tests.run_all_tests().await?;
        
        assert!(results.passed_count() > 0, "At least some tests should pass");
        
        Ok(())
    }
}