//! Database adapter implementations

use crate::{SqlResult, DatabaseConfig, PoolStats, MigrationInfo};
use crate::traits::{TxnLogReader, TxnLogWriter, DatabaseAdapter, Transaction};
use deltalakedb_core::{Table, Commit, Protocol, Metadata, Action};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};

pub mod postgres;
pub mod sqlite;
pub mod duckdb;

pub use postgres::PostgresAdapter;
pub use sqlite::SQLiteAdapter;
pub use duckdb::DuckDBAdapter;

/// Base adapter functionality shared across implementations
pub struct BaseAdapter {
    config: DatabaseConfig,
    database_type: &'static str,
}

impl BaseAdapter {
    /// Create a new base adapter
    pub fn new(config: DatabaseConfig, database_type: &'static str) -> Self {
        Self {
            config,
            database_type,
        }
    }

    /// Get the database type
    pub fn database_type(&self) -> &'static str {
        self.database_type
    }

    /// Get the configuration
    pub fn get_config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Get database-specific optimization hints
    pub async fn get_optimization_hints(&self) -> SqlResult<Vec<String>> {
        match self.database_type {
            "postgresql" => Ok(vec![
                "Use connection pooling for high concurrency".to_string(),
                "Consider partitioning large tables by table_id".to_string(),
                "Use JSONB for metadata storage with GIN indexes".to_string(),
                "Enable pg_stat_statements for query optimization".to_string(),
            ]),
            "sqlite" => Ok(vec![
                "Use WAL mode for better concurrency".to_string(),
                "Consider page_size tuning for workload".to_string(),
                "Use PRAGMA optimize for query planning".to_string(),
                "Monitor database size for memory usage".to_string(),
            ]),
            "duckdb" => Ok(vec![
                "Use DuckDB's analytical query optimization".to_string(),
                "Consider materialized views for frequent queries".to_string(),
                "Leverage columnar storage for metadata queries".to_string(),
                "Use DuckDB's parallel query execution".to_string(),
            ]),
            _ => Ok(vec!["No specific optimizations available".to_string()]),
        }
    }
}

/// Common transaction implementation
pub struct BaseTransaction {
    database_type: &'static str,
    is_committed: bool,
}

impl BaseTransaction {
    /// Create a new base transaction
    pub fn new(database_type: &'static str) -> Self {
        Self {
            database_type,
            is_committed: false,
        }
    }

    /// Get the database type for this transaction
    pub fn database_type(&self) -> &'static str {
        self.database_type
    }

    /// Check if transaction has been committed
    pub fn is_committed(&self) -> bool {
        self.is_committed
    }

    /// Mark transaction as committed
    pub fn mark_committed(&mut self) {
        self.is_committed = true;
    }
}

/// Connection pool manager for different database types
pub struct PoolManager;

impl PoolManager {
    /// Get pool statistics for a database type
    pub fn get_stats(database_type: &str, pool_size: u32, active: u32) -> PoolStats {
        let total = pool_size;
        let idle = pool_size.saturating_sub(active);

        PoolStats::new(total, active, idle, pool_size)
    }

    /// Calculate optimal pool size based on workload
    pub fn calculate_optimal_pool_size(
        database_type: &str,
        expected_concurrent_queries: u32,
        connection_overhead_factor: f32,
    ) -> u32 {
        let base_size = match database_type {
            "postgresql" => expected_concurrent_queries,
            "sqlite" => 1, // SQLite doesn't benefit much from connection pooling
            "duckdb" => expected_concurrent_queries / 2, // DuckDB uses fewer connections
            _ => expected_concurrent_queries,
        };

        (base_size as f32 * connection_overhead_factor) as u32
    }

    /// Get recommended pool configuration for a database type
    pub fn get_pool_config(database_type: &str) -> PoolConfig {
        match database_type {
            "postgresql" => PoolConfig {
                max_size: 20,
                min_size: 5,
                connection_timeout: 30,
                idle_timeout: 600,
                max_lifetime: 1800,
            },
            "sqlite" => PoolConfig {
                max_size: 1,
                min_size: 1,
                connection_timeout: 30,
                idle_timeout: 0, // Keep connection alive
                max_lifetime: 0, // No max lifetime for SQLite
            },
            "duckdb" => PoolConfig {
                max_size: 10,
                min_size: 2,
                connection_timeout: 30,
                idle_timeout: 300,
                max_lifetime: 900,
            },
            _ => PoolConfig::default(),
        }
    }
}

/// Pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum pool size
    pub max_size: u32,
    /// Minimum pool size
    pub min_size: u32,
    /// Connection timeout in seconds
    pub connection_timeout: u64,
    /// Idle timeout in seconds
    pub idle_timeout: u64,
    /// Maximum connection lifetime in seconds
    pub max_lifetime: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 10,
            min_size: 1,
            connection_timeout: 30,
            idle_timeout: 300,
            max_lifetime: 1800,
        }
    }
}

/// Helper functions for database operations
pub mod utils {
    use super::*;
    use crate::error::SqlError;

    /// Convert a JSON value to a string representation for different database types
    pub fn json_to_db_string(json: &Value, database_type: &str) -> SqlResult<String> {
        match database_type {
            "postgresql" => Ok(json.to_string()), // JSONB handles this directly
            "sqlite" | "duckdb" => Ok(json.to_string()), // Both handle JSON directly
            _ => Err(SqlError::database_specific(
                database_type,
                "JSON serialization not supported",
            )),
        }
    }

    /// Parse a database timestamp to UTC DateTime
    pub fn parse_timestamp(timestamp_str: &str) -> SqlResult<DateTime<Utc>> {
        timestamp_str.parse()
            .map_err(|e: chrono::ParseError| SqlError::ChronoError(e))
    }

    /// Format a DateTime for database storage
    pub fn format_timestamp(timestamp: DateTime<Utc>) -> String {
        timestamp.to_rfc3339()
    }

    /// Validate table path format
    pub fn validate_table_path(table_path: &str) -> SqlResult<()> {
        if table_path.is_empty() {
            return Err(SqlError::validation_error("Table path cannot be empty"));
        }

        if table_path.len() > 1000 {
            return Err(SqlError::validation_error("Table path too long (max 1000 characters)"));
        }

        // Add more validation as needed
        Ok(())
    }

    /// Extract table name from path
    pub fn extract_table_name(table_path: &str) -> String {
        table_path
            .split('/')
            .last()
            .unwrap_or(table_path)
            .to_string()
    }

    /// Generate a safe SQL identifier
    pub fn safe_identifier(name: &str) -> String {
        name.replace('"', "\"\"")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_adapter_creation() {
        let config = DatabaseConfig::default();
        let adapter = BaseAdapter::new(config, "postgresql");

        assert_eq!(adapter.database_type(), "postgresql");
        assert_eq!(adapter.get_config().url, "sqlite::memory:");
    }

    #[test]
    fn test_optimization_hints() {
        let config = DatabaseConfig::default();
        let postgres_adapter = BaseAdapter::new(config.clone(), "postgresql");
        let sqlite_adapter = BaseAdapter::new(config.clone(), "sqlite");
        let duckdb_adapter = BaseAdapter::new(config, "duckdb");

        let tokio_runtime = tokio::runtime::Runtime::new().unwrap();

        let postgres_hints = tokio_runtime.block_on(postgres_adapter.get_optimization_hints()).unwrap();
        let sqlite_hints = tokio_runtime.block_on(sqlite_adapter.get_optimization_hints()).unwrap();
        let duckdb_hints = tokio_runtime.block_on(duckdb_adapter.get_optimization_hints()).unwrap();

        assert!(!postgres_hints.is_empty());
        assert!(!sqlite_hints.is_empty());
        assert!(!duckdb_hints.is_empty());

        assert!(postgres_hints.iter().any(|h| h.contains("connection pooling")));
        assert!(sqlite_hints.iter().any(|h| h.contains("WAL mode")));
        assert!(duckdb_hints.iter().any(|h| h.contains("analytical query")));
    }

    #[test]
    fn test_pool_manager() {
        let stats = PoolManager::get_stats("postgresql", 10, 3);
        assert_eq!(stats.total_connections, 10);
        assert_eq!(stats.active_connections, 3);
        assert_eq!(stats.idle_connections, 7);
        assert_eq!(stats.max_size, 10);

        let optimal_size = PoolManager::calculate_optimal_pool_size("postgresql", 10, 1.5);
        assert_eq!(optimal_size, 15);

        let config = PoolManager::get_pool_config("postgresql");
        assert_eq!(config.max_size, 20);
        assert_eq!(config.min_size, 5);
    }

    #[test]
    fn test_base_transaction() {
        let mut tx = BaseTransaction::new("postgresql");
        assert!(!tx.is_committed());
        assert_eq!(tx.database_type(), "postgresql");

        tx.mark_committed();
        assert!(tx.is_committed());
    }

    #[test]
    fn test_utils() {
        use serde_json::json;

        let json_value = json!({"test": "value"});

        let db_string = utils::json_to_db_string(&json_value, "postgresql").unwrap();
        assert_eq!(db_string, "{\"test\":\"value\"}");

        let table_name = utils::extract_table_name("path/to/my_table");
        assert_eq!(table_name, "my_table");

        assert!(utils::validate_table_path("valid/path").is_ok());
        assert!(utils::validate_table_path("").is_err());
        assert!(utils::validate_table_path(&"a".repeat(1001)).is_err());
    }
}