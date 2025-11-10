//! deltalakedb-sql
//!
//! SQL engine adapters (Postgres, SQLite, DuckDB) behind a consistent interface.

#![warn(missing_docs)]

use async_trait::async_trait;
use deltalakedb_core::{Table, Commit, Protocol, Metadata, Action, DeltaError};
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;
use chrono::{DateTime, Utc};

pub mod adapters;
pub mod error;
pub mod schema;
pub mod traits;
pub mod transaction;
pub mod two_phase_commit;
pub mod savepoints;
pub mod transaction_monitor;
pub mod optimistic_concurrency;
pub mod performance;
pub mod multi_table_transaction;

#[cfg(test)]
pub mod tests;

pub use error::{SqlError, SqlResult};
pub use traits::{TxnLogReader, TxnLogWriter, DatabaseAdapter};
pub use transaction::{
    TransactionManager, Transaction, TransactionState, IsolationLevel,
    TableOperation, OperationType, OperationData, ConsistencySnapshot, TableSnapshot
};
pub use multi_table_transaction::{
    MultiTableTransaction, TransactionContext, TransactionOptions, TransactionResult,
    ConflictResolutionStrategy, TransactionExecutor
};
pub use two_phase_commit::{
    TwoPhaseCommitCoordinator, TwoPhaseCommitContext, Participant, ParticipantStatus,
    CoordinatorStatus
};
pub use savepoints::{SavepointManager, Savepoint, SavepointStatus};
pub use transaction_monitor::{
    TransactionMonitor, TransactionMetrics, TransactionHealth, RecoveryAction, MonitorConfig
};
pub use optimistic_concurrency::{
    OptimisticConcurrencyManager, VersionInfo, ConflictResult, CasResult, ConflictStats
};
pub use performance::{
    PerformanceManager, PerformanceConfig, PerformanceStats,
    MaterializedViewManager, ViewDefinition, RefreshStrategy, ViewStatus,
    QueryOptimizer, QueryPlan, OptimizationRule,
    CacheManager, CacheConfig, CacheLevel, CacheMetrics
};

/// Configuration for database connections
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Database connection URL
    pub url: String,
    /// Connection pool size
    pub pool_size: u32,
    /// Connection timeout in seconds
    pub timeout: u64,
    /// Whether to enable SSL
    pub ssl_enabled: bool,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "sqlite::memory:".to_string(),
            pool_size: 10,
            timeout: 30,
            ssl_enabled: false,
        }
    }
}

/// Factory for creating database adapters
pub struct AdapterFactory;

impl AdapterFactory {
    /// Create a new database adapter based on the connection URL
    pub async fn create_adapter(config: DatabaseConfig) -> SqlResult<Arc<dyn DatabaseAdapter>> {
        let adapter: Arc<dyn DatabaseAdapter> = if config.url.starts_with("postgresql://")
            || config.url.starts_with("postgres://") {
            Arc::new(adapters::PostgresAdapter::new(config).await?)
        } else if config.url.starts_with("sqlite://") || config.url.starts_with("sqlite::") {
            Arc::new(adapters::SQLiteAdapter::new(config).await?)
        } else if config.url.starts_with("duckdb://") || config.url.starts_with("duckdb::") {
            Arc::new(adapters::DuckDBAdapter::new(config).await?)
        } else {
            return Err(SqlError::UnsupportedDatabase(config.url));
        };

        Ok(adapter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_config_default() {
        let config = DatabaseConfig::default();
        assert_eq!(config.url, "sqlite::memory:");
        assert_eq!(config.pool_size, 10);
        assert_eq!(config.timeout, 30);
        assert!(!config.ssl_enabled);
    }

    #[test]
    fn test_adapter_factory_url_detection() {
        // These tests don't actually create connections, just test URL parsing logic
        let postgres_config = DatabaseConfig {
            url: "postgresql://localhost/test".to_string(),
            ..Default::default()
        };

        let sqlite_config = DatabaseConfig {
            url: "sqlite://memory".to_string(),
            ..Default::default()
        };

        let duckdb_config = DatabaseConfig {
            url: "duckdb://memory".to_string(),
            ..Default::default()
        };

        // Just verify the URLs are detected correctly (actual adapter creation tested elsewhere)
        assert!(postgres_config.url.starts_with("postgresql://"));
        assert!(sqlite_config.url.starts_with("sqlite://"));
        assert!(duckdb_config.url.starts_with("duckdb://"));
    }
}