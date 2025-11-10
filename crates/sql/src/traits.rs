//! Core traits for SQL adapters

use async_trait::async_trait;
use crate::{SqlResult, DatabaseConfig};
use deltalakedb_core::{Table, Commit, Protocol, Metadata, Action};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// Trait for reading Delta metadata from SQL databases
#[async_trait]
pub trait TxnLogReader: Send + Sync {
    /// Read a table by its UUID
    async fn read_table(&self, table_id: Uuid) -> SqlResult<Option<Table>>;

    /// Read a table by its path
    async fn read_table_by_path(&self, table_path: &str) -> SqlResult<Option<Table>>;

    /// Read the latest protocol for a table
    async fn read_protocol(&self, table_id: Uuid) -> SqlResult<Option<Protocol>>;

    /// Read the latest metadata for a table
    async fn read_metadata(&self, table_id: Uuid) -> SqlResult<Option<Metadata>>;

    /// Read a specific commit by its version
    async fn read_commit(&self, table_id: Uuid, version: i64) -> SqlResult<Option<Commit>>;

    /// Read commits in a version range
    async fn read_commits_range(
        &self,
        table_id: Uuid,
        start_version: Option<i64>,
        end_version: Option<i64>,
        limit: Option<u32>,
    ) -> SqlResult<Vec<Commit>>;

    /// Read the latest commit for a table
    async fn read_latest_commit(&self, table_id: Uuid) -> SqlResult<Option<Commit>>;

    /// Read actions from a commit
    async fn read_commit_actions(&self, commit_id: Uuid) -> SqlResult<Vec<Action>>;

    /// List all tables
    async fn list_tables(&self, limit: Option<u32>, offset: Option<u32>) -> SqlResult<Vec<Table>>;

    /// Check if a table exists
    async fn table_exists(&self, table_path: &str) -> SqlResult<bool>;

    /// Get table version
    async fn get_table_version(&self, table_id: Uuid) -> SqlResult<Option<i64>>;

    /// Count commits for a table
    async fn count_commits(&self, table_id: Uuid) -> SqlResult<i64>;

    /// Read files added to a table (for metadata queries)
    async fn read_table_files(
        &self,
        table_id: Uuid,
        start_version: Option<i64>,
        end_version: Option<i64>,
    ) -> SqlResult<Vec<Value>>;
}

/// Trait for writing Delta metadata to SQL databases
#[async_trait]
pub trait TxnLogWriter: Send + Sync {
    /// Create a new table
    async fn create_table(&self, table: &Table) -> SqlResult<Table>;

    /// Update an existing table
    async fn update_table(&self, table: &Table) -> SqlResult<Table>;

    /// Delete a table
    async fn delete_table(&self, table_id: Uuid) -> SqlResult<bool>;

    /// Write a commit
    async fn write_commit(&self, commit: &Commit) -> SqlResult<Commit>;

    /// Write multiple commits in a transaction
    async fn write_commits(&self, commits: &[Commit]) -> SqlResult<Vec<Commit>>;

    /// Update table protocol
    async fn update_protocol(&self, table_id: Uuid, protocol: &Protocol) -> SqlResult<()>;

    /// Update table metadata
    async fn update_metadata(&self, table_id: Uuid, metadata: &Metadata) -> SqlResult<()>;

    /// Vacuum old commits (cleanup)
    async fn vacuum_commits(&self, table_id: Uuid, keep_last_n: i64) -> SqlResult<i64>;

    /// Get next commit version for a table
    async fn get_next_version(&self, table_id: Uuid) -> SqlResult<i64>;

    /// Begin a transaction
    async fn begin_transaction(&self) -> SqlResult<Box<dyn Transaction>>;

    /// Check table health and connectivity
    async fn health_check(&self) -> SqlResult<bool>;
}

/// Trait for database-specific operations and management
#[async_trait]
pub trait DatabaseAdapter: Send + Sync + TxnLogReader + TxnLogWriter {
    /// Get the database type
    fn database_type(&self) -> &'static str;

    /// Get the database version
    async fn database_version(&self) -> SqlResult<String>;

    /// Initialize the database schema
    async fn initialize_schema(&self) -> SqlResult<()>;

    /// Check if the schema is up to date
    async fn check_schema_version(&self) -> SqlResult<bool>;

    /// Run database migrations
    async fn migrate_schema(&self) -> SqlResult<()>;

    /// Create database-specific indexes
    async fn create_indexes(&self) -> SqlResult<()>;

    /// Get connection pool statistics
    async fn pool_stats(&self) -> SqlResult<PoolStats>;

    /// Test database connectivity
    async fn test_connection(&self) -> SqlResult<bool>;

    /// Get database-specific configuration
    fn get_config(&self) -> &DatabaseConfig;

    /// Close all connections and cleanup resources
    async fn close(&self) -> SqlResult<()>;

    /// Execute a raw SQL query (for debugging/maintenance)
    async fn execute_raw(&self, query: &str, params: &[Value]) -> SqlResult<Vec<HashMap<String, Value>>>;

    /// Get database-specific optimizations
    async fn get_optimization_hints(&self) -> SqlResult<Vec<String>>;

    // Multi-table transaction support methods

    /// Begin a distributed transaction with isolation level
    async fn begin_transaction_with_isolation(&self, isolation_level: IsolationLevel) -> SqlResult<Box<dyn Transaction>>;

    /// Check if database supports native savepoints
    async fn supports_savepoints(&self) -> bool;

    /// Check if database supports two-phase commit
    async fn supports_two_phase_commit(&self) -> bool;

    /// Get supported isolation levels
    async fn supported_isolation_levels(&self) -> Vec<IsolationLevel>;

    /// Create a savepoint within a transaction
    async fn create_savepoint(&self, transaction_id: Uuid, savepoint_name: &str) -> SqlResult<()>;

    /// Rollback to a savepoint
    async fn rollback_to_savepoint(&self, transaction_id: Uuid, savepoint_name: &str) -> SqlResult<()>;

    /// Release a savepoint
    async fn release_savepoint(&self, transaction_id: Uuid, savepoint_name: &str) -> SqlResult<()>;

    /// Prepare transaction for two-phase commit
    async fn prepare_transaction(&self, transaction_id: Uuid) -> SqlResult<()>;

    /// Commit prepared transaction
    async fn commit_prepared(&self, transaction_id: Uuid) -> SqlResult<()>;

    /// Rollback prepared transaction
    async fn rollback_prepared(&self, transaction_id: Uuid) -> SqlResult<()>;

    /// Get transaction status
    async fn get_transaction_status(&self, transaction_id: Uuid) -> SqlResult<TransactionStatus>;

    /// List active transactions
    async fn list_active_transactions(&self) -> SqlResult<Vec<Uuid>>;

    /// Set transaction timeout
    async fn set_transaction_timeout(&self, transaction_id: Uuid, timeout: Duration) -> SqlResult<()>;

    /// Check for deadlocks
    async fn detect_deadlocks(&self) -> SqlResult<Vec<DeadlockInfo>>;

    /// Resolve deadlock by victimizing a transaction
    async fn resolve_deadlock(&self, victim_transaction_id: Uuid) -> SqlResult<()>;
}

/// Trait for database transactions
#[async_trait]
pub trait Transaction: Send + Sync {
    /// Commit the transaction
    async fn commit(self: Box<Self>) -> SqlResult<()>;

    /// Rollback the transaction
    async fn rollback(self: Box<Self>) -> SqlResult<()>;

    /// Write a commit within this transaction
    async fn write_commit(&mut self, commit: &Commit) -> SqlResult<Commit>;

    /// Update table metadata within this transaction
    async fn update_table(&mut self, table: &Table) -> SqlResult<Table>;

    /// Create a table within this transaction
    async fn create_table(&mut self, table: &Table) -> SqlResult<Table>;

    /// Delete a table within this transaction
    async fn delete_table(&mut self, table_id: Uuid) -> SqlResult<bool>;
}

/// Connection pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Total connections in the pool
    pub total_connections: u32,
    /// Active connections
    pub active_connections: u32,
    /// Idle connections
    pub idle_connections: u32,
    /// Maximum pool size
    pub max_size: u32,
    /// Pool utilization percentage
    pub utilization_percent: f32,
}

impl PoolStats {
    /// Create a new pool statistics instance
    pub fn new(total: u32, active: u32, idle: u32, max_size: u32) -> Self {
        let utilization_percent = if max_size > 0 {
            (active as f32 / max_size as f32) * 100.0
        } else {
            0.0
        };

        Self {
            total_connections: total,
            active_connections: active,
            idle_connections: idle,
            max_size,
            utilization_percent,
        }
    }

    /// Check if the pool is at capacity
    pub fn is_at_capacity(&self) -> bool {
        self.active_connections >= self.max_size
    }

    /// Check if the pool has idle connections available
    pub fn has_idle_connections(&self) -> bool {
        self.idle_connections > 0
    }
}

/// Schema migration information
#[derive(Debug, Clone)]
pub struct MigrationInfo {
    /// Migration version
    pub version: i64,
    /// Migration name
    pub name: String,
    /// Migration description
    pub description: String,
    /// Whether the migration has been applied
    pub applied: bool,
    /// When the migration was applied (if applicable)
    pub applied_at: Option<DateTime<Utc>>,
}

impl MigrationInfo {
    /// Create a new migration info
    pub fn new(version: i64, name: String, description: String) -> Self {
        Self {
            version,
            name,
            description,
            applied: false,
            applied_at: None,
        }
    }

    /// Mark the migration as applied
    pub fn mark_applied(&mut self, applied_at: DateTime<Utc>) {
        self.applied = true;
        self.applied_at = Some(applied_at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_stats_creation() {
        let stats = PoolStats::new(10, 3, 7, 15);
        assert_eq!(stats.total_connections, 10);
        assert_eq!(stats.active_connections, 3);
        assert_eq!(stats.idle_connections, 7);
        assert_eq!(stats.max_size, 15);
        assert!(!stats.is_at_capacity());
        assert!(stats.has_idle_connections());
        assert_eq!(stats.utilization_percent, 20.0); // 3/15 * 100
    }

    #[test]
    fn test_migration_info_creation() {
        let mut migration = MigrationInfo::new(
            1,
            "initial_schema".to_string(),
            "Create initial database schema".to_string(),
        );

        assert_eq!(migration.version, 1);
        assert_eq!(migration.name, "initial_schema");
        assert_eq!(migration.description, "Create initial database schema");
        assert!(!migration.applied);
        assert!(migration.applied_at.is_none());

        let now = Utc::now();
        migration.mark_applied(now);
        assert!(migration.applied);
        assert_eq!(migration.applied_at, Some(now));
    }
}

/// Transaction isolation levels
#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    /// Read committed - reads only see committed data
    ReadCommitted,
    /// Repeatable read - reads see a snapshot as of transaction start
    RepeatableRead,
    /// Serializable - transaction behaves as if executed serially
    Serializable,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::ReadCommitted
    }
}

/// Transaction status
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionStatus {
    /// Transaction is active and running
    Active,
    /// Transaction is prepared for two-phase commit
    Prepared,
    /// Transaction has been committed
    Committed,
    /// Transaction has been rolled back
    RolledBack,
    /// Transaction failed due to error
    Failed,
    /// Transaction is in unknown state
    Unknown,
}

/// Deadlock information
#[derive(Debug, Clone)]
pub struct DeadlockInfo {
    /// Unique deadlock ID
    pub deadlock_id: Uuid,
    /// List of transactions involved in the deadlock
    pub involved_transactions: Vec<Uuid>,
    /// Description of the deadlock
    pub description: String,
    /// When the deadlock was detected
    pub detected_at: DateTime<Utc>,
    /// Recommended victim transaction (if any)
    pub recommended_victim: Option<Uuid>,
}

impl DeadlockInfo {
    /// Create new deadlock information
    pub fn new(
        deadlock_id: Uuid,
        involved_transactions: Vec<Uuid>,
        description: String,
    ) -> Self {
        Self {
            deadlock_id,
            involved_transactions,
            description,
            detected_at: Utc::now(),
            recommended_victim: None,
        }
    }

    /// Set the recommended victim transaction
    pub fn set_victim(&mut self, victim_transaction_id: Uuid) {
        self.recommended_victim = Some(victim_transaction_id);
    }

    /// Check if a transaction is involved in the deadlock
    pub fn involves_transaction(&self, transaction_id: Uuid) -> bool {
        self.involved_transactions.contains(&transaction_id)
    }
}