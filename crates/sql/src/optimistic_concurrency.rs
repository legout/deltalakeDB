//! Optimistic concurrency control for multi-table transactions
//!
//! This module provides comprehensive optimistic concurrency control including
//! version-based locking, CAS operations, conflict detection, and automatic retry
//! mechanisms with exponential backoff.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};

use crate::{SqlResult, DatabaseAdapter};
use crate::traits::IsolationLevel;
use deltalakedb_core::{Table, Commit};

/// Version information for optimistic locking
#[derive(Debug, Clone)]
pub struct VersionInfo {
    /// Current version number
    pub version: i64,
    /// When this version was created
    pub created_at: DateTime<Utc>,
    /// Transaction that created this version
    pub created_by: Option<Uuid>,
    /// Whether this version is currently locked
    pub locked: bool,
    /// Lock holder transaction ID
    pub lock_holder: Option<Uuid>,
    /// Lock expiration time
    pub lock_expires_at: Option<DateTime<Utc>>,
}

impl VersionInfo {
    /// Create new version info
    pub fn new(version: i64, created_by: Option<Uuid>) -> Self {
        Self {
            version,
            created_at: Utc::now(),
            created_by,
            locked: false,
            lock_holder: None,
            lock_expires_at: None,
        }
    }

    /// Check if version is locked by a specific transaction
    pub fn is_locked_by(&self, transaction_id: Uuid) -> bool {
        self.locked && self.lock_holder == Some(transaction_id)
    }

    /// Check if version lock has expired
    pub fn is_lock_expired(&self) -> bool {
        if let Some(expires_at) = self.lock_expires_at {
            Utc::now() > expires_at
        } else {
            false
        }
    }

    /// Acquire lock for a transaction
    pub fn acquire_lock(&mut self, transaction_id: Uuid, lock_duration: Duration) -> bool {
        if self.locked && !self.is_lock_expired() {
            return false; // Already locked by someone else
        }

        self.locked = true;
        self.lock_holder = Some(transaction_id);
        self.lock_expires_at = Some(Utc::now() + chrono::Duration::from_std(lock_duration).unwrap());
        true
    }

    /// Release lock
    pub fn release_lock(&mut self, transaction_id: Uuid) -> bool {
        if self.is_locked_by(transaction_id) {
            self.locked = false;
            self.lock_holder = None;
            self.lock_expires_at = None;
            true
        } else {
            false
        }
    }
}

/// Conflict detection result
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictResult {
    /// No conflict detected
    NoConflict,
    /// Version conflict - expected version doesn't match
    VersionConflict { expected: i64, actual: i64 },
    /// Lock conflict - resource is locked by another transaction
    LockConflict { lock_holder: Uuid },
    /// Write-write conflict - multiple transactions modifying same resource
    WriteWriteConflict { conflicting_transactions: Vec<Uuid> },
    /// Read-write conflict - read and write operations conflict
    ReadWriteConflict { reader: Uuid, writer: Uuid },
}

/// CAS (Compare-And-Swap) operation result
#[derive(Debug, Clone)]
pub struct CasResult {
    /// Whether the operation succeeded
    pub success: bool,
    /// New version if successful
    pub new_version: Option<i64>,
    /// Conflict information if failed
    pub conflict: Option<ConflictResult>,
    /// Operation duration
    pub duration: Duration,
}

/// Optimistic concurrency manager
#[derive(Debug)]
pub struct OptimisticConcurrencyManager {
    /// Database adapter
    adapter: Arc<dyn DatabaseAdapter>,
    /// Version cache for tables
    table_versions: Arc<RwLock<HashMap<Uuid, VersionInfo>>>,
    /// Active locks by transaction
    transaction_locks: Arc<RwLock<HashMap<Uuid, HashSet<Uuid>>>>, // transaction_id -> table_ids
    /// Conflict detection history
    conflict_history: Arc<Mutex<Vec<ConflictInfo>>>,
    /// Default lock duration
    default_lock_duration: Duration,
    /// Maximum retry attempts
    max_retries: u32,
    /// Base retry delay
    base_retry_delay: Duration,
}

/// Conflict information for tracking and analysis
#[derive(Debug, Clone)]
pub struct ConflictInfo {
    /// Conflict ID
    pub conflict_id: Uuid,
    /// Type of conflict
    pub conflict_type: ConflictResult,
    /// Table involved
    pub table_id: Uuid,
    /// Transactions involved
    pub involved_transactions: Vec<Uuid>,
    /// When conflict was detected
    pub detected_at: DateTime<Utc>,
    /// Whether conflict was resolved
    pub resolved: bool,
}

impl OptimisticConcurrencyManager {
    /// Create new optimistic concurrency manager
    pub fn new(adapter: Arc<dyn DatabaseAdapter>) -> Self {
        Self {
            adapter,
            table_versions: Arc::new(RwLock::new(HashMap::new())),
            transaction_locks: Arc::new(RwLock::new(HashMap::new())),
            conflict_history: Arc::new(Mutex::new(Vec::new())),
            default_lock_duration: Duration::from_secs(30),
            max_retries: 5,
            base_retry_delay: Duration::from_millis(100),
        }
    }

    /// Get current version for a table
    pub async fn get_table_version(&self, table_id: Uuid) -> SqlResult<i64> {
        // Check cache first
        {
            let versions = self.table_versions.read().await;
            if let Some(version_info) = versions.get(&table_id) {
                return Ok(version_info.version);
            }
        }

        // Load from database
        let version = self.adapter.get_table_version(table_id).await?;
        let current_version = version.unwrap_or(0);

        // Update cache
        {
            let mut versions = self.table_versions.write().await;
            versions.insert(table_id, VersionInfo::new(current_version, None));
        }

        Ok(current_version)
    }

    /// Acquire optimistic lock on a table
    pub async fn acquire_lock(
        &self,
        table_id: Uuid,
        transaction_id: Uuid,
        expected_version: Option<i64>,
    ) -> SqlResult<CasResult> {
        let start_time = Instant::now();

        // Get current version
        let current_version = self.get_table_version(table_id).await?;

        // Check version conflict
        if let Some(expected) = expected_version {
            if expected != current_version {
                let conflict = ConflictResult::VersionConflict {
                    expected,
                    actual: current_version,
                };

                self.record_conflict(table_id, conflict.clone(), vec![transaction_id]).await;

                return Ok(CasResult {
                    success: false,
                    new_version: None,
                    conflict: Some(conflict),
                    duration: start_time.elapsed(),
                });
            }
        }

        // Try to acquire lock
        let mut versions = self.table_versions.write().await;
        let version_info = versions.entry(table_id).or_insert_with(|| {
            VersionInfo::new(current_version, Some(transaction_id))
        });

        if !version_info.acquire_lock(transaction_id, self.default_lock_duration) {
            let conflict = ConflictResult::LockConflict {
                lock_holder: version_info.lock_holder.unwrap_or_default(),
            };

            self.record_conflict(table_id, conflict.clone(), vec![
                transaction_id,
                version_info.lock_holder.unwrap_or_default(),
            ]).await;

            return Ok(CasResult {
                success: false,
                new_version: None,
                conflict: Some(conflict),
                duration: start_time.elapsed(),
            });
        }

        // Record transaction lock
        {
            let mut tx_locks = self.transaction_locks.write().await;
            tx_locks.entry(transaction_id).or_insert_with(HashSet::new).insert(table_id);
        }

        Ok(CasResult {
            success: true,
            new_version: Some(current_version),
            conflict: None,
            duration: start_time.elapsed(),
        })
    }

    /// Release optimistic lock on a table
    pub async fn release_lock(&self, table_id: Uuid, transaction_id: Uuid) -> SqlResult<bool> {
        // Update version info
        {
            let mut versions = self.table_versions.write().await;
            if let Some(version_info) = versions.get_mut(&table_id) {
                let released = version_info.release_lock(transaction_id);
                if !released {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        }

        // Remove from transaction locks
        {
            let mut tx_locks = self.transaction_locks.write().await;
            if let Some(locks) = tx_locks.get_mut(&transaction_id) {
                locks.remove(&table_id);
                if locks.is_empty() {
                    tx_locks.remove(&transaction_id);
                }
            }
        }

        Ok(true)
    }

    /// Perform Compare-And-Swap operation on table version
    pub async fn compare_and_swap(
        &self,
        table_id: Uuid,
        transaction_id: Uuid,
        expected_version: i64,
        new_version: i64,
    ) -> SqlResult<CasResult> {
        let start_time = Instant::now();

        // First acquire lock
        let lock_result = self.acquire_lock(table_id, transaction_id, Some(expected_version)).await?;
        if !lock_result.success {
            return Ok(lock_result);
        }

        // Check if version has changed since we acquired lock
        let current_version = self.get_table_version(table_id).await?;
        if current_version != expected_version {
            // Release lock and return conflict
            self.release_lock(table_id, transaction_id).await?;

            let conflict = ConflictResult::VersionConflict {
                expected: expected_version,
                actual: current_version,
            };

            self.record_conflict(table_id, conflict.clone(), vec![transaction_id]).await;

            return Ok(CasResult {
                success: false,
                new_version: None,
                conflict: Some(conflict),
                duration: start_time.elapsed(),
            });
        }

        // Update version in cache
        {
            let mut versions = self.table_versions.write().await;
            if let Some(version_info) = versions.get_mut(&table_id) {
                version_info.version = new_version;
                version_info.created_by = Some(transaction_id);
            }
        }

        // Note: In a real implementation, this would also update the database
        // For now, we're only managing the in-memory cache

        Ok(CasResult {
            success: true,
            new_version: Some(new_version),
            conflict: None,
            duration: start_time.elapsed(),
        })
    }

    /// Detect conflicts for a set of tables
    pub async fn detect_conflicts(
        &self,
        table_ids: &[Uuid],
        transaction_id: Uuid,
    ) -> SqlResult<Vec<ConflictResult>> {
        let mut conflicts = Vec::new();

        for &table_id in table_ids {
            // Check version consistency
            let current_version = self.get_table_version(table_id).await?;

            let versions = self.table_versions.read().await;
            if let Some(version_info) = versions.get(&table_id) {
                // Check for lock conflicts
                if version_info.locked && !version_info.is_locked_by(transaction_id) {
                    if !version_info.is_lock_expired() {
                        conflicts.push(ConflictResult::LockConflict {
                            lock_holder: version_info.lock_holder.unwrap_or_default(),
                        });
                    }
                }
            }
        }

        // Check for write-write conflicts by looking at active transactions
        let tx_locks = self.transaction_locks.read().await;
        for (&other_tx_id, other_tables) in tx_locks.iter() {
            if other_tx_id != transaction_id {
                // Check for overlapping table access
                let overlapping_tables: Vec<_> = table_ids.iter()
                    .filter(|&table_id| other_tables.contains(table_id))
                    .collect();

                if !overlapping_tables.is_empty() {
                    conflicts.push(ConflictResult::WriteWriteConflict {
                        conflicting_transactions: vec![transaction_id, other_tx_id],
                    });
                }
            }
        }

        Ok(conflicts)
    }

    /// Execute operation with automatic retry
    pub async fn execute_with_retry<F, Fut, T>(
        &self,
        operation: F,
        isolation_level: IsolationLevel,
    ) -> SqlResult<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = SqlResult<T>>,
    {
        let mut attempt = 0;
        let mut base_delay = self.base_retry_delay;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    attempt += 1;

                    // Check if this is a retryable error
                    if !self.is_retryable_error(&e) || attempt >= self.max_retries {
                        return Err(e);
                    }

                    // Calculate exponential backoff delay
                    let delay = self.calculate_backoff_delay(attempt, base_delay);
                    base_delay = delay; // Update base delay for next iteration

                    // Wait before retrying
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Release all locks for a transaction
    pub async fn release_transaction_locks(&self, transaction_id: Uuid) -> SqlResult<()> {
        let table_ids = {
            let tx_locks = self.transaction_locks.read().await;
            tx_locks.get(&transaction_id).cloned().unwrap_or_default()
        };

        for table_id in table_ids {
            if let Err(e) = self.release_lock(table_id, transaction_id).await {
                eprintln!("Error releasing lock for table {}: {:?}", table_id, e);
            }
        }

        Ok(())
    }

    /// Get statistics about conflicts
    pub async fn get_conflict_stats(&self) -> ConflictStats {
        let history = self.conflict_history.lock().await;
        let total_conflicts = history.len();

        let mut conflicts_by_type = HashMap::new();
        let mut conflicts_by_table = HashMap::new();

        for conflict in history.iter() {
            // Count by type
            let type_key = format!("{:?}", conflict.conflict_type);
            *conflicts_by_type.entry(type_key).or_insert(0) += 1;

            // Count by table
            *conflicts_by_table.entry(conflict.table_id).or_insert(0) += 1;
        }

        let recent_conflicts = history.iter()
            .filter(|c| Utc::now().signed_duration_since(c.detected_at).num_minutes() < 60)
            .count();

        ConflictStats {
            total_conflicts,
            recent_conflicts,
            conflicts_by_type,
            conflicts_by_table,
        }
    }

    /// Clean up expired locks
    pub async fn cleanup_expired_locks(&self) -> SqlResult<usize> {
        let mut cleaned_count = 0;
        let now = Utc::now();

        let versions = self.table_versions.read().await;
        let expired_locks: Vec<_> = versions.iter()
            .filter(|(_, version_info)| {
                version_info.locked &&
                version_info.lock_expires_at.map_or(false, |expires_at| now > expires_at)
            })
            .map(|(table_id, version_info)| (*table_id, version_info.lock_holder.unwrap_or_default()))
            .collect();

        drop(versions);

        for (table_id, transaction_id) in expired_locks {
            if self.release_lock(table_id, transaction_id).await.unwrap_or(false) {
                cleaned_count += 1;
            }
        }

        Ok(cleaned_count)
    }

    /// Check if error is retryable
    fn is_retryable_error(&self, error: &crate::error::SqlError) -> bool {
        match error {
            crate::error::SqlError::TransactionError(msg) => {
                msg.contains("conflict") ||
                msg.contains("timeout") ||
                msg.contains("deadlock") ||
                msg.contains("version")
            }
            crate::error::SqlError::DatabaseError(_) => true, // Most database errors are retryable
            _ => false,
        }
    }

    /// Calculate exponential backoff delay
    fn calculate_backoff_delay(&self, attempt: u32, base_delay: Duration) -> Duration {
        // Exponential backoff with jitter
        let exponential_delay = base_delay * 2_u32.pow(attempt.saturating_sub(1));
        let max_delay = Duration::from_secs(30); // Cap at 30 seconds
        let delay = std::cmp::min(exponential_delay, max_delay);

        // Add simple jitter based on attempt number (deterministic)
        let jitter_factor = 0.9 + (attempt % 5) as f64 * 0.05; // 0.9 to 1.1
        Duration::from_millis((delay.as_millis() as f64 * jitter_factor) as u64)
    }

    /// Record conflict for analysis
    async fn record_conflict(
        &self,
        table_id: Uuid,
        conflict_type: ConflictResult,
        involved_transactions: Vec<Uuid>,
    ) {
        let conflict_info = ConflictInfo {
            conflict_id: Uuid::new_v4(),
            conflict_type,
            table_id,
            involved_transactions,
            detected_at: Utc::now(),
            resolved: false,
        };

        let mut history = self.conflict_history.lock().await;
        history.push(conflict_info);

        // Keep only last 1000 conflicts to prevent memory growth
        if history.len() > 1000 {
            history.drain(0..history.len() - 1000);
        }
    }

    /// Set default lock duration
    pub fn set_default_lock_duration(&mut self, duration: Duration) {
        self.default_lock_duration = duration;
    }

    /// Set maximum retry attempts
    pub fn set_max_retries(&mut self, max_retries: u32) {
        self.max_retries = max_retries;
    }

    /// Set base retry delay
    pub fn set_base_retry_delay(&mut self, delay: Duration) {
        self.base_retry_delay = delay;
    }
}

/// Conflict statistics
#[derive(Debug, Clone)]
pub struct ConflictStats {
    /// Total number of conflicts detected
    pub total_conflicts: usize,
    /// Number of conflicts in the last hour
    pub recent_conflicts: usize,
    /// Conflicts broken down by type
    pub conflicts_by_type: HashMap<String, usize>,
    /// Conflicts broken down by table
    pub conflicts_by_table: HashMap<Uuid, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::SQLiteAdapter;
    use crate::DatabaseConfig;

    #[tokio::test]
    async fn test_version_info_creation() {
        let version_info = VersionInfo::new(1, Some(Uuid::new_v4()));

        assert_eq!(version_info.version, 1);
        assert!(!version_info.locked);
        assert!(version_info.lock_holder.is_none());
    }

    #[tokio::test]
    async fn test_version_locking() {
        let mut version_info = VersionInfo::new(1, None);
        let tx_id = Uuid::new_v4();
        let lock_duration = Duration::from_secs(10);

        // Acquire lock
        assert!(version_info.acquire_lock(tx_id, lock_duration));
        assert!(version_info.is_locked_by(tx_id));
        assert!(!version_info.is_lock_expired());

        // Try to acquire with different transaction
        let other_tx = Uuid::new_v4();
        assert!(!version_info.acquire_lock(other_tx, lock_duration));

        // Release lock
        assert!(version_info.release_lock(tx_id));
        assert!(!version_info.locked);
        assert!(version_info.lock_holder.is_none());
    }

    #[tokio::test]
    async fn test_optimistic_concurrency_manager() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let manager = OptimisticConcurrencyManager::new(adapter.clone());

        let table_id = Uuid::new_v4();
        let tx_id = Uuid::new_v4();

        // Test lock acquisition
        let result = manager.acquire_lock(table_id, tx_id, None).await.unwrap();
        assert!(result.success);

        // Test lock release
        let released = manager.release_lock(table_id, tx_id).await.unwrap();
        assert!(released);
    }

    #[tokio::test]
    async fn test_cas_operation() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let manager = OptimisticConcurrencyManager::new(adapter.clone());

        let table_id = Uuid::new_v4();
        let tx_id = Uuid::new_v4();

        // Perform CAS operation
        let result = manager.compare_and_swap(table_id, tx_id, 0, 1).await.unwrap();
        assert!(result.success);
        assert_eq!(result.new_version, Some(1));
    }

    #[tokio::test]
    async fn test_conflict_detection() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let manager = OptimisticConcurrencyManager::new(adapter.clone());

        let table_id = Uuid::new_v4();
        let tx1 = Uuid::new_v4();
        let tx2 = Uuid::new_v4();

        // Acquire lock with first transaction
        manager.acquire_lock(table_id, tx1, None).await.unwrap();

        // Try to detect conflicts with second transaction
        let conflicts = manager.detect_conflicts(&[table_id], tx2).await.unwrap();
        assert!(!conflicts.is_empty());
        assert!(matches!(conflicts[0], ConflictResult::LockConflict { .. }));
    }

    #[tokio::test]
    async fn test_exponential_backoff() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let manager = OptimisticConcurrencyManager::new(adapter.clone());

        // Test backoff calculation
        let delay1 = manager.calculate_backoff_delay(1, Duration::from_millis(100));
        let delay2 = manager.calculate_backoff_delay(2, Duration::from_millis(100));

        // Second attempt should have longer delay
        assert!(delay2 > delay1);
        assert!(delay2 <= Duration::from_millis(400)); // Should be around 200ms with jitter

        // Should not exceed maximum
        let delay_max = manager.calculate_backoff_delay(10, Duration::from_millis(100));
        assert!(delay_max <= Duration::from_secs(30));
    }
}