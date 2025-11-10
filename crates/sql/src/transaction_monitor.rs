//! Transaction monitoring and recovery for timeout and deadlock handling
//!
//! This module provides comprehensive monitoring of active transactions, including
//! timeout detection, deadlock identification, and automatic recovery mechanisms.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, sleep};

use crate::{SqlResult, DatabaseAdapter};
use crate::traits::{TransactionStatus, DeadlockInfo};
use crate::transaction::{Transaction, TransactionManager};

/// Transaction health status
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionHealth {
    /// Transaction is healthy and progressing normally
    Healthy,
    /// Transaction is approaching timeout
    Warning,
    /// Transaction has timed out
    Timeout,
    /// Transaction is involved in a deadlock
    Deadlocked,
    /// Transaction is in an unknown state
    Unknown,
    /// Transaction has failed
    Failed,
}

/// Transaction monitoring metrics
#[derive(Debug, Clone)]
pub struct TransactionMetrics {
    /// Transaction ID
    pub transaction_id: Uuid,
    /// Current health status
    pub health: TransactionHealth,
    /// Transaction duration
    pub duration: Duration,
    /// Number of operations performed
    pub operation_count: usize,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Last activity timestamp
    pub last_activity: DateTime<Utc>,
    /// Resources held (tables, locks, etc.)
    pub held_resources: HashSet<Uuid>,
    /// Blocking transactions (if any)
    pub blocking_transactions: HashSet<Uuid>,
    /// Blocked by transactions (if any)
    pub blocked_by: HashSet<Uuid>,
    /// Estimated completion time
    pub estimated_completion: Option<DateTime<Utc>>,
}

impl TransactionMetrics {
    /// Create new transaction metrics
    pub fn new(transaction_id: Uuid) -> Self {
        Self {
            transaction_id,
            health: TransactionHealth::Healthy,
            duration: Duration::from_secs(0),
            operation_count: 0,
            retry_count: 0,
            last_activity: Utc::now(),
            held_resources: HashSet::new(),
            blocking_transactions: HashSet::new(),
            blocked_by: HashSet::new(),
            estimated_completion: None,
        }
    }

    /// Update transaction health based on current state
    pub fn update_health(&mut self, timeout_threshold: Duration, warning_threshold: Duration) {
        if self.duration > timeout_threshold {
            self.health = TransactionHealth::Timeout;
        } else if self.duration > warning_threshold {
            self.health = TransactionHealth::Warning;
        } else if !self.blocked_by.is_empty() {
            self.health = TransactionHealth::Deadlocked;
        } else if self.retry_count > 3 {
            self.health = TransactionHealth::Failed;
        } else {
            self.health = TransactionHealth::Healthy;
        }
    }

    /// Check if transaction is in distress
    pub fn is_in_distress(&self) -> bool {
        matches!(self.health,
            TransactionHealth::Timeout |
            TransactionHealth::Deadlocked |
            TransactionHealth::Failed
        )
    }

    /// Check if transaction is approaching timeout
    pub fn is_warning(&self) -> bool {
        self.health == TransactionHealth::Warning
    }
}

/// Transaction recovery action
#[derive(Debug, Clone)]
pub enum RecoveryAction {
    /// No action needed
    None,
    /// Extend transaction timeout
    ExtendTimeout(Duration),
    /// Retry the transaction
    Retry,
    /// Rollback the transaction
    Rollback,
    /// Kill the transaction (force rollback)
    Kill,
    /// Escalate to manual intervention
    Escalate,
}

/// Transaction monitor configuration
#[derive(Debug, Clone)]
pub struct MonitorConfig {
    /// Monitoring interval
    pub monitor_interval: Duration,
    /// Transaction timeout threshold
    pub timeout_threshold: Duration,
    /// Warning threshold (before timeout)
    pub warning_threshold: Duration,
    /// Deadlock detection interval
    pub deadlock_check_interval: Duration,
    /// Maximum automatic retry attempts
    pub max_retries: u32,
    /// Whether to enable automatic recovery
    pub auto_recovery_enabled: bool,
    /// Whether to enable deadlock detection
    pub deadlock_detection_enabled: bool,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            monitor_interval: Duration::from_secs(5),
            timeout_threshold: Duration::from_secs(60),
            warning_threshold: Duration::from_secs(45),
            deadlock_check_interval: Duration::from_secs(10),
            max_retries: 3,
            auto_recovery_enabled: true,
            deadlock_detection_enabled: true,
        }
    }
}

/// Transaction monitor for health checking and recovery
pub struct TransactionMonitor {
    /// Database adapter
    adapter: Arc<dyn DatabaseAdapter>,
    /// Transaction manager
    transaction_manager: Arc<TransactionManager>,
    /// Monitor configuration
    config: MonitorConfig,
    /// Active transaction metrics
    transaction_metrics: Arc<RwLock<HashMap<Uuid, TransactionMetrics>>>,
    /// Monitoring task handle
    monitor_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Deadlock detection task handle
    deadlock_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Recovery statistics
    recovery_stats: Arc<RwLock<RecoveryStats>>,
}

/// Recovery statistics
#[derive(Debug, Clone, Default)]
pub struct RecoveryStats {
    /// Total timeouts handled
    pub timeouts_handled: u64,
    /// Total deadlocks resolved
    pub deadlocks_resolved: u64,
    /// Total automatic rollbacks
    pub automatic_rollbacks: u64,
    /// Total manual escalations
    pub manual_escalations: u64,
    /// Successful recoveries
    pub successful_recoveries: u64,
    /// Failed recoveries
    pub failed_recoveries: u64,
}

impl TransactionMonitor {
    /// Create a new transaction monitor
    pub fn new(
        adapter: Arc<dyn DatabaseAdapter>,
        transaction_manager: Arc<TransactionManager>,
        config: MonitorConfig,
    ) -> Self {
        Self {
            adapter,
            transaction_manager,
            config,
            transaction_metrics: Arc::new(RwLock::new(HashMap::new())),
            monitor_handle: Arc::new(Mutex::new(None)),
            deadlock_handle: Arc::new(Mutex::new(None)),
            recovery_stats: Arc::new(RwLock::new(RecoveryStats::default())),
        }
    }

    /// Start monitoring transactions
    pub async fn start(&self) -> SqlResult<()> {
        // Start health monitoring
        let health_monitor = self.start_health_monitor().await?;
        *self.monitor_handle.lock().await = Some(health_monitor);

        // Start deadlock detection if enabled
        if self.config.deadlock_detection_enabled {
            let deadlock_monitor = self.start_deadlock_monitor().await?;
            *self.deadlock_handle.lock().await = Some(deadlock_monitor);
        }

        Ok(())
    }

    /// Stop monitoring
    pub async fn stop(&self) -> SqlResult<()> {
        // Stop health monitor
        if let Some(handle) = self.monitor_handle.lock().await.take() {
            handle.abort();
        }

        // Stop deadlock monitor
        if let Some(handle) = self.deadlock_handle.lock().await.take() {
            handle.abort();
        }

        Ok(())
    }

    /// Register a transaction for monitoring
    pub async fn register_transaction(&self, transaction: &Transaction) -> SqlResult<()> {
        let metrics = TransactionMetrics::new(transaction.transaction_id);
        let mut transaction_metrics = self.transaction_metrics.write().await;
        transaction_metrics.insert(transaction.transaction_id, metrics);
        Ok(())
    }

    /// Unregister a transaction from monitoring
    pub async fn unregister_transaction(&self, transaction_id: Uuid) -> SqlResult<()> {
        let mut transaction_metrics = self.transaction_metrics.write().await;
        transaction_metrics.remove(&transaction_id);
        Ok(())
    }

    /// Get metrics for a specific transaction
    pub async fn get_transaction_metrics(&self, transaction_id: Uuid) -> Option<TransactionMetrics> {
        let transaction_metrics = self.transaction_metrics.read().await;
        transaction_metrics.get(&transaction_id).cloned()
    }

    /// Get all transaction metrics
    pub async fn get_all_metrics(&self) -> HashMap<Uuid, TransactionMetrics> {
        let transaction_metrics = self.transaction_metrics.read().await;
        transaction_metrics.clone()
    }

    /// Get recovery statistics
    pub async fn get_recovery_stats(&self) -> RecoveryStats {
        let stats = self.recovery_stats.read().await;
        stats.clone()
    }

    /// Manually trigger recovery for a transaction
    pub async fn trigger_recovery(&self, transaction_id: Uuid) -> SqlResult<RecoveryAction> {
        let metrics = {
            let transaction_metrics = self.transaction_metrics.read().await;
            transaction_metrics.get(&transaction_id).cloned()
        };

        if let Some(mut metrics) = metrics {
            let action = self.determine_recovery_action(&metrics).await?;
            self.execute_recovery_action(transaction_id, &action).await?;
            Ok(action)
        } else {
            Err(crate::error::SqlError::TransactionError(
                "Transaction not found for recovery".to_string()
            ))
        }
    }

    /// Update configuration
    pub async fn update_config(&mut self, config: MonitorConfig) -> SqlResult<()> {
        self.config = config;

        // Restart monitors with new configuration
        self.stop().await?;
        self.start().await?;

        Ok(())
    }

    /// Start health monitoring task
    async fn start_health_monitor(&self) -> SqlResult<tokio::task::JoinHandle<()>> {
        let transaction_metrics = self.transaction_metrics.clone();
        let transaction_manager = self.transaction_manager.clone();
        let config = self.config.clone();
        let recovery_stats = self.recovery_stats.clone();

        let handle = tokio::spawn(async move {
            let mut interval = interval(config.monitor_interval);
            interval.tick().await; // Skip first tick

            loop {
                interval.tick().await;

                let metrics_to_update = {
                    let metrics = transaction_metrics.read().await;
                    metrics.keys().copied().collect::<Vec<_>>()
                };

                for transaction_id in metrics_to_update {
                    if let Err(e) = Self::check_and_recover_transaction(
                        transaction_id,
                        &transaction_manager,
                        &transaction_metrics,
                        &config,
                        &recovery_stats,
                    ).await {
                        eprintln!("Error checking transaction {}: {:?}", transaction_id, e);
                    }
                }
            }
        });

        Ok(handle)
    }

    /// Start deadlock detection task
    async fn start_deadlock_monitor(&self) -> SqlResult<tokio::task::JoinHandle<()>> {
        let adapter = self.adapter.clone();
        let transaction_metrics = self.transaction_metrics.clone();
        let recovery_stats = self.recovery_stats.clone();
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            let mut interval = interval(config.deadlock_check_interval);
            interval.tick().await; // Skip first tick

            loop {
                interval.tick().await;

                if let Err(e) = Self::detect_and_resolve_deadlocks(
                    &adapter,
                    &transaction_metrics,
                    &recovery_stats,
                ).await {
                    eprintln!("Error in deadlock detection: {:?}", e);
                }
            }
        });

        Ok(handle)
    }

    /// Check and recover a single transaction
    async fn check_and_recover_transaction(
        transaction_id: Uuid,
        transaction_manager: &Arc<TransactionManager>,
        transaction_metrics: &Arc<RwLock<HashMap<Uuid, TransactionMetrics>>>,
        config: &MonitorConfig,
        recovery_stats: &Arc<RwLock<RecoveryStats>>,
    ) -> SqlResult<()> {
        // Get transaction
        let transaction = transaction_manager.get_transaction(transaction_id).await?;
        if transaction.is_none() {
            // Transaction no longer exists, remove from monitoring
            let mut metrics = transaction_metrics.write().await;
            metrics.remove(&transaction_id);
            return Ok(());
        }

        // Update metrics
        {
            let mut metrics = transaction_metrics.write().await;
            if let Some(tx_metrics) = metrics.get_mut(&transaction_id) {
                // Update duration and health
                tx_metrics.duration = Utc::now().signed_duration_since(tx_metrics.last_activity).to_std().unwrap_or(Duration::from_secs(0));
                tx_metrics.update_health(config.timeout_threshold, config.warning_threshold);
                tx_metrics.last_activity = Utc::now();

                // Get operation count from transaction
                tx_metrics.operation_count = transaction.as_ref().unwrap().operation_count().await;
            }
        }

        // Check if recovery is needed
        let metrics = {
            let metrics_map = transaction_metrics.read().await;
            metrics_map.get(&transaction_id).cloned()
        };

        if let Some(tx_metrics) = metrics {
            if tx_metrics.is_in_distress() && config.auto_recovery_enabled {
                let action = Self::determine_recovery_action_static(&tx_metrics).await?;

                match Self::execute_recovery_action_static(
                    transaction_id,
                    &action,
                    transaction_manager,
                ).await {
                    Ok(_) => {
                        // Update recovery stats
                        let mut stats = recovery_stats.write().await;
                        match action {
                            RecoveryAction::Rollback | RecoveryAction::Kill => {
                                stats.automatic_rollbacks += 1;
                            }
                            RecoveryAction::Escalate => {
                                stats.manual_escalations += 1;
                            }
                            _ => {}
                        }
                        stats.successful_recoveries += 1;
                    }
                    Err(e) => {
                        eprintln!("Failed to execute recovery for transaction {}: {:?}", transaction_id, e);
                        let mut stats = recovery_stats.write().await;
                        stats.failed_recoveries += 1;
                    }
                }
            }
        }

        Ok(())
    }

    /// Detect and resolve deadlocks
    async fn detect_and_resolve_deadlocks(
        adapter: &Arc<dyn DatabaseAdapter>,
        transaction_metrics: &Arc<RwLock<HashMap<Uuid, TransactionMetrics>>>,
        recovery_stats: &Arc<RwLock<RecoveryStats>>,
    ) -> SqlResult<()> {
        // Get deadlocks from database
        let deadlocks = adapter.detect_deadlocks().await?;

        for deadlock in deadlocks {
            // Find the transaction with the least work done (victim selection)
            let victim_tx_id = Self::select_deadlock_victim(&deadlock, transaction_metrics).await;

            if let Some(victim_id) = victim_tx_id {
                // Resolve deadlock by rolling back victim
                if let Err(e) = adapter.resolve_deadlock(victim_id).await {
                    eprintln!("Failed to resolve deadlock by rolling back {}: {:?}", victim_id, e);
                    continue;
                }

                // Update metrics
                {
                    let mut metrics = transaction_metrics.write().await;
                    if let Some(tx_metrics) = metrics.get_mut(&victim_id) {
                        tx_metrics.health = TransactionHealth::Failed;
                    }
                }

                // Update recovery stats
                let mut stats = recovery_stats.write().await;
                stats.deadlocks_resolved += 1;
            }
        }

        Ok(())
    }

    /// Determine recovery action for a transaction
    async fn determine_recovery_action(&self, metrics: &TransactionMetrics) -> SqlResult<RecoveryAction> {
        Self::determine_recovery_action_static(metrics).await
    }

    /// Static version of determine_recovery_action
    async fn determine_recovery_action_static(metrics: &TransactionMetrics) -> SqlResult<RecoveryAction> {
        match metrics.health {
            TransactionHealth::Timeout => {
                if metrics.retry_count < 3 {
                    Ok(RecoveryAction::Retry)
                } else {
                    Ok(RecoveryAction::Rollback)
                }
            }
            TransactionHealth::Deadlocked => {
                // For deadlocks, we typically need to rollback one of the transactions
                Ok(RecoveryAction::Kill)
            }
            TransactionHealth::Failed => {
                Ok(RecoveryAction::Rollback)
            }
            TransactionHealth::Warning => {
                // For warning state, we might extend timeout
                Ok(RecoveryAction::ExtendTimeout(Duration::from_secs(30)))
            }
            _ => Ok(RecoveryAction::None),
        }
    }

    /// Execute recovery action
    async fn execute_recovery_action(&self, transaction_id: Uuid, action: &RecoveryAction) -> SqlResult<()> {
        Self::execute_recovery_action_static(transaction_id, action, &self.transaction_manager).await
    }

    /// Static version of execute_recovery_action
    async fn execute_recovery_action_static(
        transaction_id: Uuid,
        action: &RecoveryAction,
        transaction_manager: &Arc<TransactionManager>,
    ) -> SqlResult<()> {
        match action {
            RecoveryAction::None => Ok(()),
            RecoveryAction::ExtendTimeout(_) => {
                // In a real implementation, this would extend the transaction timeout
                eprintln!("Extending timeout for transaction {}", transaction_id);
                Ok(())
            }
            RecoveryAction::Retry => {
                eprintln!("Retrying transaction {}", transaction_id);
                // In a real implementation, this would restart the transaction
                Ok(())
            }
            RecoveryAction::Rollback | RecoveryAction::Kill => {
                if let Some(transaction) = transaction_manager.get_transaction(transaction_id).await? {
                    transaction.rollback().await?;
                }
                transaction_manager.remove_transaction(transaction_id).await?;
                Ok(())
            }
            RecoveryAction::Escalate => {
                eprintln!("Escalating transaction {} for manual intervention", transaction_id);
                // In a real implementation, this would create alerts/tickets
                Ok(())
            }
        }
    }

    /// Select deadlock victim based on transaction metrics
    async fn select_deadlock_victim(
        deadlock: &DeadlockInfo,
        transaction_metrics: &Arc<RwLock<HashMap<Uuid, TransactionMetrics>>>,
    ) -> Option<Uuid> {
        let metrics = transaction_metrics.read().await;
        let mut best_candidate = None;
        let mut min_work_done = None;

        for tx_id in &deadlock.involved_transactions {
            if let Some(tx_metrics) = metrics.get(tx_id) {
                let work_done = tx_metrics.operation_count;
                if min_work_done.is_none() || work_done < min_work_done.unwrap() {
                    min_work_done = Some(work_done);
                    best_candidate = Some(*tx_id);
                }
            }
        }

        best_candidate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::SQLiteAdapter;
    use crate::DatabaseConfig;
    use crate::transaction::{TransactionManager, IsolationLevel};

    #[tokio::test]
    async fn test_transaction_metrics_creation() {
        let transaction_id = Uuid::new_v4();
        let metrics = TransactionMetrics::new(transaction_id);

        assert_eq!(metrics.transaction_id, transaction_id);
        assert_eq!(metrics.health, TransactionHealth::Healthy);
        assert_eq!(metrics.operation_count, 0);
        assert_eq!(metrics.retry_count, 0);
        assert!(metrics.is_in_distress() == false);
    }

    #[tokio::test]
    async fn test_transaction_health_update() {
        let transaction_id = Uuid::new_v4();
        let mut metrics = TransactionMetrics::new(transaction_id);

        // Simulate long-running transaction
        metrics.duration = Duration::from_secs(50);
        metrics.update_health(Duration::from_secs(60), Duration::from_secs(45));
        assert_eq!(metrics.health, TransactionHealth::Warning);

        // Simulate timeout
        metrics.duration = Duration::from_secs(70);
        metrics.update_health(Duration::from_secs(60), Duration::from_secs(45));
        assert_eq!(metrics.health, TransactionHealth::Timeout);
        assert!(metrics.is_in_distress());
    }

    #[tokio::test]
    async fn test_recovery_action_determination() {
        let mut metrics = TransactionMetrics::new(Uuid::new_v4());

        // Test timeout scenario
        metrics.health = TransactionHealth::Timeout;
        metrics.retry_count = 1;
        let action = TransactionMonitor::determine_recovery_action_static(&metrics).await.unwrap();
        assert!(matches!(action, RecoveryAction::Retry));

        // Test deadlock scenario
        metrics.health = TransactionHealth::Deadlocked;
        let action = TransactionMonitor::determine_recovery_action_static(&metrics).await.unwrap();
        assert!(matches!(action, RecoveryAction::Kill));

        // Test warning scenario
        metrics.health = TransactionHealth::Warning;
        let action = TransactionMonitor::determine_recovery_action_static(&metrics).await.unwrap();
        assert!(matches!(action, RecoveryAction::ExtendTimeout(_)));
    }

    #[tokio::test]
    async fn test_monitor_creation() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let tx_manager = Arc::new(TransactionManager::new(adapter.clone()));
        let monitor_config = MonitorConfig::default();

        let monitor = TransactionMonitor::new(adapter, tx_manager, monitor_config);

        assert_eq!(monitor.get_all_metrics().await.len(), 0);
        assert_eq!(monitor.get_recovery_stats().await.timeouts_handled, 0);
    }

    #[tokio::test]
    async fn test_transaction_registration() {
        let config = DatabaseConfig::default();
        let adapter = Arc::new(SQLiteAdapter::new(config).await.unwrap());
        let tx_manager = Arc::new(TransactionManager::new(adapter.clone()));
        let monitor_config = MonitorConfig::default();

        let monitor = TransactionMonitor::new(adapter.clone(), tx_manager.clone(), monitor_config);

        // Create and register a transaction
        let transaction = tx_manager.begin_transaction(None, None, None).await.unwrap();
        monitor.register_transaction(&transaction).await.unwrap();

        // Check that metrics were created
        let metrics = monitor.get_transaction_metrics(transaction.transaction_id).await;
        assert!(metrics.is_some());
        assert_eq!(metrics.unwrap().transaction_id, transaction.transaction_id);

        // Cleanup
        monitor.unregister_transaction(transaction.transaction_id).await.unwrap();
        tx_manager.remove_transaction(transaction.transaction_id).await.unwrap();
    }
}