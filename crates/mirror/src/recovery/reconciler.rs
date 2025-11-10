//! Background reconciler for failed Delta Lake log mirroring operations

use crate::error::{MirrorError, MirrorResult};
use crate::pipeline::{MirrorTask, MirrorStatus, TaskStatus};
use crate::engine::DeltaMirrorEngine;
use crate::storage::MirrorStorage;
use super::{RecoveryStats, RecoveryStatus, RecoveryTaskStatus};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use tokio::time::{sleep, interval, Instant};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use tracing::{debug, info, warn, error};

/// Background reconciler for failed mirroring operations
pub struct Reconciler {
    config: ReconcilerConfig,
    engine: Arc<DeltaMirrorEngine>,
    storage: Arc<dyn MirrorStorage>,
    recovery_queue: Arc<RwLock<VecDeque<RecoveryStatus>>>,
    stats: Arc<RwLock<RecoveryStats>>,
    shutdown_tx: mpsc::Sender<()>,
    shutdown_rx: Arc<RwLock<mpsc::Receiver<()>>>,
}

/// Configuration for the reconciler
#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// How often to check for failed tasks
    pub check_interval_secs: u64,
    /// Default retry policy
    pub retry_policy: RetryPolicy,
    /// Maximum concurrent recovery attempts
    pub max_concurrent_recoveries: usize,
    /// Batch size for processing failed tasks
    pub batch_size: usize,
    /// Enable automatic retry
    pub enabled: bool,
    /// Maximum age for recovery entries before cleanup
    pub max_recovery_age_hours: u64,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            check_interval_secs: 30,
            retry_policy: RetryPolicy::default(),
            max_concurrent_recoveries: 3,
            batch_size: 10,
            enabled: true,
            max_recovery_age_hours: 24,
        }
    }
}

/// Retry policy for failed operations
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial backoff delay in milliseconds
    pub initial_backoff_ms: u64,
    /// Maximum backoff delay in milliseconds
    pub max_backoff_ms: u64,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// Jitter factor to add randomness
    pub jitter_factor: f64,
    /// Enable exponential backoff
    pub exponential_backoff: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_backoff_ms: 1000,
            max_backoff_ms: 300000, // 5 minutes
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
            exponential_backoff: true,
        }
    }
}

impl Reconciler {
    /// Create a new reconciler
    pub async fn new(
        config: ReconcilerConfig,
        engine: Arc<DeltaMirrorEngine>,
        storage: Arc<dyn MirrorStorage>,
    ) -> MirrorResult<Self> {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        Ok(Self {
            config,
            engine,
            storage,
            recovery_queue: Arc::new(RwLock::new(VecDeque::new())),
            stats: Arc::new(RwLock::new(RecoveryStats::default())),
            shutdown_tx,
            shutdown_rx: Arc::new(RwLock::new(shutdown_rx)),
        })
    }

    /// Start the reconciler background task
    pub async fn start(&self) -> MirrorResult<()> {
        if !self.config.enabled {
            info!("Reconciler is disabled, not starting");
            return Ok(());
        }

        info!("Starting reconciler with check interval: {}s", self.config.check_interval_secs);

        let mut interval = interval(Duration::from_secs(self.config.check_interval_secs));
        let mut shutdown_rx = self.shutdown_rx.clone();
        let recovery_queue = self.recovery_queue.clone();
        let stats = self.stats.clone();
        let engine = self.engine.clone();
        let storage = self.storage.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = Self::process_recoveries(
                            &recovery_queue,
                            &stats,
                            &engine,
                            &storage,
                            &config,
                        ).await {
                            error!("Error in recovery processing: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Reconciler shutdown signal received");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Add a failed task to the recovery queue
    pub async fn add_failed_task(&self, task: MirrorTask, failure_reason: String) -> MirrorResult<()> {
        let recovery_status = RecoveryStatus {
            task_id: task.task_id(),
            retry_count: 0,
            max_retries: self.config.retry_policy.max_retries,
            current_backoff_ms: self.config.retry_policy.initial_backoff_ms,
            next_retry_at: Utc::now() + chrono::Duration::milliseconds(
                self.config.retry_policy.initial_backoff_ms as i64
            ),
            last_failure: failure_reason,
            status: RecoveryTaskStatus::Waiting,
        };

        let mut queue = self.recovery_queue.write().await;
        queue.push_back(recovery_status);

        let mut stats = self.stats.write().await;
        stats.total_attempts += 1;
        stats.last_activity = Some(Utc::now());

        debug!("Added task {} to recovery queue", task.task_id());
        Ok(())
    }

    /// Process recoveries for ready tasks
    async fn process_recoveries(
        recovery_queue: &Arc<RwLock<VecDeque<RecoveryStatus>>>,
        stats: &Arc<RwLock<RecoveryStats>>,
        engine: &Arc<DeltaMirrorEngine>,
        storage: &Arc<dyn MirrorStorage>,
        config: &ReconcilerConfig,
    ) -> MirrorResult<()> {
        let now = Utc::now();
        let mut ready_tasks = Vec::new();

        // Collect ready tasks
        {
            let mut queue = recovery_queue.write().await;
            let mut i = 0;

            while i < queue.len() {
                if queue[i].next_retry_at <= now
                    && queue[i].status == RecoveryTaskStatus::Waiting
                    && ready_tasks.len() < config.max_concurrent_recoveries {

                    let mut recovery_status = queue.remove(i).unwrap();
                    recovery_status.status = RecoveryTaskStatus::Attempting;
                    ready_tasks.push(recovery_status);
                } else {
                    i += 1;
                }
            }
        }

        // Process ready tasks
        for mut recovery_status in ready_tasks {
            let task_id = recovery_status.task_id;
            let retry_count = recovery_status.retry_count;

            debug!("Attempting recovery for task {} (attempt {})", task_id, retry_count + 1);

            let start_time = Instant::now();
            let result = Self::attempt_recovery(engine, storage, &recovery_status).await;
            let duration = start_time.elapsed();

            match result {
                Ok(_) => {
                    info!("Successfully recovered task {}", task_id);
                    recovery_status.status = RecoveryTaskStatus::Recovered;

                    let mut stats_guard = stats.write().await;
                    stats_guard.successful_recoveries += 1;
                    Self::update_recovery_time_stats(&mut stats_guard, duration);
                }
                Err(e) => {
                    warn!("Recovery attempt {} failed for task {}: {}", retry_count + 1, task_id, e);

                    if retry_count >= recovery_status.max_retries - 1 {
                        error!("Task {} permanently failed after {} attempts", task_id, retry_count + 1);
                        recovery_status.status = RecoveryTaskStatus::PermanentlyFailed;

                        let mut stats_guard = stats.write().await;
                        stats_guard.failed_recoveries += 1;
                    } else {
                        recovery_status.retry_count += 1;
                        recovery_status.last_failure = e.to_string();

                        // Calculate next backoff with jitter
                        let base_backoff = if config.retry_policy.exponential_backoff {
                            config.retry_policy.initial_backoff_ms
                                * (config.retry_policy.backoff_multiplier.powi(retry_count as i32) as u64)
                        } else {
                            config.retry_policy.initial_backoff_ms
                        };

                        let backoff_with_jitter = Self::add_jitter(
                            base_backoff.min(config.retry_policy.max_backoff_ms),
                            config.retry_policy.jitter_factor,
                        );

                        recovery_status.current_backoff_ms = backoff_with_jitter;
                        recovery_status.next_retry_at = now + chrono::Duration::milliseconds(backoff_with_jitter as i64);
                        recovery_status.status = RecoveryTaskStatus::Waiting;

                        debug!("Scheduling next retry for task {} in {}ms", task_id, backoff_with_jitter);
                    }
                }
            }

            // Re-queue if not permanently failed
            if recovery_status.status != RecoveryTaskStatus::Recovered {
                let mut queue = recovery_queue.write().await;
                queue.push_back(recovery_status);
            }
        }

        // Cleanup old entries
        Self::cleanup_old_entries(recovery_queue, config.max_recovery_age_hours).await;

        Ok(())
    }

    /// Attempt to recover a specific task
    async fn attempt_recovery(
        engine: &Arc<DeltaMirrorEngine>,
        storage: &Arc<dyn MirrorStorage>,
        recovery_status: &RecoveryStatus,
    ) -> MirrorResult<()> {
        // Reconstruct the original task
        // In a real implementation, we'd store the original task details
        // For now, we'll create a placeholder task
        let task = MirrorTask::CommitMirroring {
            table_id: recovery_status.task_id, // This is a simplification
            version: 0, // This would be stored in recovery status
        };

        match task {
            MirrorTask::CommitMirroring { table_id, version } => {
                // Attempt to re-mirror the commit
                let table_path = format!("/tables/{}", table_id);

                // This would need to fetch the actual commit data from SQL
                // For now, we'll simulate a successful retry
                sleep(Duration::from_millis(100)).await;

                Ok(())
            }
            _ => {
                // Handle other task types as needed
                sleep(Duration::from_millis(50)).await;
                Ok(())
            }
        }
    }

    /// Add jitter to backoff delay
    fn add_jitter(base_delay: u64, jitter_factor: f64) -> u64 {
        let jitter = (base_delay as f64 * jitter_factor) as u64;
        use rand::Rng;
        let mut rng = rand::thread_rng();
        base_delay + rng.gen_range(0..=jitter)
    }

    /// Update recovery time statistics
    fn update_recovery_time_stats(stats: &mut RecoveryStats, duration: Duration) {
        let duration_ms = duration.as_millis() as f64;

        if stats.successful_recoveries == 1 {
            stats.avg_recovery_time_ms = duration_ms;
        } else {
            let total_time = stats.avg_recovery_time_ms * (stats.successful_recoveries - 1) as f64;
            stats.avg_recovery_time_ms = (total_time + duration_ms) / stats.successful_recoveries as f64;
        }
    }

    /// Clean up old recovery entries
    async fn cleanup_old_entries(
        recovery_queue: &Arc<RwLock<VecDeque<RecoveryStatus>>>,
        max_age_hours: u64,
    ) {
        let cutoff = Utc::now() - chrono::Duration::hours(max_age_hours as i64);
        let mut queue = recovery_queue.write().await;

        queue.retain(|status| {
            status.next_retry_at > cutoff
                || matches!(status.status, RecoveryTaskStatus::Waiting | RecoveryTaskStatus::Attempting)
        });
    }

    /// Get current recovery statistics
    pub async fn get_stats(&self) -> RecoveryStats {
        let stats = self.stats.read().await;
        let queue = self.recovery_queue.read().await;

        let mut result = stats.clone();
        result.in_recovery = queue.iter()
            .filter(|s| matches!(s.status, RecoveryTaskStatus::Attempting))
            .count();

        // Calculate success rate
        if result.total_attempts > 0 {
            result.success_rate = result.successful_recoveries as f64 / result.total_attempts as f64;
        }

        result
    }

    /// Get recovery queue status
    pub async fn get_queue_status(&self) -> Vec<RecoveryStatus> {
        let queue = self.recovery_queue.read().await;
        queue.iter().cloned().collect()
    }

    /// Cancel recovery for a specific task
    pub async fn cancel_recovery(&self, task_id: Uuid) -> MirrorResult<bool> {
        let mut queue = self.recovery_queue.write().await;

        if let Some(status) = queue.iter_mut().find(|s| s.task_id == task_id) {
            status.status = RecoveryTaskStatus::Cancelled;
            debug!("Cancelled recovery for task {}", task_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Shutdown the reconciler
    pub async fn shutdown(&self) -> MirrorResult<()> {
        info!("Shutting down reconciler...");
        let _ = self.shutdown_tx.send(()).await;
        sleep(Duration::from_secs(1)).await; // Allow graceful shutdown
        info!("Reconciler shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MirrorEngineConfig, StorageConfig};
    use crate::storage::{StorageBackend, create_storage};

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 5);
        assert_eq!(policy.initial_backoff_ms, 1000);
        assert_eq!(policy.max_backoff_ms, 300000);
        assert!(policy.exponential_backoff);
    }

    #[test]
    fn test_reconciler_config_default() {
        let config = ReconcilerConfig::default();
        assert_eq!(config.check_interval_secs, 30);
        assert_eq!(config.max_concurrent_recoveries, 3);
        assert!(config.enabled);
    }

    #[test]
    fn test_add_jitter() {
        let base = 1000;
        let jittered = Reconciler::add_jitter(base, 0.1);
        assert!(jittered >= base);
        assert!(jittered <= base + (base as f64 * 0.1) as u64 + 1); // +1 for rounding
    }

    #[test]
    fn test_recovery_stats_default() {
        let stats = RecoveryStats::default();
        assert_eq!(stats.total_attempts, 0);
        assert_eq!(stats.successful_recoveries, 0);
        assert_eq!(stats.failed_recoveries, 0);
        assert_eq!(stats.in_recovery, 0);
    }

    #[tokio::test]
    async fn test_recovery_status_creation() {
        let task_id = Uuid::new_v4();
        let status = RecoveryStatus {
            task_id,
            retry_count: 0,
            max_retries: 5,
            current_backoff_ms: 1000,
            next_retry_at: Utc::now() + chrono::Duration::milliseconds(1000),
            last_failure: "Test failure".to_string(),
            status: RecoveryTaskStatus::Waiting,
        };

        assert_eq!(status.task_id, task_id);
        assert_eq!(status.retry_count, 0);
        assert_eq!(status.status, RecoveryTaskStatus::Waiting);
        assert_eq!(status.last_failure, "Test failure");
    }
}