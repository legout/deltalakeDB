//! Monitoring and observability for Delta Lake log mirroring

use crate::error::{MirrorError, MirrorResult};
use crate::pipeline::{MirrorPipeline, MirrorStatus, TaskStatus};
use crate::engine::DeltaMirrorEngine;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::{interval, sleep};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod metrics;
pub mod health;
pub mod alerts;

pub use metrics::MirrorMetrics;
pub use health::MirrorHealth;
pub use alerts::{MirrorAlert, AlertSeverity, AlertManager};

/// Monitoring system for Delta Lake log mirroring operations
pub struct MirrorMonitor {
    config: MonitoringConfig,
    engine: Arc<DeltaMirrorEngine>,
    pipeline: Arc<MirrorPipeline>,
    metrics_collector: Arc<RwLock<MetricsCollector>>,
    health_checker: Arc<RwLock<HealthChecker>>,
    alert_manager: Arc<RwLock<AlertManager>>,
    is_running: Arc<RwLock<bool>>,
}

/// Configuration for monitoring components
#[derive(Debug, Clone)]
pub struct MonitoringConfig {
    /// Metrics collection interval in seconds
    pub metrics_interval_secs: u64,
    /// Health check interval in seconds
    pub health_check_interval_secs: u64,
    /// Lag monitoring threshold in seconds
    pub lag_threshold_secs: u64,
    /// Alert configuration
    pub alert_config: AlertConfig,
    /// Performance monitoring configuration
    pub performance_config: PerformanceMonitoringConfig,
    /// Whether to enable detailed logging
    pub enable_detailed_logging: bool,
}

/// Alert configuration
#[derive(Debug, Clone)]
pub struct AlertConfig {
    /// Whether alerts are enabled
    pub enabled: bool,
    /// Alert channels
    pub channels: Vec<AlertChannel>,
    /// Alert cooldown period in seconds
    pub cooldown_secs: u64,
    /// Maximum alerts per hour
    pub max_alerts_per_hour: usize,
}

/// Alert channel configuration
#[derive(Debug, Clone)]
pub enum AlertChannel {
    /// Log alerts
    Log,
    /// Send to webhook
    Webhook { url: String },
    /// Send to email (placeholder)
    Email { address: String },
    /// Send to monitoring system (placeholder)
    Monitoring { endpoint: String },
}

/// Performance monitoring configuration
#[derive(Debug, Clone)]
pub struct PerformanceMonitoringConfig {
    /// Whether to track file sizes
    pub track_file_sizes: bool,
    /// Whether to track processing times
    pub track_processing_times: bool,
    /// Whether to track queue depths
    pub track_queue_depths: bool,
    /// Whether to track error rates
    pub track_error_rates: bool,
    /// Performance percentiles to track
    pub percentiles: Vec<f64>,
}

/// Internal metrics collector
struct MetricsCollector {
    system_metrics: MirrorMetrics,
    table_metrics: HashMap<Uuid, TableMetrics>,
    performance_samples: Vec<PerformanceSample>,
    last_updated: DateTime<Utc>,
}

/// Table-specific metrics
#[derive(Debug, Clone)]
struct TableMetrics {
    table_id: Uuid,
    table_path: String,
    latest_version: i64,
    latest_mirrored_version: i64,
    lag_seconds: u64,
    total_commits: u64,
    mirrored_commits: u64,
    failed_commits: u64,
    last_mirrored_at: Option<DateTime<Utc>>,
    avg_processing_time_ms: f64,
    bytes_mirrored: u64,
    files_mirrored: u64,
}

/// Performance data sample
#[derive(Debug, Clone)]
struct PerformanceSample {
    timestamp: DateTime<Utc>,
    operation_type: String,
    duration_ms: u64,
    bytes_processed: u64,
    queue_depth: usize,
    active_workers: usize,
    success: bool,
}

/// Health checker component
struct HealthChecker {
    overall_health: MirrorHealth,
    component_health: HashMap<String, ComponentHealth>,
    last_check: DateTime<Utc>,
}

/// Component health status
#[derive(Debug, Clone)]
struct ComponentHealth {
    name: String,
    status: HealthStatus,
    last_check: DateTime<Utc>,
    error_message: Option<String>,
    response_time_ms: u64,
    metrics: HashMap<String, f64>,
}

/// Health status enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

impl MirrorMonitor {
    /// Create a new mirror monitor
    pub async fn new(
        config: MonitoringConfig,
        engine: Arc<DeltaMirrorEngine>,
        pipeline: Arc<MirrorPipeline>,
    ) -> MirrorResult<Self> {
        let metrics_collector = Arc::new(RwLock::new(MetricsCollector {
            system_metrics: MirrorMetrics::default(),
            table_metrics: HashMap::new(),
            performance_samples: Vec::new(),
            last_updated: Utc::now(),
        }));

        let health_checker = Arc::new(RwLock::new(HealthChecker {
            overall_health: MirrorHealth::default(),
            component_health: HashMap::new(),
            last_check: Utc::now(),
        }));

        let alert_manager = Arc::new(RwLock::new(AlertManager::new(
            config.alert_config.clone(),
        )));

        Ok(Self {
            config,
            engine,
            pipeline,
            metrics_collector,
            health_checker,
            alert_manager,
            is_running: Arc::new(RwLock::new(false)),
        })
    }

    /// Start the monitoring system
    pub async fn start(&self) -> MirrorResult<()> {
        let mut is_running = self.is_running.write().await;
        if *is_running {
            return Ok(());
        }

        *is_running = true;
        tracing::info!("Starting MirrorMonitor");

        // Start metrics collection
        let metrics_interval = Duration::from_secs(self.config.metrics_interval_secs);
        let metrics_collector = self.metrics_collector.clone();
        let pipeline = self.pipeline.clone();
        let engine = self.engine.clone();

        tokio::spawn(async move {
            let mut interval = interval(metrics_interval);
            loop {
                interval.tick().await;
                if let Err(e) = Self::collect_metrics(&metrics_collector, &pipeline, &engine).await {
                    tracing::error!("Error collecting metrics: {}", e);
                }
            }
        });

        // Start health checks
        let health_interval = Duration::from_secs(self.config.health_check_interval_secs);
        let health_checker = self.health_checker.clone();
        let alert_manager = self.alert_manager.clone();

        tokio::spawn(async move {
            let mut interval = interval(health_interval);
            loop {
                interval.tick().await;
                if let Err(e) = Self::perform_health_checks(&health_checker, &alert_manager).await {
                    tracing::error!("Error performing health checks: {}", e);
                }
            }
        });

        // Start lag monitoring
        let lag_threshold = Duration::from_secs(self.config.lag_threshold_secs);
        let monitor_clone = self.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60)); // Check every minute
            loop {
                interval.tick().await;
                if let Err(e) = Self::monitor_lag(&monitor_clone, lag_threshold).await {
                    tracing::error!("Error monitoring lag: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Stop the monitoring system
    pub async fn stop(&self) -> MirrorResult<()> {
        let mut is_running = self.is_running.write().await;
        *is_running = false;
        tracing::info!("MirrorMonitor stopped");
        Ok(())
    }

    /// Get current metrics
    pub async fn get_metrics(&self) -> MirrorResult<MirrorMetrics> {
        let collector = self.metrics_collector.read().await;
        Ok(collector.system_metrics.clone())
    }

    /// Get table-specific metrics
    pub async fn get_table_metrics(&self, table_id: Uuid) -> MirrorResult<Option<TableMetrics>> {
        let collector = self.metrics_collector.read().await;
        Ok(collector.table_metrics.get(&table_id).cloned())
    }

    /// Get table status
    pub async fn get_table_status(&self, table_id: Uuid) -> MirrorResult<MirrorStatus> {
        // Get latest tasks for this table from the pipeline
        let tasks = self.pipeline.get_tasks_by_status(TaskStatus::Running).await;

        // Find the task for this table (simplified approach)
        for task in tasks {
            if task.table_id == table_id {
                return Ok(task);
            }
        }

        // If no running task, get the most recent completed task
        let completed_tasks = self.pipeline.get_tasks_by_status(TaskStatus::Completed).await;
        for task in completed_tasks {
            if task.table_id == table_id {
                return Ok(task);
            }
        }

        // No tasks found for this table
        Err(MirrorError::validation_error("No tasks found for table"))
    }

    /// Perform health check
    pub async fn health_check(&self) -> MirrorResult<MirrorHealth> {
        let checker = self.health_checker.read().await;
        Ok(checker.overall_health.clone())
    }

    /// Get recent alerts
    pub async fn get_recent_alerts(&self, limit: Option<usize>) -> MirrorResult<Vec<MirrorAlert>> {
        let alert_manager = self.alert_manager.read().await;
        Ok(alert_manager.get_recent_alerts(limit))
    }

    /// Collect metrics from pipeline and engine
    async fn collect_metrics(
        metrics_collector: &Arc<RwLock<MetricsCollector>>,
        pipeline: &Arc<MirrorPipeline>,
        engine: &Arc<DeltaMirrorEngine>,
    ) -> MirrorResult<()> {
        let mut collector = metrics_collector.write().await;

        // Get pipeline stats
        let pipeline_stats = pipeline.get_stats().await;

        // Update system metrics
        collector.system_metrics.pending_tasks = pipeline_stats.pending_tasks;
        collector.system_metrics.processing_tasks = pipeline_stats.processing_tasks;
        collector.system_metrics.completed_tasks = pipeline_stats.completed_tasks;
        collector.system_metrics.worker_count = pipeline_stats.worker_count;
        collector.system_metrics.queue_utilization =
            pipeline_stats.pending_tasks as f64 / pipeline_stats.max_queue_size as f64;

        collector.last_updated = Utc::now();

        if collector.system_metrics.pending_tasks > 0 {
            tracing::debug!(
                "Metrics updated: {} pending, {} processing, {} completed",
                collector.system_metrics.pending_tasks,
                collector.system_metrics.processing_tasks,
                collector.system_metrics.completed_tasks
            );
        }

        Ok(())
    }

    /// Perform health checks
    async fn perform_health_checks(
        health_checker: &Arc<RwLock<HealthChecker>>,
        alert_manager: &Arc<RwLock<AlertManager>>,
    ) -> MirrorResult<()> {
        let mut checker = health_checker.write().await;

        // Check overall system health
        let now = Utc::now();
        let time_since_last_check = now.signed_duration_since(checker.last_check).num_seconds();

        // Update overall health
        checker.overall_health.status = if time_since_last_check > 300 {
            HealthStatus::Unhealthy
        } else if time_since_last_check > 60 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        checker.overall_health.last_check = now;
        checker.last_check = now;

        // Send alerts if unhealthy
        if checker.overall_health.status != HealthStatus::Healthy {
            let alert = MirrorAlert {
                id: Uuid::new_v4(),
                severity: match checker.overall_health.status {
                    HealthStatus::Unhealthy => AlertSeverity::Critical,
                    HealthStatus::Degraded => AlertSeverity::Warning,
                    _ => AlertSeverity::Info,
                },
                title: "System Health Issue".to_string(),
                message: format!("System health is: {:?}", checker.overall_health.status),
                timestamp: now,
                component: "system".to_string(),
                metadata: HashMap::new(),
            };

            let mut alert_mgr = alert_manager.write().await;
            alert_mgr.send_alert(alert).await?;
        }

        Ok(())
    }

    /// Monitor for replication lag
    async fn monitor_lag(monitor: &MirrorMonitor, threshold: Duration) -> MirrorResult<()> {
        let collector = monitor.metrics_collector.read().await;

        for (table_id, table_metrics) in &collector.table_metrics {
            let lag_duration = Duration::from_secs(table_metrics.lag_seconds);

            if lag_duration > threshold {
                let alert = MirrorAlert {
                    id: Uuid::new_v4(),
                    severity: if lag_duration > threshold * 2 {
                        AlertSeverity::Critical
                    } else {
                        AlertSeverity::Warning
                    },
                    title: "Replication Lag Detected".to_string(),
                    message: format!(
                        "Table {} has replication lag of {} seconds (threshold: {} seconds)",
                        table_metrics.table_path, table_metrics.lag_seconds, threshold.as_secs()
                    ),
                    timestamp: Utc::now(),
                    component: "replication".to_string(),
                    metadata: {
                        let mut meta = HashMap::new();
                        meta.insert("table_id".to_string(), table_id.to_string());
                        meta.insert("lag_seconds".to_string(), table_metrics.lag_seconds.to_string());
                        meta.insert("threshold_seconds".to_string(), threshold.as_secs().to_string());
                        meta
                    },
                };

                let mut alert_mgr = monitor.alert_manager.write().await;
                alert_mgr.send_alert(alert).await?;
            }
        }

        Ok(())
    }
}

impl Clone for MirrorMonitor {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            engine: self.engine.clone(),
            pipeline: self.pipeline.clone(),
            metrics_collector: self.metrics_collector.clone(),
            health_checker: self.health_checker.clone(),
            alert_manager: self.alert_manager.clone(),
            is_running: self.is_running.clone(),
        }
    }
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            metrics_interval_secs: 30,
            health_check_interval_secs: 60,
            lag_threshold_secs: 300, // 5 minutes
            alert_config: AlertConfig::default(),
            performance_config: PerformanceMonitoringConfig::default(),
            enable_detailed_logging: false,
        }
    }
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            channels: vec![AlertChannel::Log],
            cooldown_secs: 300, // 5 minutes
            max_alerts_per_hour: 50,
        }
    }
}

impl Default for PerformanceMonitoringConfig {
    fn default() -> Self {
        Self {
            track_file_sizes: true,
            track_processing_times: true,
            track_queue_depths: true,
            track_error_rates: true,
            percentiles: vec![0.5, 0.9, 0.95, 0.99],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MirrorEngineConfig;
    use crate::storage::create_test_storage;

    #[tokio::test]
    async fn test_monitor_creation() {
        let config = MonitoringConfig::default();
        let storage = storage::create_test_storage().await.unwrap();
        let engine_config = MirrorEngineConfig::default();
        let engine = Arc::new(DeltaMirrorEngine::new(engine_config, storage).await.unwrap());
        let pipeline = Arc::new(MirrorPipeline::new(engine_config, engine.clone(), storage.clone()).await.unwrap());

        let monitor = MirrorMonitor::new(config, engine, pipeline).await.unwrap();

        let metrics = monitor.get_metrics().await.unwrap();
        assert_eq!(metrics.pending_tasks, 0);
        assert_eq!(metrics.processing_tasks, 0);
    }

    #[tokio::test]
    async fn test_monitor_lifecycle() {
        let config = MonitoringConfig::default();
        let storage = storage::create_test_storage().await.unwrap();
        let engine_config = MirrorEngineConfig::default();
        let engine = Arc::new(DeltaMirrorEngine::new(engine_config, storage).await.unwrap());
        let pipeline = Arc::new(MirrorPipeline::new(engine_config, engine.clone(), storage.clone()).await.unwrap());

        let monitor = MirrorMonitor::new(config, engine, pipeline).await.unwrap();

        // Start monitoring
        monitor.start().await.unwrap();

        // Give it a moment to collect some metrics
        sleep(Duration::from_millis(100)).await;

        // Stop monitoring
        monitor.stop().await.unwrap();
    }

    #[test]
    fn test_monitoring_config_defaults() {
        let config = MonitoringConfig::default();
        assert_eq!(config.metrics_interval_secs, 30);
        assert_eq!(config.health_check_interval_secs, 60);
        assert_eq!(config.lag_threshold_secs, 300);
        assert!(config.alert_config.enabled);
    }

    #[test]
    fn test_health_status_transitions() {
        let mut health = HealthStatus::Healthy;

        // Simulate degradation
        health = HealthStatus::Degraded;
        assert_eq!(health, HealthStatus::Degraded);

        // Simulate failure
        health = HealthStatus::Unhealthy;
        assert_eq!(health, HealthStatus::Unhealthy);

        // Recovery
        health = HealthStatus::Healthy;
        assert_eq!(health, HealthStatus::Healthy);
    }
}