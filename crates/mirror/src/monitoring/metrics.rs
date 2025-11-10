//! Metrics collection for Delta Lake log mirroring

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// Comprehensive metrics for Delta Lake log mirroring operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorMetrics {
    /// System-wide metrics
    pub system: SystemMetrics,
    /// Task execution metrics
    pub tasks: TaskMetrics,
    /// Performance metrics
    pub performance: PerformanceMetrics,
    /// Error metrics
    pub errors: ErrorMetrics,
    /// Storage metrics
    pub storage: StorageMetrics,
    /// Table-specific metrics
    pub tables: HashMap<String, TableSummary>,
    /// Timestamp when metrics were collected
    pub timestamp: DateTime<Utc>,
}

/// System-wide metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    /// Number of tasks currently pending
    pub pending_tasks: usize,
    /// Number of tasks currently processing
    pub processing_tasks: usize,
    /// Number of completed tasks
    pub completed_tasks: usize,
    /// Number of failed tasks
    pub failed_tasks: usize,
    /// Number of worker processes
    pub worker_count: usize,
    /// Queue utilization as percentage (0.0 to 1.0)
    pub queue_utilization: f64,
    /// System uptime in seconds
    pub uptime_seconds: u64,
    /// Memory usage in MB
    pub memory_usage_mb: u64,
    /// CPU usage as percentage (0.0 to 1.0)
    pub cpu_usage: f64,
}

/// Task execution metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetrics {
    /// Total tasks processed
    pub total_tasks: u64,
    /// Tasks completed successfully
    pub successful_tasks: u64,
    /// Tasks failed
    pub failed_tasks: u64,
    /// Tasks cancelled
    pub cancelled_tasks: u64,
    /// Average task duration in milliseconds
    pub avg_task_duration_ms: f64,
    /// Maximum task duration in milliseconds
    pub max_task_duration_ms: u64,
    /// Minimum task duration in milliseconds
    pub min_task_duration_ms: u64,
    /// Task success rate as percentage (0.0 to 1.0)
    pub success_rate: f64,
    /// Task throughput per second
    pub throughput_per_sec: f64,
}

/// Performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Files processed per second
    pub files_per_sec: f64,
    /// Bytes processed per second
    pub bytes_per_sec: f64,
    /// Average file size in bytes
    pub avg_file_size_bytes: f64,
    /// Total bytes processed
    pub total_bytes_processed: u64,
    /// Total files processed
    pub total_files_processed: u64,
    /// Latency percentiles in milliseconds
    pub latency_percentiles: LatencyPercentiles,
    /// Queue depth metrics
    pub queue_metrics: QueueMetrics,
}

/// Latency percentiles for performance monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    /// 50th percentile (median)
    pub p50: f64,
    /// 90th percentile
    pub p90: f64,
    /// 95th percentile
    pub p95: f64,
    /// 99th percentile
    pub p99: f64,
    /// Maximum observed latency
    pub max: f64,
}

/// Queue metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMetrics {
    /// Current queue depth
    pub current_depth: usize,
    /// Average queue depth
    pub avg_depth: f64,
    /// Maximum queue depth observed
    pub max_depth: usize,
    /// Average time in queue in milliseconds
    pub avg_time_in_queue_ms: f64,
    /// Maximum time in queue in milliseconds
    pub max_time_in_queue_ms: u64,
}

/// Error metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMetrics {
    /// Total error count
    pub total_errors: u64,
    /// Error rate as percentage (0.0 to 1.0)
    pub error_rate: f64,
    /// Errors by type
    pub errors_by_type: HashMap<String, u64>,
    /// Recent errors (last hour)
    pub recent_errors: Vec<ErrorSummary>,
    /// Most common error types
    pub top_error_types: Vec<ErrorTypeSummary>,
}

/// Error summary for recent errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorSummary {
    /// Error timestamp
    pub timestamp: DateTime<Utc>,
    /// Error type
    pub error_type: String,
    /// Error message
    pub message: String,
    /// Component that produced the error
    pub component: String,
    /// Task ID if applicable
    pub task_id: Option<String>,
}

/// Error type summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorTypeSummary {
    /// Error type
    pub error_type: String,
    /// Count of this error type
    pub count: u64,
    /// Percentage of total errors
    pub percentage: f64,
    /// Last occurrence
    pub last_occurrence: DateTime<Utc>,
}

/// Storage metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetrics {
    /// Storage backend type
    pub backend_type: String,
    /// Total files stored
    pub total_files: u64,
    /// Total bytes stored
    pub total_bytes: u64,
    /// Storage operations per second
    pub operations_per_sec: f64,
    /// Average operation latency in milliseconds
    pub avg_operation_latency_ms: f64,
    /// Storage health status
    pub health_status: String,
    /// Available storage space (if available)
    pub available_space_bytes: Option<u64>,
    /// Storage utilization percentage (0.0 to 1.0)
    pub utilization: f64,
}

/// Table summary metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSummary {
    /// Table identifier
    pub table_id: String,
    /// Table path
    pub table_path: String,
    /// Latest version in source
    pub latest_version: i64,
    /// Latest mirrored version
    pub latest_mirrored_version: i64,
    /// Replication lag in seconds
    pub lag_seconds: u64,
    /// Total commits for this table
    pub total_commits: u64,
    /// Successful commits
    pub successful_commits: u64,
    /// Failed commits
    pub failed_commits: u64,
    /// Bytes mirrored for this table
    pub bytes_mirrored: u64,
    /// Files mirrored for this table
    pub files_mirrored: u64,
    /// Average processing time in milliseconds
    pub avg_processing_time_ms: f64,
    /// Last mirroring activity
    pub last_activity: DateTime<Utc>,
}

impl MirrorMetrics {
    /// Create empty metrics
    pub fn new() -> Self {
        Self {
            system: SystemMetrics::default(),
            tasks: TaskMetrics::default(),
            performance: PerformanceMetrics::default(),
            errors: ErrorMetrics::default(),
            storage: StorageMetrics::default(),
            tables: HashMap::new(),
            timestamp: Utc::now(),
        }
    }

    /// Update metrics timestamp
    pub fn update_timestamp(&mut self) {
        self.timestamp = Utc::now();
    }

    /// Get overall health score (0.0 to 1.0)
    pub fn health_score(&self) -> f64 {
        let system_health = if self.system.memory_usage_mb < 1024 { 1.0 } else { 0.5 };
        let task_health = self.tasks.success_rate;
        let error_health = 1.0 - self.errors.error_rate;
        let storage_health = if self.storage.health_status == "healthy" { 1.0 } else { 0.5 };

        (system_health + task_health + error_health + storage_health) / 4.0
    }

    /// Check if system is healthy
    pub fn is_healthy(&self) -> bool {
        self.health_score() >= 0.8
            && self.system.memory_usage_mb < 2048
            && self.tasks.success_rate >= 0.95
            && self.errors.error_rate < 0.05
    }

    /// Get summary string
    pub fn summary(&self) -> String {
        format!(
            "Tasks: {} pending, {} processing, {} completed | Success: {:.1}% | Throughput: {:.1} files/s",
            self.system.pending_tasks,
            self.system.processing_tasks,
            self.system.completed_tasks,
            self.tasks.success_rate * 100.0,
            self.performance.files_per_sec
        )
    }
}

impl Default for MirrorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for SystemMetrics {
    fn default() -> Self {
        Self {
            pending_tasks: 0,
            processing_tasks: 0,
            completed_tasks: 0,
            failed_tasks: 0,
            worker_count: 0,
            queue_utilization: 0.0,
            uptime_seconds: 0,
            memory_usage_mb: 0,
            cpu_usage: 0.0,
        }
    }
}

impl Default for TaskMetrics {
    fn default() -> Self {
        Self {
            total_tasks: 0,
            successful_tasks: 0,
            failed_tasks: 0,
            cancelled_tasks: 0,
            avg_task_duration_ms: 0.0,
            max_task_duration_ms: 0,
            min_task_duration_ms: u64::MAX,
            success_rate: 1.0,
            throughput_per_sec: 0.0,
        }
    }
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            files_per_sec: 0.0,
            bytes_per_sec: 0.0,
            avg_file_size_bytes: 0.0,
            total_bytes_processed: 0,
            total_files_processed: 0,
            latency_percentiles: LatencyPercentiles::default(),
            queue_metrics: QueueMetrics::default(),
        }
    }
}

impl Default for LatencyPercentiles {
    fn default() -> Self {
        Self {
            p50: 0.0,
            p90: 0.0,
            p95: 0.0,
            p99: 0.0,
            max: 0.0,
        }
    }
}

impl Default for QueueMetrics {
    fn default() -> Self {
        Self {
            current_depth: 0,
            avg_depth: 0.0,
            max_depth: 0,
            avg_time_in_queue_ms: 0.0,
            max_time_in_queue_ms: 0,
        }
    }
}

impl Default for ErrorMetrics {
    fn default() -> Self {
        Self {
            total_errors: 0,
            error_rate: 0.0,
            errors_by_type: HashMap::new(),
            recent_errors: Vec::new(),
            top_error_types: Vec::new(),
        }
    }
}

impl Default for StorageMetrics {
    fn default() -> Self {
        Self {
            backend_type: "unknown".to_string(),
            total_files: 0,
            total_bytes: 0,
            operations_per_sec: 0.0,
            avg_operation_latency_ms: 0.0,
            health_status: "unknown".to_string(),
            available_space_bytes: None,
            utilization: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = MirrorMetrics::new();
        assert_eq!(metrics.system.pending_tasks, 0);
        assert_eq!(metrics.tasks.success_rate, 1.0);
        assert_eq!(metrics.errors.error_rate, 0.0);
        assert!(metrics.tables.is_empty());
    }

    #[test]
    fn test_health_score_calculation() {
        let mut metrics = MirrorMetrics::new();
        metrics.tasks.success_rate = 0.9;
        metrics.errors.error_rate = 0.1;
        metrics.system.memory_usage_mb = 500;
        metrics.storage.health_status = "healthy".to_string();

        let health_score = metrics.health_score();
        assert!(health_score >= 0.7 && health_score <= 1.0);
    }

    #[test]
    fn test_healthy_status() {
        let mut metrics = MirrorMetrics::new();
        metrics.tasks.success_rate = 0.96;
        metrics.errors.error_rate = 0.02;
        metrics.system.memory_usage_mb = 1000;
        metrics.storage.health_status = "healthy".to_string();

        assert!(metrics.is_healthy());
    }

    #[test]
    fn test_unhealthy_status() {
        let mut metrics = MirrorMetrics::new();
        metrics.tasks.success_rate = 0.8;
        metrics.errors.error_rate = 0.3;

        assert!(!metrics.is_healthy());
    }

    #[test]
    fn test_metrics_summary() {
        let mut metrics = MirrorMetrics::new();
        metrics.system.pending_tasks = 5;
        metrics.system.processing_tasks = 3;
        metrics.system.completed_tasks = 100;
        metrics.tasks.success_rate = 0.95;
        metrics.performance.files_per_sec = 10.5;

        let summary = metrics.summary();
        assert!(summary.contains("5 pending"));
        assert!(summary.contains("3 processing"));
        assert!(summary.contains("100 completed"));
        assert!(summary.contains("95.0%"));
        assert!(summary.contains("10.5 files/s"));
    }

    #[test]
    fn test_latency_percentiles() {
        let percentiles = LatencyPercentiles {
            p50: 100.0,
            p90: 200.0,
            p95: 300.0,
            p99: 500.0,
            max: 1000.0,
        };

        assert_eq!(percentiles.p50, 100.0);
        assert_eq!(percentiles.p99, 500.0);
        assert_eq!(percentiles.max, 1000.0);
    }
}