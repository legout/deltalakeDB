//! Failure recovery and retry mechanisms for Delta Lake log mirroring

use crate::error::{MirrorError, MirrorResult};
use crate::pipeline::{MirrorTask, MirrorStatus, TaskStatus};
use crate::engine::DeltaMirrorEngine;
use crate::storage::MirrorStorage;
use crate::monitoring::metrics::MirrorMetrics;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use tokio::time::{sleep, interval, Instant};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod reconciler;
pub mod circuit_breaker;

pub use reconciler::{Reconciler, ReconcilerConfig, RetryPolicy};
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState};

/// Failure recovery configuration
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Reconciler configuration
    pub reconciler: ReconcilerConfig,
    /// Circuit breaker configuration
    pub circuit_breaker: CircuitBreakerConfig,
    /// Enable automatic recovery
    pub enabled: bool,
    /// Maximum concurrent recovery tasks
    pub max_concurrent_recoveries: usize,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            reconciler: ReconcilerConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            enabled: true,
            max_concurrent_recoveries: 3,
        }
    }
}

/// Recovery statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryStats {
    /// Total recovery attempts
    pub total_attempts: u64,
    /// Successful recoveries
    pub successful_recoveries: u64,
    /// Failed recoveries (permanently failed)
    pub failed_recoveries: u64,
    /// Currently in recovery
    pub in_recovery: usize,
    /// Average recovery time in milliseconds
    pub avg_recovery_time_ms: f64,
    /// Recovery success rate
    pub success_rate: f64,
    /// Last recovery activity
    pub last_activity: Option<DateTime<Utc>>,
}

impl Default for RecoveryStats {
    fn default() -> Self {
        Self {
            total_attempts: 0,
            successful_recoveries: 0,
            failed_recoveries: 0,
            in_recovery: 0,
            avg_recovery_time_ms: 0.0,
            success_rate: 0.0,
            last_activity: None,
        }
    }
}

/// Recovery status for a specific task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryStatus {
    /// Task being recovered
    pub task_id: Uuid,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Maximum allowed retries
    pub max_retries: u32,
    /// Current backoff delay in milliseconds
    pub current_backoff_ms: u64,
    /// Next retry attempt time
    pub next_retry_at: DateTime<Utc>,
    /// Last failure reason
    pub last_failure: String,
    /// Recovery status
    pub status: RecoveryTaskStatus,
}

/// Recovery task status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecoveryTaskStatus {
    /// Waiting for backoff period
    Waiting,
    /// Currently attempting recovery
    Attempting,
    /// Successfully recovered
    Recovered,
    /// Permanently failed (max retries exceeded)
    PermanentlyFailed,
    /// Cancelled
    Cancelled,
}