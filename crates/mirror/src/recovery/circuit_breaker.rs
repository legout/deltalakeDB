//! Circuit breaker pattern for handling persistent failures

use crate::error::{MirrorError, MirrorResult, StorageError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{sleep, Instant};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn, info, error};

/// Circuit breaker state
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CircuitState {
    /// Circuit is closed, operations proceed normally
    Closed,
    /// Circuit is open, operations are rejected immediately
    Open,
    /// Circuit is half-open, limited operations are allowed to test recovery
    HalfOpen,
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit
    pub failure_threshold: u32,
    /// Number of successful operations required to close the circuit
    pub recovery_threshold: u32,
    /// How long to wait before attempting recovery (in seconds)
    pub recovery_timeout_secs: u64,
    /// Percentage of operations allowed in half-open state (0.0 to 1.0)
    pub half_open_success_rate: f64,
    /// Minimum number of operations before evaluating success rate
    pub min_operations: u32,
    /// Enable circuit breaker
    pub enabled: bool,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_threshold: 3,
            recovery_timeout_secs: 60,
            half_open_success_rate: 0.6,
            min_operations: 5,
            enabled: true,
        }
    }
}

/// Circuit breaker statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerStats {
    /// Current state
    pub state: CircuitState,
    /// Total operations attempted
    pub total_operations: u64,
    /// Successful operations
    pub successful_operations: u64,
    /// Failed operations
    pub failed_operations: u64,
    /// Current consecutive failures
    pub current_failures: u32,
    /// Current consecutive successes
    pub current_successes: u32,
    /// Number of times the circuit opened
    pub times_opened: u64,
    /// Last state change
    pub last_state_change: Option<DateTime<Utc>>,
    /// Last operation time
    pub last_operation: Option<DateTime<Utc>>,
    /// Average operation duration in milliseconds
    pub avg_operation_duration_ms: f64,
}

impl Default for CircuitBreakerStats {
    fn default() -> Self {
        Self {
            state: CircuitState::Closed,
            total_operations: 0,
            successful_operations: 0,
            failed_operations: 0,
            current_failures: 0,
            current_successes: 0,
            times_opened: 0,
            last_state_change: None,
            last_operation: None,
            avg_operation_duration_ms: 0.0,
        }
    }
}

/// Circuit breaker for protecting against persistent failures
pub struct CircuitBreaker<T> {
    /// Circuit breaker state
    state: Arc<RwLock<CircuitBreakerState<T>>>,
    /// Configuration
    config: CircuitBreakerConfig,
}

/// Internal circuit breaker state
struct CircuitBreakerState<T> {
    /// Current circuit state
    circuit_state: CircuitState,
    /// Consecutive failures
    consecutive_failures: u32,
    /// Consecutive successes
    consecutive_successes: u32,
    /// Statistics
    stats: CircuitBreakerStats,
    /// Last state change
    last_state_change: Instant,
    /// Recovery attempt start time
    recovery_start: Option<Instant>,
    /// Operations in current half-open window
    half_open_operations: u32,
    /// Successful operations in current half-open window
    half_open_successes: u32,
    /// Underlying service
    service: Option<T>,
}

impl<T> CircuitBreaker<T> {
    /// Create a new circuit breaker
    pub fn new(service: T, config: CircuitBreakerConfig) -> Self {
        let state = CircuitBreakerState {
            circuit_state: CircuitState::Closed,
            consecutive_failures: 0,
            consecutive_successes: 0,
            stats: CircuitBreakerStats::default(),
            last_state_change: Instant::now(),
            recovery_start: None,
            half_open_operations: 0,
            half_open_successes: 0,
            service: Some(service),
        };

        Self {
            state: Arc::new(RwLock::new(state)),
            config,
        }
    }

    /// Execute an operation through the circuit breaker
    pub async fn execute<F, R, E>(&self, operation: F) -> Result<R, MirrorError>
    where
        F: FnOnce(&T) -> Result<R, E>,
        E: Into<MirrorError>,
    {
        if !self.config.enabled {
            // Circuit breaker disabled, execute directly
            let state_guard = self.state.read().await;
            let service = state_guard.service.as_ref()
                .ok_or_else(|| MirrorError::config_error("Service not available"))?;
            return operation(service).map_err(Into::into);
        }

        let start_time = Instant::now();

        // Check if operation should be allowed
        if !self.should_allow_operation().await? {
            return Err(MirrorError::StorageError(
                StorageError::BackendError {
                    backend: "CircuitBreaker".to_string(),
                    error: "Circuit breaker is open".to_string(),
                }
            ));
        }

        // Execute the operation
        let result = {
            let state_guard = self.state.read().await;
            let service = state_guard.service.as_ref()
                .ok_or_else(|| MirrorError::config_error("Service not available"))?;
            operation(service).map_err(Into::into)
        };

        let duration = start_time.elapsed();

        // Update circuit breaker state based on result
        match result {
            Ok(result) => {
                self.record_success(duration).await;
                Ok(result)
            }
            Err(error) => {
                self.record_failure(&error).await;
                Err(error)
            }
        }
    }

    /// Check if operation should be allowed
    async fn should_allow_operation(&self) -> MirrorResult<bool> {
        let mut state_guard = self.state.write().await;
        let now = Instant::now();

        match state_guard.circuit_state {
            CircuitState::Closed => {
                Ok(true)
            }
            CircuitState::Open => {
                // Check if recovery timeout has passed
                let time_since_open = now.duration_since(state_guard.last_state_change);
                let recovery_timeout = Duration::from_secs(self.config.recovery_timeout_secs);

                if time_since_open >= recovery_timeout {
                    // Transition to half-open
                    info!("Circuit breaker transitioning to half-open state");
                    state_guard.circuit_state = CircuitState::HalfOpen;
                    state_guard.recovery_start = Some(now);
                    state_guard.half_open_operations = 0;
                    state_guard.half_open_successes = 0;
                    state_guard.update_stats_change(CircuitState::HalfOpen, now);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            CircuitState::HalfOpen => {
                // Allow operations based on success rate and minimum operations
                if state_guard.half_open_operations < self.config.min_operations {
                    Ok(true)
                } else {
                    let success_rate = state_guard.half_open_successes as f64
                        / state_guard.half_open_operations as f64;
                    Ok(success_rate >= self.config.half_open_success_rate)
                }
            }
        }
    }

    /// Record a successful operation
    async fn record_success(&self, duration: Duration) {
        let mut state_guard = self.state.write().await;
        let now = Instant::now();

        state_guard.consecutive_failures = 0;
        state_guard.consecutive_successes += 1;
        state_guard.stats.successful_operations += 1;
        state_guard.stats.current_successes = state_guard.consecutive_successes;
        state_guard.stats.last_operation = Some(Utc::now());

        // Update average operation duration
        self.update_avg_duration(&mut state_guard.stats, duration);

        match state_guard.circuit_state {
            CircuitState::Closed => {
                // In closed state, just reset failure count
                state_guard.stats.current_failures = 0;
            }
            CircuitState::HalfOpen => {
                state_guard.half_open_operations += 1;
                state_guard.half_open_successes += 1;

                // Check if we should close the circuit
                if state_guard.consecutive_successes >= self.config.recovery_threshold {
                    info!("Circuit breaker closing after {} consecutive successes",
                          state_guard.consecutive_successes);
                    state_guard.circuit_state = CircuitState::Closed;
                    state_guard.recovery_start = None;
                    state_guard.update_stats_change(CircuitState::Closed, now);
                }
            }
            CircuitState::Open => {
                // Shouldn't happen, but handle gracefully
                warn!("Success recorded while circuit is open");
            }
        }

        state_guard.stats.total_operations += 1;
    }

    /// Record a failed operation
    async fn record_failure(&self, error: &MirrorError) {
        let mut state_guard = self.state.write().await;
        let now = Instant::now();

        state_guard.consecutive_failures += 1;
        state_guard.consecutive_successes = 0;
        state_guard.stats.failed_operations += 1;
        state_guard.stats.current_failures = state_guard.consecutive_failures;
        state_guard.stats.current_successes = 0;
        state_guard.stats.last_operation = Some(Utc::now());

        match state_guard.circuit_state {
            CircuitState::Closed => {
                // Check if we should open the circuit
                if state_guard.consecutive_failures >= self.config.failure_threshold {
                    warn!("Circuit breaker opening after {} consecutive failures: {}",
                          state_guard.consecutive_failures, error);
                    state_guard.circuit_state = CircuitState::Open;
                    state_guard.stats.times_opened += 1;
                    state_guard.update_stats_change(CircuitState::Open, now);
                }
            }
            CircuitState::HalfOpen => {
                state_guard.half_open_operations += 1;

                // Any failure in half-open should open the circuit again
                warn!("Circuit breaker opening again after failure in half-open state: {}", error);
                state_guard.circuit_state = CircuitState::Open;
                state_guard.stats.times_opened += 1;
                state_guard.recovery_start = None;
                state_guard.update_stats_change(CircuitState::Open, now);
            }
            CircuitState::Open => {
                // Already open, just update stats
            }
        }

        state_guard.stats.total_operations += 1;
    }

    /// Update average operation duration
    fn update_avg_duration(&self, stats: &mut CircuitBreakerStats, duration: Duration) {
        let duration_ms = duration.as_millis() as f64;
        let total_ops = stats.successful_operations as f64;

        if total_ops == 1.0 {
            stats.avg_operation_duration_ms = duration_ms;
        } else {
            let total_time = stats.avg_operation_duration_ms * (total_ops - 1.0);
            stats.avg_operation_duration_ms = total_time / total_ops + duration_ms / total_ops;
        }
    }

    /// Get current circuit breaker statistics
    pub async fn get_stats(&self) -> CircuitBreakerStats {
        let state_guard = self.state.read().await;
        state_guard.stats.clone()
    }

    /// Get current circuit state
    pub async fn get_state(&self) -> CircuitState {
        let state_guard = self.state.read().await;
        state_guard.circuit_state.clone()
    }

    /// Manually open the circuit
    pub async fn force_open(&self) {
        let mut state_guard = self.state.write().await;
        let now = Instant::now();

        if state_guard.circuit_state != CircuitState::Open {
            info!("Manually opening circuit breaker");
            state_guard.circuit_state = CircuitState::Open;
            state_guard.stats.times_opened += 1;
            state_guard.update_stats_change(CircuitState::Open, now);
        }
    }

    /// Manually close the circuit
    pub async fn force_close(&self) {
        let mut state_guard = self.state.write().await;
        let now = Instant::now();

        if state_guard.circuit_state != CircuitState::Closed {
            info!("Manually closing circuit breaker");
            state_guard.circuit_state = CircuitState::Closed;
            state_guard.consecutive_failures = 0;
            state_guard.consecutive_successes = 0;
            state_guard.recovery_start = None;
            state_guard.update_stats_change(CircuitState::Closed, now);
        }
    }

    /// Reset circuit breaker to initial state
    pub async fn reset(&self) {
        let mut state_guard = self.state.write().await;
        let now = Instant::now();

        info!("Resetting circuit breaker");
        state_guard.circuit_state = CircuitState::Closed;
        state_guard.consecutive_failures = 0;
        state_guard.consecutive_successes = 0;
        state_guard.stats = CircuitBreakerStats::default();
        state_guard.last_state_change = now;
        state_guard.recovery_start = None;
        state_guard.half_open_operations = 0;
        state_guard.half_open_successes = 0;
    }

    /// Check if the circuit is currently open
    pub async fn is_open(&self) -> bool {
        let state_guard = self.state.read().await;
        state_guard.circuit_state == CircuitState::Open
    }

    /// Get the underlying service (for direct access when needed)
    pub async fn get_service(&self) -> Option<T>
    where
        T: Clone,
    {
        let state_guard = self.state.read().await;
        state_guard.service.clone()
    }
}

impl<T: Clone> Clone for CircuitBreaker<T> {
    fn clone(&self) -> Self {
        // Note: This creates a new circuit breaker with the same service
        // but separate state. For sharing state, use Arc<CircuitBreaker>.
        let state = CircuitBreakerState {
            circuit_state: CircuitState::Closed,
            consecutive_failures: 0,
            consecutive_successes: 0,
            stats: CircuitBreakerStats::default(),
            last_state_change: Instant::now(),
            recovery_start: None,
            half_open_operations: 0,
            half_open_successes: 0,
            service: None, // Will need to be set separately
        };

        Self {
            state: Arc::new(RwLock::new(state)),
            config: self.config.clone(),
        }
    }
}

/// Extension trait for CircuitBreakerState
impl<T> CircuitBreakerState<T> {
    /// Update statistics when state changes
    fn update_stats_change(&mut self, new_state: CircuitState, now: Instant) {
        self.last_state_change = now;
        self.stats.state = new_state.clone();
        self.stats.last_state_change = Some(Utc::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StorageError;

    #[derive(Clone)]
    struct TestService {
        should_fail: bool,
    }

    impl TestService {
        fn new() -> Self {
            Self { should_fail: false }
        }

        fn failing() -> Self {
            Self { should_fail: true }
        }

        async fn operation(&self) -> Result<String, StorageError> {
            if self.should_fail {
                Err(StorageError::Generic("Test failure".to_string()))
            } else {
                Ok("success".to_string())
            }
        }
    }

    #[test]
    fn test_circuit_breaker_config_default() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.recovery_threshold, 3);
        assert_eq!(config.recovery_timeout_secs, 60);
        assert!(config.enabled);
    }

    #[test]
    fn test_circuit_breaker_stats_default() {
        let stats = CircuitBreakerStats::default();
        assert_eq!(stats.state, CircuitState::Closed);
        assert_eq!(stats.total_operations, 0);
        assert_eq!(stats.successful_operations, 0);
        assert_eq!(stats.failed_operations, 0);
    }

    #[tokio::test]
    async fn test_circuit_breaker_creation() {
        let service = TestService::new();
        let config = CircuitBreakerConfig::default();
        let breaker = CircuitBreaker::new(service, config);

        assert_eq!(breaker.get_state().await, CircuitState::Closed);
        let stats = breaker.get_stats().await;
        assert_eq!(stats.total_operations, 0);
    }

    #[tokio::test]
    async fn test_successful_operation() {
        let service = TestService::new();
        let config = CircuitBreakerConfig::default();
        let breaker = CircuitBreaker::new(service, config);

        let result = breaker.execute(|s| {
            // Simulate successful operation
            Ok("test_result")
        }).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test_result");

        let stats = breaker.get_stats().await;
        assert_eq!(stats.successful_operations, 1);
        assert_eq!(stats.failed_operations, 0);
        assert_eq!(stats.state, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_disabled() {
        let service = TestService::new();
        let mut config = CircuitBreakerConfig::default();
        config.enabled = false;
        let breaker = CircuitBreaker::new(service, config);

        // Should work even if service would fail
        let result = breaker.execute(|_| {
            Err(StorageError::Generic("Should fail"))
        }).await;

        // Should not be caught by circuit breaker
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_force_operations() {
        let service = TestService::new();
        let config = CircuitBreakerConfig::default();
        let breaker = CircuitBreaker::new(service, config);

        assert_eq!(breaker.get_state().await, CircuitState::Closed);
        assert!(!breaker.is_open().await);

        breaker.force_open().await;
        assert_eq!(breaker.get_state().await, CircuitState::Open);
        assert!(breaker.is_open().await);

        breaker.force_close().await;
        assert_eq!(breaker.get_state().await, CircuitState::Closed);
        assert!(!breaker.is_open().await);

        breaker.reset().await;
        assert_eq!(breaker.get_state().await, CircuitState::Closed);

        let stats = breaker.get_stats().await;
        assert_eq!(stats.total_operations, 0);
    }
}