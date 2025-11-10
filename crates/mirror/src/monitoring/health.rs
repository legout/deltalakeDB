//! Health checking system for Delta Lake log mirroring

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use super::HealthStatus;

/// Comprehensive health status for Delta Lake log mirroring system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorHealth {
    /// Overall system health status
    pub status: HealthStatus,
    /// Last health check timestamp
    pub last_check: DateTime<Utc>,
    /// Health check duration in milliseconds
    pub check_duration_ms: u64,
    /// Component-specific health status
    pub components: HashMap<String, ComponentHealth>,
    /// System-wide health indicators
    pub indicators: HealthIndicators,
    /// Health summary message
    pub summary: String,
    /// Recommendations for improving health
    pub recommendations: Vec<String>,
}

/// Health status for individual components
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name
    pub name: String,
    /// Component type
    pub component_type: ComponentType,
    /// Health status
    pub status: HealthStatus,
    /// Last check timestamp
    pub last_check: DateTime<Utc>,
    /// Response time in milliseconds
    pub response_time_ms: u64,
    /// Error message if unhealthy
    pub error_message: Option<String>,
    /// Component-specific metrics
    pub metrics: HashMap<String, f64>,
    /// Component dependencies
    pub dependencies: Vec<String>,
    /// Health check method used
    pub check_method: String,
}

/// Component types in the mirroring system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComponentType {
    /// Storage backend
    Storage,
    /// Processing pipeline
    Pipeline,
    /// JSON generator
    Generator,
    /// SQL adapter connection
    SqlAdapter,
    /// Metrics collector
    Metrics,
    /// Alert system
    Alerts,
    /// System resources
    System,
    /// Network connectivity
    Network,
}

/// System health indicators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthIndicators {
    /// Memory usage percentage (0.0 to 1.0)
    pub memory_usage: f64,
    /// CPU usage percentage (0.0 to 1.0)
    pub cpu_usage: f64,
    /// Disk usage percentage (0.0 to 1.0)
    pub disk_usage: f64,
    /// Network latency in milliseconds
    pub network_latency_ms: f64,
    /// Queue utilization percentage (0.0 to 1.0)
    pub queue_utilization: f64,
    /// Error rate percentage (0.0 to 1.0)
    pub error_rate: f64,
    /// Task success rate percentage (0.0 to 1.0)
    pub success_rate: f64,
    /// System uptime in seconds
    pub uptime_seconds: u64,
}

/// Health check configuration
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Health check interval in seconds
    pub interval_seconds: u64,
    /// Timeout for health checks in milliseconds
    pub timeout_ms: u64,
    /// Number of consecutive failures before marking unhealthy
    pub failure_threshold: u32,
    /// Number of consecutive successes before marking healthy
    pub recovery_threshold: u32,
    /// Component-specific configurations
    pub component_configs: HashMap<ComponentType, ComponentHealthConfig>,
}

/// Component-specific health check configuration
#[derive(Debug, Clone)]
pub struct ComponentHealthConfig {
    /// Whether this component is critical for overall health
    pub critical: bool,
    /// Expected response time threshold in milliseconds
    pub response_time_threshold_ms: u64,
    /// Custom health check parameters
    pub custom_params: HashMap<String, f64>,
}

/// Health check result from a component
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    /// Component name
    pub component: String,
    /// Health status
    pub status: HealthStatus,
    /// Response time in milliseconds
    pub response_time_ms: u64,
    /// Error message if applicable
    pub error_message: Option<String>,
    /// Component metrics
    pub metrics: HashMap<String, f64>,
    /// Additional health details
    pub details: HashMap<String, String>,
}

impl MirrorHealth {
    /// Create a new health status
    pub fn new() -> Self {
        Self {
            status: HealthStatus::Unknown,
            last_check: Utc::now(),
            check_duration_ms: 0,
            components: HashMap::new(),
            indicators: HealthIndicators::default(),
            summary: "Health check not performed".to_string(),
            recommendations: Vec::new(),
        }
    }

    /// Create healthy status
    pub fn healthy() -> Self {
        let mut health = Self::new();
        health.status = HealthStatus::Healthy;
        health.summary = "All systems operational".to_string();
        health
    }

    /// Create unhealthy status with message
    pub fn unhealthy(message: &str) -> Self {
        let mut health = Self::new();
        health.status = HealthStatus::Unhealthy;
        health.summary = message.to_string();
        health.recommendations.push("Investigate component failures".to_string());
        health
    }

    /// Create degraded status
    pub fn degraded(message: &str) -> Self {
        let mut health = Self::new();
        health.status = HealthStatus::Degraded;
        health.summary = message.to_string();
        health.recommendations.push("Monitor system performance".to_string());
        health
    }

    /// Add component health status
    pub fn add_component(&mut self, component: ComponentHealth) {
        self.components.insert(component.name.clone(), component);
        self.update_overall_status();
    }

    /// Update overall system status based on components
    pub fn update_overall_status(&mut self) {
        let critical_components: Vec<_> = self.components
            .values()
            .filter(|c| matches!(c.component_type, ComponentType::Storage | ComponentType::Pipeline | ComponentType::SqlAdapter))
            .collect();

        let unhealthy_critical = critical_components.iter()
            .any(|c| c.status == HealthStatus::Unhealthy);

        let degraded_critical = critical_components.iter()
            .any(|c| c.status == HealthStatus::Degraded);

        self.status = if unhealthy_critical {
            HealthStatus::Unhealthy
        } else if degraded_critical {
            HealthStatus::Degraded
        } else if self.components.values().any(|c| c.status == HealthStatus::Unhealthy) {
            HealthStatus::Degraded
        } else if self.components.values().any(|c| c.status == HealthStatus::Degraded) {
            HealthStatus::Degraded
        } else if self.components.values().all(|c| c.status == HealthStatus::Healthy) {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        };

        self.update_summary();
    }

    /// Update health summary based on current status
    pub fn update_summary(&mut self) {
        self.summary = match self.status {
            HealthStatus::Healthy => "All systems operational".to_string(),
            HealthStatus::Degraded => {
                let degraded_count = self.components.values()
                    .filter(|c| c.status == HealthStatus::Degraded)
                    .count();
                format!("{} components showing degraded performance", degraded_count)
            }
            HealthStatus::Unhealthy => {
                let unhealthy_count = self.components.values()
                    .filter(|c| c.status == HealthStatus::Unhealthy)
                    .count();
                format!("{} components are unhealthy", unhealthy_count)
            }
            HealthStatus::Unknown => "Health status unknown".to_string(),
        };

        self.update_recommendations();
    }

    /// Update recommendations based on current health status
    pub fn update_recommendations(&mut self) {
        self.recommendations.clear();

        // Memory recommendations
        if self.indicators.memory_usage > 0.8 {
            self.recommendations.push("High memory usage detected - consider scaling up".to_string());
        }

        // CPU recommendations
        if self.indicators.cpu_usage > 0.8 {
            self.recommendations.push("High CPU usage detected - check for bottlenecks".to_string());
        }

        // Queue recommendations
        if self.indicators.queue_utilization > 0.9 {
            self.recommendations.push("High queue utilization - consider increasing worker count".to_string());
        }

        // Error rate recommendations
        if self.indicators.error_rate > 0.1 {
            self.recommendations.push("High error rate detected - investigate component health".to_string());
        }

        // Component-specific recommendations
        for component in self.components.values() {
            match component.status {
                HealthStatus::Unhealthy => {
                    self.recommendations.push(format!("{} component requires attention", component.name));
                }
                HealthStatus::Degraded => {
                    self.recommendations.push(format!("{} component showing degraded performance", component.name));
                }
                _ => {}
            }
        }
    }

    /// Get health score (0.0 to 1.0)
    pub fn health_score(&self) -> f64 {
        let component_scores: Vec<f64> = self.components.values()
            .map(|c| match c.status {
                HealthStatus::Healthy => 1.0,
                HealthStatus::Degraded => 0.6,
                HealthStatus::Unhealthy => 0.2,
                HealthStatus::Unknown => 0.5,
            })
            .collect();

        if component_scores.is_empty() {
            return 0.5;
        }

        let avg_component_score = component_scores.iter().sum::<f64>() / component_scores.len() as f64;

        // Factor in system indicators
        let indicator_score = (
            (1.0 - self.indicators.memory_usage) +
            (1.0 - self.indicators.cpu_usage) +
            (1.0 - self.indicators.error_rate) +
            self.indicators.success_rate
        ) / 4.0;

        (avg_component_score + indicator_score) / 2.0
    }

    /// Check if system is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self.status, HealthStatus::Healthy) && self.health_score() >= 0.8
    }

    /// Get component by name
    pub fn get_component(&self, name: &str) -> Option<&ComponentHealth> {
        self.components.get(name)
    }

    /// Get unhealthy components
    pub fn unhealthy_components(&self) -> Vec<&ComponentHealth> {
        self.components.values()
            .filter(|c| c.status == HealthStatus::Unhealthy)
            .collect()
    }

    /// Get degraded components
    pub fn degraded_components(&self) -> Vec<&ComponentHealth> {
        self.components.values()
            .filter(|c| c.status == HealthStatus::Degraded)
            .collect()
    }

    /// Merge with another health status (keeping most recent information)
    pub fn merge(&mut self, other: MirrorHealth) {
        if other.last_check > self.last_check {
            self.status = other.status;
            self.last_check = other.last_check;
            self.check_duration_ms = other.check_duration_ms;
            self.components = other.components;
            self.indicators = other.indicators;
            self.summary = other.summary;
            self.recommendations = other.recommendations;
        }
    }
}

impl Default for MirrorHealth {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for HealthIndicators {
    fn default() -> Self {
        Self {
            memory_usage: 0.0,
            cpu_usage: 0.0,
            disk_usage: 0.0,
            network_latency_ms: 0.0,
            queue_utilization: 0.0,
            error_rate: 0.0,
            success_rate: 1.0,
            uptime_seconds: 0,
        }
    }
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 60,
            timeout_ms: 5000,
            failure_threshold: 3,
            recovery_threshold: 2,
            component_configs: {
                let mut configs = HashMap::new();
                configs.insert(ComponentType::Storage, ComponentHealthConfig {
                    critical: true,
                    response_time_threshold_ms: 1000,
                    custom_params: HashMap::new(),
                });
                configs.insert(ComponentType::Pipeline, ComponentHealthConfig {
                    critical: true,
                    response_time_threshold_ms: 500,
                    custom_params: HashMap::new(),
                });
                configs.insert(ComponentType::SqlAdapter, ComponentHealthConfig {
                    critical: true,
                    response_time_threshold_ms: 2000,
                    custom_params: HashMap::new(),
                });
                configs
            },
        }
    }
}

impl ComponentHealth {
    /// Create new component health
    pub fn new(name: &str, component_type: ComponentType) -> Self {
        Self {
            name: name.to_string(),
            component_type,
            status: HealthStatus::Unknown,
            last_check: Utc::now(),
            response_time_ms: 0,
            error_message: None,
            metrics: HashMap::new(),
            dependencies: Vec::new(),
            check_method: "unknown".to_string(),
        }
    }

    /// Mark as healthy
    pub fn healthy(&mut self, response_time_ms: u64) {
        self.status = HealthStatus::Healthy;
        self.response_time_ms = response_time_ms;
        self.last_check = Utc::now();
        self.error_message = None;
    }

    /// Mark as degraded
    pub fn degraded(&mut self, response_time_ms: u64, message: &str) {
        self.status = HealthStatus::Degraded;
        self.response_time_ms = response_time_ms;
        self.last_check = Utc::now();
        self.error_message = Some(message.to_string());
    }

    /// Mark as unhealthy
    pub fn unhealthy(&mut self, response_time_ms: u64, message: &str) {
        self.status = HealthStatus::Unhealthy;
        self.response_time_ms = response_time_ms;
        self.last_check = Utc::now();
        self.error_message = Some(message.to_string());
    }

    /// Check if component is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self.status, HealthStatus::Healthy)
    }

    /// Add metric
    pub fn add_metric(&mut self, key: &str, value: f64) {
        self.metrics.insert(key.to_string(), value);
    }

    /// Get metric
    pub fn get_metric(&self, key: &str) -> Option<f64> {
        self.metrics.get(key).copied()
    }
}

impl HealthCheckResult {
    /// Create successful result
    pub fn success(component: &str, response_time_ms: u64) -> Self {
        Self {
            component: component.to_string(),
            status: HealthStatus::Healthy,
            response_time_ms,
            error_message: None,
            metrics: HashMap::new(),
            details: HashMap::new(),
        }
    }

    /// Create failed result
    pub fn failure(component: &str, response_time_ms: u64, error: &str) -> Self {
        Self {
            component: component.to_string(),
            status: HealthStatus::Unhealthy,
            response_time_ms,
            error_message: Some(error.to_string()),
            metrics: HashMap::new(),
            details: HashMap::new(),
        }
    }

    /// Create degraded result
    pub fn degraded(component: &str, response_time_ms: u64, message: &str) -> Self {
        Self {
            component: component.to_string(),
            status: HealthStatus::Degraded,
            response_time_ms,
            error_message: Some(message.to_string()),
            metrics: HashMap::new(),
            details: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_creation() {
        let health = MirrorHealth::new();
        assert_eq!(health.status, HealthStatus::Unknown);
        assert!(health.components.is_empty());
        assert_eq!(health.summary, "Health check not performed");
    }

    #[test]
    fn test_healthy_status() {
        let health = MirrorHealth::healthy();
        assert_eq!(health.status, HealthStatus::Healthy);
        assert_eq!(health.summary, "All systems operational");
        assert!(health.recommendations.is_empty());
    }

    #[test]
    fn test_unhealthy_status() {
        let health = MirrorHealth::unhealthy("Storage backend unavailable");
        assert_eq!(health.status, HealthStatus::Unhealthy);
        assert!(health.summary.contains("Storage backend unavailable"));
        assert!(!health.recommendations.is_empty());
    }

    #[test]
    fn test_component_health() {
        let mut component = ComponentHealth::new("test-storage", ComponentType::Storage);
        assert_eq!(component.name, "test-storage");
        assert_eq!(component.status, HealthStatus::Unknown);

        component.healthy(100);
        assert_eq!(component.status, HealthStatus::Healthy);
        assert_eq!(component.response_time_ms, 100);
        assert!(component.error_message.is_none());
    }

    #[test]
    fn test_overall_status_calculation() {
        let mut health = MirrorHealth::new();

        // Add healthy components
        health.add_component(ComponentHealth::new("storage", ComponentType::Storage));
        health.add_component(ComponentHealth::new("pipeline", ComponentType::Pipeline));

        // Should be unknown (components not checked yet)
        assert_eq!(health.status, HealthStatus::Unknown);

        // Mark components as healthy
        for component in health.components.values_mut() {
            component.healthy(100);
        }
        health.update_overall_status();
        assert_eq!(health.status, HealthStatus::Healthy);

        // Mark one component as degraded
        if let Some(storage) = health.components.get_mut("storage") {
            storage.degraded(500, "Slow response");
        }
        health.update_overall_status();
        assert_eq!(health.status, HealthStatus::Degraded);

        // Mark critical component as unhealthy
        if let Some(storage) = health.components.get_mut("storage") {
            storage.unhealthy(1000, "Connection failed");
        }
        health.update_overall_status();
        assert_eq!(health.status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_score() {
        let mut health = MirrorHealth::healthy();
        assert_eq!(health.health_score(), 1.0);

        health.indicators.memory_usage = 0.5;
        health.indicators.cpu_usage = 0.3;
        health.indicators.error_rate = 0.1;
        health.indicators.success_rate = 0.9;

        let score = health.health_score();
        assert!(score >= 0.7 && score <= 1.0);
    }

    #[test]
    fn test_component_health_lifecycle() {
        let mut component = ComponentHealth::new("test", ComponentType::Generator);

        // Initially unknown
        assert!(!component.is_healthy());
        assert_eq!(component.status, HealthStatus::Unknown);

        // Mark as healthy
        component.healthy(50);
        assert!(component.is_healthy());
        assert_eq!(component.status, HealthStatus::Healthy);

        // Mark as degraded
        component.degraded(200, "Slow processing");
        assert!(!component.is_healthy());
        assert_eq!(component.status, HealthStatus::Degraded);

        // Mark as unhealthy
        component.unhealthy(1000, "Processing failed");
        assert!(!component.is_healthy());
        assert_eq!(component.status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_check_result() {
        let success = HealthCheckResult::success("storage", 100);
        assert_eq!(success.status, HealthStatus::Healthy);
        assert_eq!(success.response_time_ms, 100);
        assert!(success.error_message.is_none());

        let failure = HealthCheckResult::failure("storage", 1000, "Connection timeout");
        assert_eq!(failure.status, HealthStatus::Unhealthy);
        assert_eq!(failure.response_time_ms, 1000);
        assert!(failure.error_message.is_some());
        assert!(failure.error_message.unwrap().contains("Connection timeout"));
    }
}