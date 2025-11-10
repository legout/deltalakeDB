//! Alert management system for Delta Lake log mirroring

use crate::error::{MirrorError, MirrorResult};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

/// Alert severity levels
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Informational alert
    Info = 0,
    /// Warning level alert
    Warning = 1,
    /// Error level alert
    Error = 2,
    /// Critical level alert
    Critical = 3,
}

/// Alert types for different monitoring scenarios
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertType {
    /// Health check failure
    HealthCheckFailure,
    /// Performance degradation
    PerformanceDegradation,
    /// Replication lag
    ReplicationLag,
    /// Storage backend issue
    StorageIssue,
    /// Queue overflow
    QueueOverflow,
    /// High error rate
    HighErrorRate,
    /// Component unavailable
    ComponentUnavailable,
    /// Resource exhaustion
    ResourceExhaustion,
    /// Custom alert type
    Custom(String),
}

/// Alert definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorAlert {
    /// Unique alert identifier
    pub id: Uuid,
    /// Alert severity
    pub severity: AlertSeverity,
    /// Alert type
    pub alert_type: AlertType,
    /// Alert title
    pub title: String,
    /// Detailed alert message
    pub message: String,
    /// Alert timestamp
    pub timestamp: DateTime<Utc>,
    /// Component that generated the alert
    pub component: String,
    /// Table ID if table-specific
    pub table_id: Option<Uuid>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
    /// Whether alert is acknowledged
    pub acknowledged: bool,
    /// Alert resolution timestamp
    pub resolved_at: Option<DateTime<Utc>>,
    /// Alert tags
    pub tags: Vec<String>,
}

/// Alert rule definition
#[derive(Debug, Clone)]
pub struct AlertRule {
    /// Rule name
    pub name: String,
    /// Alert type this rule generates
    pub alert_type: AlertType,
    /// Severity for alerts from this rule
    pub severity: AlertSeverity,
    /// Rule expression (simplified)
    pub condition: AlertCondition,
    /// Alert title template
    pub title_template: String,
    /// Alert message template
    pub message_template: String,
    /// Tags to add to alerts
    pub tags: Vec<String>,
    /// Whether rule is enabled
    pub enabled: bool,
    /// Cooldown period between alerts
    pub cooldown_duration: Duration,
    /// Maximum alerts per hour
    pub max_alerts_per_hour: Option<usize>,
}

/// Alert condition types
#[derive(Debug, Clone)]
pub enum AlertCondition {
    /// Simple threshold condition
    Threshold {
        /// Metric name
        metric: String,
        /// Threshold value
        threshold: f64,
        /// Comparison operator
        operator: ComparisonOperator,
    },
    /// Rate condition
    Rate {
        /// Metric name
        metric: String,
        /// Rate threshold
        threshold: f64,
        /// Time window in seconds
        window_seconds: u64,
    },
    /// Health status condition
    HealthStatus {
        /// Component name
        component: String,
        /// Expected health status
        expected_status: String,
    },
    /// Composite condition (AND/OR)
    Composite {
        /// Logical operator
        operator: LogicalOperator,
        /// Sub-conditions
        conditions: Vec<AlertCondition>,
    },
    /// Always triggers
    Always,
    /// Never triggers
    Never,
}

/// Comparison operators for threshold conditions
#[derive(Debug, Clone)]
pub enum ComparisonOperator {
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    Equal,
    NotEqual,
}

/// Logical operators for composite conditions
#[derive(Debug, Clone)]
pub enum LogicalOperator {
    And,
    Or,
}

/// Alert manager for handling alert generation and routing
pub struct AlertManager {
    config: AlertConfig,
    alert_rules: HashMap<String, AlertRule>,
    recent_alerts: VecDeque<MirrorAlert>,
    alert_counts: HashMap<String, (DateTime<Utc>, usize)>, // (last_hour_start, count)
    alert_history: VecDeque<MirrorAlert>,
    max_history_size: usize,
    max_recent_size: usize,
}

/// Alert configuration
#[derive(Debug, Clone)]
pub struct AlertConfig {
    /// Whether alerts are enabled
    pub enabled: bool,
    /// Alert channels for routing
    pub channels: Vec<AlertChannel>,
    /// Cooldown period between similar alerts
    pub cooldown_secs: u64,
    /// Maximum alerts per hour globally
    pub max_alerts_per_hour: usize,
    /// Alert retention period in days
    pub retention_days: u64,
    /// Default severity for unclassified alerts
    pub default_severity: AlertSeverity,
}

/// Alert channel for routing alerts
#[derive(Debug, Clone)]
pub enum AlertChannel {
    /// Log alerts
    Log,
    /// Send to webhook
    Webhook {
        url: String,
        timeout_secs: u64,
        retry_attempts: u32,
    },
    /// Send to email (placeholder)
    Email {
        address: String,
        smtp_server: String,
    },
    /// Send to monitoring system (placeholder)
    Monitoring {
        endpoint: String,
        api_key: Option<String>,
    },
    /// Send to Slack (placeholder)
    Slack {
        webhook_url: String,
        channel: Option<String>,
    },
}

/// Alert statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertStats {
    /// Total alerts generated
    pub total_alerts: u64,
    /// Alerts by severity
    pub alerts_by_severity: HashMap<AlertSeverity, u64>,
    /// Alerts by type
    pub alerts_by_type: HashMap<String, u64>,
    /// Alerts by component
    pub alerts_by_component: HashMap<String, u64>,
    /// Unacknowledged alert count
    pub unacknowledged_count: usize,
    /// Alerts in last hour
    pub last_hour_count: usize,
    /// Last alert timestamp
    pub last_alert_at: Option<DateTime<Utc>>,
}

impl AlertManager {
    /// Create new alert manager
    pub fn new(config: AlertConfig) -> Self {
        Self {
            config,
            alert_rules: HashMap::new(),
            recent_alerts: VecDeque::new(),
            alert_counts: HashMap::new(),
            alert_history: VecDeque::new(),
            max_history_size: 10000,
            max_recent_size: 1000,
        }
    }

    /// Send an alert
    pub async fn send_alert(&mut self, alert: MirrorAlert) -> MirrorResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Check cooldown
        if self.is_on_cooldown(&alert) {
            tracing::debug!("Alert on cooldown: {}", alert.title);
            return Ok(());
        }

        // Check hourly limit
        if self.exceeds_hourly_limit() {
            tracing::warn!("Hourly alert limit exceeded, dropping alert: {}", alert.title);
            return Ok(());
        }

        // Store alert
        self.store_alert(alert.clone());

        // Route to channels
        for channel in &self.config.channels {
            if let Err(e) = self.route_to_channel(&alert, channel).await {
                tracing::error!("Failed to route alert to channel: {}", e);
            }
        }

        tracing::warn!(
            "Alert sent: [{}] {} - {}",
            self.severity_string(&alert.severity),
            alert.title,
            alert.message
        );

        Ok(())
    }

    /// Send alert with simplified parameters
    pub async fn send_simple_alert(
        &mut self,
        severity: AlertSeverity,
        title: &str,
        message: &str,
        component: &str,
    ) -> MirrorResult<()> {
        let alert = MirrorAlert {
            id: Uuid::new_v4(),
            severity,
            alert_type: AlertType::Custom(title.to_string()),
            title: title.to_string(),
            message: message.to_string(),
            timestamp: Utc::now(),
            component: component.to_string(),
            table_id: None,
            metadata: HashMap::new(),
            acknowledged: false,
            resolved_at: None,
            tags: Vec::new(),
        };

        self.send_alert(alert).await
    }

    /// Get recent alerts
    pub fn get_recent_alerts(&self, limit: Option<usize>) -> Vec<MirrorAlert> {
        let limit = limit.unwrap_or(self.max_recent_size);
        self.recent_alerts
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get alert statistics
    pub fn get_alert_stats(&self) -> AlertStats {
        let mut stats = AlertStats {
            total_alerts: self.alert_history.len() as u64,
            alerts_by_severity: HashMap::new(),
            alerts_by_type: HashMap::new(),
            alerts_by_component: HashMap::new(),
            unacknowledged_count: 0,
            last_hour_count: 0,
            last_alert_at: None,
        };

        let one_hour_ago = Utc::now() - chrono::Duration::hours(1);

        for alert in &self.alert_history {
            // Count by severity
            *stats.alerts_by_severity.entry(alert.severity.clone()).or_insert(0) += 1;

            // Count by type
            let type_name = match &alert.alert_type {
                AlertType::Custom(name) => name.clone(),
                _ => format!("{:?}", alert.alert_type),
            };
            *stats.alerts_by_type.entry(type_name).or_insert(0) += 1;

            // Count by component
            *stats.alerts_by_component.entry(alert.component.clone()).or_insert(0) += 1;

            // Count unacknowledged
            if !alert.acknowledged {
                stats.unacknowledged_count += 1;
            }

            // Count recent alerts
            if alert.timestamp > one_hour_ago {
                stats.last_hour_count += 1;
            }

            // Track last alert time
            if stats.last_alert_at.is_none() || alert.timestamp > stats.last_alert_at.unwrap() {
                stats.last_alert_at = Some(alert.timestamp);
            }
        }

        stats
    }

    /// Acknowledge an alert
    pub fn acknowledge_alert(&mut self, alert_id: Uuid) -> MirrorResult<bool> {
        for alert in &mut self.alert_history {
            if alert.id == alert_id {
                alert.acknowledged = true;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Resolve an alert
    pub fn resolve_alert(&mut self, alert_id: Uuid) -> MirrorResult<bool> {
        for alert in &mut self.alert_history {
            if alert.id == alert_id {
                alert.resolved_at = Some(Utc::now());
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Add alert rule
    pub fn add_alert_rule(&mut self, rule: AlertRule) {
        self.alert_rules.insert(rule.name.clone(), rule);
    }

    /// Remove alert rule
    pub fn remove_alert_rule(&mut self, name: &str) -> Option<AlertRule> {
        self.alert_rules.remove(name)
    }

    /// Evaluate alert rules (placeholder for rule engine)
    pub async fn evaluate_rules(&mut self, metrics: &HashMap<String, f64>) -> MirrorResult<Vec<MirrorAlert>> {
        let mut alerts = Vec::new();

        for rule in self.alert_rules.values() {
            if !rule.enabled {
                continue;
            }

            if self.evaluate_rule_condition(&rule.condition, metrics) {
                let alert = MirrorAlert {
                    id: Uuid::new_v4(),
                    severity: rule.severity.clone(),
                    alert_type: rule.alert_type.clone(),
                    title: rule.title_template.clone(),
                    message: rule.message_template.clone(),
                    timestamp: Utc::now(),
                    component: "rule-engine".to_string(),
                    table_id: None,
                    metadata: HashMap::new(),
                    acknowledged: false,
                    resolved_at: None,
                    tags: rule.tags.clone(),
                };

                alerts.push(alert);
            }
        }

        Ok(alerts)
    }

    /// Store alert in history and recent lists
    fn store_alert(&mut self, alert: MirrorAlert) {
        // Add to recent alerts
        self.recent_alerts.push_back(alert.clone());
        if self.recent_alerts.len() > self.max_recent_size {
            self.recent_alerts.pop_front();
        }

        // Add to history
        self.alert_history.push_back(alert);
        if self.alert_history.len() > self.max_history_size {
            self.alert_history.pop_front();
        }

        // Update hourly counts
        let now = Utc::now();
        let hour_key = format!("{}-{}", now.date_naive(), now.hour());

        if let Some((hour_start, count)) = self.alert_counts.get_mut(&hour_key) {
            *count += 1;
        } else {
            self.alert_counts.insert(hour_key, (now, 1));
        }

        // Clean up old hourly counts (older than 25 hours)
        let cutoff = now - chrono::Duration::hours(25);
        self.alert_counts.retain(|_, (hour_start, _)| *hour_start > cutoff);
    }

    /// Check if alert is on cooldown
    fn is_on_cooldown(&self, alert: &MirrorAlert) -> bool {
        let cooldown_duration = Duration::from_secs(self.config.cooldown_secs);

        for recent_alert in &self.recent_alerts {
            if recent_alert.component == alert.component
                && recent_alert.alert_type == alert.alert_type
                && recent_alert.severity == alert.severity {

                let time_since = alert.timestamp.signed_duration_since(recent_alert.timestamp);
                if time_since < chrono::Duration::from_std(cooldown_duration).unwrap() {
                    return true;
                }
            }
        }

        false
    }

    /// Check if hourly limit is exceeded
    fn exceeds_hourly_limit(&self) -> bool {
        let now = Utc::now();
        let hour_key = format!("{}-{}", now.date_naive(), now.hour());

        if let Some((_, count)) = self.alert_counts.get(&hour_key) {
            *count >= self.config.max_alerts_per_hour
        } else {
            false
        }
    }

    /// Route alert to specific channel
    async fn route_to_channel(&self, alert: &MirrorAlert, channel: &AlertChannel) -> MirrorResult<()> {
        match channel {
            AlertChannel::Log => {
                match alert.severity {
                    AlertSeverity::Info => tracing::info!("{}", alert.message),
                    AlertSeverity::Warning => tracing::warn!("{}", alert.message),
                    AlertSeverity::Error => tracing::error!("{}", alert.message),
                    AlertSeverity::Critical => tracing::error!("CRITICAL: {}", alert.message),
                }
            }
            AlertChannel::Webhook { url, timeout_secs, retry_attempts } => {
                self.send_webhook_alert(alert, url, *timeout_secs, *retry_attempts).await?;
            }
            AlertChannel::Email { .. } => {
                // Placeholder for email implementation
                tracing::debug!("Email alert not implemented: {}", alert.title);
            }
            AlertChannel::Monitoring { .. } => {
                // Placeholder for monitoring system integration
                tracing::debug!("Monitoring alert not implemented: {}", alert.title);
            }
            AlertChannel::Slack { .. } => {
                // Placeholder for Slack integration
                tracing::debug!("Slack alert not implemented: {}", alert.title);
            }
        }

        Ok(())
    }

    /// Send alert via webhook
    async fn send_webhook_alert(
        &self,
        alert: &MirrorAlert,
        url: &str,
        timeout_secs: u64,
        retry_attempts: u32,
    ) -> MirrorResult<()> {
        let client = reqwest::Client::new();
        let payload = serde_json::json!({
            "alert_id": alert.id,
            "severity": format!("{:?}", alert.severity),
            "title": alert.title,
            "message": alert.message,
            "component": alert.component,
            "timestamp": alert.timestamp,
            "metadata": alert.metadata,
        });

        let mut attempts = 0;
        while attempts <= retry_attempts {
            match client
                .post(url)
                .json(&payload)
                .timeout(Duration::from_secs(timeout_secs))
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(());
                    } else {
                        tracing::warn!("Webhook returned status: {}", response.status());
                    }
                }
                Err(e) => {
                    tracing::warn!("Webhook request failed: {}", e);
                }
            }

            attempts += 1;
            if attempts <= retry_attempts {
                sleep(Duration::from_secs(2_u64.pow(attempts))).await;
            }
        }

        Err(MirrorError::pipeline_error("Failed to send webhook alert after retries"))
    }

    /// Evaluate a rule condition
    fn evaluate_rule_condition(&self, condition: &AlertCondition, metrics: &HashMap<String, f64>) -> bool {
        match condition {
            AlertCondition::Threshold { metric, threshold, operator } => {
                if let Some(value) = metrics.get(metric) {
                    match operator {
                        ComparisonOperator::GreaterThan => value > threshold,
                        ComparisonOperator::LessThan => value < threshold,
                        ComparisonOperator::GreaterThanOrEqual => value >= threshold,
                        ComparisonOperator::LessThanOrEqual => value <= threshold,
                        ComparisonOperator::Equal => (value - threshold).abs() < f64::EPSILON,
                        ComparisonOperator::NotEqual => (value - threshold).abs() >= f64::EPSILON,
                    }
                } else {
                    false
                }
            }
            AlertCondition::Always => true,
            AlertCondition::Never => false,
            _ => false, // Placeholder for other condition types
        }
    }

    /// Convert severity to string
    fn severity_string(&self, severity: &AlertSeverity) -> &'static str {
        match severity {
            AlertSeverity::Info => "INFO",
            AlertSeverity::Warning => "WARN",
            AlertSeverity::Error => "ERROR",
            AlertSeverity::Critical => "CRIT",
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
            retention_days: 30,
            default_severity: AlertSeverity::Warning,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alert_creation() {
        let alert = MirrorAlert {
            id: Uuid::new_v4(),
            severity: AlertSeverity::Warning,
            alert_type: AlertType::HealthCheckFailure,
            title: "Test Alert".to_string(),
            message: "This is a test alert".to_string(),
            timestamp: Utc::now(),
            component: "test".to_string(),
            table_id: None,
            metadata: HashMap::new(),
            acknowledged: false,
            resolved_at: None,
            tags: Vec::new(),
        };

        assert_eq!(alert.severity, AlertSeverity::Warning);
        assert_eq!(alert.title, "Test Alert");
        assert!(!alert.acknowledged);
    }

    #[test]
    fn test_alert_manager() {
        let config = AlertConfig::default();
        let manager = AlertManager::new(config);

        assert_eq!(manager.get_recent_alerts(Some(10)).len(), 0);

        let stats = manager.get_alert_stats();
        assert_eq!(stats.total_alerts, 0);
        assert_eq!(stats.unacknowledged_count, 0);
    }

    #[test]
    fn test_alert_severity_ordering() {
        assert!(AlertSeverity::Info < AlertSeverity::Warning);
        assert!(AlertSeverity::Warning < AlertSeverity::Error);
        assert!(AlertSeverity::Error < AlertSeverity::Critical);
    }

    #[test]
    fn test_alert_condition() {
        let mut metrics = HashMap::new();
        metrics.insert("cpu_usage".to_string(), 0.85);

        let condition = AlertCondition::Threshold {
            metric: "cpu_usage".to_string(),
            threshold: 0.8,
            operator: ComparisonOperator::GreaterThan,
        };

        let config = AlertConfig::default();
        let manager = AlertManager::new(config);

        assert!(manager.evaluate_rule_condition(&condition, &metrics));

        metrics.insert("cpu_usage".to_string(), 0.75);
        assert!(!manager.evaluate_rule_condition(&condition, &metrics));
    }

    #[tokio::test]
    async fn test_send_simple_alert() {
        let config = AlertConfig::default();
        let mut manager = AlertManager::new(config);

        manager.send_simple_alert(
            AlertSeverity::Error,
            "Test Error",
            "This is a test error",
            "test-component",
        ).await.unwrap();

        let recent = manager.get_recent_alerts(Some(10));
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].severity, AlertSeverity::Error);
        assert_eq!(recent[0].title, "Test Error");
    }

    #[test]
    fn test_alert_stats() {
        let config = AlertConfig::default();
        let mut manager = AlertManager::new(config);

        // Simulate adding some alerts
        let alert1 = MirrorAlert {
            id: Uuid::new_v4(),
            severity: AlertSeverity::Warning,
            alert_type: AlertType::PerformanceDegradation,
            title: "Warning Alert".to_string(),
            message: "Warning message".to_string(),
            timestamp: Utc::now(),
            component: "component1".to_string(),
            table_id: None,
            metadata: HashMap::new(),
            acknowledged: false,
            resolved_at: None,
            tags: Vec::new(),
        };

        manager.store_alert(alert1);

        let stats = manager.get_alert_stats();
        assert_eq!(stats.total_alerts, 1);
        assert_eq!(stats.unacknowledged_count, 1);
        assert_eq!(stats.alerts_by_severity.get(&AlertSeverity::Warning), Some(&1));
    }
}