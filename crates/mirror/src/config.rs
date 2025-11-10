//! Configuration management for Delta Lake log mirroring

use crate::{
    error::{MirrorError, MirrorResult},
    recovery::{RecoveryConfig, ReconcilerConfig, CircuitBreakerConfig},
    MirrorConfig, StorageConfig, MirroringConfig, PerformanceConfig,
    S3Config, AzureConfig, GcsConfig, LocalConfig, StorageBackend,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the Delta mirror engine
#[derive(Debug, Clone)]
pub struct MirrorEngineConfig {
    /// Storage configuration
    pub storage: StorageConfig,
    /// Mirroring behavior configuration
    pub mirroring: MirroringConfig,
    /// Performance configuration
    pub performance: PerformanceConfig,
    /// Monitoring configuration
    pub monitoring: MonitoringConfig,
    /// Recovery configuration
    pub recovery: RecoveryConfig,
    /// JSON generation configuration
    pub json_generation: JsonGenerationConfig,
    /// Parquet generation configuration
    pub parquet_generation: ParquetGenerationConfig,
}

/// Monitoring configuration
#[derive(Debug, Clone)]
pub struct MonitoringConfig {
    /// Whether monitoring is enabled
    pub enabled: bool,
    /// Metrics collection interval in seconds
    pub metrics_interval_secs: u64,
    /// Health check interval in seconds
    pub health_check_interval_secs: u64,
    /// Lag monitoring threshold in seconds
    pub lag_threshold_secs: u64,
    /// Alert configuration
    pub alerts: AlertConfig,
    /// Metrics export configuration
    pub metrics_export: MetricsExportConfig,
}

/// Alert configuration
#[derive(Debug, Clone)]
pub struct AlertConfig {
    /// Whether alerts are enabled
    pub enabled: bool,
    /// Alert channels
    pub channels: Vec<AlertChannel>,
    /// Alert rules
    pub rules: Vec<AlertRule>,
}

/// Alert channel configuration
#[derive(Debug, Clone)]
pub enum AlertChannel {
    /// Log alerts
    Log,
    /// Prometheus metrics
    Prometheus,
    /// Webhook URL
    Webhook(String),
    /// Email configuration
    Email {
        /// SMTP server
        smtp_server: String,
        /// Port
        port: u16,
        /// Username
        username: String,
        /// Password
        password: String,
        /// From address
        from: String,
        /// To addresses
        to: Vec<String>,
    },
}

/// Alert rule configuration
#[derive(Debug, Clone)]
pub struct AlertRule {
    /// Rule name
    pub name: String,
    /// Alert condition
    pub condition: AlertCondition,
    /// Severity level
    pub severity: AlertSeverity,
    /// Whether the rule is enabled
    pub enabled: bool,
}

/// Alert condition
#[derive(Debug, Clone)]
pub enum AlertCondition {
    /// Lag exceeds threshold
    LagExceedsThreshold {
        /// Threshold in seconds
        threshold: u64,
    },
    /// Error rate exceeds threshold
    ErrorRateExceedsThreshold {
        /// Error rate threshold (0.0 to 1.0)
        threshold: f64,
        /// Time window in seconds
        window_secs: u64,
    },
    /// Queue depth exceeds threshold
    QueueDepthExceedsThreshold {
        /// Queue depth threshold
        threshold: usize,
    },
    /// Storage health check failure
    StorageHealthFailure,
    /// Custom condition
    Custom {
        /// Condition expression
        expression: String,
        /// Evaluation interval in seconds
        interval_secs: u64,
    },
}

/// Alert severity levels
#[derive(Debug, Clone, PartialEq)]
pub enum AlertSeverity {
    /// Informational
    Info,
    /// Warning
    Warning,
    /// Error
    Error,
    /// Critical
    Critical,
}

/// Metrics export configuration
#[derive(Debug, Clone)]
pub struct MetricsExportConfig {
    /// Whether metrics export is enabled
    pub enabled: bool,
    /// Export format
    pub format: MetricsFormat,
    /// Export interval in seconds
    pub interval_secs: u64,
    /// Export target configuration
    pub target: MetricsTarget,
}

/// Metrics export format
#[derive(Debug, Clone)]
pub enum MetricsFormat {
    /// Prometheus format
    Prometheus,
    /// JSON format
    Json,
    /// InfluxDB line protocol
    InfluxLine,
}

/// Metrics export target
#[derive(Debug, Clone)]
pub enum MetricsTarget {
    /// Export to file
    File {
        /// File path
        path: PathBuf,
    },
    /// Export to HTTP endpoint
    Http {
        /// URL
        url: String,
        /// HTTP headers
        headers: Vec<(String, String)>,
    },
    /// Export to stdout
    Stdout,
}

/// JSON generation configuration
#[derive(Debug, Clone)]
pub struct JsonGenerationConfig {
    /// Whether JSON compression is enabled
    pub compression_enabled: bool,
    /// Compression algorithm
    pub compression_algorithm: CompressionAlgorithm,
    /// JSON formatting options
    pub formatting: JsonFormatting,
    /// Validation settings
    pub validation: JsonValidation,
}

/// JSON formatting options
#[derive(Debug, Clone)]
pub struct JsonFormatting {
    /// Whether to pretty print JSON
    pub pretty_print: bool,
    /// Indentation size for pretty printing
    pub indent_size: usize,
    /// Whether to sort object keys
    pub sort_keys: bool,
}

/// JSON validation settings
#[derive(Debug, Clone)]
pub struct JsonValidation {
    /// Whether to validate generated JSON
    pub enabled: bool,
    /// Strict validation mode
    pub strict: bool,
    /// Custom validation rules
    pub custom_rules: Vec<JsonValidationRule>,
}

/// JSON validation rule
#[derive(Debug, Clone)]
pub struct JsonValidationRule {
    /// Rule name
    pub name: String,
    /// JSON path expression
    pub path: String,
    /// Expected value or condition
    pub condition: JsonValidationCondition,
}

/// JSON validation condition
#[derive(Debug, Clone)]
pub enum JsonValidationCondition {
    /// Field exists
    Exists,
    /// Field has specific value
    Equals(String),
    /// Field matches regex
    Matches(String),
    /// Field is one of allowed values
    InValues(Vec<String>),
}

/// Parquet generation configuration
#[derive(Debug, Clone)]
pub struct ParquetGenerationConfig {
    /// Whether Parquet compression is enabled
    pub compression_enabled: bool,
    /// Compression algorithm
    pub compression_algorithm: ParquetCompressionAlgorithm,
    /// Parquet writer settings
    pub writer_settings: ParquetWriterSettings,
    /// Checkpoint generation settings
    pub checkpoint: CheckpointConfig,
}

/// Parquet compression algorithms
#[derive(Debug, Clone)]
pub enum ParquetCompressionAlgorithm {
    /// No compression
    None,
    /// Snappy compression
    Snappy,
    /// Gzip compression
    Gzip,
    /// LZO compression
    Lzo,
    /// Brotli compression
    Brotli,
    /// ZSTD compression
    Zstd { level: i32 },
}

/// Parquet writer settings
#[derive(Debug, Clone)]
pub struct ParquetWriterSettings {
    /// Row group size
    pub row_group_size: usize,
    /// Write batch size
    pub write_batch_size: usize,
    /// Data page size
    pub data_page_size: usize,
    /// Dictionary page size
    pub dictionary_page_size: usize,
    /// Whether to enable statistics
    pub enable_statistics: bool,
}

/// Checkpoint configuration
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Whether checkpoint generation is enabled
    pub enabled: bool,
    /// Checkpoint interval (number of commits)
    pub interval: usize,
    /// Maximum size of checkpoint files in bytes
    pub max_size_bytes: u64,
    /// Whether to generate compact checkpoints
    pub compact_checkpoints: bool,
    /// Checkpoint retention policy
    pub retention: CheckpointRetention,
}

/// Checkpoint retention policy
#[derive(Debug, Clone)]
pub struct CheckpointRetention {
    /// Maximum number of checkpoints to retain
    pub max_count: usize,
    /// Maximum age of checkpoints in seconds
    pub max_age_secs: u64,
}

/// Compression algorithms
#[derive(Debug, Clone)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// Gzip compression
    Gzip { level: u8 },
    /// Brotli compression
    Brotli { level: u8 },
    /// LZ4 compression
    Lz4,
}

/// Environment variable configuration loader
pub struct EnvironmentConfig;

impl EnvironmentConfig {
    /// Load configuration from environment variables
    pub fn load() -> MirrorResult<MirrorEngineConfig> {
        let storage = Self::load_storage_config()?;
        let mirroring = Self::load_mirroring_config()?;
        let performance = Self::load_performance_config()?;
        let monitoring = Self::load_monitoring_config()?;
        let recovery = Self::load_recovery_config()?;
        let json_generation = Self::load_json_generation_config()?;
        let parquet_generation = Self::load_parquet_generation_config()?;

        Ok(MirrorEngineConfig {
            storage,
            mirroring,
            performance,
            monitoring,
            recovery,
            json_generation,
            parquet_generation,
        })
    }

    /// Load storage configuration from environment
    fn load_storage_config() -> MirrorResult<StorageConfig> {
        let backend_str = std::env::var("DELTA_MIRROR_STORAGE_BACKEND")
            .unwrap_or_else(|_| "local".to_string());

        let backend = match backend_str.as_str() {
            "s3" => StorageBackend::S3,
            "azure" => StorageBackend::Azure,
            "gcs" => StorageBackend::Gcs,
            "local" => StorageBackend::Local,
            _ => return Err(MirrorError::config_error(
                format!("Invalid storage backend: {}", backend_str)
            )),
        };

        let s3_config = if backend == StorageBackend::S3 {
            Some(S3Config {
                region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
                bucket: std::env::var("AWS_S3_BUCKET")
                    .map_err(|_| MirrorError::config_error("AWS_S3_BUCKET is required for S3 backend"))?,
                access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
                secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
                session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
                endpoint: std::env::var("AWS_ENDPOINT").ok(),
                force_path_style: std::env::var("AWS_FORCE_PATH_STYLE")
                    .unwrap_or_else(|_| "false".to_string())
                    .parse()
                    .unwrap_or(false),
            })
        } else {
            None
        };

        let azure_config = if backend == StorageBackend::Azure {
            Some(AzureConfig {
                account_name: std::env::var("AZURE_STORAGE_ACCOUNT")
                    .map_err(|_| MirrorError::config_error("AZURE_STORAGE_ACCOUNT is required for Azure backend"))?,
                account_key: std::env::var("AZURE_STORAGE_KEY").ok(),
                connection_string: std::env::var("AZURE_STORAGE_CONNECTION_STRING").ok(),
                container: std::env::var("AZURE_STORAGE_CONTAINER")
                    .map_err(|_| MirrorError::config_error("AZURE_STORAGE_CONTAINER is required for Azure backend"))?,
                endpoint: std::env::var("AZURE_STORAGE_ENDPOINT").ok(),
            })
        } else {
            None
        };

        let gcs_config = if backend == StorageBackend::Gcs {
            Some(GcsConfig {
                service_account_key: std::env::var("GCP_SERVICE_ACCOUNT_KEY").ok(),
                bucket: std::env::var("GCP_STORAGE_BUCKET")
                    .map_err(|_| MirrorError::config_error("GCP_STORAGE_BUCKET is required for GCS backend"))?,
                endpoint: std::env::var("GCP_STORAGE_ENDPOINT").ok(),
            })
        } else {
            None
        };

        let local_config = if backend == StorageBackend::Local {
            Some(LocalConfig {
                base_dir: std::env::var("DELTA_MIRROR_LOCAL_DIR")
                    .unwrap_or_else(|_| "/tmp/deltalakedb".to_string())
                    .into(),
                create_dirs: std::env::var("DELTA_MIRROR_LOCAL_CREATE_DIRS")
                    .unwrap_or_else(|_| "true".to_string())
                    .parse()
                    .unwrap_or(true),
            })
        } else {
            None
        };

        Ok(StorageConfig {
            backend,
            s3_config,
            azure_config,
            gcs_config,
            local_config,
        })
    }

    /// Load mirroring configuration from environment
    fn load_mirroring_config() -> MirrorResult<MirroringConfig> {
        Ok(MirroringConfig {
            async_mode: std::env::var("DELTA_MIRROR_ASYNC_MODE")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            max_retries: std::env::var("DELTA_MIRROR_MAX_RETRIES")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),
            retry_delay_ms: std::env::var("DELTA_MIRROR_RETRY_DELAY_MS")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .unwrap_or(1000),
            max_retry_delay_ms: std::env::var("DELTA_MIRROR_MAX_RETRY_DELAY_MS")
                .unwrap_or_else(|_| "30000".to_string())
                .parse()
                .unwrap_or(30000),
            backoff_multiplier: std::env::var("DELTA_MIRROR_BACKOFF_MULTIPLIER")
                .unwrap_or_else(|_| "2.0".to_string())
                .parse()
                .unwrap_or(2.0),
            auto_mirroring: std::env::var("DELTA_MIRROR_AUTO_MIRRORING")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            excluded_tables: std::env::var("DELTA_MIRROR_EXCLUDED_TABLES")
                .unwrap_or_else(|_| "".to_string())
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.trim().to_string())
                .collect(),
        })
    }

    /// Load performance configuration from environment
    fn load_performance_config() -> MirrorResult<PerformanceConfig> {
        Ok(PerformanceConfig {
            max_concurrent_tasks: std::env::var("DELTA_MIRROR_MAX_CONCURRENT_TASKS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
            task_queue_size: std::env::var("DELTA_MIRROR_TASK_QUEUE_SIZE")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .unwrap_or(1000),
            operation_timeout_secs: std::env::var("DELTA_MIRROR_OPERATION_TIMEOUT_SECS")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
            batch_size: std::env::var("DELTA_MIRROR_BATCH_SIZE")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100),
            compression_enabled: std::env::var("DELTA_MIRROR_COMPRESSION_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
        })
    }

    /// Load monitoring configuration from environment
    fn load_monitoring_config() -> MirrorResult<MonitoringConfig> {
        let monitoring_enabled = std::env::var("DELTA_MIRROR_MONITORING_ENABLED")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);

        Ok(MonitoringConfig {
            enabled: monitoring_enabled,
            metrics_interval_secs: std::env::var("DELTA_MIRROR_METRICS_INTERVAL_SECS")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .unwrap_or(60),
            health_check_interval_secs: std::env::var("DELTA_MIRROR_HEALTH_CHECK_INTERVAL_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
            lag_threshold_secs: std::env::var("DELTA_MIRROR_LAG_THRESHOLD_SECS")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
            alerts: AlertConfig {
                enabled: std::env::var("DELTA_MIRROR_ALERTS_ENABLED")
                    .unwrap_or_else(|_| "false".to_string())
                    .parse()
                    .unwrap_or(false),
                channels: vec![AlertChannel::Log],
                rules: vec![],
            },
            metrics_export: MetricsExportConfig {
                enabled: std::env::var("DELTA_MIRROR_METRICS_EXPORT_ENABLED")
                    .unwrap_or_else(|_| "false".to_string())
                    .parse()
                    .unwrap_or(false),
                format: MetricsFormat::Json,
                interval_secs: std::env::var("DELTA_MIRROR_METRICS_EXPORT_INTERVAL_SECS")
                    .unwrap_or_else(|_| "60".to_string())
                    .parse()
                    .unwrap_or(60),
                target: MetricsTarget::Stdout,
            },
        })
    }

    /// Load JSON generation configuration from environment
    fn load_json_generation_config() -> MirrorResult<JsonGenerationConfig> {
        Ok(JsonGenerationConfig {
            compression_enabled: std::env::var("DELTA_MIRROR_JSON_COMPRESSION_ENABLED")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            compression_algorithm: CompressionAlgorithm::None,
            formatting: JsonFormatting {
                pretty_print: std::env::var("DELTA_MIRROR_JSON_PRETTY_PRINT")
                    .unwrap_or_else(|_| "false".to_string())
                    .parse()
                    .unwrap_or(false),
                indent_size: std::env::var("DELTA_MIRROR_JSON_INDENT_SIZE")
                    .unwrap_or_else(|_| "2".to_string())
                    .parse()
                    .unwrap_or(2),
                sort_keys: std::env::var("DELTA_MIRROR_JSON_SORT_KEYS")
                    .unwrap_or_else(|_| "true".to_string())
                    .parse()
                    .unwrap_or(true),
            },
            validation: JsonValidation {
                enabled: std::env::var("DELTA_MIRROR_JSON_VALIDATION_ENABLED")
                    .unwrap_or_else(|_| "true".to_string())
                    .parse()
                    .unwrap_or(true),
                strict: std::env::var("DELTA_MIRROR_JSON_VALIDATION_STRICT")
                    .unwrap_or_else(|_| "true".to_string())
                    .parse()
                    .unwrap_or(true),
                custom_rules: vec![],
            },
        })
    }

    /// Load Parquet generation configuration from environment
    fn load_parquet_generation_config() -> MirrorResult<ParquetGenerationConfig> {
        Ok(ParquetGenerationConfig {
            compression_enabled: std::env::var("DELTA_MIRROR_PARQUET_COMPRESSION_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            compression_algorithm: ParquetCompressionAlgorithm::Snappy,
            writer_settings: ParquetWriterSettings {
                row_group_size: std::env::var("DELTA_MIRROR_PARQUET_ROW_GROUP_SIZE")
                    .unwrap_or_else(|_| "10000".to_string())
                    .parse()
                    .unwrap_or(10000),
                write_batch_size: std::env::var("DELTA_MIRROR_PARQUET_WRITE_BATCH_SIZE")
                    .unwrap_or_else(|_| "1000".to_string())
                    .parse()
                    .unwrap_or(1000),
                data_page_size: std::env::var("DELTA_MIRROR_PARQUET_DATA_PAGE_SIZE")
                    .unwrap_or_else(|_| "1024".to_string())
                    .parse()
                    .unwrap_or(1024),
                dictionary_page_size: std::env::var("DELTA_MIRROR_PARQUET_DICT_PAGE_SIZE")
                    .unwrap_or_else(|_| "512".to_string())
                    .parse()
                    .unwrap_or(512),
                enable_statistics: std::env::var("DELTA_MIRROR_PARQUET_ENABLE_STATS")
                    .unwrap_or_else(|_| "true".to_string())
                    .parse()
                    .unwrap_or(true),
            },
            checkpoint: CheckpointConfig {
                enabled: std::env::var("DELTA_MIRROR_CHECKPOINT_ENABLED")
                    .unwrap_or_else(|_| "true".to_string())
                    .parse()
                    .unwrap_or(true),
                interval: std::env::var("DELTA_MIRROR_CHECKPOINT_INTERVAL")
                    .unwrap_or_else(|_| "10".to_string())
                    .parse()
                    .unwrap_or(10),
                max_size_bytes: std::env::var("DELTA_MIRROR_CHECKPOINT_MAX_SIZE_BYTES")
                    .unwrap_or_else(|_| "1073741824".to_string())
                    .parse()
                    .unwrap_or(1073741824), // 1GB
                compact_checkpoints: std::env::var("DELTA_MIRROR_CHECKPOINT_COMPACT")
                    .unwrap_or_else(|_| "true".to_string())
                    .parse()
                    .unwrap_or(true),
                retention: CheckpointRetention {
                    max_count: std::env::var("DELTA_MIRROR_CHECKPOINT_RETENTION_COUNT")
                        .unwrap_or_else(|_| "5".to_string())
                        .parse()
                        .unwrap_or(5),
                    max_age_secs: std::env::var("DELTA_MIRROR_CHECKPOINT_RETENTION_AGE_SECS")
                        .unwrap_or_else(|_| "86400".to_string())
                        .parse()
                        .unwrap_or(86400), // 24 hours
                },
            },
        })
    }
}

/// Convert configuration to Duration
pub fn secs_to_duration(secs: u64) -> Duration {
    Duration::from_secs(secs)
}

/// Validate configuration
pub fn validate_config(config: &MirrorEngineConfig) -> MirrorResult<()> {
    // Validate storage configuration
    match config.storage.backend {
        StorageBackend::S3 => {
            if config.storage.s3_config.is_none() {
                return Err(MirrorError::config_error("S3 configuration is required for S3 backend"));
            }
        }
        StorageBackend::Azure => {
            if config.storage.azure_config.is_none() {
                return Err(MirrorError::config_error("Azure configuration is required for Azure backend"));
            }
        }
        StorageBackend::Gcs => {
            if config.storage.gcs_config.is_none() {
                return Err(MirrorError::config_error("GCS configuration is required for GCS backend"));
            }
        }
        StorageBackend::Local => {
            if config.storage.local_config.is_none() {
                return Err(MirrorError::config_error("Local configuration is required for Local backend"));
            }
        }
    }

    // Validate performance settings
    if config.performance.max_concurrent_tasks == 0 {
        return Err(MirrorError::config_error("max_concurrent_tasks must be greater than 0"));
    }

    if config.performance.task_queue_size == 0 {
        return Err(MirrorError::config_error("task_queue_size must be greater than 0"));
    }

    if config.performance.operation_timeout_secs == 0 {
        return Err(MirrorError::config_error("operation_timeout_secs must be greater than 0"));
    }

    // Validate mirroring settings
    if config.mirroring.max_retries == 0 {
        return Err(MirrorError::config_error("max_retries must be greater than 0"));
    }

    if config.mirroring.retry_delay_ms == 0 {
        return Err(MirrorError::config_error("retry_delay_ms must be greater than 0"));
    }

    if config.mirroring.backoff_multiplier <= 1.0 {
        return Err(MirrorError::config_error("backoff_multiplier must be greater than 1.0"));
    }

    Ok(())
}

impl Default for MirrorEngineConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            mirroring: MirroringConfig::default(),
            performance: PerformanceConfig::default(),
            monitoring: MonitoringConfig::default(),
            recovery: RecoveryConfig::default(),
            json_generation: JsonGenerationConfig::default(),
            parquet_generation: ParquetGenerationConfig::default(),
        }
    }
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            metrics_interval_secs: 60,
            health_check_interval_secs: 30,
            lag_threshold_secs: 300,
            alerts: AlertConfig::default(),
            metrics_export: MetricsExportConfig::default(),
        }
    }
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            channels: vec![AlertChannel::Log],
            rules: vec![],
        }
    }
}

impl Default for MetricsExportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            format: MetricsFormat::Json,
            interval_secs: 60,
            target: MetricsTarget::Stdout,
        }
    }
}

impl Default for JsonGenerationConfig {
    fn default() -> Self {
        Self {
            compression_enabled: false,
            compression_algorithm: CompressionAlgorithm::None,
            formatting: JsonFormatting {
                pretty_print: false,
                indent_size: 2,
                sort_keys: true,
            },
            validation: JsonValidation {
                enabled: true,
                strict: true,
                custom_rules: vec![],
            },
        }
    }
}

impl Default for ParquetGenerationConfig {
    fn default() -> Self {
        Self {
            compression_enabled: true,
            compression_algorithm: ParquetCompressionAlgorithm::Snappy,
            writer_settings: ParquetWriterSettings {
                row_group_size: 10000,
                write_batch_size: 1000,
                data_page_size: 1024,
                dictionary_page_size: 512,
                enable_statistics: true,
            },
            checkpoint: CheckpointConfig {
                enabled: true,
                interval: 10,
                max_size_bytes: 1073741824, // 1GB
                compact_checkpoints: true,
                retention: CheckpointRetention {
                    max_count: 5,
                    max_age_secs: 86400, // 24 hours
                },
            },
        }
    }

    /// Load recovery configuration from environment
    fn load_recovery_config() -> MirrorResult<RecoveryConfig> {
        let reconciler_config = ReconcilerConfig {
            check_interval_secs: std::env::var("DELTA_MIRROR_RECONCILER_CHECK_INTERVAL_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
            retry_policy: crate::recovery::RetryPolicy {
                max_retries: std::env::var("DELTA_MIRROR_MAX_RETRIES")
                    .unwrap_or_else(|_| "5".to_string())
                    .parse()
                    .unwrap_or(5),
                initial_backoff_ms: std::env::var("DELTA_MIRROR_INITIAL_BACKOFF_MS")
                    .unwrap_or_else(|_| "1000".to_string())
                    .parse()
                    .unwrap_or(1000),
                max_backoff_ms: std::env::var("DELTA_MIRROR_MAX_BACKOFF_MS")
                    .unwrap_or_else(|_| "300000".to_string())
                    .parse()
                    .unwrap_or(300000),
                backoff_multiplier: std::env::var("DELTA_MIRROR_BACKOFF_MULTIPLIER")
                    .unwrap_or_else(|_| "2.0".to_string())
                    .parse()
                    .unwrap_or(2.0),
                jitter_factor: std::env::var("DELTA_MIRROR_JITTER_FACTOR")
                    .unwrap_or_else(|_| "0.1".to_string())
                    .parse()
                    .unwrap_or(0.1),
                exponential_backoff: std::env::var("DELTA_MIRROR_EXPONENTIAL_BACKOFF")
                    .unwrap_or_else(|_| "true".to_string())
                    .parse()
                    .unwrap_or(true),
            },
            max_concurrent_recoveries: std::env::var("DELTA_MIRROR_MAX_CONCURRENT_RECOVERIES")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),
            batch_size: std::env::var("DELTA_MIRROR_RECOVERY_BATCH_SIZE")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
            enabled: std::env::var("DELTA_MIRROR_RECOVERY_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            max_recovery_age_hours: std::env::var("DELTA_MIRROR_MAX_RECOVERY_AGE_HOURS")
                .unwrap_or_else(|_| "24".to_string())
                .parse()
                .unwrap_or(24),
        };

        let circuit_breaker_config = CircuitBreakerConfig {
            failure_threshold: std::env::var("DELTA_MIRROR_CIRCUIT_BREAKER_FAILURE_THRESHOLD")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            recovery_threshold: std::env::var("DELTA_MIRROR_CIRCUIT_BREAKER_RECOVERY_THRESHOLD")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),
            recovery_timeout_secs: std::env::var("DELTA_MIRROR_CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .unwrap_or(60),
            half_open_success_rate: std::env::var("DELTA_MIRROR_CIRCUIT_BREAKER_HALF_OPEN_SUCCESS_RATE")
                .unwrap_or_else(|_| "0.6".to_string())
                .parse()
                .unwrap_or(0.6),
            min_operations: std::env::var("DELTA_MIRROR_CIRCUIT_BREAKER_MIN_OPERATIONS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            enabled: std::env::var("DELTA_MIRROR_CIRCUIT_BREAKER_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
        };

        Ok(RecoveryConfig {
            reconciler: reconciler_config,
            circuit_breaker: circuit_breaker_config,
            enabled: std::env::var("DELTA_MIRROR_RECOVERY_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            max_concurrent_recoveries: std::env::var("DELTA_MIRROR_MAX_CONCURRENT_RECOVERIES")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_default_config() {
        let config = MirrorEngineConfig::default();
        assert_eq!(config.storage.backend, StorageBackend::Local);
        assert!(config.monitoring.enabled);
        assert!(config.json_generation.validation.enabled);
        assert!(config.parquet_generation.checkpoint.enabled);
    }

    #[test]
    fn test_config_validation() {
        let mut config = MirrorEngineConfig::default();

        // Valid config should pass
        assert!(validate_config(&config).is_ok());

        // Invalid concurrent tasks should fail
        config.performance.max_concurrent_tasks = 0;
        assert!(validate_config(&config).is_err());

        // Reset and test S3 config validation
        config = MirrorEngineConfig::default();
        config.storage.backend = StorageBackend::S3;
        assert!(validate_config(&config).is_err()); // S3 config is missing
    }

    #[test]
    fn test_environment_config() {
        // Test that environment loading doesn't panic
        // Note: This test doesn't validate specific values since environment
        // variables vary in test environments
        let result = EnvironmentConfig::load();
        // Should succeed with default values
        assert!(result.is_ok());
    }

    #[test]
    fn test_duration_conversion() {
        let duration = secs_to_duration(60);
        assert_eq!(duration.as_secs(), 60);
    }
}