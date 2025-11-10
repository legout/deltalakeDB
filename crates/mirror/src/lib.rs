//! deltalakedb-mirror
//!
//! Deterministic serializer for `_delta_log` artifacts (JSON + Parquet checkpoints).
//!
//! This crate provides the bridge between SQL-based Delta metadata storage and
//! canonical Delta Lake transaction log files, ensuring compatibility with
//! the broader Delta Lake ecosystem.

#![warn(missing_docs)]

use async_trait::async_trait;
use deltalakedb_core::{Table, Commit, Protocol, Metadata, Action};
use deltalakedb_sql::traits::TxnLogReader;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;
use chrono::{DateTime, Utc};

pub mod engine;
pub mod generators;
pub mod storage;
pub mod pipeline;
pub mod monitoring;
pub mod config;
pub mod error;

pub use engine::DeltaMirrorEngine;
pub use generators::{DeltaJsonGenerator, DeltaFormat};
pub use storage::{MirrorStorage, MirrorConfig, StorageBackend};
pub use pipeline::{MirrorPipeline, MirrorTask, MirrorStatus};
pub use monitoring::{MirrorMonitor, MirrorMetrics, MirrorHealth};
pub use config::MirrorEngineConfig;
pub use error::{MirrorError, MirrorResult};

/// Main entry point for Delta Lake log mirroring operations
pub struct DeltaMirror {
    engine: Arc<DeltaMirrorEngine>,
    storage: Arc<dyn MirrorStorage>,
    pipeline: Arc<MirrorPipeline>,
    monitor: Arc<MirrorMonitor>,
}

impl DeltaMirror {
    /// Create a new DeltaMirror instance with the given configuration
    pub async fn new(config: MirrorEngineConfig) -> MirrorResult<Self> {
        // Initialize storage backend
        let storage = storage::create_storage(&config.storage)?;

        // Create core engine
        let engine = Arc::new(DeltaMirrorEngine::new(
            config.clone(),
            storage.clone(),
        ).await?);

        // Create processing pipeline
        let pipeline = Arc::new(MirrorPipeline::new(
            config.clone(),
            engine.clone(),
            storage.clone(),
        ).await?);

        // Create monitoring system
        let monitor = Arc::new(MirrorMonitor::new(
            config.monitoring,
            engine.clone(),
            pipeline.clone(),
        ).await?);

        Ok(Self {
            engine,
            storage,
            pipeline,
            monitor,
        })
    }

    /// Mirror a single commit from SQL to Delta format
    pub async fn mirror_commit(
        &self,
        table_id: Uuid,
        version: i64,
    ) -> MirrorResult<MirrorStatus> {
        let task = MirrorTask::CommitMirroring {
            table_id,
            version,
        };

        self.pipeline.submit_task(task).await
    }

    /// Mirror all commits from a table
    pub async fn mirror_table(
        &self,
        table_id: Uuid,
    ) -> MirrorResult<MirrorStatus> {
        let task = MirrorTask::TableMirroring {
            table_id,
        };

        self.pipeline.submit_task(task).await
    }

    /// Force generation of checkpoint for a table
    pub async fn checkpoint_table(
        &self,
        table_id: Uuid,
        version: Option<i64>,
    ) -> MirrorResult<MirrorStatus> {
        let task = MirrorTask::CheckpointGeneration {
            table_id,
            version,
        };

        self.pipeline.submit_task(task).await
    }

    /// Get current mirroring status for a table
    pub async fn get_table_status(
        &self,
        table_id: Uuid,
    ) -> MirrorResult<MirrorStatus> {
        self.monitor.get_table_status(table_id).await
    }

    /// Get mirroring metrics
    pub async fn get_metrics(&self) -> MirrorResult<MirrorMetrics> {
        self.monitor.get_metrics().await
    }

    /// Get health status of the mirroring system
    pub async fn health_check(&self) -> MirrorResult<MirrorHealth> {
        self.monitor.health_check().await
    }

    /// Start the mirroring system
    pub async fn start(&self) -> MirrorResult<()> {
        self.pipeline.start().await?;
        self.monitor.start().await?;
        tracing::info!("DeltaMirror system started successfully");
        Ok(())
    }

    /// Stop the mirroring system gracefully
    pub async fn stop(&self) -> MirrorResult<()> {
        tracing::info!("Stopping DeltaMirror system...");

        // Stop accepting new tasks
        self.pipeline.shutdown().await?;

        // Wait for current tasks to complete
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

        // Stop monitoring
        self.monitor.stop().await?;

        tracing::info!("DeltaMirror system stopped successfully");
        Ok(())
    }
}

/// Configuration for Delta mirroring operations
#[derive(Debug, Clone)]
pub struct MirrorConfig {
    /// Base path for Delta log files
    pub base_path: PathBuf,
    /// Object storage configuration
    pub storage: StorageConfig,
    /// Mirroring behavior settings
    pub mirroring: MirroringConfig,
    /// Performance tuning parameters
    pub performance: PerformanceConfig,
}

/// Object storage configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Storage backend type
    pub backend: StorageBackend,
    /// S3-specific configuration
    pub s3_config: Option<S3Config>,
    /// Azure-specific configuration
    pub azure_config: Option<AzureConfig>,
    /// GCS-specific configuration
    pub gcs_config: Option<GcsConfig>,
    /// Local filesystem configuration
    pub local_config: Option<LocalConfig>,
}

/// S3 storage configuration
#[derive(Debug, Clone)]
pub struct S3Config {
    /// AWS region
    pub region: String,
    /// S3 bucket name
    pub bucket: String,
    /// AWS access key ID
    pub access_key_id: Option<String>,
    /// AWS secret access key
    pub secret_access_key: Option<String>,
    /// Session token for temporary credentials
    pub session_token: Option<String>,
    /// Custom endpoint URL
    pub endpoint: Option<String>,
    /// Force path style addressing
    pub force_path_style: bool,
}

/// Azure Blob storage configuration
#[derive(Debug, Clone)]
pub struct AzureConfig {
    /// Storage account name
    pub account_name: String,
    /// Storage account key
    pub account_key: Option<String>,
    /// Connection string
    pub connection_string: Option<String>,
    /// Container name
    pub container: String,
    /// Custom endpoint
    pub endpoint: Option<String>,
}

/// Google Cloud Storage configuration
#[derive(Debug, Clone)]
pub struct GcsConfig {
    /// Service account key JSON
    pub service_account_key: Option<String>,
    /// Bucket name
    pub bucket: String,
    /// Custom endpoint
    pub endpoint: Option<String>,
}

/// Local filesystem configuration
#[derive(Debug, Clone)]
pub struct LocalConfig {
    /// Base directory for storing files
    pub base_dir: PathBuf,
    /// Create directories if they don't exist
    pub create_dirs: bool,
}

/// Mirroring behavior configuration
#[derive(Debug, Clone)]
pub struct MirroringConfig {
    /// Whether mirroring is synchronous or asynchronous
    pub async_mode: bool,
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial retry delay in milliseconds
    pub retry_delay_ms: u64,
    /// Maximum retry delay in milliseconds
    pub max_retry_delay_ms: u64,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// Whether to enable mirroring for all tables by default
    pub auto_mirroring: bool,
    /// Tables to exclude from mirroring
    pub excluded_tables: Vec<String>,
}

/// Performance tuning configuration
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    /// Maximum number of concurrent mirroring tasks
    pub max_concurrent_tasks: usize,
    /// Size of the task queue
    pub task_queue_size: usize,
    /// Timeout for individual operations in seconds
    pub operation_timeout_secs: u64,
    /// Batch size for bulk operations
    pub batch_size: usize,
    /// Whether to enable compression for output files
    pub compression_enabled: bool,
    /// Checkpoint generation interval (every N versions)
    pub checkpoint_interval: Option<u64>,
    /// Minimum number of actions required for checkpoint generation
    pub min_actions_for_checkpoint: Option<usize>,
    /// Maximum checkpoint file size in bytes
    pub max_checkpoint_size_bytes: Option<usize>,
    /// Row group size for Parquet files
    pub parquet_row_group_size: Option<usize>,
    /// Parquet compression algorithm
    pub parquet_compression: Option<String>,
}

/// Storage backend type
#[derive(Debug, Clone, PartialEq)]
pub enum StorageBackend {
    /// Amazon S3
    S3,
    /// Azure Blob Storage
    Azure,
    /// Google Cloud Storage
    Gcs,
    /// Local filesystem
    Local,
}

/// Mirroring status information
#[derive(Debug, Clone)]
pub struct MirrorStatus {
    /// Unique identifier for this mirroring operation
    pub task_id: Uuid,
    /// Table identifier
    pub table_id: Uuid,
    /// Current status
    pub status: TaskStatus,
    /// Progress percentage (0.0 to 1.0)
    pub progress: f64,
    /// Start time of the operation
    pub started_at: DateTime<Utc>,
    /// Estimated completion time
    pub estimated_completion: Option<DateTime<Utc>>,
    /// Error message if failed
    pub error_message: Option<String>,
    /// Number of files processed
    pub files_processed: usize,
    /// Total number of files to process
    pub total_files: usize,
    /// Bytes written
    pub bytes_written: u64,
}

/// Task execution status
#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    /// Task is queued
    Queued,
    /// Task is running
    Running,
    /// Task completed successfully
    Completed,
    /// Task failed
    Failed,
    /// Task was cancelled
    Cancelled,
}

impl Default for MirrorConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("_delta_log"),
            storage: StorageConfig::default(),
            mirroring: MirroringConfig::default(),
            performance: PerformanceConfig::default(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackend::Local,
            s3_config: None,
            azure_config: None,
            gcs_config: None,
            local_config: Some(LocalConfig {
                base_dir: PathBuf::from("/tmp/deltalakedb"),
                create_dirs: true,
            }),
        }
    }
}

impl Default for MirroringConfig {
    fn default() -> Self {
        Self {
            async_mode: true,
            max_retries: 3,
            retry_delay_ms: 1000,
            max_retry_delay_ms: 30000,
            backoff_multiplier: 2.0,
            auto_mirroring: false,
            excluded_tables: Vec::new(),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 10,
            task_queue_size: 1000,
            operation_timeout_secs: 300,
            batch_size: 100,
            compression_enabled: true,
            checkpoint_interval: Some(10),
            min_actions_for_checkpoint: Some(5),
            max_checkpoint_size_bytes: Some(1024 * 1024 * 100), // 100MB
            parquet_row_group_size: Some(1024),
            parquet_compression: Some("snappy".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mirror_config_defaults() {
        let config = MirrorConfig::default();
        assert_eq!(config.base_path, PathBuf::from("_delta_log"));
        assert_eq!(config.storage.backend, StorageBackend::Local);
        assert!(config.mirroring.async_mode);
        assert_eq!(config.performance.max_concurrent_tasks, 10);
    }

    #[test]
    fn test_storage_config() {
        let local_config = StorageConfig {
            backend: StorageBackend::Local,
            s3_config: None,
            azure_config: None,
            gcs_config: None,
            local_config: Some(LocalConfig {
                base_dir: PathBuf::from("/tmp"),
                create_dirs: true,
            }),
        };

        assert_eq!(local_config.backend, StorageBackend::Local);
        assert!(local_config.local_config.is_some());
    }

    #[test]
    fn test_mirror_status() {
        let status = MirrorStatus {
            task_id: Uuid::new_v4(),
            table_id: Uuid::new_v4(),
            status: TaskStatus::Running,
            progress: 0.5,
            started_at: Utc::now(),
            estimated_completion: None,
            error_message: None,
            files_processed: 5,
            total_files: 10,
            bytes_written: 1024,
        };

        assert_eq!(status.status, TaskStatus::Running);
        assert_eq!(status.progress, 0.5);
        assert_eq!(status.files_processed, 5);
        assert_eq!(status.total_files, 10);
    }
}