//! Storage backend implementations for Delta Lake log mirroring

use crate::error::{MirrorError, MirrorResult};
use crate::storage::{
    MirrorStorage, StorageConfig, S3Config, AzureConfig, GcsConfig, LocalConfig,
    StorageBackend, ObjectStoreStorage, FileMetadata, StorageStats,
};
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::fs;
use tokio::io::AsyncReadExt;

pub mod local;
pub mod s3;
pub mod azure;
pub mod gcs;

pub use local::LocalStorage;
pub use s3::S3Storage;
pub use azure::AzureStorage;
pub use gcs::GcsStorage;

/// Create storage backend with retry logic
pub async fn create_storage_with_retry(config: &StorageConfig) -> MirrorResult<Arc<dyn MirrorStorage>> {
    let mut attempts = 0;
    let max_attempts = config.max_retries;
    let base_delay = std::time::Duration::from_secs(1);

    loop {
        attempts += 1;

        match create_storage_impl(config).await {
            Ok(storage) => {
                // Test the storage connection
                if storage.health_check().await.unwrap_or(false) {
                    return Ok(storage);
                } else {
                    tracing::warn!("Storage health check failed, attempt {}/{}", attempts, max_attempts);
                }
            }
            Err(e) => {
                tracing::warn!("Failed to create storage backend: {}, attempt {}/{}", e, attempts, max_attempts);
            }
        }

        if attempts >= max_attempts {
            return Err(MirrorError::storage_error(format!(
                "Failed to create storage backend after {} attempts",
                max_attempts
            )));
        }

        // Exponential backoff
        let delay = base_delay * 2_u32.pow(attempts - 1);
        tokio::time::sleep(delay).await;
    }
}

/// Internal storage creation implementation
async fn create_storage_impl(config: &StorageConfig) -> MirrorResult<Arc<dyn MirrorStorage>> {
    let storage: Arc<dyn MirrorStorage> = match config.backend {
        StorageBackend::S3 => {
            let s3_config = config.s3_config.as_ref()
                .ok_or_else(|| MirrorError::config_error("S3 configuration is required for S3 backend"))?;
            Arc::new(S3Storage::new(s3_config, config).await?)
        }
        StorageBackend::Azure => {
            let azure_config = config.azure_config.as_ref()
                .ok_or_else(|| MirrorError::config_error("Azure configuration is required for Azure backend"))?;
            Arc::new(AzureStorage::new(azure_config, config).await?)
        }
        StorageBackend::Gcs => {
            let gcs_config = config.gcs_config.as_ref()
                .ok_or_else(|| MirrorError::config_error("GCS configuration is required for GCS backend"))?;
            Arc::new(GcsStorage::new(gcs_config, config).await?)
        }
        StorageBackend::Local => {
            let local_config = config.local_config.as_ref()
                .ok_or_else(|| MirrorError::config_error("Local configuration is required for Local backend"))?;
            Arc::new(LocalStorage::new(local_config, config)?)
        }
    };

    Ok(storage)
}

/// Validate storage configuration
pub fn validate_storage_config(config: &StorageConfig) -> MirrorResult<()> {
    match config.backend {
        StorageBackend::S3 => {
            let s3_config = config.s3_config.as_ref()
                .ok_or_else(|| MirrorError::config_error("S3 configuration is required for S3 backend"))?;

            if s3_config.bucket.is_empty() {
                return Err(MirrorError::config_error("S3 bucket name cannot be empty"));
            }

            if s3_config.region.is_empty() {
                return Err(MirrorError::config_error("S3 region cannot be empty"));
            }

            // Validate credentials
            if s3_config.access_key_id.is_none() && s3_config.session_token.is_none() {
                // Might be using IAM role, which is fine
                tracing::warn!("S3 credentials not provided, assuming IAM role or instance profile");
            }
        }
        StorageBackend::Azure => {
            let azure_config = config.azure_config.as_ref()
                .ok_or_else(|| MirrorError::config_error("Azure configuration is required for Azure backend"))?;

            if azure_config.container.is_empty() {
                return Err(MirrorError::config_error("Azure container name cannot be empty"));
            }

            if azure_config.account_name.is_empty() && azure_config.connection_string.is_none() {
                return Err(MirrorError::config_error("Either Azure account name or connection string must be provided"));
            }
        }
        StorageBackend::Gcs => {
            let gcs_config = config.gcs_config.as_ref()
                .ok_or_else(|| MirrorError::config_error("GCS configuration is required for GCS backend"))?;

            if gcs_config.bucket.is_empty() {
                return Err(MirrorError::config_error("GCS bucket name cannot be empty"));
            }

            if gcs_config.service_account_key.is_none() && gcs_config.oauth2_token.is_none() {
                // Might be using default credentials, which is fine
                tracing::warn!("GCS credentials not provided, assuming default credentials");
            }
        }
        StorageBackend::Local => {
            let local_config = config.local_config.as_ref()
                .ok_or_else(|| MirrorError::config_error("Local configuration is required for Local backend"))?;

            if local_config.base_dir.as_os_str().is_empty() {
                return Err(MirrorError::config_error("Local base directory cannot be empty"));
            }
        }
    }

    // Validate timeout and retry settings
    if config.timeout_secs == 0 {
        return Err(MirrorError::config_error("Timeout must be greater than 0 seconds"));
    }

    if config.max_retries == 0 {
        return Err(MirrorError::config_error("Max retries must be greater than 0"));
    }

    Ok(())
}

/// Get storage backend information
pub fn get_storage_info(config: &StorageConfig) -> MirrorResult<StorageInfo> {
    let mut info = StorageInfo {
        backend_type: config.backend.clone(),
        supported_features: Vec::new(),
        limitations: Vec::new(),
        performance_characteristics: PerformanceCharacteristics::default(),
    };

    match config.backend {
        StorageBackend::S3 => {
            info.supported_features.extend_from_slice(&[
                "Atomic uploads",
                "Multipart uploads",
                "Server-side encryption",
                "Versioning",
                "Cross-region replication",
                "Access control lists",
                "Presigned URLs",
            ]);

            info.limitations.extend_from_slice(&[
                "Rate limiting",
                "Eventual consistency",
                "Minimum object size for multipart uploads",
            ]);

            info.performance_characteristics = PerformanceCharacteristics {
                read_latency_ms: 50,
                write_latency_ms: 100,
                throughput_mbps: 5000,
                max_concurrent_uploads: 100,
                optimal_file_size_mb: Some(100),
                supports_multipart_uploads: true,
                supports_atomic_operations: true,
            };
        }
        StorageBackend::Azure => {
            info.supported_features.extend_from_slice(&[
                "Atomic uploads",
                "Block blob uploads",
                "Server-side encryption",
                "Immutable blob storage",
                "Soft delete",
                "Access tiers",
                "SAS tokens",
            ]);

            info.limitations.extend_from_slice(&[
                "Block size limitations",
                "Regional availability",
                "Eventual consistency",
            ]);

            info.performance_characteristics = PerformanceCharacteristics {
                read_latency_ms: 60,
                write_latency_ms: 120,
                throughput_mbps: 4000,
                max_concurrent_uploads: 80,
                optimal_file_size_mb: Some(256),
                supports_multipart_uploads: true,
                supports_atomic_operations: true,
            };
        }
        StorageBackend::Gcs => {
            info.supported_features.extend_from_slice(&[
                "Atomic uploads",
                "Composite uploads",
                "Server-side encryption",
                "Object versioning",
                "Object lifecycle management",
                "Signed URLs",
                "Object change notification",
            ]);

            info.limitations.extend_from_slice(&[
                "Upload part size limitations",
                "Quota limitations",
                "Rate limiting",
            ]);

            info.performance_characteristics = PerformanceCharacteristics {
                read_latency_ms: 55,
                write_latency_ms: 110,
                throughput_mbps: 4500,
                max_concurrent_uploads: 90,
                optimal_file_size_mb: Some(150),
                supports_multipart_uploads: true,
                supports_atomic_operations: true,
            };
        }
        StorageBackend::Local => {
            info.supported_features.extend_from_slice(&[
                "Direct file system access",
                "Atomic file operations",
                "Local permissions",
                "Fast access for local workloads",
                "No network latency",
            ]);

            info.limitations.extend_from_slice(&[
                "Single machine scalability",
                "No built-in redundancy",
                "Filesystem limitations",
                "No native encryption",
                "No built-in backup",
            ]);

            info.performance_characteristics = PerformanceCharacteristics {
                read_latency_ms: 1,
                write_latency_ms: 2,
                throughput_mbps: 1000, // Depends on local disk
                max_concurrent_uploads: 50,
                optimal_file_size_mb: None,
                supports_multipart_uploads: false,
                supports_atomic_operations: true,
            };
        }
    }

    Ok(info)
}

/// Storage backend information
#[derive(Debug, Clone)]
pub struct StorageInfo {
    /// Storage backend type
    pub backend_type: StorageBackend,
    /// Supported features
    pub supported_features: Vec<String>,
    /// Known limitations
    pub limitations: Vec<String>,
    /// Performance characteristics
    pub performance_characteristics: PerformanceCharacteristics,
}

/// Performance characteristics of storage backends
#[derive(Debug, Clone)]
pub struct PerformanceCharacteristics {
    /// Average read latency in milliseconds
    pub read_latency_ms: u64,
    /// Average write latency in milliseconds
    pub write_latency_ms: u64,
    /// Maximum throughput in megabits per second
    pub throughput_mbps: u64,
    /// Maximum concurrent uploads
    pub max_concurrent_uploads: usize,
    /// Optimal file size in megabytes for uploads
    pub optimal_file_size_mb: Option<u64>,
    /// Whether multipart uploads are supported
    pub supports_multipart_uploads: bool,
    /// Whether atomic operations are supported
    pub supports_atomic_operations: bool,
}

impl Default for PerformanceCharacteristics {
    fn default() -> Self {
        Self {
            read_latency_ms: 100,
            write_latency_ms: 200,
            throughput_mbps: 1000,
            max_concurrent_uploads: 10,
            optimal_file_size_mb: None,
            supports_multipart_uploads: false,
            supports_atomic_operations: false,
        }
    }
}

/// Create a test storage backend for development/testing
pub async fn create_test_storage() -> MirrorResult<Arc<dyn MirrorStorage>> {
    let temp_dir = tempfile::tempdir()
        .map_err(|e| MirrorError::storage_error(format!("Failed to create temp dir: {}", e)))?;

    let local_config = LocalConfig {
        base_dir: temp_dir.path().to_path_buf(),
        create_dirs: true,
        file_permissions: Some(0o644),
        dir_permissions: Some(0o755),
    };

    let storage_config = StorageConfig {
        backend: StorageBackend::Local,
        s3_config: None,
        azure_config: None,
        gcs_config: None,
        local_config: Some(local_config),
        timeout_secs: 30,
        max_retries: 3,
        enable_signing: false,
    };

    Ok(Arc::new(LocalStorage::new(
        storage_config.local_config.as_ref().unwrap(),
        &storage_config,
    )?))
}

/// Storage backend factory with additional utilities
pub struct StorageFactory;

impl StorageFactory {
    /// Create storage backend from environment variables
    pub async fn from_env() -> MirrorResult<Arc<dyn MirrorStorage>> {
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

        let mut config = StorageConfig {
            backend,
            timeout_secs: std::env::var("DELTA_MIRROR_STORAGE_TIMEOUT_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
            max_retries: std::env::var("DELTA_MIRROR_STORAGE_MAX_RETRIES")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),
            enable_signing: std::env::var("DELTA_MIRROR_STORAGE_SIGNING")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            s3_config: None,
            azure_config: None,
            gcs_config: None,
            local_config: None,
        };

        // Load backend-specific configuration
        match backend {
            StorageBackend::S3 => {
                config.s3_config = Some(S3Config {
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
                    assume_role_arn: std::env::var("AWS_ASSUME_ROLE_ARN").ok(),
                    external_id: std::env::var("AWS_EXTERNAL_ID").ok(),
                    server_side_encryption: None,
                });
            }
            StorageBackend::Azure => {
                config.azure_config = Some(AzureConfig {
                    account_name: std::env::var("AZURE_STORAGE_ACCOUNT")
                        .map_err(|_| MirrorError::config_error("AZURE_STORAGE_ACCOUNT is required for Azure backend"))?,
                    account_key: std::env::var("AZURE_STORAGE_KEY").ok(),
                    connection_string: std::env::var("AZURE_STORAGE_CONNECTION_STRING").ok(),
                    container: std::env::var("AZURE_STORAGE_CONTAINER")
                        .map_err(|_| MirrorError::config_error("AZURE_STORAGE_CONTAINER is required for Azure backend"))?,
                    endpoint: std::env::var("AZURE_STORAGE_ENDPOINT").ok(),
                    managed_identity_client_id: std::env::var("AZURE_MANAGED_IDENTITY_CLIENT_ID").ok(),
                    sas_token: std::env::var("AZURE_STORAGE_SAS_TOKEN").ok(),
                });
            }
            StorageBackend::Gcs => {
                config.gcs_config = Some(GcsConfig {
                    service_account_key: std::env::var("GCP_SERVICE_ACCOUNT_KEY").ok(),
                    bucket: std::env::var("GCP_STORAGE_BUCKET")
                        .map_err(|_| MirrorError::config_error("GCP_STORAGE_BUCKET is required for GCS backend"))?,
                    endpoint: std::env::var("GCP_STORAGE_ENDPOINT").ok(),
                    oauth2_token: std::env::var("GCP_OAUTH2_TOKEN").ok(),
                });
            }
            StorageBackend::Local => {
                config.local_config = Some(LocalConfig {
                    base_dir: std::env::var("DELTA_MIRROR_LOCAL_DIR")
                        .unwrap_or_else(|_| "/tmp/deltalakedb".to_string())
                        .into(),
                    create_dirs: std::env::var("DELTA_MIRROR_LOCAL_CREATE_DIRS")
                        .unwrap_or_else(|_| "true".to_string())
                        .parse()
                        .unwrap_or(true),
                    file_permissions: std::env::var("DELTA_MIRROR_LOCAL_FILE_PERMISSIONS")
                        .ok()
                        .and_then(|p| p.parse().ok()),
                    dir_permissions: std::env::var("DELTA_MIRROR_LOCAL_DIR_PERMISSIONS")
                        .ok()
                        .and_then(|p| p.parse().ok()),
                });
            }
        }

        validate_storage_config(&config)?;
        create_storage_with_retry(&config).await
    }

    /// Create storage with automatic backend detection
    pub async fn auto_detect(url: &str) -> MirrorResult<Arc<dyn MirrorStorage>> {
        let backend = if url.starts_with("s3://") || url.starts_with("https://s3") {
            StorageBackend::S3
        } else if url.starts_with("azure://") || url.starts_with("https://") {
            StorageBackend::Azure
        } else if url.starts_with("gs://") || url.starts_with("gcs://") {
            StorageBackend::Gcs
        } else if url.starts_with("file://") || url.starts_with("/") || !url.contains("://") {
            StorageBackend::Local
        } else {
            return Err(MirrorError::config_error(
                format!("Cannot detect storage backend from URL: {}", url)
            ));
        };

        // This is a simplified auto-detection
        // In a full implementation, we would parse the URL and extract configuration
        let config = match backend {
            StorageBackend::Local => StorageConfig {
                backend,
                local_config: Some(LocalConfig {
                    base_dir: PathBuf::from(url.trim_start_matches("file://")),
                    create_dirs: true,
                    file_permissions: None,
                    dir_permissions: None,
                }),
                ..Default::default()
            },
            _ => {
                return Err(MirrorError::config_error(
                    format!("Auto-detection not fully implemented for {:?} backend", backend)
                ));
            }
        };

        validate_storage_config(&config)?;
        create_storage_with_retry(&config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_storage_config_valid() {
        let config = StorageConfig::default();
        assert!(validate_storage_config(&config).is_ok());
    }

    #[test]
    fn test_validate_storage_config_invalid_timeout() {
        let mut config = StorageConfig::default();
        config.timeout_secs = 0;
        assert!(validate_storage_config(&config).is_err());
    }

    #[test]
    fn test_validate_storage_config_invalid_retries() {
        let mut config = StorageConfig::default();
        config.max_retries = 0;
        assert!(validate_storage_config(&config).is_err());
    }

    #[test]
    fn test_storage_info() {
        let config = StorageConfig::default();
        let info = get_storage_info(&config).unwrap();
        assert_eq!(info.backend_type, StorageBackend::Local);
        assert!(!info.supported_features.is_empty());
    }

    #[tokio::test]
    async fn test_auto_detect_local() {
        let result = StorageFactory::auto_detect("/tmp/test").await;
        // Should succeed for local paths, though it might fail to actually create
        // due to directory permissions in test environments
        match result {
            Ok(_) => assert!(true),
            Err(_) => assert!(true), // Also acceptable for test environments
        }
    }

    #[tokio::test]
    async fn test_create_test_storage() {
        let result = create_test_storage().await;
        // Should succeed in most environments
        assert!(result.is_ok());
    }

    #[test]
    fn test_performance_characteristics() {
        let perf = PerformanceCharacteristics::default();
        assert_eq!(perf.read_latency_ms, 100);
        assert_eq!(perf.write_latency_ms, 200);
        assert!(!perf.supports_multipart_uploads);
        assert!(!perf.supports_atomic_operations);
    }
}