//! Object storage abstraction for Delta Lake log files

use crate::error::{MirrorError, MirrorResult, StorageError};
use crate::generators::DeltaFile;
use async_trait::async_trait;
use object_store::{path::Path, ObjectStore};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

pub mod backends;

pub use backends::*;

/// Storage abstraction for Delta Lake log mirroring
#[async_trait]
pub trait MirrorStorage: Send + Sync {
    /// Upload a file to storage
    async fn put_file(&self, path: &str, content: Vec<u8>) -> MirrorResult<()>;

    /// Upload a file with metadata
    async fn put_file_with_metadata(
        &self,
        path: &str,
        content: Vec<u8>,
        metadata: HashMap<String, String>,
    ) -> MirrorResult<()>;

    /// Download a file from storage
    async fn get_file(&self, path: &str) -> MirrorResult<Vec<u8>>;

    /// Check if a file exists
    async fn file_exists(&self, path: &str) -> MirrorResult<bool>;

    /// Delete a file from storage
    async fn delete_file(&self, path: &str) -> MirrorResult<()>;

    /// List files in a directory
    async fn list_files(&self, prefix: &str) -> MirrorResult<Vec<String>>;

    /// List files with pagination
    async fn list_files_paginated(
        &self,
        prefix: &str,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> MirrorResult<Vec<String>>;

    /// Get file metadata
    async fn get_file_metadata(&self, path: &str) -> MirrorResult<FileMetadata>;

    /// Upload a Delta file
    async fn put_delta_file(&self, table_path: &str, delta_file: &DeltaFile) -> MirrorResult<()> {
        let file_path = format!("{}/{}", table_path.trim_end_matches('/'), delta_file.file_name);

        if !delta_file.metadata.is_empty() {
            self.put_file_with_metadata(&file_path, delta_file.content.clone(), delta_file.metadata.clone()).await
        } else {
            self.put_file(&file_path, delta_file.content.clone()).await
        }
    }

    /// Download a Delta file
    async fn get_delta_file(&self, table_path: &str, file_name: &str) -> MirrorResult<Vec<u8>> {
        let file_path = format!("{}/{}", table_path.trim_end_matches('/'), file_name);
        self.get_file(&file_path).await
    }

    /// Check if Delta file exists
    async fn delta_file_exists(&self, table_path: &str, file_name: &str) -> MirrorResult<bool> {
        let file_path = format!("{}/{}", table_path.trim_end_matches('/'), file_name);
        self.file_exists(&file_path).await
    }

    /// Get Delta log directory contents
    async fn list_delta_log(&self, table_path: &str) -> MirrorResult<Vec<String>> {
        let log_path = format!("{}/_delta_log/", table_path.trim_end_matches('/'));
        self.list_files(&log_path).await
    }

    /// List Delta JSON files
    async fn list_delta_json_files(&self, table_path: &str) -> MirrorResult<Vec<String>> {
        let log_files = self.list_delta_log(table_path).await?;
        Ok(log_files.into_iter()
            .filter(|f| f.ends_with(".json"))
            .collect())
    }

    /// List Delta checkpoint files
    async fn list_delta_checkpoint_files(&self, table_path: &str) -> MirrorResult<Vec<String>> {
        let log_files = self.list_delta_log(table_path).await?;
        Ok(log_files.into_iter()
            .filter(|f| f.ends_with(".checkpoint.parquet"))
            .collect())
    }

    /// Get storage backend type
    fn backend_type(&self) -> StorageBackend;

    /// Get storage configuration
    fn config(&self) -> &StorageConfig;

    /// Health check for storage backend
    async fn health_check(&self) -> MirrorResult<bool>;

    /// Get storage statistics
    async fn get_storage_stats(&self) -> MirrorResult<StorageStats>;
}

/// File metadata from storage
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// File path
    pub path: String,
    /// File size in bytes
    pub size: u64,
    /// Last modified timestamp
    pub last_modified: chrono::DateTime<chrono::Utc>,
    /// ETag (if available)
    pub etag: Option<String>,
    /// Content type (if available)
    pub content_type: Option<String>,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    /// Total number of files
    pub total_files: u64,
    /// Total size in bytes
    pub total_size: u64,
    /// Average file size in bytes
    pub average_file_size: f64,
    /// Largest file size in bytes
    pub largest_file_size: u64,
    /// Storage backend type
    pub backend_type: StorageBackend,
    /// Last updated timestamp
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

/// Storage configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Storage backend type
    pub backend: StorageBackend,
    /// S3 configuration (if using S3)
    pub s3_config: Option<S3Config>,
    /// Azure configuration (if using Azure)
    pub azure_config: Option<AzureConfig>,
    /// GCS configuration (if using GCS)
    pub gcs_config: Option<GcsConfig>,
    /// Local configuration (if using local filesystem)
    pub local_config: Option<LocalConfig>,
    /// Connection timeout in seconds
    pub timeout_secs: u64,
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Whether to enable request signing
    pub enable_signing: bool,
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
    /// Assume role ARN
    pub assume_role_arn: Option<String>,
    /// External ID for assume role
    pub external_id: Option<String>,
    /// Server-side encryption
    pub server_side_encryption: Option<S3Encryption>,
}

/// S3 server-side encryption options
#[derive(Debug, Clone)]
pub enum S3Encryption {
    /// AES256 encryption
    Aes256,
    /// AWS KMS encryption
    AwsKms { key_id: Option<String> },
    /// Customer-provided encryption key
    CustomerKey { key: Vec<u8>, md5: Vec<u8> },
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
    /// Managed identity client ID
    pub managed_identity_client_id: Option<String>,
    /// SAS token
    pub sas_token: Option<String>,
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
    /// OAuth2 token
    pub oauth2_token: Option<String>,
}

/// Local filesystem configuration
#[derive(Debug, Clone)]
pub struct LocalConfig {
    /// Base directory for storing files
    pub base_dir: PathBuf,
    /// Create directories if they don't exist
    pub create_dirs: bool,
    /// File permissions (octal)
    pub file_permissions: Option<u32>,
    /// Directory permissions (octal)
    pub dir_permissions: Option<u32>,
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

/// Create a storage backend based on configuration
pub fn create_storage(config: &StorageConfig) -> MirrorResult<Arc<dyn MirrorStorage>> {
    let storage: Arc<dyn MirrorStorage> = match config.backend {
        StorageBackend::S3 => {
            let s3_config = config.s3_config.as_ref()
                .ok_or_else(|| MirrorError::config_error("S3 configuration is required for S3 backend"))?;
            Arc::new(S3Storage::new(s3_config, config)?)
        }
        StorageBackend::Azure => {
            let azure_config = config.azure_config.as_ref()
                .ok_or_else(|| MirrorError::config_error("Azure configuration is required for Azure backend"))?;
            Arc::new(AzureStorage::new(azure_config, config)?)
        }
        StorageBackend::Gcs => {
            let gcs_config = config.gcs_config.as_ref()
                .ok_or_else(|| MirrorError::config_error("GCS configuration is required for GCS backend"))?;
            Arc::new(GcsStorage::new(gcs_config, config)?)
        }
        StorageBackend::Local => {
            let local_config = config.local_config.as_ref()
                .ok_or_else(|| MirrorError::config_error("Local configuration is required for Local backend"))?;
            Arc::new(LocalStorage::new(local_config, config)?)
        }
    };

    Ok(storage)
}

/// Generic storage implementation using object-store crate
pub struct ObjectStoreStorage {
    store: Arc<dyn ObjectStore>,
    config: StorageConfig,
    backend_type: StorageBackend,
}

impl ObjectStoreStorage {
    /// Create a new object store storage
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: StorageConfig,
        backend_type: StorageBackend,
    ) -> Self {
        Self {
            store,
            config,
            backend_type,
        }
    }

    /// Convert string path to object store Path
    fn to_path(path: &str) -> Path {
        Path::from(path)
    }

    /// Convert object store Path to string
    fn from_path(path: &Path) -> String {
        path.to_string()
    }

    /// Convert object store error to storage error
    fn convert_error(&self, path: &str, error: object_store::Error) -> StorageError {
        match error {
            object_store::Error::NotFound { .. } => {
                StorageError::NotFound(format!("File not found: {}", path))
            }
            object_store::Error::Generic { source, .. } => {
                StorageError::BackendError {
                    backend: format!("{:?}", self.backend_type),
                    error: source.to_string(),
                }
            }
            _ => {
                StorageError::Generic(format!("Object store error for {}: {}", path, error))
            }
        }
    }
}

#[async_trait]
impl MirrorStorage for ObjectStoreStorage {
    async fn put_file(&self, path: &str, content: Vec<u8>) -> MirrorResult<()> {
        let object_path = Self::to_path(path);

        self.store
            .put(&object_path, content.into())
            .await
            .map_err(|e| MirrorError::StorageError(
                self.convert_error(path, e)
            ))?;

        Ok(())
    }

    async fn put_file_with_metadata(
        &self,
        path: &str,
        content: Vec<u8>,
        metadata: HashMap<String, String>,
    ) -> MirrorResult<()> {
        let object_path = Self::to_path(path);

        // Convert metadata to object store metadata
        let mut object_meta = object_store::ObjectMeta::from(content);
        for (key, value) in metadata {
            object_meta = object_meta.with_custom_metadata(key, value);
        }

        self.store
            .put(&object_path, object_meta)
            .await
            .map_err(|e| MirrorError::StorageError(
                self.convert_error(path, e)
            ))?;

        Ok(())
    }

    async fn get_file(&self, path: &str) -> MirrorResult<Vec<u8>> {
        let object_path = Self::to_path(path);

        let result = self.store
            .get(&object_path)
            .await
            .map_err(|e| MirrorError::StorageError(
                self.convert_error(path, e)
            ))?;

        let bytes = result.bytes().await
            .map_err(|e| MirrorError::StorageError(
                StorageError::DownloadError {
                    path: path.to_string(),
                    error: e.to_string(),
                }
            ))?;

        Ok(bytes.to_vec())
    }

    async fn file_exists(&self, path: &str) -> MirrorResult<bool> {
        let object_path = Self::to_path(path);

        match self.store.head(&object_path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(MirrorError::StorageError(
                self.convert_error(path, e)
            )),
        }
    }

    async fn delete_file(&self, path: &str) -> MirrorResult<()> {
        let object_path = Self::to_path(path);

        self.store
            .delete(&object_path)
            .await
            .map_err(|e| MirrorError::StorageError(
                self.convert_error(path, e)
            ))?;

        Ok(())
    }

    async fn list_files(&self, prefix: &str) -> MirrorResult<Vec<String>> {
        let object_path = Self::to_path(prefix);

        let result = self.store
            .list_with_delimiter(Some(&object_path))
            .await
            .map_err(|e| MirrorError::StorageError(
                self.convert_error(prefix, e)
            ))?;

        let mut files = Vec::new();
        for object in result.objects {
            files.push(Self::from_path(&object.location));
        }

        Ok(files)
    }

    async fn list_files_paginated(
        &self,
        prefix: &str,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> MirrorResult<Vec<String>> {
        let object_path = Self::to_path(prefix);

        // Note: object_store doesn't natively support offset/limit
        // This is a simplified implementation
        let result = self.store
            .list_with_delimiter(Some(&object_path))
            .await
            .map_err(|e| MirrorError::StorageError(
                self.convert_error(prefix, e)
            ))?;

        let mut files: Vec<String> = result.objects
            .into_iter()
            .map(|obj| Self::from_path(&obj.location))
            .collect();

        // Apply offset and limit
        if let Some(offset) = offset {
            if offset < files.len() {
                files = files.into_iter().skip(offset).collect();
            } else {
                files = Vec::new();
            }
        }

        if let Some(limit) = limit {
            files.truncate(limit);
        }

        Ok(files)
    }

    async fn get_file_metadata(&self, path: &str) -> MirrorResult<FileMetadata> {
        let object_path = Self::to_path(path);

        let head = self.store
            .head(&object_path)
            .await
            .map_err(|e| MirrorError::StorageError(
                self.convert_error(path, e)
            ))?;

        Ok(FileMetadata {
            path: path.to_string(),
            size: head.size,
            last_modified: head.last_modified,
            etag: head.etag,
            content_type: head.content_type,
            metadata: head.custom_metadata
                .into_iter()
                .collect(),
        })
    }

    fn backend_type(&self) -> StorageBackend {
        self.backend_type.clone()
    }

    fn config(&self) -> &StorageConfig {
        &self.config
    }

    async fn health_check(&self) -> MirrorResult<bool> {
        // Try to list the root directory as a health check
        match self.store.list(None).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn get_storage_stats(&self) -> MirrorResult<StorageStats> {
        // This is a simplified implementation
        // A full implementation would scan the storage to calculate statistics
        Ok(StorageStats {
            total_files: 0,
            total_size: 0,
            average_file_size: 0.0,
            largest_file_size: 0,
            backend_type: self.backend_type.clone(),
            last_updated: chrono::Utc::now(),
        })
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
                file_permissions: Some(0o644),
                dir_permissions: Some(0o755),
            }),
            timeout_secs: 30,
            max_retries: 3,
            enable_signing: true,
        }
    }
}

/// Create a test storage instance for testing
#[cfg(test)]
pub async fn create_test_storage() -> MirrorResult<Arc<dyn MirrorStorage>> {
    let config = StorageConfig {
        backend: StorageBackend::Local,
        s3_config: None,
        azure_config: None,
        gcs_config: None,
        local_config: Some(LocalConfig {
            base_dir: std::env::temp_dir().join("deltalakedb_test"),
            create_dirs: true,
            file_permissions: Some(0o644),
            dir_permissions: Some(0o755),
        }),
        timeout_secs: 30,
        max_retries: 3,
        enable_signing: false,
    };

    create_storage(&config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_config_default() {
        let config = StorageConfig::default();
        assert_eq!(config.backend, StorageBackend::Local);
        assert!(config.local_config.is_some());
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_file_metadata() {
        let metadata = FileMetadata {
            path: "/test/file.json".to_string(),
            size: 1024,
            last_modified: chrono::Utc::now(),
            etag: Some("etag123".to_string()),
            content_type: Some("application/json".to_string()),
            metadata: HashMap::new(),
        };

        assert_eq!(metadata.path, "/test/file.json");
        assert_eq!(metadata.size, 1024);
        assert_eq!(metadata.etag, Some("etag123".to_string()));
    }

    #[test]
    fn test_storage_stats() {
        let stats = StorageStats {
            total_files: 100,
            total_size: 1000000,
            average_file_size: 10000.0,
            largest_file_size: 50000,
            backend_type: StorageBackend::Local,
            last_updated: chrono::Utc::now(),
        };

        assert_eq!(stats.total_files, 100);
        assert_eq!(stats.total_size, 1000000);
        assert_eq!(stats.backend_type, StorageBackend::Local);
    }

    #[test]
    fn test_s3_encryption() {
        let aes256 = S3Encryption::Aes256;
        let kms = S3Encryption::AwsKms { key_id: Some("key-123".to_string()) };
        let customer_key = S3Encryption::CustomerKey {
            key: vec![1, 2, 3, 4],
            md5: vec![5, 6, 7, 8],
        };

        match aes256 {
            S3Encryption::Aes256 => assert!(true),
            _ => assert!(false),
        }

        match kms {
            S3Encryption::AwsKms { key_id } => assert_eq!(key_id, Some("key-123".to_string())),
            _ => assert!(false),
        }

        match customer_key {
            S3Encryption::CustomerKey { key, md5 } => {
                assert_eq!(key, vec![1, 2, 3, 4]);
                assert_eq!(md5, vec![5, 6, 7, 8]);
            }
            _ => assert!(false),
        }
    }
}