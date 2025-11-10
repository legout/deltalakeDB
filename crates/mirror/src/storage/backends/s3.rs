//! Amazon S3 storage backend for Delta Lake log mirroring

use crate::error::{MirrorError, MirrorResult};
use crate::storage::{
    MirrorStorage, StorageConfig, S3Config, StorageBackend, FileMetadata, StorageStats,
    ObjectStoreStorage, S3Encryption,
};
use async_trait::async_trait;
use std::collections::HashMap;

/// Amazon S3 storage implementation
pub struct S3Storage {
    inner: ObjectStoreStorage,
}

impl S3Storage {
    /// Create a new S3 storage instance
    pub async fn new(
        config: &S3Config,
        storage_config: &StorageConfig,
    ) -> MirrorResult<Self> {
        // Create AWS S3 object store
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_region(&config.region)
            .with_bucket_name(&config.bucket);

        // Add credentials if provided
        if let Some(access_key_id) = &config.access_key_id {
            builder = builder.with_access_key_id(access_key_id);
        }

        if let Some(secret_access_key) = &config.secret_access_key {
            builder = builder.with_secret_access_key(secret_access_key);
        }

        if let Some(session_token) = &config.session_token {
            builder = builder.with_token(session_token);
        }

        // Add endpoint configuration
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        // Force path style if configured
        if config.force_path_style {
            builder = builder.with_force_path_style(true);
        }

        // Handle assume role configuration
        if let Some(assume_role_arn) = &config.assume_role_arn {
            builder = builder.with_assume_role_arn(assume_role_arn);
        }

        if let Some(external_id) = &config.external_id {
            builder = builder.with_external_id(external_id);
        }

        // Handle server-side encryption
        if let Some(encryption) = &config.server_side_encryption {
            match encryption {
                S3Encryption::Aes256 => {
                    builder = builder.with_server_side_encryption(object_store::aws::ServerSideEncryption::Aes256);
                }
                S3Encryption::AwsKms { key_id } => {
                    let kms_key = if let Some(kid) = key_id {
                        object_store::aws::KmsKey::KmsKeyId(kid.to_string())
                    } else {
                        object_store::aws::KmsKey::KmsDefaultKey
                    };
                    builder = builder.with_server_side_encryption(object_store::aws::ServerSideEncryption::AwsKms(kms_key));
                }
                S3Encryption::CustomerKey { key, md5 } => {
                    let customer_key = object_store::aws::CustomerKey::new(key.clone(), md5.clone());
                    builder = builder.with_server_side_encryption(object_store::aws::ServerSideEncryption::CustomerKey(customer_key));
                }
            }
        }

        // Build the object store
        let store = Arc::new(
            builder
                .build()
                .map_err(|e| MirrorError::StorageError(
                    crate::error::StorageError::ConnectionError(e.to_string())
                ))?
        );

        Ok(Self {
            inner: ObjectStoreStorage::new(
                store,
                storage_config.clone(),
                StorageBackend::S3,
            ),
        })
    }

    /// Create S3 storage with retry logic
    pub async fn with_retry(
        config: &S3Config,
        storage_config: &StorageConfig,
    ) -> MirrorResult<Self> {
        let mut attempts = 0;
        let max_attempts = storage_config.max_retries;
        let base_delay = std::time::Duration::from_millis(storage_config.timeout_secs * 100);

        loop {
            attempts += 1;

            match Self::new(config, storage_config).await {
                Ok(storage) => {
                    // Test the connection
                    if storage.health_check().await.unwrap_or(false) {
                        return Ok(storage);
                    } else {
                        tracing::warn!("S3 health check failed, attempt {}/{}", attempts, max_attempts);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to create S3 storage: {}, attempt {}/{}", e, attempts, max_attempts);
                }
            }

            if attempts >= max_attempts {
                return Err(MirrorError::storage_error(format!(
                    "Failed to create S3 storage after {} attempts",
                    max_attempts
                )));
            }

            // Exponential backoff with jitter
            let delay = base_delay * 2_u32.pow(attempts - 1);
            let jitter = std::time::Duration::from_millis((rand::random::<u64>() % 1000));
            tokio::time::sleep(delay + jitter).await;
        }
    }
}

#[async_trait]
impl MirrorStorage for S3Storage {
    async fn put_file(&self, path: &str, content: Vec<u8>) -> MirrorResult<()> {
        self.inner.put_file(path, content).await
    }

    async fn put_file_with_metadata(
        &self,
        path: &str,
        content: Vec<u8>,
        metadata: HashMap<String, String>,
    ) -> MirrorResult<()> {
        self.inner.put_file_with_metadata(path, content, metadata).await
    }

    async fn get_file(&self, path: &str) -> MirrorResult<Vec<u8>> {
        self.inner.get_file(path).await
    }

    async fn file_exists(&self, path: &str) -> MirrorResult<bool> {
        self.inner.file_exists(path).await
    }

    async fn delete_file(&self, path: &str) -> MirrorResult<()> {
        self.inner.delete_file(path).await
    }

    async fn list_files(&self, prefix: &str) -> MirrorResult<Vec<String>> {
        self.inner.list_files(prefix).await
    }

    async fn list_files_paginated(
        &self,
        prefix: &str,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> MirrorResult<Vec<String>> {
        self.inner.list_files_paginated(prefix, offset, limit).await
    }

    async fn get_file_metadata(&self, path: &str) -> MirrorResult<FileMetadata> {
        self.inner.get_file_metadata(path).await
    }

    async fn put_delta_file(
        &self,
        table_path: &str,
        delta_file: &crate::generators::DeltaFile,
    ) -> MirrorResult<()> {
        self.inner.put_delta_file(table_path, delta_file).await
    }

    async fn get_delta_file(&self, table_path: &str, file_name: &str) -> MirrorResult<Vec<u8>> {
        self.inner.get_delta_file(table_path, file_name).await
    }

    async fn delta_file_exists(&self, table_path: &str, file_name: &str) -> MirrorResult<bool> {
        self.inner.delta_file_exists(table_path, file_name).await
    }

    async fn list_delta_log(&self, table_path: &str) -> MirrorResult<Vec<String>> {
        self.inner.list_delta_log(table_path).await
    }

    async fn list_delta_json_files(&self, table_path: &str) -> MirrorResult<Vec<String>> {
        self.inner.list_delta_json_files(table_path).await
    }

    async fn list_delta_checkpoint_files(&self, table_path: &str) -> MirrorResult<Vec<String>> {
        self.inner.list_delta_checkpoint_files(table_path).await
    }

    fn backend_type(&self) -> StorageBackend {
        self.inner.backend_type()
    }

    fn config(&self) -> &StorageConfig {
        self.inner.config()
    }

    async fn health_check(&self) -> MirrorResult<bool> {
        self.inner.health_check().await
    }

    async fn get_storage_stats(&self) -> MirrorResult<StorageStats> {
        self.inner.get_storage_stats().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_validation() {
        let config = S3Config {
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            access_key_id: Some("key".to_string()),
            secret_access_key: Some("secret".to_string()),
            session_token: None,
            endpoint: None,
            force_path_style: false,
            assume_role_arn: None,
            external_id: None,
            server_side_encryption: None,
        };

        // Basic validation would go here
        assert!(!config.region.is_empty());
        assert!(!config.bucket.is_empty());
    }

    // Note: Actual S3 integration tests would require AWS credentials and test buckets
    // These would typically be run in CI with proper setup
}