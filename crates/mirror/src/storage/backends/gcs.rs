//! Google Cloud Storage backend for Delta Lake log mirroring

use crate::error::{MirrorError, MirrorResult};
use crate::storage::{
    MirrorStorage, StorageConfig, GcsConfig, StorageBackend, FileMetadata, StorageStats,
    ObjectStoreStorage,
};
use async_trait::async_trait;
use std::collections::HashMap;

/// Google Cloud Storage implementation
pub struct GcsStorage {
    inner: ObjectStoreStorage,
}

impl GcsStorage {
    /// Create a new GCS storage instance
    pub async fn new(
        config: &GcsConfig,
        storage_config: &StorageConfig,
    ) -> MirrorResult<Self> {
        // Create GCS object store
        let mut builder = object_store::gcp::GoogleCloudStorageBuilder::new()
            .with_bucket_name(&config.bucket);

        // Add service account key if provided
        if let Some(service_account_key) = &config.service_account_key {
            builder = builder.with_service_account_key(service_account_key);
        }

        // Add OAuth2 token if provided
        if let Some(oauth2_token) = &config.oauth2_token {
            builder = builder.with_oauth_token(oauth2_token);
        }

        // Add endpoint if provided
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
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
                StorageBackend::Gcs,
            ),
        })
    }
}

#[async_trait]
impl MirrorStorage for GcsStorage {
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
    fn test_gcs_config_validation() {
        let config = GcsConfig {
            service_account_key: Some("key".to_string()),
            bucket: "test-bucket".to_string(),
            endpoint: None,
            oauth2_token: None,
        };

        assert!(!config.bucket.is_empty());
    }
}