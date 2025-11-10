//! Azure Blob storage backend for Delta Lake log mirroring

use crate::error::{MirrorError, MirrorResult};
use crate::storage::{
    MirrorStorage, StorageConfig, AzureConfig, StorageBackend, FileMetadata, StorageStats,
    ObjectStoreStorage,
};
use async_trait::async_trait;
use std::collections::HashMap;

/// Azure Blob storage implementation
pub struct AzureStorage {
    inner: ObjectStoreStorage,
}

impl AzureStorage {
    /// Create a new Azure storage instance
    pub async fn new(
        config: &AzureConfig,
        storage_config: &StorageConfig,
    ) -> MirrorResult<Self> {
        // Create Azure Blob object store
        let mut builder = object_store::azure::MicrosoftAzureBuilder::new();

        // Use connection string if provided
        if let Some(connection_string) = &config.connection_string {
            builder = builder.with_connection_string(connection_string);
        } else {
            // Use account name and key
            builder = builder.with_account_name(&config.account_name);

            if let Some(account_key) = &config.account_key {
                builder = builder.with_access_key(account_key);
            }
        }

        // Set container name
        builder = builder.with_container_name(&config.container);

        // Add endpoint if provided
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        // Add managed identity configuration
        if let Some(client_id) = &config.managed_identity_client_id {
            builder = builder.with_managed_identity(client_id);
        }

        // Add SAS token if provided
        if let Some(sas_token) = &config.sas_token {
            builder = builder.with_sas_token(sas_token);
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
                StorageBackend::Azure,
            ),
        })
    }
}

#[async_trait]
impl MirrorStorage for AzureStorage {
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
    fn test_azure_config_validation() {
        let config = AzureConfig {
            account_name: "testaccount".to_string(),
            account_key: Some("key".to_string()),
            connection_string: None,
            container: "test-container".to_string(),
            endpoint: None,
            managed_identity_client_id: None,
            sas_token: None,
        };

        assert!(!config.account_name.is_empty());
        assert!(!config.container.is_empty());
    }
}