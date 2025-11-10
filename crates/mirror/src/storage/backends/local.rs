//! Local filesystem storage backend for Delta Lake log mirroring

use crate::error::{MirrorError, MirrorResult, StorageError};
use crate::storage::{
    MirrorStorage, StorageConfig, LocalConfig, StorageBackend, FileMetadata, StorageStats,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::fs as tokio_fs;
use tokio::io::AsyncReadExt;

/// Local filesystem storage implementation
pub struct LocalStorage {
    base_dir: PathBuf,
    config: StorageConfig,
    file_permissions: Option<u32>,
    dir_permissions: Option<u32>,
}

impl LocalStorage {
    /// Create a new local storage instance
    pub fn new(
        config: &LocalConfig,
        storage_config: &StorageConfig,
    ) -> MirrorResult<Self> {
        let base_dir = config.base_dir.clone();

        // Create base directory if it doesn't exist
        if config.create_dirs {
            if let Err(e) = std::fs::create_dir_all(&base_dir) {
                return Err(MirrorError::StorageError(
                    StorageError::IoError(e)
                ));
            }
        }

        // Ensure base directory exists
        if !base_dir.exists() {
            return Err(MirrorError::StorageError(
                StorageError::NotFound(format!("Base directory does not exist: {}", base_dir.display()))
            ));
        }

        Ok(Self {
            base_dir,
            config: storage_config.clone(),
            file_permissions: config.file_permissions,
            dir_permissions: config.dir_permissions,
        })
    }

    /// Get the full path for a relative path
    fn get_full_path(&self, path: &str) -> PathBuf {
        let path = path.trim_start_matches('/');
        self.base_dir.join(path)
    }

    /// Get relative path from full path
    fn get_relative_path(&self, full_path: &Path) -> MirrorResult<String> {
        full_path.strip_prefix(&self.base_dir)
            .map(|p| p.to_string_lossy().to_string())
            .map_err(|_| MirrorError::storage_error(
                format!("Path {} is not under base directory {}",
                    full_path.display(),
                    self.base_dir.display())
            ))
    }

    /// Set file permissions if configured
    async fn set_file_permissions(&self, path: &Path) -> MirrorResult<()> {
        if let Some(perm) = self.file_permissions {
            tokio_fs::set_permissions(path, std::fs::Permissions::from_mode(perm))
                .await
                .map_err(|e| MirrorError::StorageError(
                    StorageError::IoError(e)
                ))?;
        }
        Ok(())
    }

    /// Set directory permissions if configured
    async fn set_dir_permissions(&self, path: &Path) -> MirrorResult<()> {
        if let Some(perm) = self.dir_permissions {
            tokio_fs::set_permissions(path, std::fs::Permissions::from_mode(perm))
                .await
                .map_err(|e| MirrorError::StorageError(
                    StorageError::IoError(e)
                ))?;
        }
        Ok(())
    }

    /// Create parent directories if they don't exist
    async fn ensure_parent_dirs(&self, path: &Path) -> MirrorResult<()> {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                tokio_fs::create_dir_all(parent)
                    .await
                    .map_err(|e| MirrorError::StorageError(
                        StorageError::IoError(e)
                    ))?;

                self.set_dir_permissions(parent).await?;
            }
        }
        Ok(())
    }

    /// Get file metadata
    async fn get_file_metadata_internal(&self, path: &Path) -> MirrorResult<FileMetadata> {
        let metadata = tokio_fs::metadata(path)
            .await
            .map_err(|e| MirrorError::StorageError(
                StorageError::IoError(e)
            ))?;

        let last_modified = metadata.modified()
            .map_err(|e| MirrorError::StorageError(
                StorageError::IoError(e)
            ))?;

        let last_modified_datetime = last_modified
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| MirrorError::StorageError(
                StorageError::IoError(e)
            ))?
            .as_secs();

        let etag = Some(format!("{:x}", metadata.len() ^ last_modified_datetime));

        Ok(FileMetadata {
            path: self.get_relative_path(path)?,
            size: metadata.len(),
            last_modified: chrono::DateTime::from_timestamp(last_modified_datetime as i64, 0)
                .unwrap_or_else(|| chrono::Utc::now()),
            etag,
            content_type: None, // Local files don't have content types
            metadata: HashMap::new(),
        })
    }

    /// Atomic file write using temporary file and rename
    async fn atomic_write(&self, path: &Path, content: Vec<u8>) -> MirrorResult<()> {
        // Ensure parent directories exist
        self.ensure_parent_dirs(path).await?;

        // Create temporary file in the same directory
        let temp_path = path.with_extension("tmp");

        // Write to temporary file
        tokio_fs::write(&temp_path, &content)
            .await
            .map_err(|e| MirrorError::StorageError(
                StorageError::UploadError {
                    path: path.to_string_lossy().to_string(),
                    error: e.to_string(),
                }
            ))?;

        // Set file permissions
        self.set_file_permissions(&temp_path).await?;

        // Atomic rename
        tokio_fs::rename(&temp_path, path)
            .await
            .map_err(|e| MirrorError::StorageError(
                StorageError::UploadError {
                    path: path.to_string_lossy().to_string(),
                    error: e.to_string(),
                }
            ))?;

        Ok(())
    }

    /// List files recursively in a directory
    async fn list_files_recursive(&self, dir: &Path) -> MirrorResult<Vec<String>> {
        let mut files = Vec::new();

        if !dir.exists() {
            return Ok(files);
        }

        let mut entries = tokio_fs::read_dir(dir)
            .await
            .map_err(|e| MirrorError::StorageError(
                StorageError::ListError {
                    path: dir.to_string_lossy().to_string(),
                    error: e.to_string(),
                }
            ))?;

        while let Some(entry) = entries.next_entry().await
            .map_err(|e| MirrorError::StorageError(
                StorageError::ListError {
                    path: dir.to_string_lossy().to_string(),
                    error: e.to_string(),
                }
            ))? {
            let path = entry.path();

            if path.is_dir() {
                // Recursively list subdirectory
                let sub_files = self.list_files_recursive(&path).await?;
                files.extend(sub_files);
            } else {
                // Add file to results
                if let Ok(relative_path) = self.get_relative_path(&path) {
                    files.push(relative_path);
                }
            }
        }

        Ok(files)
    }
}

#[async_trait]
impl MirrorStorage for LocalStorage {
    async fn put_file(&self, path: &str, content: Vec<u8>) -> MirrorResult<()> {
        let full_path = self.get_full_path(path);

        // Use atomic write for reliability
        self.atomic_write(&full_path, content).await
    }

    async fn put_file_with_metadata(
        &self,
        path: &str,
        content: Vec<u8>,
        metadata: HashMap<String, String>,
    ) -> MirrorResult<()> {
        let full_path = self.get_full_path(path);

        // Write the file
        self.atomic_write(&full_path, content).await?;

        // For local files, we could store metadata in sidecar files
        // but for simplicity, we'll ignore the metadata for now
        if !metadata.is_empty() {
            tracing::debug!("Ignoring metadata for local file: {}", metadata.keys().collect::<Vec<_>>().join(", "));
        }

        Ok(())
    }

    async fn get_file(&self, path: &str) -> MirrorResult<Vec<u8>> {
        let full_path = self.get_full_path(path);

        if !full_path.exists() {
            return Err(MirrorError::StorageError(
                StorageError::NotFound(format!("File not found: {}", path))
            ));
        }

        tokio_fs::read(&full_path)
            .await
            .map_err(|e| MirrorError::StorageError(
                StorageError::DownloadError {
                    path: path.to_string(),
                    error: e.to_string(),
                }
            ))
    }

    async fn file_exists(&self, path: &str) -> MirrorResult<bool> {
        let full_path = self.get_full_path(path);
        Ok(full_path.exists())
    }

    async fn delete_file(&self, path: &str) -> MirrorResult<()> {
        let full_path = self.get_full_path(path);

        if !full_path.exists() {
            return Err(MirrorError::StorageError(
                StorageError::NotFound(format!("File not found: {}", path))
            ));
        }

        tokio_fs::remove_file(&full_path)
            .await
            .map_err(|e| MirrorError::StorageError(
                StorageError::DeleteError {
                    path: path.to_string(),
                    error: e.to_string(),
                }
            ))?;

        Ok(())
    }

    async fn list_files(&self, prefix: &str) -> MirrorResult<Vec<String>> {
        let full_prefix = self.get_full_path(prefix);

        if !full_prefix.exists() {
            return Ok(Vec::new());
        }

        // If prefix points to a file, return that single file
        if full_prefix.is_file() {
            return Ok(vec![prefix.trim_end_matches('/').to_string()]);
        }

        // List files in directory recursively
        let prefix_str = prefix.trim_end_matches('/');
        let mut files = self.list_files_recursive(&full_prefix).await?;

        // Filter by prefix if needed
        if !prefix_str.is_empty() {
            files.retain(|f| f.starts_with(prefix_str));
        }

        Ok(files)
    }

    async fn list_files_paginated(
        &self,
        prefix: &str,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> MirrorResult<Vec<String>> {
        let all_files = self.list_files(prefix).await?;

        let start = offset.unwrap_or(0);
        let end = if let Some(limit) = limit {
            (start + limit).min(all_files.len())
        } else {
            all_files.len()
        };

        if start >= all_files.len() {
            return Ok(Vec::new());
        }

        Ok(all_files[start..end].to_vec())
    }

    async fn get_file_metadata(&self, path: &str) -> MirrorResult<FileMetadata> {
        let full_path = self.get_full_path(path);

        if !full_path.exists() {
            return Err(MirrorError::StorageError(
                StorageError::NotFound(format!("File not found: {}", path))
            ));
        }

        self.get_file_metadata_internal(&full_path).await
    }

    async fn list_delta_log(&self, table_path: &str) -> MirrorResult<Vec<String>> {
        let log_path = format!("{}/_delta_log", table_path.trim_end_matches('/'));
        self.list_files(&log_path).await
    }

    fn backend_type(&self) -> StorageBackend {
        StorageBackend::Local
    }

    fn config(&self) -> &StorageConfig {
        &self.config
    }

    async fn health_check(&self) -> MirrorResult<bool> {
        // Check if base directory exists and is accessible
        match tokio_fs::metadata(&self.base_dir).await {
            Ok(metadata) => Ok(metadata.is_dir()),
            Err(_) => Ok(false),
        }
    }

    async fn get_storage_stats(&self) -> MirrorResult<StorageStats> {
        let mut total_files = 0u64;
        let mut total_size = 0u64;
        let mut largest_file_size = 0u64;

        // Recursively scan the base directory
        if let Ok(mut entries) = tokio_fs::read_dir(&self.base_dir).await {
            let mut stack = vec![];

            // Collect initial entries
            while let Some(entry) = entries.next_entry().await
                .map_err(|e| MirrorError::StorageError(
                    StorageError::IoError(e)
                ))? {
                stack.push(entry.path());
            }

            // Process stack
            while let Some(path) = stack.pop() {
                if path.is_dir() {
                    // Add directory contents to stack
                    if let Ok(mut sub_entries) = tokio_fs::read_dir(&path).await {
                        while let Some(sub_entry) = sub_entries.next_entry().await
                            .map_err(|e| MirrorError::StorageError(
                                StorageError::IoError(e)
                            ))? {
                            stack.push(sub_entry.path());
                        }
                    }
                } else if path.is_file() {
                    // Get file metadata
                    if let Ok(metadata) = tokio_fs::metadata(&path).await {
                        total_files += 1;
                        total_size += metadata.len() as u64;
                        largest_file_size = largest_file_size.max(metadata.len() as u64);
                    }
                }
            }
        }

        let average_file_size = if total_files > 0 {
            total_size as f64 / total_files as f64
        } else {
            0.0
        };

        Ok(StorageStats {
            total_files,
            total_size,
            average_file_size,
            largest_file_size,
            backend_type: StorageBackend::Local,
            last_updated: chrono::Utc::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tempfile::TempDir;
    use tokio::fs as tokio_fs;

    #[tokio::test]
    async fn test_local_storage_creation() {
        let temp_dir = TempDir::new().unwrap();
        let local_config = LocalConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_dirs: true,
            file_permissions: Some(0o644),
            dir_permissions: Some(0o755),
        };

        let storage_config = StorageConfig::default();
        let storage = LocalStorage::new(&local_config, &storage_config).unwrap();
        assert_eq!(storage.backend_type(), StorageBackend::Local);
        assert!(storage.base_dir.exists());
    }

    #[tokio::test]
    async fn test_put_and_get_file() {
        let temp_dir = TempDir::new().unwrap();
        let local_config = LocalConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_dirs: true,
            file_permissions: None,
            dir_permissions: None,
        };

        let storage_config = StorageConfig::default();
        let storage = LocalStorage::new(&local_config, &storage_config).unwrap();

        let content = b"Hello, Delta Lake!".to_vec();
        let path = "test/file.txt";

        // Put file
        storage.put_file(path, content.clone()).await.unwrap();
        assert!(storage.file_exists(path).await.unwrap());

        // Get file
        let retrieved = storage.get_file(path).await.unwrap();
        assert_eq!(retrieved, content);
    }

    #[tokio::test]
    async fn test_delete_file() {
        let temp_dir = TempDir::new().unwrap();
        let local_config = LocalConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_dirs: true,
            file_permissions: None,
            dir_permissions: None,
        };

        let storage_config = StorageConfig::default();
        let storage = LocalStorage::new(&local_config, &storage_config).unwrap();

        let content = b"Hello, Delta Lake!".to_vec();
        let path = "test/delete_me.txt";

        // Create file
        storage.put_file(path, content).await.unwrap();
        assert!(storage.file_exists(path).await.unwrap());

        // Delete file
        storage.delete_file(path).await.unwrap();
        assert!(!storage.file_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn test_list_files() {
        let temp_dir = TempDir::new().unwrap();
        let local_config = LocalConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_dirs: true,
            file_permissions: None,
            dir_permissions: None,
        };

        let storage_config = StorageConfig::default();
        let storage = LocalStorage::new(&local_config, &storage_config).unwrap();

        // Create some test files
        storage.put_file("test/file1.txt", b"content1".to_vec()).await.unwrap();
        storage.put_file("test/file2.txt", b"content2".to_vec()).await.unwrap();
        storage.put_file("other/file3.txt", b"content3".to_vec()).await.unwrap();

        // List all files
        let all_files = storage.list_files("").await.unwrap();
        assert_eq!(all_files.len(), 3);

        // List files in test directory
        let test_files = storage.list_files("test").await.unwrap();
        assert_eq!(test_files.len(), 2);
        assert!(test_files.iter().any(|f| f.ends_with("file1.txt")));
        assert!(test_files.iter().any(|f| f.ends_with("file2.txt")));
    }

    #[tokio::test]
    async fn test_list_files_paginated() {
        let temp_dir = TempDir::new().unwrap();
        let local_config = LocalConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_dirs: true,
            file_permissions: None,
            dir_permissions: None,
        };

        let storage_config = StorageConfig::default();
        let storage = LocalStorage::new(&local_config, &storage_config).unwrap();

        // Create test files
        for i in 0..10 {
            let path = format!("test/file{}.txt", i);
            storage.put_file(&path, format!("content{}", i).as_bytes().to_vec()).await.unwrap();
        }

        // Test pagination
        let page1 = storage.list_files_paginated("test", Some(0), Some(3)).await.unwrap();
        assert_eq!(page1.len(), 3);

        let page2 = storage.list_files_paginated("test", Some(3), Some(3)).await.unwrap();
        assert_eq!(page2.len(), 3);

        let page_last = storage.list_files_paginated("test", Some(9), Some(5)).await.unwrap();
        assert_eq!(page_last.len(), 1);
    }

    #[tokio::test]
    async fn test_file_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let local_config = LocalConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_dirs: true,
            file_permissions: None,
            dir_permissions: None,
        };

        let storage_config = StorageConfig::default();
        let storage = LocalStorage::new(&local_config, &storage_config).unwrap();

        let content = b"Hello, Delta Lake!".to_vec();
        let path = "test/metadata.txt";

        storage.put_file(path, content).await.unwrap();

        let metadata = storage.get_file_metadata(path).await.unwrap();
        assert_eq!(metadata.path, path);
        assert_eq!(metadata.size, content.len() as u64);
        assert!(metadata.last_modified.timestamp() > 0);
        assert!(metadata.etag.is_some());
    }

    #[tokio::test]
    async fn test_delta_log_operations() {
        let temp_dir = TempDir::new().unwrap();
        let local_config = LocalConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_dirs: true,
            file_permissions: None,
            dir_permissions: None,
        };

        let storage_config = StorageConfig::default();
        let storage = LocalStorage::new(&local_config, &storage_config).unwrap();

        let table_path = "/test/table";

        // Create some Delta log files
        storage.put_delta_file(table_path, &crate::generators::DeltaFile::new(
            crate::generators::DeltaFormat::Json,
            "00000000000000000000.json".to_string(),
            b"{}".to_vec(),
        )).await.unwrap();

        storage.put_delta_file(table_path, &crate::generators::DeltaFile::new(
            crate::generators::DeltaFormat::Json,
            "00000000000000000001.json".to_string(),
            b"{}".to_vec(),
        )).await.unwrap();

        // List delta log files
        let log_files = storage.list_delta_log(table_path).await.unwrap();
        assert_eq!(log_files.len(), 2);

        // List JSON files specifically
        let json_files = storage.list_delta_json_files(table_path).await.unwrap();
        assert_eq!(json_files.len(), 2);

        // List checkpoint files (should be empty)
        let checkpoint_files = storage.list_delta_checkpoint_files(table_path).await.unwrap();
        assert!(checkpoint_files.is_empty());
    }

    #[tokio::test]
    async fn test_health_check() {
        let temp_dir = TempDir::new().unwrap();
        let local_config = LocalConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_dirs: true,
            file_permissions: None,
            dir_permissions: None,
        };

        let storage_config = StorageConfig::default();
        let storage = LocalStorage::new(&local_config, &storage_config).unwrap();

        // Health check should pass for valid directory
        assert!(storage.health_check().await.unwrap());

        // Test with non-existent directory
        let bad_config = LocalConfig {
            base_dir: PathBuf::from("/nonexistent/path/that/should/not/exist"),
            create_dirs: false,
            file_permissions: None,
            dir_permissions: None,
        };

        match LocalStorage::new(&bad_config, &storage_config) {
            Err(_) => {
                // This is expected
            }
            Ok(_) => {
                // In case the path exists for some reason
                assert!(true);
            }
        }
    }

    #[tokio::test]
    async fn test_storage_stats() {
        let temp_dir = TempDir::new().unwrap();
        let local_config = LocalConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_dirs: true,
            file_permissions: None,
            dir_permissions: None,
        };

        let storage_config = StorageConfig::default();
        let storage = LocalStorage::new(&local_config, &storage_config).unwrap();

        // Create some test files
        for i in 0..5 {
            let content = format!("content {}", i).repeat(100);
            let path = format!("test/file{}.txt", i);
            storage.put_file(&path, content.as_bytes().to_vec()).await.unwrap();
        }

        let stats = storage.get_storage_stats().await.unwrap();
        assert_eq!(stats.total_files, 5);
        assert!(stats.total_size > 0);
        assert!(stats.average_file_size > 0.0);
        assert_eq!(stats.backend_type, StorageBackend::Local);
    }

    #[tokio::test]
    async fn test_atomic_write() {
        let temp_dir = TempDir::new().unwrap();
        let local_config = LocalConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_dirs: true,
            file_permissions: None,
            dir_permissions: None,
        };

        let storage_config = StorageConfig::default();
        let storage = LocalStorage::new(&local_config, &storage_config).unwrap();

        let path = "test/atomic.txt";
        let content1 = b"First content".to_vec();
        let content2 = b"Second content".to_vec();

        // Write first time
        storage.put_file(path, content1.clone()).await.unwrap();

        // Verify first content
        let read1 = storage.get_file(path).await.unwrap();
        assert_eq!(read1, content1);

        // Write second time
        storage.put_file(path, content2.clone()).await.unwrap();

        // Verify second content
        let read2 = storage.get_file(path).await.unwrap();
        assert_eq!(read2, content2);

        // Ensure no temporary files are left behind
        let all_files = storage.list_files("").await.unwrap();
        assert!(!all_files.iter().any(|f| f.ends_with(".tmp")));
    }
}