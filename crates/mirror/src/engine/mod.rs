//! Core Delta Mirror engine for orchestrating log mirroring operations

use crate::error::{MirrorError, MirrorResult};
use crate::generators::{DeltaGenerator, DeltaFile, DeltaJsonGenerator};
use crate::pipeline::{MirrorPipeline, MirrorTask};
use crate::storage::MirrorStorage;
use crate::config::MirrorEngineConfig;
use deltalakedb_core::{Table, Commit, Protocol, Metadata, Action};
use std::sync::Arc;
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// Core Delta Mirror engine that orchestrates mirroring operations
pub struct DeltaMirrorEngine {
    config: MirrorEngineConfig,
    storage: Arc<dyn MirrorStorage>,
    json_generator: DeltaJsonGenerator,
}

impl DeltaMirrorEngine {
    /// Create a new mirror engine
    pub async fn new(
        config: MirrorEngineConfig,
        storage: Arc<dyn MirrorStorage>,
    ) -> MirrorResult<Self> {
        let json_generator = DeltaJsonGenerator::with_options(config.json_generation.clone());

        Ok(Self {
            config,
            storage,
            json_generator,
        })
    }

    /// Get the storage backend
    pub fn storage(&self) -> Arc<dyn MirrorStorage> {
        self.storage.clone()
    }

    /// Get the JSON generator
    pub fn json_generator(&self) -> &DeltaJsonGenerator {
        &self.json_generator
    }

    /// Mirror a single commit from SQL to Delta format
    pub async fn mirror_commit(
        &self,
        table_path: &str,
        version: i64,
        commit: &Commit,
        actions: &[Action],
    ) -> MirrorResult<DeltaFile> {
        // Validate inputs
        self.validate_commit_inputs(table_path, version, commit, actions)?;

        // Generate Delta JSON file
        let delta_file = self.json_generator
            .generate_commit_file(table_path, version, actions)?;

        // Write to storage
        self.storage.put_delta_file(table_path, &delta_file).await?;

        tracing::info!(
            "Mirrored commit {} for table {} (size: {} bytes)",
            version,
            table_path,
            delta_file.size
        );

        Ok(delta_file)
    }

    /// Generate Delta metadata file
    pub async fn generate_metadata_file(
        &self,
        table_path: &str,
        version: i64,
        metadata: &Metadata,
    ) -> MirrorResult<DeltaFile> {
        let delta_file = self.json_generator
            .generate_metadata_file(table_path, version, metadata)?;

        self.storage.put_delta_file(table_path, &delta_file).await?;

        tracing::info!(
            "Generated metadata file {} for table {}",
            delta_file.file_name,
            table_path
        );

        Ok(delta_file)
    }

    /// Generate Delta protocol file
    pub async fn generate_protocol_file(
        &self,
        table_path: &str,
        version: i64,
        protocol: &Protocol,
    ) -> MirrorResult<DeltaFile> {
        let delta_file = self.json_generator
            .generate_protocol_file(table_path, version, protocol)?;

        self.storage.put_delta_file(table_path, &delta_file).await?;

        tracing::info!(
            "Generated protocol file {} for table {}",
            delta_file.file_name,
            table_path
        );

        Ok(delta_file)
    }

    /// Validate commit inputs before mirroring
    fn validate_commit_inputs(
        &self,
        table_path: &str,
        version: i64,
        commit: &Commit,
        actions: &[Action],
    ) -> MirrorResult<()> {
        if table_path.is_empty() {
            return Err(MirrorError::validation_error("Table path cannot be empty"));
        }

        if version < 0 {
            return Err(MirrorError::validation_error(
                format!("Commit version must be non-negative: {}", version)
            ));
        }

        if actions.is_empty() {
            tracing::warn!("Commit {} has no actions to mirror", version);
        }

        // Validate action types
        for action in actions {
            match action {
                Action::AddFile(add_file) => {
                    if add_file.path.is_empty() {
                        return Err(MirrorError::validation_error("AddFile action must have a file path"));
                    }
                    if add_file.size < 0 {
                        return Err(MirrorError::validation_error("AddFile size must be non-negative"));
                    }
                }
                Action::RemoveFile(remove_file) => {
                    if remove_file.path.is_empty() {
                        return Err(MirrorError::validation_error("RemoveFile action must have a file path"));
                    }
                    if remove_file.deletion_timestamp < 0 {
                        return Err(MirrorError::validation_error(
                            "RemoveFile deletion timestamp must be non-negative"
                        ));
                    }
                }
                Action::Metadata(_) => {
                    // Metadata action validation would be done by the JSON generator
                }
            }
        }

        Ok(())
    }

    /// Get table information
    pub async fn get_table_info(&self, table_path: &str) -> MirrorResult<TableInfo> {
        // This would integrate with SQL adapters to get table information
        // For now, return a placeholder
        Ok(TableInfo {
            table_path: table_path.to_string(),
            latest_version: 0,
            total_commits: 0,
            last_updated: Utc::now(),
            has_checkpoint: false,
            storage_backend: self.storage.backend_type(),
        })
    }

    /// Check if a table exists in storage
    pub async fn table_exists(&self, table_path: &str) -> MirrorResult<bool> {
        // Check if _delta_log directory exists
        let log_path = format!("{}/_delta_log", table_path.trim_end_matches('/'));
        let log_files = self.storage.list_files(&log_path).await?;

        Ok(!log_files.is_empty())
    }

    /// Get latest version for a table
    pub async fn get_latest_version(&self, table_path: &str) -> MirrorResult<Option<i64>> {
        let json_files = self.storage.list_delta_json_files(table_path).await?;

        if json_files.is_empty() {
            return Ok(None);
        }

        // Parse version numbers from file names
        let mut versions = Vec::new();
        for file_name in json_files {
            if let Some(version_str) = file_name.strip_suffix(".json") {
                if let Ok(version) = version_str.parse::<i64>() {
                    versions.push(version);
                }
            }
        }

        if versions.is_empty() {
            return Ok(None);
        }

        Ok(Some(*versions.iter().max().unwrap()))
    }

    /// Get commit history for a table
    pub async fn get_commit_history(
        &self,
        table_path: &str,
        start_version: Option<i64>,
        limit: Option<usize>,
    ) -> MirrorResult<Vec<CommitInfo>> {
        let json_files = self.storage.list_delta_json_files(table_path).await?;

        // Sort files by version number
        let mut files_with_versions = Vec::new();
        for file_name in json_files {
            if let Some(version_str) = file_name.strip_suffix(".json") {
                if let Ok(version) = version_str.parse::<i64>() {
                    files_with_versions.push((version, file_name));
                }
            }
        }

        files_with_versions.sort_by(|(a, b)| a.cmp(b));
        files_with_versions.reverse(); // Most recent first

        // Apply start version filter
        let start_idx = if let Some(start) = start_version {
            files_with_versions
                .iter()
                .position(|(v, _)| *v <= start)
                .unwrap_or(files_with_versions.len())
        } else {
            0
        };

        // Apply limit
        let end_idx = if let Some(limit) = limit {
            (start_idx + limit).min(files_with_versions.len())
        } else {
            files_with_versions.len()
        };

        let mut commit_infos = Vec::new();
        for (version, file_name) in files_with_versions[start_idx..end_idx] {
            // In a full implementation, we would read and parse the commit file
            // For now, create placeholder commit info
            commit_infos.push(CommitInfo {
                version,
                file_name: file_name.clone(),
                size: 0,
                last_modified: Utc::now(),
                action_count: 0,
            });
        }

        Ok(commit_infos)
    }

    /// Check if a table has checkpoints
    pub async fn has_checkpoints(&self, table_path: &str) -> MirrorResult<bool> {
        let checkpoint_files = self.storage.list_delta_checkpoint_files(table_path).await?;
        Ok(!checkpoint_files.is_empty())
    }

    /// Get checkpoint information for a table
    pub async fn get_checkpoint_info(
        &self,
        table_path: &str,
    ) -> MirrorResult<Vec<CheckpointInfo>> {
        let checkpoint_files = self.storage.list_delta_checkpoint_files(table_path).await?;

        let mut checkpoint_infos = Vec::new();
        for file_name in checkpoint_files {
            if let Some(version_str) = file_name.strip_suffix(".checkpoint.parquet") {
                if let Ok(version) = version_str.parse::<i64>() {
                    checkpoint_infos.push(CheckpointInfo {
                        version,
                        file_name: file_name.clone(),
                        size: 0, // Would read from file
                        created_at: Utc::now(),
                        file_count: 0, // Would read from file
                    });
                }
            }
        }

        checkpoint_infos.sort_by(|a, b| a.version.cmp(&b.version));
        Ok(checkpoint_infos)
    }

    /// Cleanup old Delta log files
    pub async fn cleanup_old_files(
        &self,
        table_path: &str,
        retain_versions: usize,
        retain_age_secs: u64,
    ) -> MirrorResult<CleanupStats> {
        let json_files = self.storage.list_delta_json_files(table_path).await?;
        let checkpoint_files = self.storage.list_delta_checkpoint_files(table_path).await?;

        let cutoff_time = Utc::now() - chrono::Duration::seconds(retain_age_secs as i64);

        let mut files_deleted = 0;
        let mut bytes_freed = 0;

        // Clean up old JSON files
        let mut json_versions = Vec::new();
        for file_name in json_files {
            if let Some(version_str) = file_name.strip_suffix(".json") {
                if let Ok(version) = version_str.parse::<i64>() {
                    json_versions.push((version, file_name));
                }
            }
        }

        json_versions.sort();
        if json_versions.len() > retain_versions {
            for (version, file_name) in json_versions.iter().take(json_versions.len() - retain_versions) {
                // Check file age
                let file_path = format!("{}/_delta_log/{}", table_path.trim_end_matches('/'), file_name);
                if let Ok(metadata) = self.storage.get_file_metadata(&file_path).await {
                    if metadata.last_modified < cutoff_time {
                        self.storage.delete_file(&file_path).await?;
                        files_deleted += 1;
                        bytes_freed += metadata.size;
                    }
                }
            }
        }

        // Clean up old checkpoint files
        let mut checkpoint_versions = Vec::new();
        for file_name in checkpoint_files {
            if let Some(version_str) = file_name.strip_suffix(".checkpoint.parquet") {
                if let Ok(version) = version_str.parse::<i64>() {
                    checkpoint_versions.push((version, file_name));
                }
            }
        }

        checkpoint_versions.sort();
        if checkpoint_versions.len() > retain_versions {
            for (version, file_name) in checkpoint_versions.iter().take(checkpoint_versions.len() - retain_versions) {
                // Check file age
                let file_path = format!("{}/_delta_log/{}", table_path.trim_end_matches('/'), file_name);
                if let Ok(metadata) = self.storage.get_file_metadata(&file_path).await {
                    if metadata.last_modified < cutoff_time {
                        self.storage.delete_file(&file_path).await?;
                        files_deleted += 1;
                        bytes_freed += metadata.size;
                    }
                }
            }
        }

        Ok(CleanupStats {
            files_deleted,
            bytes_freed,
            retention_versions,
            retention_age_secs,
        })
    }
}

/// Table information
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table path
    pub table_path: String,
    /// Latest version number
    pub latest_version: i64,
    /// Total number of commits
    pub total_commits: u64,
    /// Last update time
    pub last_updated: DateTime<Utc>,
    /// Whether table has checkpoints
    pub has_checkpoint: bool,
    /// Storage backend type
    pub storage_backend: crate::storage::StorageBackend,
}

/// Commit information
#[derive(Debug, Clone)]
pub struct CommitInfo {
    /// Commit version
    pub version: i64,
    /// File name
    pub file_name: String,
    /// File size in bytes
    pub size: u64,
    /// Last modified timestamp
    pub last_modified: DateTime<Utc>,
    /// Number of actions in commit
    pub action_count: usize,
}

/// Checkpoint information
#[derive(Debug, Clone)]
pub struct CheckpointInfo {
    /// Checkpoint version
    pub version: i64,
    /// File name
    pub file_name: String,
    /// File size in bytes
    pub size: u64,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Number of files included in checkpoint
    pub file_count: usize,
}

/// Cleanup statistics
#[derive(Debug, Clone)]
pub struct CleanupStats {
    /// Number of files deleted
    pub files_deleted: usize,
    /// Number of bytes freed
    pub bytes_freed: u64,
    /// Number of versions retained
    pub retention_versions: usize,
    /// Retention age in seconds
    pub retention_age_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_creation() {
        let config = MirrorEngineConfig::default();
        let storage = crate::storage::create_test_storage().await.unwrap();

        // This would fail in a test environment without proper async setup
        // In a real test, we'd create a proper storage configuration
    }

    #[tokio::test]
    async fn test_validate_commit_inputs() {
        let storage = crate::storage::create_test_storage().await.unwrap();
        let config = MirrorEngineConfig::default();
        let engine = DeltaMirrorEngine::new(config, storage).await.unwrap();

        // Valid commit
        let commit = deltalakedb_core::Commit {
            id: Uuid::new_v4(),
            table_id: Uuid::new_v4(),
            version: 1,
            timestamp: Utc::now(),
            operation_type: "WRITE".to_string(),
            operation_parameters: serde_json::json!({"mode": "Append"}),
            commit_info: serde_json::json!({}),
        };

        let action = deltalakedb_core::Action::AddFile(deltalakedb_core::AddFile {
            path: "test.parquet".to_string(),
            size: 1024,
            modification_time: Utc::now().timestamp_millis(),
            data_change: true,
            stats: None,
            partition_values: None,
            tags: None,
        });

        let result = engine.validate_commit_inputs("/test/table", 1, &commit, &[action]);
        assert!(result.is_ok());

        // Invalid table path
        let result = engine.validate_commit_inputs("", 1, &commit, &[action]);
        assert!(result.is_err());

        // Invalid version
        let result = engine.validate_commit_inputs("/test/table", -1, &commit, &[action]);
        assert!(result.is_err());

        // Empty file path in AddFile
        let invalid_action = deltalakedb_core::Action::AddFile(deltalakedb_core::AddFile {
            path: "".to_string(),
            size: 1024,
            modification_time: Utc::now().timestamp_millis(),
            data_change: true,
            stats: None,
            partition_values: None,
            tags: None,
        });

        let result = engine.validate_commit_inputs("/test/table", 1, &commit, &[invalid_action]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_table_operations() {
        let storage = crate::storage::create_test_storage().await.unwrap();
        let config = MirrorEngineConfig::default();
        let engine = DeltaMirrorEngine::new(config, storage).await.unwrap();

        // Test with non-existent table
        let exists = engine.table_exists("/nonexistent/table").await.unwrap();
        assert!(!exists);

        let latest_version = engine.get_latest_version("/nonexistent/table").await.unwrap();
        assert!(latest_version.is_none());

        let history = engine.get_commit_history("/nonexistent/table", None, None).await.unwrap();
        assert!(history.is_empty());
    }

    #[tokio::test]
    async fn test_checkpoint_operations() {
        let storage = crate::storage::create_test_storage().await.unwrap();
        let config = MirrorEngineConfig::default();
        let engine = DeltaMirrorEngine::new(config, storage).await.unwrap();

        // Test with non-existent table
        let has_checkpoints = engine.has_checkpoints("/nonexistent/table").await.unwrap();
        assert!(!has_checkpoints);

        let checkpoints = engine.get_checkpoint_info("/nonexistent/table").await.unwrap();
        assert!(checkpoints.is_empty());
    }
}