//! File-based transaction log implementations.

use crate::error::TxnLogResult;
use crate::reader::{TableSnapshot, TxnLogReader, TxnLogReaderExt};
use crate::writer::{Transaction, TxnLogWriter, TxnLogWriterExt};
use crate::{
    ActiveFile, AddFile, DeltaAction, Format, Metadata, Protocol, RemoveFile, TableMetadata,
    TableVersion, Txn,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::fs as async_fs;
use tokio::io::AsyncReadExt;
use tracing::{debug, error, info, warn};

/// File-based transaction log reader.
/// 
/// Reads Delta Lake transaction logs from the `_delta_log` directory,
/// maintaining compatibility with existing Delta tables.
#[derive(Debug, Clone)]
pub struct FileTxnLogReader {
    /// Table location.
    table_location: PathBuf,
    /// Delta log directory.
    log_path: PathBuf,
}

impl FileTxnLogReader {
    /// Create a new file-based reader.
    pub fn new(table_location: impl AsRef<Path>) -> TxnLogResult<Self> {
        let table_location = table_location.as_ref().to_path_buf();
        let log_path = table_location.join("_delta_log");

        if !log_path.exists() {
            return Err(crate::error::TxnLogError::table_not_found(
                log_path.to_string_lossy().to_string(),
            ));
        }

        Ok(Self {
            table_location,
            log_path,
        })
    }

    /// Get table location.
    pub fn table_location(&self) -> &Path {
        &self.table_location
    }

    /// Get log path.
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    /// List all JSON files in the log directory.
    fn list_json_files(&self) -> TxnLogResult<Vec<PathBuf>> {
        let mut files = Vec::new();

        for entry in fs::read_dir(&self.log_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                files.push(path);
            }
        }

        // Sort by version number (filename)
        files.sort_by(|a, b| {
            let a_version = Self::extract_version_from_filename(a);
            let b_version = Self::extract_version_from_filename(b);
            a_version.cmp(&b_version)
        });

        Ok(files)
    }

    /// Extract version number from filename.
    fn extract_version_from_filename(path: &Path) -> i64 {
        let filename = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("0");

        filename.parse::<i64>().unwrap_or(0)
    }

    /// Read actions from a JSON file.
    async fn read_json_file(&self, path: &Path) -> TxnLogResult<Vec<DeltaAction>> {
        let mut content = String::new();
        let mut file = async_fs::File::open(path).await?;
        file.read_to_string(&mut content).await?;

        let mut actions = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<serde_json::Value>(line) {
                Ok(value) => {
                    if let Ok(action) = serde_json::from_value::<DeltaAction>(value) {
                        actions.push(action);
                    } else {
                        warn!("Failed to parse Delta action from line: {}", line);
                    }
                }
                Err(e) => {
                    warn!("Failed to parse JSON line: {} - Error: {}", line, e);
                }
            }
        }

        Ok(actions)
    }

    /// Find the latest checkpoint file.
    fn find_latest_checkpoint(&self) -> TxnLogResult<Option<PathBuf>> {
        let mut checkpoints = Vec::new();

        for entry in fs::read_dir(&self.log_path)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                if filename.ends_with(".checkpoint.parquet") {
                    checkpoints.push(path);
                }
            }
        }

        if checkpoints.is_empty() {
            return Ok(None);
        }

        // Sort by version number
        checkpoints.sort_by(|a, b| {
            let a_version = Self::extract_checkpoint_version(a);
            let b_version = Self::extract_checkpoint_version(b);
            a_version.cmp(&b_version)
        });

        Ok(checkpoints.into_iter().last())
    }

    /// Extract version from checkpoint filename.
    fn extract_checkpoint_version(path: &Path) -> i64 {
        let filename = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("0");

        // Extract version from filename like "00000000000000000010.checkpoint"
        if let Some(version_str) = filename.split('.').next() {
            version_str.parse::<i64>().unwrap_or(0)
        } else {
            0
        }
    }

    /// Calculate active files by replaying actions up to a version.
    async fn calculate_active_files(
        &self,
        table_id: &str,
        target_version: i64,
    ) -> TxnLogResult<Vec<ActiveFile>> {
        let json_files = self.list_json_files()?;
        let mut active_files: HashMap<String, ActiveFile> = HashMap::new();

        for file_path in json_files {
            let version = Self::extract_version_from_filename(&file_path);
            if version > target_version {
                continue;
            }

            let actions = self.read_json_file(&file_path).await?;
            for action in actions {
                match action {
                    DeltaAction::Add(add) => {
                        let active_file = ActiveFile {
                            path: add.path.clone(),
                            size: add.size,
                            modification_time: add.modification_time,
                            partition_values: add.partition_values,
                            stats: add.stats,
                            version,
                        };
                        active_files.insert(add.path, active_file);
                    }
                    DeltaAction::Remove(remove) => {
                        active_files.remove(&remove.path);
                    }
                    _ => {
                        // Other actions don't affect active files
                    }
                }
            }
        }

        Ok(active_files.into_values().collect())
    }

    /// Get table metadata from actions.
    async fn extract_metadata_from_actions(
        &self,
        table_id: &str,
        version: i64,
    ) -> TxnLogResult<(Metadata, Protocol)> {
        let json_files = self.list_json_files()?;
        let mut current_metadata: Option<Metadata> = None;
        let mut current_protocol: Option<Protocol> = None;

        for file_path in json_files {
            let file_version = Self::extract_version_from_filename(&file_path);
            if file_version > version {
                continue;
            }

            let actions = self.read_json_file(&file_path).await?;
            for action in actions {
                match action {
                    DeltaAction::Metadata(metadata) => {
                        current_metadata = Some(metadata);
                    }
                    DeltaAction::Protocol(protocol) => {
                        current_protocol = Some(protocol);
                    }
                    _ => {}
                }
            }
        }

        let metadata = current_metadata.ok_or_else(|| {
            crate::error::TxnLogError::protocol("No metadata found in transaction log")
        })?;

        let protocol = current_protocol.unwrap_or_else(|| Protocol::new(1, 2));

        Ok((metadata, protocol))
    }
}

#[async_trait]
impl TxnLogReader for FileTxnLogReader {
    async fn get_table_metadata(&self, table_id: &str) -> TxnLogResult<TableMetadata> {
        let latest_version = self.get_latest_version(table_id).await?;
        self.get_table_metadata_at_version(table_id, latest_version).await
    }

    async fn get_table_metadata_at_version(
        &self,
        table_id: &str,
        version: i64,
    ) -> TxnLogResult<TableMetadata> {
        let (metadata, protocol) = self
            .extract_metadata_from_actions(table_id, version)
            .await?;

        Ok(TableMetadata {
            table_id: table_id.to_string(),
            name: table_id.to_string(), // TODO: Extract from actual metadata
            location: self.table_location.to_string_lossy().to_string(),
            version,
            protocol,
            metadata,
            created_at: Utc::now(), // TODO: Extract from actual metadata
        })
    }

    async fn get_table_metadata_at_timestamp(
        &self,
        table_id: &str,
        timestamp: DateTime<Utc>,
    ) -> TxnLogResult<TableMetadata> {
        let version = self.get_version_at_timestamp(table_id, timestamp).await?;
        self.get_table_metadata_at_version(table_id, version).await
    }

    async fn get_latest_version(&self, table_id: &str) -> TxnLogResult<i64> {
        let json_files = self.list_json_files()?;
        if json_files.is_empty() {
            return Err(crate::error::TxnLogError::table_not_found(table_id.to_string()));
        }

        let latest_file = json_files.last().unwrap();
        Ok(Self::extract_version_from_filename(latest_file))
    }

    async fn get_version_at_timestamp(
        &self,
        table_id: &str,
        timestamp: DateTime<Utc>,
    ) -> TxnLogResult<i64> {
        let json_files = self.list_json_files()?;
        let mut target_version = 0;

        for file_path in json_files {
            let version = Self::extract_version_from_filename(&file_path);
            // TODO: Extract actual timestamp from file metadata
            // For now, assume files are in chronological order
            target_version = version;
        }

        Ok(target_version)
    }

    async fn list_active_files(&self, table_id: &str) -> TxnLogResult<Vec<ActiveFile>> {
        let latest_version = self.get_latest_version(table_id).await?;
        self.list_active_files_at_version(table_id, latest_version).await
    }

    async fn list_active_files_at_version(
        &self,
        table_id: &str,
        version: i64,
    ) -> TxnLogResult<Vec<ActiveFile>> {
        self.calculate_active_files(table_id, version).await
    }

    async fn list_active_files_at_timestamp(
        &self,
        table_id: &str,
        timestamp: DateTime<Utc>,
    ) -> TxnLogResult<Vec<ActiveFile>> {
        let version = self.get_version_at_timestamp(table_id, timestamp).await?;
        self.list_active_files_at_version(table_id, version).await
    }

    async fn get_version_actions(
        &self,
        table_id: &str,
        version: i64,
    ) -> TxnLogResult<Vec<DeltaAction>> {
        let json_files = self.list_json_files()?;
        
        for file_path in json_files {
            let file_version = Self::extract_version_from_filename(&file_path);
            if file_version == version {
                return self.read_json_file(&file_path).await;
            }
        }

        Err(crate::error::TxnLogError::invalid_version(version))
    }

    async fn get_table_version(
        &self,
        table_id: &str,
        version: i64,
    ) -> TxnLogResult<TableVersion> {
        let actions = self.get_version_actions(table_id, version).await?;
        let operation = actions
            .iter()
            .find_map(|action| match action {
                DeltaAction::Add(_) => Some("WRITE".to_string()),
                DeltaAction::Remove(_) => Some("DELETE".to_string()),
                DeltaAction::Metadata(_) => Some("SET_METADATA".to_string()),
                DeltaAction::Protocol(_) => Some("SET_PROTOCOL".to_string()),
                DeltaAction::Transaction(_) => Some("TXN".to_string()),
            });

        Ok(TableVersion {
            table_id: table_id.to_string(),
            version,
            committed_at: Utc::now(), // TODO: Extract from file metadata
            committer: None,          // TODO: Extract from file metadata
            operation,
            operation_params: None,
        })
    }

    async fn list_table_versions(&self, table_id: &str) -> TxnLogResult<Vec<TableVersion>> {
        let json_files = self.list_json_files()?;
        let mut versions = Vec::new();

        for file_path in json_files {
            let version = Self::extract_version_from_filename(&file_path);
            let table_version = self.get_table_version(table_id, version).await?;
            versions.push(table_version);
        }

        Ok(versions)
    }

    async fn table_exists(&self, table_id: &str) -> TxnLogResult<bool> {
        Ok(self.log_path.exists())
    }

    async fn get_table_history(
        &self,
        table_id: &str,
        start_version: Option<i64>,
        end_version: Option<i64>,
    ) -> TxnLogResult<Vec<TableVersion>> {
        let all_versions = self.list_table_versions(table_id).await?;
        let start = start_version.unwrap_or(0);
        let end = end_version.unwrap_or(i64::MAX);

        let filtered_versions = all_versions
            .into_iter()
            .filter(|v| v.version >= start && v.version <= end)
            .collect();

        Ok(filtered_versions)
    }

    async fn get_table_history_by_timestamp(
        &self,
        table_id: &str,
        start_timestamp: Option<DateTime<Utc>>,
        end_timestamp: Option<DateTime<Utc>>,
    ) -> TxnLogResult<Vec<TableVersion>> {
        let all_versions = self.list_table_versions(table_id).await?;
        let start = start_timestamp.unwrap_or(DateTime::<Utc>::MIN_UTC);
        let end = end_timestamp.unwrap_or(DateTime::<Utc>::MAX_UTC);

        let filtered_versions = all_versions
            .into_iter()
            .filter(|v| v.committed_at >= start && v.committed_at <= end)
            .collect();

        Ok(filtered_versions)
    }
}

/// File-based transaction log writer.
/// 
/// Writes Delta Lake transaction logs to `_delta_log` directory,
/// maintaining compatibility with existing Delta tables.
#[derive(Debug, Clone)]
pub struct FileTxnLogWriter {
    /// Table location.
    table_location: PathBuf,
    /// Delta log directory.
    log_path: PathBuf,
}

impl FileTxnLogWriter {
    /// Create a new file-based writer.
    pub fn new(table_location: impl AsRef<Path>) -> TxnLogResult<Self> {
        let table_location = table_location.as_ref().to_path_buf();
        let log_path = table_location.join("_delta_log");

        // Create log directory if it doesn't exist
        fs::create_dir_all(&log_path)?;

        Ok(Self {
            table_location,
            log_path,
        })
    }

    /// Get table location.
    pub fn table_location(&self) -> &Path {
        &self.table_location
    }

    /// Get log path.
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    /// Generate filename for a version.
    fn version_filename(version: i64) -> String {
        format!("{:020}.json", version)
    }

    /// Write actions to a JSON file.
    async fn write_actions_to_file(
        &self,
        version: i64,
        actions: &[DeltaAction],
    ) -> TxnLogResult<()> {
        let filename = Self::version_filename(version);
        let file_path = self.log_path.join(filename);

        let mut content = String::new();
        for action in actions {
            let json_line = serde_json::to_string(action)?;
            content.push_str(&json_line);
            content.push('\n');
        }

        // Write to temporary file first, then rename for atomicity
        let temp_path = file_path.with_extension("tmp");
        async_fs::write(&temp_path, content).await?;

        // Atomic rename
        async_fs::rename(&temp_path, &file_path).await?;

        Ok(())
    }

    /// Read current latest version.
    async fn read_current_version(&self) -> TxnLogResult<i64> {
        let json_files = self.list_json_files()?;
        if json_files.is_empty() {
            return Ok(-1); // No versions yet
        }

        let latest_file = json_files.last().unwrap();
        Ok(Self::extract_version_from_filename(latest_file))
    }

    /// List all JSON files in log directory.
    fn list_json_files(&self) -> TxnLogResult<Vec<PathBuf>> {
        let mut files = Vec::new();

        for entry in fs::read_dir(&self.log_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                files.push(path);
            }
        }

        // Sort by version number (filename)
        files.sort_by(|a, b| {
            let a_version = Self::extract_version_from_filename(a);
            let b_version = Self::extract_version_from_filename(b);
            a_version.cmp(&b_version)
        });

        Ok(files)
    }

    /// Extract version number from filename.
    fn extract_version_from_filename(path: &Path) -> i64 {
        let filename = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("0");

        filename.parse::<i64>().unwrap_or(0)
    }
}

#[async_trait]
impl TxnLogWriter for FileTxnLogWriter {
    async fn begin_transaction(&self, table_id: &str) -> TxnLogResult<Transaction> {
        let current_version = self.read_current_version().await?;
        let transaction_id = format!("{}-{}", table_id, uuid::Uuid::new_v4());

        Ok(Transaction::new(
            table_id.to_string(),
            transaction_id,
            current_version,
        ))
    }

    async fn commit_actions(
        &self,
        table_id: &str,
        actions: Vec<DeltaAction>,
        operation: Option<String>,
        operation_params: Option<HashMap<String, String>>,
    ) -> TxnLogResult<i64> {
        let current_version = self.read_current_version().await?;
        let next_version = current_version + 1;

        self.commit_actions_with_version(
            table_id,
            next_version,
            actions,
            operation,
            operation_params,
        )
        .await?;

        Ok(next_version)
    }

    async fn commit_actions_with_version(
        &self,
        table_id: &str,
        version: i64,
        actions: Vec<DeltaAction>,
        operation: Option<String>,
        operation_params: Option<HashMap<String, String>>,
    ) -> TxnLogResult<()> {
        // Validate version
        let current_version = self.read_current_version().await?;
        if version != current_version + 1 {
            return Err(crate::error::TxnLogError::version_conflict(
                current_version + 1,
                version,
            ));
        }

        // Write actions to file
        self.write_actions_to_file(version, &actions).await?;

        info!(
            "Committed {} actions for table {} at version {}",
            actions.len(),
            table_id,
            version
        );

        Ok(())
    }

    async fn create_table(
        &self,
        table_id: &str,
        name: &str,
        location: &str,
        metadata: TableMetadata,
    ) -> TxnLogResult<()> {
        // Create initial metadata and protocol actions
        let metadata_action = DeltaAction::Metadata(metadata.metadata);
        let protocol_action = DeltaAction::Protocol(metadata.protocol);

        let actions = vec![metadata_action, protocol_action];

        self.commit_actions(table_id, actions, Some("CREATE".to_string()), None)
            .await?;

        Ok(())
    }

    async fn update_table_metadata(
        &self,
        table_id: &str,
        metadata: TableMetadata,
    ) -> TxnLogResult<()> {
        let action = DeltaAction::Metadata(metadata.metadata);
        self.commit_actions(table_id, vec![action], Some("SET_METADATA".to_string()), None)
            .await?;
        Ok(())
    }

    async fn delete_table(&self, table_id: &str) -> TxnLogResult<()> {
        // For file-based implementation, we could remove the log directory
        // but for safety, we'll just mark it as deleted
        warn!("Table deletion not implemented for file-based writer");
        Ok(())
    }

    async fn get_next_version(&self, table_id: &str) -> TxnLogResult<i64> {
        let current_version = self.read_current_version().await?;
        Ok(current_version + 1)
    }

    async fn table_exists(&self, table_id: &str) -> TxnLogResult<bool> {
        Ok(self.log_path.exists())
    }

    async fn get_table_metadata(&self, table_id: &str) -> TxnLogResult<TableMetadata> {
        let reader = FileTxnLogReader::new(&self.table_location)?;
        reader.get_table_metadata(table_id).await
    }

    async fn vacuum(
        &self,
        table_id: &str,
        retain_last_n_versions: Option<i64>,
        retain_hours: Option<i64>,
    ) -> TxnLogResult<()> {
        warn!("Vacuum not implemented for file-based writer");
        Ok(())
    }

    async fn optimize(
        &self,
        table_id: &str,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<i32>,
    ) -> TxnLogResult<()> {
        warn!("Optimize not implemented for file-based writer");
        Ok(())
    }
}

#[async_trait::async_trait]
impl TxnLogReaderExt for FileTxnLogReader {
    async fn get_latest_snapshot(&self, table_id: &str) -> TxnLogResult<TableSnapshot> {
        let metadata = self.get_table_metadata(table_id).await?;
        let files = self.list_active_files(table_id).await?;
        let version = metadata.version;
        let timestamp = metadata.created_at;

        Ok(TableSnapshot::new(metadata, files, version, timestamp))
    }
}

#[async_trait::async_trait]
impl TxnLogWriterExt for FileTxnLogWriter {
    async fn add_files(
        &self,
        table_id: &str,
        files: Vec<crate::AddFile>,
    ) -> TxnLogResult<i64> {
        let actions: Vec<DeltaAction> = files
            .into_iter()
            .map(DeltaAction::Add)
            .collect();

        self.commit_actions(
            table_id,
            actions,
            Some("WRITE".to_string()),
            None,
        )
        .await
    }

    async fn remove_files(
        &self,
        table_id: &str,
        files: Vec<crate::RemoveFile>,
    ) -> TxnLogResult<i64> {
        let actions: Vec<DeltaAction> = files
            .into_iter()
            .map(DeltaAction::Remove)
            .collect();

        self.commit_actions(
            table_id,
            actions,
            Some("DELETE".to_string()),
            None,
        )
        .await
    }

    async fn update_metadata(
        &self,
        table_id: &str,
        metadata: crate::Metadata,
    ) -> TxnLogResult<i64> {
        let actions = vec![DeltaAction::Metadata(metadata)];

        self.commit_actions(
            table_id,
            actions,
            Some("SET_METADATA".to_string()),
            None,
        )
        .await
    }

    async fn update_protocol(
        &self,
        table_id: &str,
        protocol: crate::Protocol,
    ) -> TxnLogResult<i64> {
        let actions = vec![DeltaAction::Protocol(protocol)];

        self.commit_actions(
            table_id,
            actions,
            Some("SET_PROTOCOL".to_string()),
            None,
        )
        .await
    }
}