//! Mirror engine for object storage backends.

use async_trait::async_trait;
use deltalakedb_core::error::{TxnLogError, TxnLogResult};
use deltalakedb_core::{DeltaAction, AddFile, RemoveFile, Metadata, Protocol, Txn};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

/// Serialize Delta actions to canonical Delta JSON format.
pub fn serialize_delta_json(actions: &[DeltaAction]) -> TxnLogResult<String> {
    let mut json_lines = Vec::new();
    
    for action in actions {
        let json_line = serialize_single_action(action)?;
        json_lines.push(json_line);
    }
    
    Ok(json_lines.join("\n"))
}

/// Serialize a single Delta action to JSON line format.
fn serialize_single_action(action: &DeltaAction) -> TxnLogResult<String> {
    match action {
        DeltaAction::Add(add_file) => {
            let mut json = serde_json::Map::new();
            json.insert("add".to_string(), serde_json::to_value(add_file)
                .map_err(|e| TxnLogError::serialization(format!("Failed to serialize AddFile: {}", e)))?);
            Ok(serde_json::to_string(&json)
                .map_err(|e| TxnLogError::serialization(format!("Failed to serialize AddFile JSON: {}", e)))?)
        }
        DeltaAction::Remove(remove_file) => {
            let mut json = serde_json::Map::new();
            json.insert("remove".to_string(), serde_json::to_value(remove_file)
                .map_err(|e| TxnLogError::serialization(format!("Failed to serialize RemoveFile: {}", e)))?);
            Ok(serde_json::to_string(&json)
                .map_err(|e| TxnLogError::serialization(format!("Failed to serialize RemoveFile JSON: {}", e)))?)
        }
        DeltaAction::Metadata(metadata) => {
            let mut json = serde_json::Map::new();
            json.insert("metaData".to_string(), serde_json::to_value(metadata)
                .map_err(|e| TxnLogError::serialization(format!("Failed to serialize Metadata: {}", e)))?);
            Ok(serde_json::to_string(&json)
                .map_err(|e| TxnLogError::serialization(format!("Failed to serialize Metadata JSON: {}", e)))?)
        }
        DeltaAction::Protocol(protocol) => {
            let mut json = serde_json::Map::new();
            json.insert("protocol".to_string(), serde_json::to_value(protocol)
                .map_err(|e| TxnLogError::serialization(format!("Failed to serialize Protocol: {}", e)))?);
            Ok(serde_json::to_string(&json)
                .map_err(|e| TxnLogError::serialization(format!("Failed to serialize Protocol JSON: {}", e)))?)
        }
        DeltaAction::Transaction(txn) => {
            let mut json = serde_json::Map::new();
            json.insert("txn".to_string(), serde_json::to_value(txn)
                .map_err(|e| TxnLogError::serialization(format!("Failed to serialize Txn: {}", e)))?);
            Ok(serde_json::to_string(&json)
                .map_err(|e| TxnLogError::serialization(format!("Failed to serialize Txn JSON: {}", e)))?)
        }
    }
}

/// Create Delta commit file content for a specific version.
pub fn create_delta_commit_file(table_id: &str, version: i64, actions: &[DeltaAction]) -> TxnLogResult<String> {
    let json_content = serialize_delta_json(actions)?;
    
    // Validate that we have at least one action
    if actions.is_empty() {
        return Err(TxnLogError::invalid_argument("Commit files must contain at least one action"));
    }
    
    // Log the commit creation for debugging
    tracing::debug!(
        table_id = %table_id,
        version = %version,
        action_count = %actions.len(),
        "Creating Delta commit file"
    );
    
    Ok(json_content)
}

/// Generate the path for a Delta commit file.
pub fn delta_commit_path(table_id: &str, version: i64) -> String {
    format!("_delta_log/{:020}.json", version)
}

/// Generate the path for a Delta checkpoint file.
pub fn delta_checkpoint_path(table_id: &str, version: i64) -> String {
    format!("_delta_log/{:020}.checkpoint.parquet", version)
}

/// Generate the path for a Delta sidecar file.
pub fn delta_sidecar_path(table_id: &str, version: i64, part: u32) -> String {
    format!("_delta_log/{:020}.{:05}.checkpoint.parquet", version, part)
}

/// Generate Parquet checkpoint data from SQL actions.
pub async fn create_parquet_checkpoint(
    table_id: &str,
    version: i64,
    actions: &[DeltaAction],
) -> TxnLogResult<Vec<u8>> {
    use arrow::array::{Array, ArrayRef, StringArray, Int64Array, BooleanArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::{ArrowWriter, ArrowReader};
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use bytes::Bytes;

    tracing::debug!(
        table_id = %table_id,
        version = %version,
        action_count = %actions.len(),
        "Creating Parquet checkpoint"
    );

    // Define checkpoint schema based on Delta Lake specification
    let schema = Arc::new(Schema::new(vec![
        Field::new("action", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, true), // JSON serialized action data
        Field::new("table_id", DataType::Utf8, false),
        Field::new("version", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let mut action_strings: Vec<String> = Vec::with_capacity(actions.len());
    let mut action_types: Vec<String> = Vec::with_capacity(actions.len());
    let mut table_ids: Vec<String> = Vec::with_capacity(actions.len());
    let mut versions: Vec<i64> = Vec::with_capacity(actions.len());
    let mut timestamps: Vec<i64> = Vec::with_capacity(actions.len());

    let current_timestamp = chrono::Utc::now().timestamp();

    for action in actions {
        let action_type = action.action_type();
        let action_json = serialize_single_action(action)?;
        
        action_types.push(action_type.to_string());
        action_strings.push(action_json);
        table_ids.push(table_id.to_string());
        versions.push(version);
        timestamps.push(current_timestamp);
    }

    // Create Arrow arrays
    let action_type_array = StringArray::from(action_types);
    let data_array = StringArray::from(action_strings);
    let table_id_array = StringArray::from(table_ids);
    let version_array = Int64Array::from(versions);
    let timestamp_array = Int64Array::from(timestamps);

    // Create record batch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(action_type_array) as ArrayRef,
            Arc::new(data_array) as ArrayRef,
            Arc::new(table_id_array) as ArrayRef,
            Arc::new(version_array) as ArrayRef,
            Arc::new(timestamp_array) as ArrayRef,
        ],
    ).map_err(|e| TxnLogError::Internal(format!("Failed to create Arrow record batch: {}", e)))?;

    // Write to Parquet in memory
    let mut buffer = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    {
        let mut writer = ArrowWriter::try_new(
            &mut buffer,
            schema,
            Some(props),
        ).map_err(|e| TxnLogError::Internal(format!("Failed to create Parquet writer: {}", e)))?;

        writer.write(&batch)
            .map_err(|e| TxnLogError::Internal(format!("Failed to write to Parquet: {}", e)))?;

        writer.close()
            .map_err(|e| TxnLogError::Internal(format!("Failed to close Parquet writer: {}", e)))?;
    }

    tracing::debug!(
        table_id = %table_id,
        version = %version,
        size_bytes = %buffer.len(),
        "Created Parquet checkpoint"
    );

    Ok(buffer)
}

/// Create Delta checkpoint metadata for sidecar files.
pub fn create_checkpoint_metadata(
    table_id: &str,
    version: i64,
    parts: &[String],
    size_bytes: u64,
) -> TxnLogResult<String> {
    let metadata = serde_json::json!({
        "checkpoint": {
            "version": version,
            "parts": parts,
            "size": size_bytes,
            "table_id": table_id,
            "timestamp": chrono::Utc::now().timestamp()
        }
    });

    serde_json::to_string(&metadata)
        .map_err(|e| TxnLogError::serialization(format!("Failed to serialize checkpoint metadata: {}", e)))
}

/// Determine if checkpoint should be created based on version interval.
pub fn should_create_checkpoint(version: i64, checkpoint_interval: i64) -> bool {
    version > 0 && (version % checkpoint_interval == 0)
}

/// Calculate optimal checkpoint size based on action count.
pub fn calculate_checkpoint_size(action_count: usize) -> TxnLogResult<(usize, u32)> {
    // Delta Lake checkpoint sizing guidelines:
    // - Target ~10-50MB per checkpoint file
    // - Each action ~1-2KB when serialized
    // - Split into sidecars if too large
    
    const TARGET_ACTIONS_PER_FILE: usize = 10000; // ~10-20MB per file
    const MAX_ACTIONS_PER_FILE: usize = 50000;  // ~50MB per file
    
    if action_count <= TARGET_ACTIONS_PER_FILE {
        return Ok((action_count, 1));
    }

    let parts = ((action_count as f64) / (TARGET_ACTIONS_PER_FILE as f64)).ceil() as u32;
    let actions_per_part = (action_count + parts as usize - 1) / parts as usize;

    if actions_per_part > MAX_ACTIONS_PER_FILE {
        return Err(TxnLogError::Validation(
            format!("Too many actions for checkpoint: {} (max: {})", 
                   action_count, MAX_ACTIONS_PER_FILE * parts as usize)
        ));
    }

    Ok((actions_per_part, parts))
}

/// Checkpoint cadence configuration.
#[derive(Debug, Clone)]
pub struct CheckpointCadenceConfig {
    /// Interval for version-based checkpoints
    pub version_interval: i64,
    /// Interval for time-based checkpoints (seconds)
    pub time_interval_secs: i64,
    /// Minimum number of actions to trigger checkpoint
    pub min_actions: usize,
    /// Maximum size of accumulated actions before forced checkpoint
    pub max_size_mb: u64,
    /// Enable time-based checkpointing
    pub enable_time_based: bool,
    /// Enable size-based checkpointing
    pub enable_size_based: bool,
}

impl Default for CheckpointCadenceConfig {
    fn default() -> Self {
        Self {
            version_interval: 10,        // Every 10 versions
            time_interval_secs: 3600,    // Every hour
            min_actions: 100,            // At least 100 actions
            max_size_mb: 100,           // 100MB max
            enable_time_based: true,
            enable_size_based: true,
        }
    }
}

/// Checkpoint state tracking.
#[derive(Debug, Clone)]
pub struct CheckpointState {
    /// Last checkpoint version
    pub last_checkpoint_version: i64,
    /// Last checkpoint timestamp
    pub last_checkpoint_time: chrono::DateTime<chrono::Utc>,
    /// Actions accumulated since last checkpoint
    pub accumulated_actions: usize,
    /// Size of accumulated actions in bytes
    pub accumulated_size_bytes: u64,
    /// Total checkpoints created
    pub total_checkpoints: u64,
}

impl Default for CheckpointState {
    fn default() -> Self {
        Self {
            last_checkpoint_version: 0,
            last_checkpoint_time: chrono::Utc::now(),
            accumulated_actions: 0,
            accumulated_size_bytes: 0,
            total_checkpoints: 0,
        }
    }
}

/// Checkpoint cadence manager.
#[derive(Debug)]
pub struct CheckpointCadenceManager {
    config: CheckpointCadenceConfig,
    state: CheckpointState,
}

impl CheckpointCadenceManager {
    /// Create new checkpoint cadence manager.
    pub fn new(config: CheckpointCadenceConfig) -> Self {
        Self {
            config,
            state: CheckpointState::default(),
        }
    }
    
    /// Get current checkpoint state.
    pub fn state(&self) -> &CheckpointState {
        &self.state
    }
    
    /// Update state with new actions.
    pub fn update_actions(&mut self, action_count: usize, size_bytes: u64) {
        self.state.accumulated_actions += action_count;
        self.state.accumulated_size_bytes += size_bytes;
    }
    
    /// Update state after checkpoint creation.
    pub fn update_checkpoint(&mut self, version: i64) {
        self.state.last_checkpoint_version = version;
        self.state.last_checkpoint_time = chrono::Utc::now();
        self.state.accumulated_actions = 0;
        self.state.accumulated_size_bytes = 0;
        self.state.total_checkpoints += 1;
    }
    
    /// Determine if checkpoint should be created.
    pub fn should_create_checkpoint(&self, version: i64) -> CheckpointReason {
        let mut reasons = Vec::new();
        
        // Version-based checkpointing
        if version > 0 && (version - self.state.last_checkpoint_version) >= self.config.version_interval {
            reasons.push(C checkpointReason::VersionInterval);
        }
        
        // Time-based checkpointing
        if self.config.enable_time_based {
            let elapsed_secs = (chrono::Utc::now() - self.state.last_checkpoint_time).num_seconds();
            if elapsed_secs >= self.config.time_interval_secs {
                reasons.push(CheckpointReason::TimeInterval);
            }
        }
        
        // Action count-based checkpointing
        if self.state.accumulated_actions >= self.config.min_actions {
            reasons.push(CheckpointReason::ActionCount);
        }
        
        // Size-based checkpointing
        if self.config.enable_size_based {
            let size_mb = self.state.accumulated_size_bytes / (1024 * 1024);
            if size_mb >= self.config.max_size_mb {
                reasons.push(CheckpointReason::SizeLimit);
            }
        }
        
        // Return the highest priority reason
        reasons.into_iter().max_by_key(|reason| reason.priority()).unwrap_or(CheckpointReason::None)
    }
    
    /// Get checkpoint statistics.
    pub fn get_stats(&self) -> CheckpointStats {
        CheckpointStats {
            total_checkpoints: self.state.total_checkpoints,
            last_checkpoint_version: self.state.last_checkpoint_version,
            last_checkpoint_time: self.state.last_checkpoint_time,
            accumulated_actions: self.state.accumulated_actions,
            accumulated_size_mb: self.state.accumulated_size_bytes / (1024 * 1024),
            next_checkpoint_version: self.state.last_checkpoint_version + self.config.version_interval,
        }
    }
}

/// Reason for checkpoint creation.
#[derive(Debug, Clone, PartialEq)]
pub enum CheckpointReason {
    /// No checkpoint needed
    None,
    /// Version interval reached
    VersionInterval,
    /// Time interval reached
    TimeInterval,
    /// Minimum action count reached
    ActionCount,
    /// Size limit reached
    SizeLimit,
}

impl CheckpointReason {
    /// Get priority of this reason (higher = more important).
    pub fn priority(&self) -> u8 {
        match self {
            CheckpointReason::None => 0,
            CheckpointReason::VersionInterval => 3,
            CheckpointReason::SizeLimit => 4, // Highest priority - safety
            CheckpointReason::ActionCount => 2,
            CheckpointReason::TimeInterval => 1,
        }
    }
    
    /// Get description of this reason.
    pub fn description(&self) -> &'static str {
        match self {
            CheckpointReason::None => "No checkpoint needed",
            CheckpointReason::VersionInterval => "Version interval reached",
            CheckpointReason::TimeInterval => "Time interval reached",
            CheckpointReason::ActionCount => "Minimum action count reached",
            CheckpointReason::SizeLimit => "Size limit reached",
        }
    }
}

/// Checkpoint statistics.
#[derive(Debug, Clone)]
pub struct CheckpointStats {
    /// Total checkpoints created
    pub total_checkpoints: u64,
    /// Last checkpoint version
    pub last_checkpoint_version: i64,
    /// Last checkpoint timestamp
    pub last_checkpoint_time: chrono::DateTime<chrono::Utc>,
    /// Actions accumulated since last checkpoint
    pub accumulated_actions: usize,
    /// Size accumulated since last checkpoint (MB)
    pub accumulated_size_mb: u64,
    /// Next checkpoint version (if version-based)
    pub next_checkpoint_version: i64,
}

/// Result of a mirroring operation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MirroringResult {
    /// Table identifier
    pub table_id: String,
    /// Version that was mirrored
    pub version: i64,
    /// Whether the operation was successful
    pub success: bool,
    /// Error message if operation failed
    pub error: Option<String>,
    /// Duration of the operation
    pub duration: Duration,
    /// Number of files mirrored
    pub files_count: u64,
    /// Total size of mirrored files in bytes
    pub total_size: u64,
}

/// Configuration for mirroring operations.
#[derive(Debug, Clone)]
pub struct MirroringConfig {
    /// Enable automatic mirroring after commits
    pub enable_auto_mirroring: bool,
    /// Maximum number of concurrent mirroring operations
    pub max_concurrent_mirrors: u32,
    /// Timeout for mirroring operations in seconds
    pub mirroring_timeout_secs: u64,
    /// Retry configuration
    pub retry_config: MirroringRetryConfig,
    /// Object storage configuration
    pub storage_config: ObjectStorageConfig,
}

impl Default for MirroringConfig {
    fn default() -> Self {
        Self {
            enable_auto_mirroring: true,
            max_concurrent_mirrors: 5,
            mirroring_timeout_secs: 300, // 5 minutes
            retry_config: MirroringRetryConfig::default(),
            storage_config: ObjectStorageConfig::default(),
        }
    }
}

/// Retry configuration for mirroring operations.
#[derive(Debug, Clone)]
pub struct MirroringRetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Base delay for exponential backoff in milliseconds
    pub base_delay_ms: u64,
    /// Maximum delay for exponential backoff in milliseconds
    pub max_delay_ms: u64,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
}

impl Default for MirroringRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        }
    }
}

/// Configuration for object storage backends.
#[derive(Debug, Clone)]
pub struct ObjectStorageConfig {
    /// Storage backend type
    pub backend_type: StorageBackend,
    /// Connection endpoint
    pub endpoint: String,
    /// Access key
    pub access_key: String,
    /// Secret key
    pub secret_key: String,
    /// Bucket name
    pub bucket: String,
    /// Region (for AWS S3)
    pub region: Option<String>,
    /// Additional configuration parameters
    pub additional_params: HashMap<String, String>,
}

impl Default for ObjectStorageConfig {
    fn default() -> Self {
        Self {
            backend_type: StorageBackend::Local,
            endpoint: "http://localhost:9000".to_string(),
            access_key: "".to_string(),
            secret_key: "".to_string(),
            bucket: "deltalakedb".to_string(),
            region: None,
            additional_params: HashMap::new(),
        }
    }
}

/// Supported storage backends.
#[derive(Debug, Clone, PartialEq)]
pub enum StorageBackend {
    /// Local filesystem
    Local,
    /// Amazon S3
    S3,
    /// MinIO
    MinIO,
    /// SeaweedFS
    SeaweedFS,
    /// RustFS
    RustFS,
    /// Garage
    Garage,
}

/// Mirror engine trait for different storage backends.
#[async_trait]
pub trait MirrorEngine: Send + Sync + std::fmt::Debug {
    /// Mirror files for a specific table version.
    async fn mirror_table_version(
        &self,
        table_id: &str,
        version: i64,
        files: &[deltalakedb_core::DeltaAction],
    ) -> TxnLogResult<MirroringResult>;

    /// Check if a file exists in the mirror.
    async fn file_exists(&self, path: &Path) -> TxnLogResult<bool>;

    /// Get file metadata from the mirror.
    async fn get_file_metadata(&self, path: &Path) -> TxnLogResult<FileMetadata>;

    /// Delete a file from the mirror.
    async fn delete_file(&self, path: &Path) -> TxnLogResult<()>;

    /// List files in a directory.
    async fn list_files(&self, prefix: &Path) -> TxnLogResult<Vec<String>>;

    /// Get mirror engine status.
    async fn get_status(&self) -> TxnLogResult<MirrorStatus>;
}

/// File metadata from the mirror.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// File path
    pub path: String,
    /// File size in bytes
    pub size: u64,
    /// Last modified timestamp
    pub last_modified: chrono::DateTime<chrono::Utc>,
    /// ETag if available
    pub etag: Option<String>,
    /// Content type
    pub content_type: Option<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Mirror engine status.
#[derive(Debug, Clone)]
pub struct MirrorStatus {
    /// Whether the mirror is healthy
    pub healthy: bool,
    /// Backend type
    pub backend_type: StorageBackend,
    /// Endpoint
    pub endpoint: String,
    /// Last successful operation time
    pub last_success: Option<chrono::DateTime<chrono::Utc>>,
    /// Total operations performed
    pub total_operations: u64,
    /// Failed operations
    pub failed_operations: u64,
    /// Average operation duration
    pub avg_duration_ms: u64,
    /// Additional status information
    pub details: HashMap<String, String>,
}

/// Object storage client for different backends.
#[derive(Debug)]
pub struct ObjectStorageClient {
    config: ObjectStorageConfig,
    client: Box<dyn ObjectStorageBackend>,
}

/// Trait for object storage backends.
#[async_trait]
pub trait ObjectStorageBackend: Send + Sync + std::fmt::Debug {
    /// Write data to object storage.
    async fn write(&self, path: &str, data: Vec<u8>) -> TxnLogResult<()>;
    
    /// Read data from object storage.
    async fn read(&self, path: &str) -> TxnLogResult<Vec<u8>>;
    
    /// Check if object exists.
    async fn exists(&self, path: &str) -> TxnLogResult<bool>;
    
    /// Delete object from storage.
    async fn delete(&self, path: &str) -> TxnLogResult<()>;
    
    /// List objects with prefix.
    async fn list(&self, prefix: &str) -> TxnLogResult<Vec<String>>;
    
    /// Get object metadata.
    async fn metadata(&self, path: &str) -> TxnLogResult<FileMetadata>;
    
    /// Atomically rename/move object.
    async fn rename(&self, from: &str, to: &str) -> TxnLogResult<()>;
    
    /// Write data atomically using temporary file and rename.
    async fn write_atomic(&self, path: &str, data: Vec<u8>) -> TxnLogResult<()> {
        let temp_path = format!("{}.tmp.{}", path, uuid::Uuid::new_v4());
        
        // Write to temporary file first
        self.write(&temp_path, data).await?;
        
        // Atomically rename to final location
        self.rename(&temp_path, path).await?;
        
        Ok(())
    }
}

/// Local filesystem storage backend.
#[derive(Debug)]
pub struct LocalFileSystemBackend {
    base_path: std::path::PathBuf,
}

impl LocalFileSystemBackend {
    /// Create new local filesystem backend.
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: std::path::PathBuf::from(base_path),
        }
    }
}

#[async_trait]
impl ObjectStorageBackend for LocalFileSystemBackend {
    async fn write(&self, path: &str, data: Vec<u8>) -> TxnLogResult<()> {
        let full_path = self.base_path.join(path);
        
        // Create parent directories if they don't exist
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await
                .map_err(|e| TxnLogError::ObjectStorage(format!("Failed to create directory: {}", e)))?;
        }
        
        tokio::fs::write(&full_path, data).await
            .map_err(|e| TxnLogError::ObjectStorage(format!("Failed to write file: {}", e)))?;
        
        tracing::debug!(path = %path, size = %data.len(), "Wrote file to local storage");
        Ok(())
    }
    
    async fn read(&self, path: &str) -> TxnLogResult<Vec<u8>> {
        let full_path = self.base_path.join(path);
        
        tokio::fs::read(&full_path).await
            .map_err(|e| TxnLogError::ObjectStorage(format!("Failed to read file: {}", e)))
    }
    
    async fn exists(&self, path: &str) -> TxnLogResult<bool> {
        let full_path = self.base_path.join(path);
        
        tokio::fs::metadata(&full_path).await
            .map(|_| true)
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(false)
                } else {
                    Err(TxnLogError::ObjectStorage(format!("Failed to check file existence: {}", e)))
                }
            })
    }
    
    async fn delete(&self, path: &str) -> TxnLogResult<()> {
        let full_path = self.base_path.join(path);
        
        tokio::fs::remove_file(&full_path).await
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(()) // File already deleted, that's fine
                } else {
                    Err(TxnLogError::ObjectStorage(format!("Failed to delete file: {}", e)))
                }
            })
    }
    
    async fn list(&self, prefix: &str) -> TxnLogResult<Vec<String>> {
        let prefix_path = self.base_path.join(prefix);
        let parent_dir = match prefix_path.parent() {
            Some(p) => p.to_path_buf(),
            None => self.base_path.clone(),
        };
        
        let mut entries = tokio::fs::read_dir(&parent_dir).await
            .map_err(|e| TxnLogError::ObjectStorage(format!("Failed to list directory: {}", e)))?;
        
        let mut files = Vec::new();
        while let Some(entry) = entries.next_entry().await
            .map_err(|e| TxnLogError::ObjectStorage(format!("Failed to read directory entry: {}", e)))? {
            
            let path = entry.path();
            if path.is_file() {
                if let Some(relative_path) = path.strip_prefix(&self.base_path).ok() {
                    if let Some(path_str) = relative_path.to_str() {
                        if path_str.starts_with(prefix) {
                            files.push(path_str.to_string());
                        }
                    }
                }
            }
        }
        
        files.sort();
        Ok(files)
    }
    
    async fn metadata(&self, path: &str) -> TxnLogResult<FileMetadata> {
        let full_path = self.base_path.join(path);
        
        let metadata = tokio::fs::metadata(&full_path).await
            .map_err(|e| TxnLogError::ObjectStorage(format!("Failed to get file metadata: {}", e)))?;
        
        let modified = metadata.modified()
            .map_err(|e| TxnLogError::ObjectStorage(format!("Failed to get modification time: {}", e)))?;
        let modified_datetime = chrono::DateTime::from(modified);
        
        Ok(FileMetadata {
            path: path.to_string(),
            size: metadata.len() as u64,
            last_modified: modified_datetime,
            etag: None,
            content_type: None,
            metadata: std::collections::HashMap::new(),
        })
    }
    
    async fn rename(&self, from: &str, to: &str) -> TxnLogResult<()> {
        let from_path = self.base_path.join(from);
        let to_path = self.base_path.join(to);
        
        // Create parent directory for target if it doesn't exist
        if let Some(parent) = to_path.parent() {
            tokio::fs::create_dir_all(parent).await
                .map_err(|e| TxnLogError::ObjectStorage(format!("Failed to create target directory: {}", e)))?;
        }
        
        tokio::fs::rename(&from_path, &to_path).await
            .map_err(|e| TxnLogError::ObjectStorage(format!("Failed to rename file: {}", e)))?;
        
        tracing::debug!(from = %from, to = %to, "Renamed file atomically");
        Ok(())
    }
}

impl ObjectStorageClient {
    /// Create new object storage client.
    pub async fn new(config: ObjectStorageConfig) -> TxnLogResult<Self> {
        let backend: Box<dyn ObjectStorageBackend> = match config.backend_type {
            StorageBackend::Local => {
                Box::new(LocalFileSystemBackend::new(&config.bucket))
            }
            StorageBackend::S3 | StorageBackend::MinIO | StorageBackend::SeaweedFS | 
            StorageBackend::RustFS | StorageBackend::Garage => {
                return Err(TxnLogError::NotImplemented(
                    format!("Object storage backend {:?} not yet implemented", config.backend_type)
                ));
            }
        };
        
        Ok(Self { config, client: backend })
    }
    
    /// Write Delta commit file atomically.
    pub async fn write_commit_file(
        &self,
        table_id: &str,
        version: i64,
        actions: &[DeltaAction],
    ) -> TxnLogResult<String> {
        let content = create_delta_commit_file(table_id, version, actions)?;
        let path = delta_commit_path(table_id, version);
        
        // Use atomic write to ensure commit is only visible when complete
        self.client.write_atomic(&path, content.into_bytes()).await?;
        
        tracing::info!(
            table_id = %table_id,
            version = %version,
            path = %path,
            action_count = %actions.len(),
            "Wrote Delta commit file atomically"
        );
        
        Ok(path)
    }
    
    /// Write Parquet checkpoint file atomically.
    pub async fn write_checkpoint_file(
        &self,
        table_id: &str,
        version: i64,
        actions: &[DeltaAction],
        part: Option<u32>,
    ) -> TxnLogResult<String> {
        let checkpoint_data = create_parquet_checkpoint(table_id, version, actions).await?;
        let path = match part {
            Some(p) => delta_sidecar_path(table_id, version, p),
            None => delta_checkpoint_path(table_id, version),
        };
        
        // Use atomic write for checkpoints as well
        self.client.write_atomic(&path, checkpoint_data).await?;
        
        tracing::info!(
            table_id = %table_id,
            version = %version,
            path = %path,
            size_bytes = %checkpoint_data.len(),
            part = ?part,
            "Wrote Parquet checkpoint file atomically"
        );
        
        Ok(path)
    }
    
    /// Write multiple checkpoint sidecar files.
    pub async fn write_checkpoint_sidecars(
        &self,
        table_id: &str,
        version: i64,
        actions: &[DeltaAction],
    ) -> TxnLogResult<Vec<String>> {
        let (actions_per_part, parts) = calculate_checkpoint_size(actions.len())?;
        
        if parts == 1 {
            // Single checkpoint file
            let path = self.write_checkpoint_file(table_id, version, actions, None).await?;
            return Ok(vec![path]);
        }
        
        // Multiple sidecar files
        let mut paths = Vec::new();
        for part in 1..=parts {
            let start_idx = (part as usize - 1) * actions_per_part;
            let end_idx = std::cmp::min(start_idx + actions_per_part, actions.len());
            let part_actions = &actions[start_idx..end_idx];
            
            let path = self.write_checkpoint_file(table_id, version, part_actions, Some(part)).await?;
            paths.push(path);
        }
        
        // Write checkpoint metadata
        let metadata = create_checkpoint_metadata(table_id, version, &paths, 0)?;
        let metadata_path = format!("_delta_log/{:020}.checkpoint.json", version);
        self.client.write(&metadata_path, metadata.into_bytes()).await?;
        
        tracing::info!(
            table_id = %table_id,
            version = %version,
            parts_count = %parts,
            actions_count = %actions.len(),
            "Wrote checkpoint sidecars"
        );
        
        Ok(paths)
    }
    
    /// Check if file exists in storage.
    pub async fn file_exists(&self, path: &str) -> TxnLogResult<bool> {
        self.client.exists(path).await
    }
    
    /// Get file metadata.
    pub async fn get_file_metadata(&self, path: &str) -> TxnLogResult<FileMetadata> {
        self.client.metadata(path).await
    }
    
    /// Delete file from storage.
    pub async fn delete_file(&self, path: &str) -> TxnLogResult<()> {
        self.client.delete(path).await
    }
    
    /// List files with prefix.
    pub async fn list_files(&self, prefix: &str) -> TxnLogResult<Vec<String>> {
        self.client.list(prefix).await
    }
    
    /// Validate that a commit file is complete and valid.
    pub async fn validate_commit(&self, table_id: &str, version: i64) -> TxnLogResult<bool> {
        let path = delta_commit_path(table_id, version);
        
        // Check if file exists
        if !self.client.exists(&path).await? {
            return Ok(false);
        }
        
        // Read and validate content
        let content = self.client.read(&path).await?;
        if content.is_empty() {
            return Ok(false);
        }
        
        // Try to parse as JSON lines to ensure validity
        let lines: Vec<&str> = content.split('\n').filter(|s| !s.trim().is_empty()).collect();
        if lines.is_empty() {
            return Ok(false);
        }
        
        // Validate each line is valid JSON
        for line in lines {
            if serde_json::from_str::<serde_json::Value>(line).is_err() {
                return Ok(false);
            }
        }
        
        Ok(true)
    }
    
    /// Perform atomic commit sequence with validation.
    pub async fn atomic_commit_sequence(
        &self,
        table_id: &str,
        version: i64,
        actions: &[DeltaAction],
    ) -> TxnLogResult<Vec<String>> {
        let mut written_files = Vec::new();
        
        // Write commit file atomically
        let commit_path = self.write_commit_file(table_id, version, actions).await?;
        written_files.push(commit_path.clone());
        
        // Validate commit was written correctly
        if !self.validate_commit(table_id, version).await? {
            // Cleanup on failure
            for file in &written_files {
                let _ = self.client.delete(file).await;
            }
            return Err(TxnLogError::Internal("Commit validation failed".to_string()));
        }
        
        // Write checkpoint if needed (after successful commit validation)
        if should_create_checkpoint(version, 10) {
            match self.write_checkpoint_sidecars(table_id, version, actions).await {
                Ok(mut checkpoint_files) => {
                    written_files.append(&mut checkpoint_files);
                }
                Err(e) => {
                    // Checkpoint failure is non-fatal for the commit
                    tracing::warn!(error = %e, "Checkpoint creation failed, but commit succeeded");
                }
            }
        }
        
        Ok(written_files)
    }
}

/// Real mirror engine implementation using object storage.
#[derive(Debug)]
pub struct ObjectStorageMirrorEngine {
    storage_client: ObjectStorageClient,
    config: MirroringConfig,
    checkpoint_manager: CheckpointCadenceManager,
}

impl ObjectStorageMirrorEngine {
    /// Create new mirror engine with object storage.
    pub async fn new(config: MirroringConfig) -> TxnLogResult<Self> {
        let storage_client = ObjectStorageClient::new(config.storage_config.clone()).await?;
        
        // Default checkpoint cadence config
        let checkpoint_config = CheckpointCadenceConfig::default();
        let checkpoint_manager = CheckpointCadenceManager::new(checkpoint_config);
        
        Ok(Self {
            storage_client,
            config,
            checkpoint_manager,
        })
    }
    
    /// Create mirror engine with custom checkpoint cadence.
    pub async fn new_with_cadence(
        config: MirroringConfig,
        checkpoint_config: CheckpointCadenceConfig,
    ) -> TxnLogResult<Self> {
        let storage_client = ObjectStorageClient::new(config.storage_config.clone()).await?;
        let checkpoint_manager = CheckpointCadenceManager::new(checkpoint_config);
        
        Ok(Self {
            storage_client,
            config,
            checkpoint_manager,
        })
    }
    
    /// Get checkpoint statistics.
    pub fn get_checkpoint_stats(&self) -> CheckpointStats {
        self.checkpoint_manager.get_stats()
    }
}

#[async_trait]
impl MirrorEngine for ObjectStorageMirrorEngine {
    async fn mirror_table_version(
        &self,
        table_id: &str,
        version: i64,
        actions: &[deltalakedb_core::DeltaAction],
    ) -> TxnLogResult<MirroringResult> {
        let start_time = std::time::Instant::now();
        
        tracing::info!(
            table_id = %table_id,
            version = %version,
            action_count = %actions.len(),
            "Starting table version mirroring"
        );

        let mut files_written = 0;
        let mut total_size = 0;
        let mut error_message = None;

        // Write commit file
        match self.storage_client.write_commit_file(table_id, version, actions).await {
            Ok(path) => {
                files_written += 1;
                if let Ok(metadata) = self.storage_client.get_file_metadata(&path).await {
                    total_size += metadata.size;
                }
            }
            Err(e) => {
                error_message = Some(format!("Failed to write commit file: {}", e));
                tracing::error!(error = %error_message.as_ref().unwrap(), "Commit file write failed");
            }
        }

        // Update checkpoint manager with action information
        let action_size_bytes = actions.iter().map(|a| {
            // Estimate action size (rough approximation)
            match a {
                DeltaAction::Add(_) => 200,
                DeltaAction::Remove(_) => 150,
                DeltaAction::Metadata(_) => 300,
                DeltaAction::Protocol(_) => 100,
                DeltaAction::Transaction(_) => 120,
            }
        }).sum::<usize>() as u64;
        
        self.checkpoint_manager.update_actions(actions.len(), action_size_bytes);
        
        // Check if checkpoint should be created
        let checkpoint_reason = self.checkpoint_manager.should_create_checkpoint(version);
        
        if checkpoint_reason != CheckpointReason::None && error_message.is_none() {
            tracing::info!(
                table_id = %table_id,
                version = %version,
                reason = %checkpoint_reason.description(),
                "Creating checkpoint"
            );
            
            match self.storage_client.write_checkpoint_sidecars(table_id, version, actions).await {
                Ok(paths) => {
                    files_written += paths.len();
                    for path in &paths {
                        if let Ok(metadata) = self.storage_client.get_file_metadata(path).await {
                            total_size += metadata.size;
                        }
                    }
                    
                    // Update checkpoint manager state
                    self.checkpoint_manager.update_checkpoint(version);
                    
                    tracing::info!(
                        table_id = %table_id,
                        version = %version,
                        files_count = %paths.len(),
                        "Checkpoint created successfully"
                    );
                }
                Err(e) => {
                    // Non-fatal error, log but don't fail the entire operation
                    tracing::warn!(error = %e, "Checkpoint write failed, but commit succeeded");
                }
            }
        }

        let duration = start_time.elapsed();
        let success = error_message.is_none();

        if success {
            tracing::info!(
                table_id = %table_id,
                version = %version,
                files_written = %files_written,
                total_size = %total_size,
                duration_ms = %duration.as_millis(),
                "Table version mirroring completed successfully"
            );
        }

        Ok(MirroringResult {
            table_id: table_id.to_string(),
            version,
            success,
            error: error_message,
            duration,
            files_count: files_written as u64,
            total_size,
        })
    }

    async fn file_exists(&self, path: &Path) -> TxnLogResult<bool> {
        let path_str = path.to_str().ok_or_else(|| {
            TxnLogError::Validation("Invalid path string".to_string())
        })?;
        self.storage_client.file_exists(path_str).await
    }

    async fn get_file_metadata(&self, path: &Path) -> TxnLogResult<FileMetadata> {
        let path_str = path.to_str().ok_or_else(|| {
            TxnLogError::Validation("Invalid path string".to_string())
        })?;
        self.storage_client.get_file_metadata(path_str).await
    }

    async fn delete_file(&self, path: &Path) -> TxnLogResult<()> {
        let path_str = path.to_str().ok_or_else(|| {
            TxnLogError::Validation("Invalid path string".to_string())
        })?;
        self.storage_client.delete_file(path_str).await
    }

    async fn list_files(&self, prefix: &Path) -> TxnLogResult<Vec<String>> {
        let prefix_str = prefix.to_str().ok_or_else(|| {
            TxnLogError::Validation("Invalid path string".to_string())
        })?;
        self.storage_client.list_files(prefix_str).await
    }

    async fn get_status(&self) -> TxnLogResult<MirrorStatus> {
        Ok(MirrorStatus {
            healthy: true,
            backend_type: self.config.storage_config.backend_type.clone(),
            endpoint: self.config.storage_config.endpoint.clone(),
            last_success: Some(chrono::Utc::now()),
            total_operations: 0, // TODO: Track operations
            failed_operations: 0, // TODO: Track failures
            avg_duration_ms: 0, // TODO: Track average duration
            details: HashMap::new(),
        })
    }
}

/// No-op mirror engine for testing.
#[derive(Debug)]
pub struct NoOpMirrorEngine;

#[async_trait]
impl MirrorEngine for NoOpMirrorEngine {
    async fn mirror_table_version(
        &self,
        table_id: &str,
        version: i64,
        _files: &[deltalakedb_core::DeltaAction],
    ) -> TxnLogResult<MirroringResult> {
        Ok(MirroringResult {
            table_id: table_id.to_string(),
            version,
            success: true,
            error: None,
            duration: Duration::from_millis(1),
            files_count: 0,
            total_size: 0,
        })
    }

    async fn file_exists(&self, _path: &Path) -> TxnLogResult<bool> {
        Ok(false)
    }

    async fn get_file_metadata(&self, _path: &Path) -> TxnLogResult<FileMetadata> {
        Err(TxnLogError::not_implemented("File metadata not implemented"))
    }

    async fn delete_file(&self, _path: &Path) -> TxnLogResult<()> {
        Ok(())
    }

    async fn list_files(&self, _prefix: &Path) -> TxnLogResult<Vec<String>> {
        Ok(vec![])
    }

    async fn get_status(&self) -> TxnLogResult<MirrorStatus> {
        Ok(MirrorStatus {
            healthy: true,
            backend_type: StorageBackend::Local,
            endpoint: "noop".to_string(),
            last_success: Some(chrono::Utc::now()),
            total_operations: 0,
            failed_operations: 0,
            avg_duration_ms: 0,
            details: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalakedb_core::{AddFile, RemoveFile, Metadata, Protocol, Txn, Format};

    #[tokio::test]
    async fn test_noop_mirror_engine() {
        let engine = NoOpMirrorEngine;
        
        let result = engine.mirror_table_version("test_table", 1, &[]).await.unwrap();
        assert!(result.success);
        assert_eq!(result.table_id, "test_table");
        assert_eq!(result.version, 1);
        
        let status = engine.get_status().await.unwrap();
        assert!(status.healthy);
        assert_eq!(status.backend_type, StorageBackend::Local);
    }

    #[test]
    fn test_serialize_add_file() {
        let add_file = AddFile::new(
            "test.parquet".to_string(),
            1024,
            1234567890,
            true,
        ).with_partition_value("date".to_string(), "2023-01-01".to_string());

        let action = DeltaAction::Add(add_file);
        let json = serialize_single_action(&action).unwrap();
        
        assert!(json.contains("\"add\""));
        assert!(json.contains("\"test.parquet\""));
        assert!(json.contains("\"date\":\"2023-01-01\""));
    }

    #[test]
    fn test_serialize_remove_file() {
        let remove_file = RemoveFile::new("test.parquet".to_string(), true)
            .with_deletion_timestamp(1234567890);

        let action = DeltaAction::Remove(remove_file);
        let json = serialize_single_action(&action).unwrap();
        
        assert!(json.contains("\"remove\""));
        assert!(json.contains("\"test.parquet\""));
        assert!(json.contains("\"deletionTimestamp\":1234567890"));
    }

    #[test]
    fn test_serialize_metadata() {
        let format = Format::new("parquet".to_string());
        let metadata = Metadata::new(
            "test-id".to_string(),
            "{\"type\":\"struct\",\"fields\":[]}".to_string(),
            format,
        ).with_partition_column("date".to_string());

        let action = DeltaAction::Metadata(metadata);
        let json = serialize_single_action(&action).unwrap();
        
        assert!(json.contains("\"metaData\""));
        assert!(json.contains("\"test-id\""));
        assert!(json.contains("\"partitionColumns\":[\"date\"]"));
    }

    #[test]
    fn test_serialize_protocol() {
        let protocol = Protocol::new(2, 3);
        let action = DeltaAction::Protocol(protocol);
        let json = serialize_single_action(&action).unwrap();
        
        assert!(json.contains("\"protocol\""));
        assert!(json.contains("\"minReaderVersion\":2"));
        assert!(json.contains("\"minWriterVersion\":3"));
    }

    #[test]
    fn test_serialize_transaction() {
        let txn = Txn::new("app-123".to_string(), 10, 1234567890);
        let action = DeltaAction::Transaction(txn);
        let json = serialize_single_action(&action).unwrap();
        
        assert!(json.contains("\"txn\""));
        assert!(json.contains("\"app-123\""));
        assert!(json.contains("\"version\":10"));
    }

    #[test]
    fn test_serialize_multiple_actions() {
        let actions = vec![
            DeltaAction::Protocol(Protocol::new(1, 2)),
            DeltaAction::Metadata(Metadata::new(
                "test-id".to_string(),
                "{\"type\":\"struct\"}".to_string(),
                Format::default(),
            )),
            DeltaAction::Add(AddFile::new("data.parquet".to_string(), 2048, 1234567890, true)),
        ];

        let json = serialize_delta_json(&actions).unwrap();
        let lines: Vec<&str> = json.split('\n').collect();
        
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("\"protocol\""));
        assert!(lines[1].contains("\"metaData\""));
        assert!(lines[2].contains("\"add\""));
    }

    #[test]
    fn test_create_delta_commit_file() {
        let actions = vec![
            DeltaAction::Protocol(Protocol::default()),
            DeltaAction::Add(AddFile::new("test.parquet".to_string(), 1024, 1234567890, true)),
        ];

        let content = create_delta_commit_file("test_table", 1, &actions).unwrap();
        let lines: Vec<&str> = content.split('\n').collect();
        
        assert_eq!(lines.len(), 2);
        assert!(content.contains("\"protocol\""));
        assert!(content.contains("\"add\""));
    }

    #[test]
    fn test_create_delta_commit_file_empty_actions() {
        let actions = vec![];
        let result = create_delta_commit_file("test_table", 1, &actions);
        assert!(result.is_err());
    }

    #[test]
    fn test_delta_commit_path() {
        let path = delta_commit_path("test_table", 1);
        assert_eq!(path, "_delta_log/00000000000000000001.json");
        
        let path = delta_commit_path("test_table", 123);
        assert_eq!(path, "_delta_log/00000000000000000123.json");
    }

    #[test]
    fn test_delta_checkpoint_path() {
        let path = delta_checkpoint_path("test_table", 10);
        assert_eq!(path, "_delta_log/00000000000000000010.checkpoint.parquet");
    }

    #[test]
    fn test_delta_sidecar_path() {
        let path = delta_sidecar_path("test_table", 10, 1);
        assert_eq!(path, "_delta_log/00000000000000000010.00001.checkpoint.parquet");
        
        let path = delta_sidecar_path("test_table", 10, 123);
        assert_eq!(path, "_delta_log/00000000000000000010.00123.checkpoint.parquet");
    }

    #[tokio::test]
    async fn test_create_parquet_checkpoint() {
        let actions = vec![
            DeltaAction::Protocol(Protocol::new(1, 2)),
            DeltaAction::Metadata(Metadata::new(
                "test-id".to_string(),
                "{\"type\":\"struct\"}".to_string(),
                Format::default(),
            )),
            DeltaAction::Add(AddFile::new("data.parquet".to_string(), 2048, 1234567890, true)),
        ];

        let checkpoint_data = create_parquet_checkpoint("test_table", 10, &actions).await.unwrap();
        
        // Verify we got some Parquet data
        assert!(!checkpoint_data.is_empty());
        assert!(checkpoint_data.len() > 100); // Should be at least some reasonable size
        
        // Verify Parquet magic bytes (PAR1)
        assert_eq!(&checkpoint_data[0..4], b"PAR1");
    }

    #[test]
    fn test_create_checkpoint_metadata() {
        let parts = vec![
            "_delta_log/00000000000000000010.00001.checkpoint.parquet".to_string(),
            "_delta_log/00000000000000000010.00002.checkpoint.parquet".to_string(),
        ];

        let metadata = create_checkpoint_metadata("test_table", 10, &parts, 1024).unwrap();
        
        assert!(metadata.contains("\"checkpoint\""));
        assert!(metadata.contains("\"version\":10"));
        assert!(metadata.contains("\"size\":1024"));
        assert!(metadata.contains("\"test_table\""));
        assert!(metadata.contains("\"parts\""));
    }

    #[test]
    fn test_should_create_checkpoint() {
        // Every 10 versions
        assert!(!should_create_checkpoint(5, 10));
        assert!(should_create_checkpoint(10, 10));
        assert!(!should_create_checkpoint(15, 10));
        assert!(should_create_checkpoint(20, 10));
        
        // Every 5 versions
        assert!(!should_create_checkpoint(3, 5));
        assert!(should_create_checkpoint(5, 5));
        assert!(should_create_checkpoint(10, 5));
    }

    #[test]
    fn test_calculate_checkpoint_size() {
        // Small number of actions
        let (actions_per_part, parts) = calculate_checkpoint_size(100).unwrap();
        assert_eq!(actions_per_part, 100);
        assert_eq!(parts, 1);
        
        // Medium number of actions
        let (actions_per_part, parts) = calculate_checkpoint_size(25000).unwrap();
        assert_eq!(actions_per_part, 12500);
        assert_eq!(parts, 2);
        
        // Large number of actions
        let (actions_per_part, parts) = calculate_checkpoint_size(50000).unwrap();
        assert_eq!(actions_per_part, 10000);
        assert_eq!(parts, 5);
        
        // Too many actions
        let result = calculate_checkpoint_size(200000);
        assert!(result.is_err());
    }

    #[test]
    fn test_checkpoint_size_edge_cases() {
        // Zero actions
        let (actions_per_part, parts) = calculate_checkpoint_size(0).unwrap();
        assert_eq!(actions_per_part, 0);
        assert_eq!(parts, 1);
        
        // Exactly at boundary
        let (actions_per_part, parts) = calculate_checkpoint_size(10000).unwrap();
        assert_eq!(actions_per_part, 10000);
        assert_eq!(parts, 1);
        
        // Just over boundary
        let (actions_per_part, parts) = calculate_checkpoint_size(10001).unwrap();
        assert_eq!(actions_per_part, 5001);
        assert_eq!(parts, 2);
    }

    #[tokio::test]
    async fn test_local_filesystem_backend() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalFileSystemBackend::new(temp_dir.path().to_str().unwrap());
        
        // Test write and read
        let test_data = b"Hello, World!";
        backend.write("test.txt", test_data.to_vec()).await.unwrap();
        
        let read_data = backend.read("test.txt").await.unwrap();
        assert_eq!(read_data, test_data);
        
        // Test exists
        assert!(backend.exists("test.txt").await.unwrap());
        assert!(!backend.exists("nonexistent.txt").await.unwrap());
        
        // Test metadata
        let metadata = backend.metadata("test.txt").await.unwrap();
        assert_eq!(metadata.path, "test.txt");
        assert_eq!(metadata.size, test_data.len() as u64);
        
        // Test delete
        backend.delete("test.txt").await.unwrap();
        assert!(!backend.exists("test.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_object_storage_client() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = ObjectStorageConfig {
            backend_type: StorageBackend::Local,
            endpoint: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            bucket: temp_dir.path().to_str().unwrap().to_string(),
            region: None,
            additional_params: std::collections::HashMap::new(),
        };
        
        let client = ObjectStorageClient::new(config).await.unwrap();
        
        // Test write commit file
        let actions = vec![
            DeltaAction::Protocol(Protocol::default()),
            DeltaAction::Add(AddFile::new("test.parquet".to_string(), 1024, 1234567890, true)),
        ];
        
        let path = client.write_commit_file("test_table", 1, &actions).await.unwrap();
        assert_eq!(path, "_delta_log/00000000000000000001.json");
        assert!(client.file_exists(&path).await.unwrap());
        
        // Test write checkpoint file
        let checkpoint_path = client.write_checkpoint_file("test_table", 1, &actions, None).await.unwrap();
        assert_eq!(checkpoint_path, "_delta_log/00000000000000000001.checkpoint.parquet");
        assert!(client.file_exists(&checkpoint_path).await.unwrap());
        
        // Test list files
        let files = client.list_files("_delta_log/").await.unwrap();
        assert!(files.contains(&path));
        assert!(files.contains(&checkpoint_path));
    }

    #[tokio::test]
    async fn test_write_checkpoint_sidecars() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = ObjectStorageConfig {
            backend_type: StorageBackend::Local,
            endpoint: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            bucket: temp_dir.path().to_str().unwrap().to_string(),
            region: None,
            additional_params: std::collections::HashMap::new(),
        };
        
        let client = ObjectStorageClient::new(config).await.unwrap();
        
        // Create many actions to trigger sidecar splitting
        let mut actions = Vec::new();
        for i in 0..25000 {
            actions.push(DeltaAction::Add(AddFile::new(
                format!("file_{}.parquet", i),
                1024,
                1234567890 + i as i64,
                true,
            )));
        }
        
        let paths = client.write_checkpoint_sidecars("test_table", 10, &actions).await.unwrap();
        
        // Should create multiple sidecar files
        assert!(paths.len() > 1);
        assert!(paths.len() <= 3); // 25000 actions should split into 3 parts
        
        // Verify all files exist
        for path in &paths {
            assert!(client.file_exists(path).await.unwrap());
            assert!(path.contains(".checkpoint.parquet"));
        }
        
        // Check for metadata file
        let metadata_path = "_delta_log/00000000000000000010.checkpoint.json";
        assert!(client.file_exists(metadata_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_atomic_write_operations() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalFileSystemBackend::new(temp_dir.path().to_str().unwrap());
        
        // Test atomic write
        let test_data = b"Atomic test data";
        backend.write_atomic("atomic_test.txt", test_data.to_vec()).await.unwrap();
        
        // Verify file exists and has correct content
        assert!(backend.exists("atomic_test.txt").await.unwrap());
        let read_data = backend.read("atomic_test.txt").await.unwrap();
        assert_eq!(read_data, test_data);
        
        // Test atomic rename
        backend.rename("atomic_test.txt", "renamed_test.txt").await.unwrap();
        assert!(!backend.exists("atomic_test.txt").await.unwrap());
        assert!(backend.exists("renamed_test.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_commit_validation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = ObjectStorageConfig {
            backend_type: StorageBackend::Local,
            endpoint: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            bucket: temp_dir.path().to_str().unwrap().to_string(),
            region: None,
            additional_params: std::collections::HashMap::new(),
        };
        
        let client = ObjectStorageClient::new(config).await.unwrap();
        
        // Write a valid commit
        let actions = vec![
            DeltaAction::Protocol(Protocol::default()),
            DeltaAction::Add(AddFile::new("test.parquet".to_string(), 1024, 1234567890, true)),
        ];
        
        let commit_path = client.write_commit_file("test_table", 1, &actions).await.unwrap();
        
        // Validate the commit
        assert!(client.validate_commit("test_table", 1).await.unwrap());
        
        // Test validation of non-existent commit
        assert!(!client.validate_commit("test_table", 999).await.unwrap());
        
        // Test validation of corrupted commit
        let corrupted_path = "_delta_log/00000000000000000002.json";
        client.client.write(&corrupted_path, b"invalid json content".to_vec()).await.unwrap();
        assert!(!client.validate_commit("test_table", 2).await.unwrap());
    }

    #[tokio::test]
    async fn test_atomic_commit_sequence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = ObjectStorageConfig {
            backend_type: StorageBackend::Local,
            endpoint: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            bucket: temp_dir.path().to_str().unwrap().to_string(),
            region: None,
            additional_params: std::collections::HashMap::new(),
        };
        
        let client = ObjectStorageClient::new(config).await.unwrap();
        
        let actions = vec![
            DeltaAction::Protocol(Protocol::default()),
            DeltaAction::Add(AddFile::new("test.parquet".to_string(), 1024, 1234567890, true)),
        ];
        
        // Test atomic commit sequence
        let written_files = client.atomic_commit_sequence("test_table", 10, &actions).await.unwrap();
        
        // Should have written commit file and checkpoint (version 10 triggers checkpoint)
        assert!(written_files.len() >= 1);
        assert!(written_files.iter().any(|f| f.contains(".json")));
        
        // Verify commit is valid
        assert!(client.validate_commit("test_table", 10).await.unwrap());
        
        // Verify files exist
        for file in &written_files {
            assert!(client.file_exists(file).await.unwrap());
        }
    }

    #[test]
    fn test_checkpoint_cadence_config() {
        let config = CheckpointCadenceConfig::default();
        assert_eq!(config.version_interval, 10);
        assert_eq!(config.time_interval_secs, 3600);
        assert_eq!(config.min_actions, 100);
        assert_eq!(config.max_size_mb, 100);
        assert!(config.enable_time_based);
        assert!(config.enable_size_based);
    }

    #[test]
    fn test_checkpoint_cadence_manager() {
        let config = CheckpointCadenceConfig {
            version_interval: 5,
            time_interval_secs: 1800,
            min_actions: 50,
            max_size_mb: 50,
            enable_time_based: true,
            enable_size_based: true,
        };
        
        let mut manager = CheckpointCadenceManager::new(config);
        
        // Initially no checkpoint needed
        assert_eq!(manager.should_create_checkpoint(1), CheckpointReason::None);
        
        // Update with some actions
        manager.update_actions(30, 1000);
        assert_eq!(manager.should_create_checkpoint(1), CheckpointReason::None);
        
        // Reach minimum action count
        manager.update_actions(20, 1000); // Total 50 actions
        assert_eq!(manager.should_create_checkpoint(1), CheckpointReason::ActionCount);
        
        // Update checkpoint
        manager.update_checkpoint(5);
        assert_eq!(manager.should_create_checkpoint(6), CheckpointReason::None);
        
        // Reach version interval
        assert_eq!(manager.should_create_checkpoint(10), CheckpointReason::VersionInterval);
    }

    #[test]
    fn test_checkpoint_reason_priority() {
        assert!(CheckpointReason::SizeLimit.priority() > CheckpointReason::VersionInterval.priority());
        assert!(CheckpointReason::VersionInterval.priority() > CheckpointReason::ActionCount.priority());
        assert!(CheckpointReason::ActionCount.priority() > CheckpointReason::TimeInterval.priority());
        assert!(CheckpointReason::TimeInterval.priority() > CheckpointReason::None.priority());
    }

    #[test]
    fn test_checkpoint_stats() {
        let config = CheckpointCadenceConfig::default();
        let mut manager = CheckpointCadenceManager::new(config);
        
        // Update with actions and checkpoint
        manager.update_actions(100, 1000000); // 1MB
        manager.update_checkpoint(10);
        
        let stats = manager.get_stats();
        assert_eq!(stats.total_checkpoints, 1);
        assert_eq!(stats.last_checkpoint_version, 10);
        assert_eq!(stats.accumulated_actions, 0);
        assert_eq!(stats.accumulated_size_mb, 0);
        assert_eq!(stats.next_checkpoint_version, 20);
    }

    #[test]
    fn test_size_based_checkpointing() {
        let config = CheckpointCadenceConfig {
            version_interval: 100, // Very high
            time_interval_secs: 86400, // Very high
            min_actions: 1000, // Very high
            max_size_mb: 10, // Low - triggers on size
            enable_time_based: true,
            enable_size_based: true,
        };
        
        let mut manager = CheckpointCadenceManager::new(config);
        
        // Add actions that exceed size limit
        manager.update_actions(50, 11 * 1024 * 1024); // 11MB
        
        // Should trigger due to size limit
        assert_eq!(manager.should_create_checkpoint(1), CheckpointReason::SizeLimit);
    }

    #[tokio::test]
    async fn test_mirror_engine_with_cadence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage_config = ObjectStorageConfig {
            backend_type: StorageBackend::Local,
            endpoint: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            bucket: temp_dir.path().to_str().unwrap().to_string(),
            region: None,
            additional_params: std::collections::HashMap::new(),
        };
        
        let mirroring_config = MirroringConfig {
            enable_auto_mirroring: true,
            max_concurrent_mirrors: 5,
            mirroring_timeout_secs: 300,
            retry_config: MirroringRetryConfig::default(),
            storage_config,
        };
        
        let checkpoint_config = CheckpointCadenceConfig {
            version_interval: 2, // Checkpoint every 2 versions
            time_interval_secs: 3600,
            min_actions: 1,
            max_size_mb: 100,
            enable_time_based: true,
            enable_size_based: true,
        };
        
        let engine = ObjectStorageMirrorEngine::new_with_cadence(mirroring_config, checkpoint_config)
            .await.unwrap();
        
        let actions = vec![
            DeltaAction::Protocol(Protocol::default()),
            DeltaAction::Add(AddFile::new("test.parquet".to_string(), 1024, 1234567890, true)),
        ];
        
        // Version 1 - should not checkpoint (interval is 2)
        let result1 = engine.mirror_table_version("test_table", 1, &actions).await.unwrap();
        assert!(result1.success);
        assert_eq!(result1.files_count, 1); // Only commit file
        
        // Version 2 - should checkpoint
        let result2 = engine.mirror_table_version("test_table", 2, &actions).await.unwrap();
        assert!(result2.success);
        assert!(result2.files_count >= 2); // Commit + checkpoint
        
        // Check stats
        let stats = engine.get_checkpoint_stats();
        assert_eq!(stats.total_checkpoints, 1);
        assert_eq!(stats.last_checkpoint_version, 2);
    }
}