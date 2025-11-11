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
            reasons.push(CheckpointReason::VersionInterval);
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

/// Retry configuration for mirroring operations.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Base delay for exponential backoff in milliseconds
    pub base_delay_ms: u64,
    /// Maximum delay for exponential backoff in milliseconds
    pub max_delay_ms: u64,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// Jitter factor to add randomness (0.0 to 1.0)
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay_ms: 1000,
            max_delay_ms: 30000, // 30 seconds
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }
}

/// Retry attempt information.
#[derive(Debug, Clone)]
pub struct RetryAttempt {
    /// Attempt number (1-based)
    pub attempt_number: u32,
    /// Delay before this attempt in milliseconds
    pub delay_ms: u64,
    /// Error from this attempt (if failed)
    pub error: Option<String>,
    /// Timestamp of this attempt
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Retry result with attempt history.
#[derive(Debug, Clone)]
pub struct RetryResult<T> {
    /// Final result (success or last error)
    pub result: TxnLogResult<T>,
    /// Total number of attempts made
    pub total_attempts: u32,
    /// Total time spent retrying in milliseconds
    pub total_duration_ms: u64,
    /// History of all attempts
    pub attempts: Vec<RetryAttempt>,
}

/// Retryable operation trait.
pub trait RetryableOperation {
    type Output;
    
    /// Execute the operation and return result.
    async fn execute(&self) -> TxnLogResult<Self::Output>;
    
    /// Check if the error is retryable.
    fn is_retryable_error(&self, error: &TxnLogError) -> bool;
}

/// Exponential backoff retry executor.
#[derive(Debug)]
pub struct RetryExecutor {
    config: RetryConfig,
}

impl RetryExecutor {
    /// Create new retry executor with configuration.
    pub fn new(config: RetryConfig) -> Self {
        Self { config }
    }
    
    /// Execute operation with exponential backoff retry.
    pub async fn execute_with_retry<T>(&self, operation: impl RetryableOperation<Output = T>) -> RetryResult<T> {
        let start_time = std::time::Instant::now();
        let mut attempts = Vec::new();
        
        for attempt in 1..=self.config.max_attempts {
            let attempt_start = std::time::Instant::now();
            
            // Calculate delay for this attempt
            let delay_ms = if attempt == 1 {
                0 // No delay for first attempt
            } else {
                self.calculate_delay(attempt - 1)
            };
            
            // Record attempt info
            let attempt_info = RetryAttempt {
                attempt_number: attempt,
                delay_ms,
                error: None,
                timestamp: chrono::Utc::now(),
            };
            
            // Wait before retry (except for first attempt)
            if delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            
            // Execute the operation
            let result = operation.execute().await;
            
            // Handle result
            match result {
                Ok(output) => {
                    let mut success_attempt = attempt_info.clone();
                    attempts.push(success_attempt);
                    
                    return RetryResult {
                        result: Ok(output),
                        total_attempts: attempt,
                        total_duration_ms: start_time.elapsed().as_millis(),
                        attempts,
                    };
                }
                Err(error) => {
                    // Check if error is retryable
                    if !operation.is_retryable_error(&error) {
                        let mut final_attempt = attempt_info.clone();
                        final_attempt.error = Some(format!("{}", error));
                        attempts.push(final_attempt);
                        
                        return RetryResult {
                            result: Err(error),
                            total_attempts: attempt,
                            total_duration_ms: start_time.elapsed().as_millis(),
                            attempts,
                        };
                    }
                    
                    // Record failed attempt
                    let mut failed_attempt = attempt_info.clone();
                    failed_attempt.error = Some(format!("{}", error));
                    attempts.push(failed_attempt);
                    
                    tracing::warn!(
                        attempt = %attempt,
                        error = %error,
                        delay_ms = %delay_ms,
                        next_attempt_in_ms = %if attempt < self.config.max_attempts { 
                            self.calculate_delay(attempt) 
                        } else { 0 },
                        "Operation failed, retrying"
                    );
                    
                    // Continue to next attempt if not max attempts
                    if attempt == self.config.max_attempts {
                        return RetryResult {
                            result: Err(error),
                            total_attempts: attempt,
                            total_duration_ms: start_time.elapsed().as_millis(),
                            attempts,
                        };
                    }
                }
            }
        }
        
        unreachable!() // Should never reach here
    }
    
    /// Calculate delay with exponential backoff and jitter.
    fn calculate_delay(&self, attempt: u32) -> u64 {
        let base_delay = self.config.base_delay_ms as f64;
        let multiplier = self.config.backoff_multiplier.powf(attempt as f64);
        let exponential_delay = base_delay * multiplier;
        
        // Apply jitter
        let jitter_range = exponential_delay * self.config.jitter_factor;
        let jitter = (rand::random::<f64>() - 0.5) * 2.0 * jitter_range;
        let final_delay = exponential_delay + jitter;
        
        // Clamp to maximum delay
        final_delay.min(self.config.max_delay_ms as f64).max(0.0) as u64
    }
}

/// Mirroring operation with retry logic.
#[derive(Debug)]
pub struct MirroringOperation {
    table_id: String,
    version: i64,
    actions: Vec<DeltaAction>,
    engine: std::sync::Arc<ObjectStorageMirrorEngine>,
}

impl MirroringOperation {
    /// Create new mirroring operation.
    pub fn new(
        table_id: String,
        version: i64,
        actions: Vec<DeltaAction>,
        engine: std::sync::Arc<ObjectStorageMirrorEngine>,
    ) -> Self {
        Self {
            table_id,
            version,
            actions,
            engine,
        }
    }
}

#[async_trait]
impl RetryableOperation for MirroringOperation {
    type Output = MirroringResult;
    
    async fn execute(&self) -> TxnLogResult<Self::Output> {
        self.engine.mirror_table_version(&self.table_id, self.version, &self.actions).await
    }
    
    fn is_retryable_error(&self, error: &TxnLogError) -> bool {
        match error {
            TxnLogError::ObjectStorage(_) => true,
            TxnLogError::DatabaseError(_) => false, // Database errors are not retryable at mirroring level
            TxnLogError::ConnectionError(_) => true,
            TxnLogError::Internal(_) => false, // Internal errors are not retryable
            TxnLogError::Validation(_) => false, // Validation errors are not retryable
            TxnLogError::NotImplemented(_) => false,
            TxnLogError::Protocol { .. } => false,
            TxnLogError::VersionConflict { .. } => false,
            TxnLogError::TableNotFound { .. } => false,
            TxnLogError::InvalidVersion { .. } => false,
            TxnLogError::InvalidTimestamp { .. } => false,
            TxnLogError::Configuration { .. } => false,
        }
    }
}

/// Comprehensive mirroring statistics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MirroringStats {
    /// Total tables being mirrored
    pub total_tables: u64,
    /// Tables with successful mirroring
    pub successful_tables: u64,
    /// Tables with failed mirroring
    pub failed_tables: u64,
    /// Tables currently in progress
    pub in_progress_tables: u64,
    /// Total versions mirrored
    pub total_versions: u64,
    /// Versions mirrored in last hour
    pub versions_last_hour: u64,
    /// Versions mirrored in last 24 hours
    pub versions_last_24h: u64,
    /// Average versions per hour
    pub avg_versions_per_hour: f64,
    /// Total data mirrored (bytes)
    pub total_data_bytes: u64,
    /// Data mirrored in last hour (bytes)
    pub data_last_hour_bytes: u64,
    /// Oldest pending version age (seconds)
    pub oldest_pending_age_secs: Option<u64>,
    /// Queue depth (pending operations)
    pub queue_depth: u64,
}

impl Default for MirroringStats {
    fn default() -> Self {
        Self {
            total_tables: 0,
            successful_tables: 0,
            failed_tables: 0,
            in_progress_tables: 0,
            total_versions: 0,
            versions_last_hour: 0,
            versions_last_24h: 0,
            avg_versions_per_hour: 0.0,
            total_data_bytes: 0,
            data_last_hour_bytes: 0,
            oldest_pending_age_secs: None,
            queue_depth: 0,
        }
    }
}

/// Health check result.
#[derive(Debug, Clone, serde::Serialize)]
pub struct HealthCheckResult {
    /// Health check name
    pub name: String,
    /// Whether the check passed
    pub healthy: bool,
    /// Check result message
    pub message: String,
    /// Timestamp of the check
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Duration of the check in milliseconds
    pub duration_ms: u64,
    /// Additional details
    pub details: HashMap<String, String>,
}

/// Performance metrics for mirroring operations.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PerformanceMetrics {
    /// Operations per second
    pub ops_per_second: f64,
    /// Average latency in milliseconds
    pub avg_latency_ms: f64,
    /// P50 latency in milliseconds
    pub p50_latency_ms: f64,
    /// P95 latency in milliseconds
    pub p95_latency_ms: f64,
    /// P99 latency in milliseconds
    pub p99_latency_ms: f64,
    /// Throughput in bytes per second
    pub throughput_bytes_per_sec: f64,
    /// Error rate as percentage
    pub error_rate_percent: f64,
    /// Retry rate as percentage
    pub retry_rate_percent: f64,
    /// CPU usage percentage (if available)
    pub cpu_usage_percent: Option<f64>,
    /// Memory usage in bytes
    pub memory_usage_bytes: Option<u64>,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            ops_per_second: 0.0,
            avg_latency_ms: 0.0,
            p50_latency_ms: 0.0,
            p95_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            throughput_bytes_per_sec: 0.0,
            error_rate_percent: 0.0,
            retry_rate_percent: 0.0,
            cpu_usage_percent: None,
            memory_usage_bytes: None,
        }
    }
}

/// Mirroring operation tracker for status monitoring.
#[derive(Debug)]
pub struct MirroringTracker {
    /// Operation history
    operations: Vec<MirroringOperationRecord>,
    /// Maximum history size
    max_history_size: usize,
    /// Statistics
    stats: MirroringStats,
    /// Performance metrics
    performance: PerformanceMetrics,
    /// Health check results
    health_checks: HashMap<String, HealthCheckResult>,
}

/// Record of a mirroring operation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MirroringOperationRecord {
    /// Operation ID
    pub operation_id: String,
    /// Table ID
    pub table_id: String,
    /// Version
    pub version: i64,
    /// Operation start time
    pub start_time: chrono::DateTime<chrono::Utc>,
    /// Operation end time
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    /// Operation status
    pub status: MirroringOperationStatus,
    /// Number of files processed
    pub files_count: u64,
    /// Total size in bytes
    pub total_size_bytes: u64,
    /// Error message if failed
    pub error_message: Option<String>,
    /// Retry attempts
    pub retry_attempts: u32,
    /// Duration in milliseconds
    pub duration_ms: Option<u64>,
}

/// Status of a mirroring operation.
#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub enum MirroringOperationStatus {
    /// Operation is queued
    Queued,
    /// Operation is in progress
    InProgress,
    /// Operation completed successfully
    Completed,
    /// Operation failed
    Failed,
    /// Operation was cancelled
    Cancelled,
}

impl MirroringTracker {
    /// Create new mirroring tracker.
    pub fn new(max_history_size: usize) -> Self {
        Self {
            operations: Vec::new(),
            max_history_size,
            stats: MirroringStats::default(),
            performance: PerformanceMetrics::default(),
            health_checks: HashMap::new(),
        }
    }
    
    /// Record the start of a mirroring operation.
    pub fn start_operation(&mut self, operation_id: String, table_id: String, version: i64) {
        let record = MirroringOperationRecord {
            operation_id,
            table_id: table_id.clone(),
            version,
            start_time: chrono::Utc::now(),
            end_time: None,
            status: MirroringOperationStatus::InProgress,
            files_count: 0,
            total_size_bytes: 0,
            error_message: None,
            retry_attempts: 0,
            duration_ms: None,
        };
        
        self.operations.push(record);
        self.stats.in_progress_tables += 1;
        self.stats.queue_depth = self.operations.iter()
            .filter(|op| op.status == MirroringOperationStatus::Queued)
            .count() as u64;
        
        // Trim history if needed
        if self.operations.len() > self.max_history_size {
            self.operations.remove(0);
        }
    }
    
    /// Record the completion of a mirroring operation.
    pub fn complete_operation(
        &mut self,
        operation_id: &str,
        success: bool,
        files_count: u64,
        total_size_bytes: u64,
        error_message: Option<String>,
        retry_attempts: u32,
    ) {
        if let Some(record) = self.operations.iter_mut()
            .find(|op| op.operation_id == operation_id) {
            
            let end_time = chrono::Utc::now();
            let duration_ms = (end_time - record.start_time).num_millis() as u64;
            
            record.end_time = Some(end_time);
            record.status = if success {
                MirroringOperationStatus::Completed
            } else {
                MirroringOperationStatus::Failed
            };
            record.files_count = files_count;
            record.total_size_bytes = total_size_bytes;
            record.error_message = error_message;
            record.retry_attempts = retry_attempts;
            record.duration_ms = Some(duration_ms);
            
            // Update statistics
            self.stats.in_progress_tables = self.stats.in_progress_tables.saturating_sub(1);
            
            if success {
                self.stats.successful_tables += 1;
                self.stats.total_versions += 1;
                self.stats.total_data_bytes += total_size_bytes;
            } else {
                self.stats.failed_tables += 1;
            }
            
            // Update time-based statistics
            self.update_time_based_stats();
            
            // Update performance metrics
            self.update_performance_metrics();
        }
    }
    
    /// Record a health check result.
    pub fn record_health_check(&mut self, check_name: String, result: HealthCheckResult) {
        self.health_checks.insert(check_name, result);
    }
    
    /// Get current mirroring statistics.
    pub fn get_stats(&self) -> &MirroringStats {
        &self.stats
    }
    
    /// Get current performance metrics.
    pub fn get_performance_metrics(&self) -> &PerformanceMetrics {
        &self.performance
    }
    
    /// Get health check results.
    pub fn get_health_checks(&self) -> &HashMap<String, HealthCheckResult> {
        &self.health_checks
    }
    
    /// Get recent operations.
    pub fn get_recent_operations(&self, limit: usize) -> &[MirroringOperationRecord] {
        let start = if self.operations.len() > limit {
            self.operations.len() - limit
        } else {
            0
        };
        &self.operations[start..]
    }
    
    /// Update time-based statistics.
    fn update_time_based_stats(&mut self) {
        let now = chrono::Utc::now();
        let one_hour_ago = now - chrono::Duration::hours(1);
        let one_day_ago = now - chrono::Duration::days(1);
        
        // Count versions in time windows
        self.stats.versions_last_hour = self.operations.iter()
            .filter(|op| op.status == MirroringOperationStatus::Completed)
            .filter(|op| op.start_time >= one_hour_ago)
            .count() as u64;
            
        self.stats.versions_last_24h = self.operations.iter()
            .filter(|op| op.status == MirroringOperationStatus::Completed)
            .filter(|op| op.start_time >= one_day_ago)
            .count() as u64;
        
        // Calculate average versions per hour
        let hours_since_start = if let Some(first_op) = self.operations.first() {
            (now - first_op.start_time).num_hours().max(1)
        } else {
            1
        };
        
        self.stats.avg_versions_per_hour = self.stats.total_versions as f64 / hours_since_start as f64;
        
        // Calculate data in last hour
        self.stats.data_last_hour_bytes = self.operations.iter()
            .filter(|op| op.status == MirroringOperationStatus::Completed)
            .filter(|op| op.start_time >= one_hour_ago)
            .map(|op| op.total_size_bytes)
            .sum();
        
        // Find oldest pending operation
        self.stats.oldest_pending_age_secs = self.operations.iter()
            .filter(|op| op.status == MirroringOperationStatus::Queued || 
                        op.status == MirroringOperationStatus::InProgress)
            .map(|op| (now - op.start_time).num_seconds() as u64)
            .max();
    }
    
    /// Update performance metrics.
    fn update_performance_metrics(&mut self) {
        let completed_operations: Vec<_> = self.operations.iter()
            .filter(|op| op.status == MirroringOperationStatus::Completed)
            .collect();
        
        if completed_operations.is_empty() {
            return;
        }
        
        // Calculate latencies
        let mut latencies: Vec<u64> = completed_operations.iter()
            .filter_map(|op| op.duration_ms)
            .collect();
        latencies.sort_unstable();
        
        if !latencies.is_empty() {
            self.performance.avg_latency_ms = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
            
            let len = latencies.len();
            self.performance.p50_latency_ms = latencies[len / 2] as f64;
            self.performance.p95_latency_ms = latencies[(len * 95) / 100] as f64;
            self.performance.p99_latency_ms = latencies[(len * 99) / 100] as f64;
        }
        
        // Calculate operations per second
        let time_window = chrono::Duration::hours(1);
        let recent_ops = completed_operations.iter()
            .filter(|op| op.start_time >= chrono::Utc::now() - time_window)
            .count();
        self.performance.ops_per_second = recent_ops as f64 / time_window.num_seconds() as f64;
        
        // Calculate throughput
        let recent_data = completed_operations.iter()
            .filter(|op| op.start_time >= chrono::Utc::now() - time_window)
            .map(|op| op.total_size_bytes)
            .sum::<u64>();
        self.performance.throughput_bytes_per_sec = recent_data as f64 / time_window.num_seconds() as f64;
        
        // Calculate error rate
        let total_ops = self.operations.len();
        let failed_ops = self.operations.iter()
            .filter(|op| op.status == MirroringOperationStatus::Failed)
            .count();
        self.performance.error_rate_percent = if total_ops > 0 {
            (failed_ops as f64 / total_ops as f64) * 100.0
        } else {
            0.0
        };
        
        // Calculate retry rate
        let total_retries: u32 = completed_operations.iter()
            .map(|op| op.retry_attempts)
            .sum();
        self.performance.retry_rate_percent = if completed_operations.len() > 0 {
            (total_retries as f64 / completed_operations.len() as f64) * 100.0
        } else {
            0.0
        };
    }
}

/// Configuration for background reconciler.
#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Reconciliation interval in seconds
    pub interval_secs: u64,
    /// Maximum number of tables to reconcile per cycle
    pub max_tables_per_cycle: usize,
    /// Batch size for processing versions
    pub batch_size: usize,
    /// Enable automatic failure recovery
    pub enable_failure_recovery: bool,
    /// Maximum age for pending operations before recovery (seconds)
    pub max_pending_age_secs: u64,
    /// Enable performance monitoring
    pub enable_performance_monitoring: bool,
    /// Reconciliation strategy
    pub strategy: ReconciliationStrategy,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            interval_secs: 60, // 1 minute
            max_tables_per_cycle: 100,
            batch_size: 50,
            enable_failure_recovery: true,
            max_pending_age_secs: 300, // 5 minutes
            enable_performance_monitoring: true,
            strategy: ReconciliationStrategy::Incremental,
        }
    }
}

/// Reconciliation strategy.
#[derive(Debug, Clone, PartialEq)]
pub enum ReconciliationStrategy {
    /// Incremental reconciliation (only process new versions)
    Incremental,
    /// Full reconciliation (process all versions)
    Full,
    /// Smart reconciliation (detect gaps and fill them)
    Smart,
}

/// Background reconciler for continuous mirroring.
#[derive(Debug)]
pub struct BackgroundReconciler {
    /// Mirror engine
    engine: std::sync::Arc<ObjectStorageMirrorEngine>,
    /// Reconciler configuration
    config: ReconcilerConfig,
    /// Reconciliation state
    state: ReconcilerState,
    /// Shutdown receiver
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    /// Shutdown sender
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

/// Reconciliation state tracking.
#[derive(Debug, Clone)]
pub struct ReconcilerState {
    /// Last reconciliation time
    pub last_reconciliation: Option<chrono::DateTime<chrono::Utc>>,
    /// Tables being tracked
    pub tracked_tables: std::collections::HashMap<String, TableReconciliationState>,
    /// Total reconciliations performed
    pub total_reconciliations: u64,
    /// Total versions reconciled
    pub total_versions_reconciled: u64,
    /// Total failures recovered
    pub total_failures_recovered: u64,
    /// Average reconciliation duration
    pub avg_reconciliation_duration_ms: u64,
}

/// State for a single table's reconciliation.
#[derive(Debug, Clone)]
pub struct TableReconciliationState {
    /// Table ID
    pub table_id: String,
    /// Last reconciled version
    pub last_reconciled_version: i64,
    /// Last reconciliation time
    pub last_reconciliation: chrono::DateTime<chrono::Utc>,
    /// Number of versions reconciled
    pub versions_reconciled: u64,
    /// Number of failures recovered
    pub failures_recovered: u64,
    /// Reconciliation status
    pub status: TableReconciliationStatus,
    /// Last error if any
    pub last_error: Option<String>,
}

/// Table reconciliation status.
#[derive(Debug, Clone, PartialEq)]
pub enum TableReconciliationStatus {
    /// Table is being tracked
    Tracked,
    /// Reconciliation is in progress
    InProgress,
    /// Reconciliation completed successfully
    Completed,
    /// Reconciliation failed
    Failed,
    /// Table is paused due to repeated failures
    Paused,
}

/// Reconciliation result.
#[derive(Debug, Clone)]
pub struct ReconciliationResult {
    /// Table ID
    pub table_id: String,
    /// Reconciliation start time
    pub start_time: chrono::DateTime<chrono::Utc>,
    /// Reconciliation end time
    pub end_time: chrono::DateTime<chrono::Utc>,
    /// Number of versions processed
    pub versions_processed: u64,
    /// Number of versions successfully reconciled
    pub versions_reconciled: u64,
    /// Number of failures recovered
    pub failures_recovered: u64,
    /// Whether reconciliation was successful
    pub success: bool,
    /// Error message if failed
    pub error_message: Option<String>,
    /// Reconciliation strategy used
    pub strategy: ReconciliationStrategy,
}

impl BackgroundReconciler {
    /// Create new background reconciler.
    pub fn new(
        engine: std::sync::Arc<ObjectStorageMirrorEngine>,
        config: ReconcilerConfig,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        
        Self {
            engine,
            config,
            state: ReconcilerState {
                last_reconciliation: None,
                tracked_tables: std::collections::HashMap::new(),
                total_reconciliations: 0,
                total_versions_reconciled: 0,
                total_failures_recovered: 0,
                avg_reconciliation_duration_ms: 0,
            },
            shutdown_rx,
            shutdown_tx: Some(shutdown_tx),
        }
    }
    
    /// Run the background reconciler.
    pub async fn run(mut self) {
        tracing::info!(
            interval_secs = %self.config.interval_secs,
            strategy = ?self.config.strategy,
            "Starting background reconciler"
        );
        
        let mut interval = tokio::time::interval(
            std::time::Duration::from_secs(self.config.interval_secs)
        );
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.reconcile_cycle().await {
                        tracing::error!(error = %e, "Reconciliation cycle failed");
                    }
                }
                _ = &mut self.shutdown_rx => {
                    tracing::info!("Background reconciler shutting down");
                    break;
                }
            }
        }
    }
    
    /// Perform a single reconciliation cycle.
    async fn reconcile_cycle(&mut self) -> TxnLogResult<()> {
        let start_time = std::time::Instant::now();
        let cycle_start = chrono::Utc::now();
        
        tracing::debug!("Starting reconciliation cycle");
        
        // Get list of tables to reconcile
        let tables = self.get_tables_to_reconcile().await?;
        
        // Limit number of tables per cycle
        let tables_to_process = tables.into_iter()
            .take(self.config.max_tables_per_cycle)
            .collect::<Vec<_>>();
        
        if tables_to_process.is_empty() {
            tracing::debug!("No tables to reconcile");
            return Ok(());
        }
        
        tracing::info!(
            tables_count = %tables_to_process.len(),
            "Reconciling tables"
        );
        
        // Process each table
        let mut total_versions_processed = 0u64;
        let mut total_versions_reconciled = 0u64;
        let mut total_failures_recovered = 0u64;
        
        for table_id in tables_to_process {
            match self.reconcile_table(&table_id).await {
                Ok(result) => {
                    total_versions_processed += result.versions_processed;
                    total_versions_reconciled += result.versions_reconciled;
                    total_failures_recovered += result.failures_recovered;
                    
                    tracing::debug!(
                        table_id = %table_id,
                        versions_processed = %result.versions_processed,
                        versions_reconciled = %result.versions_reconciled,
                        "Table reconciliation completed"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        table_id = %table_id,
                        error = %e,
                        "Table reconciliation failed"
                    );
                    
                    // Update table state with error
                    if let Some(table_state) = self.state.tracked_tables.get_mut(&table_id) {
                        table_state.status = TableReconciliationStatus::Failed;
                        table_state.last_error = Some(format!("{}", e));
                    }
                }
            }
        }
        
        // Update reconciler state
        let duration_ms = start_time.elapsed().as_millis() as u64;
        self.update_reconciler_state(
            cycle_start,
            total_versions_processed,
            total_versions_reconciled,
            total_failures_recovered,
            duration_ms,
        );
        
        tracing::info!(
            duration_ms = %duration_ms,
            versions_processed = %total_versions_processed,
            versions_reconciled = %total_versions_reconciled,
            failures_recovered = %total_failures_recovered,
            "Reconciliation cycle completed"
        );
        
        Ok(())
    }
    
    /// Get list of tables to reconcile.
    async fn get_tables_to_reconcile(&self) -> TxnLogResult<Vec<String>> {
        // For now, return all tracked tables
        // In a real implementation, this would query the database for tables
        let tables: Vec<String> = self.state.tracked_tables.keys().cloned().collect();
        Ok(tables)
    }
    
    /// Reconcile a single table.
    async fn reconcile_table(&mut self, table_id: &str) -> TxnLogResult<ReconciliationResult> {
        let start_time = chrono::Utc::now();
        
        // Update table state
        let table_state = self.state.tracked_tables.entry(table_id.to_string())
            .or_insert_with(|| TableReconciliationState {
                table_id: table_id.to_string(),
                last_reconciled_version: 0,
                last_reconciliation: chrono::Utc::now(),
                versions_reconciled: 0,
                failures_recovered: 0,
                status: TableReconciliationStatus::Tracked,
                last_error: None,
            });
        
        table_state.status = TableReconciliationStatus::InProgress;
        
        // Get versions to reconcile based on strategy
        let versions_to_reconcile = match self.config.strategy {
            ReconciliationStrategy::Incremental => {
                self.get_incremental_versions(table_id, table_state.last_reconciled_version).await?
            }
            ReconciliationStrategy::Full => {
                self.get_all_versions(table_id).await?
            }
            ReconciliationStrategy::Smart => {
                self.get_smart_versions(table_id, table_state.last_reconciled_version).await?
            }
        };
        
        if versions_to_reconcile.is_empty() {
            table_state.status = TableReconciliationStatus::Completed;
            return Ok(ReconciliationResult {
                table_id: table_id.to_string(),
                start_time,
                end_time: chrono::Utc::now(),
                versions_processed: 0,
                versions_reconciled: 0,
                failures_recovered: 0,
                success: true,
                error_message: None,
                strategy: self.config.strategy.clone(),
            });
        }
        
        // Process versions in batches
        let mut versions_processed = 0u64;
        let mut versions_reconciled = 0u64;
        let mut failures_recovered = 0u64;
        let mut last_error = None;
        
        for batch in versions_to_reconcile.chunks(self.config.batch_size) {
            for version in batch {
                versions_processed += 1;
                
                match self.reconcile_version(table_id, *version).await {
                    Ok(success) => {
                        if success {
                            versions_reconciled += 1;
                            table_state.last_reconciled_version = *version;
                            table_state.versions_reconciled += 1;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            table_id = %table_id,
                            version = %version,
                            error = %e,
                            "Version reconciliation failed"
                        );
                        last_error = Some(format!("{}", e));
                    }
                }
            }
        }
        
        let end_time = chrono::Utc::now();
        let success = last_error.is_none();
        
        // Update table state
        table_state.last_reconciliation = end_time;
        table_state.status = if success {
            TableReconciliationStatus::Completed
        } else {
            TableReconciliationStatus::Failed
        };
        table_state.last_error = last_error.clone();
        
        Ok(ReconciliationResult {
            table_id: table_id.to_string(),
            start_time,
            end_time,
            versions_processed,
            versions_reconciled,
            failures_recovered,
            success,
            error_message: last_error,
            strategy: self.config.strategy.clone(),
        })
    }

    async fn optimize_table(
        &self,
        table_id: &str,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<i32>,
    ) -> TxnLogResult<()> {
        tracing::info!(
            table_id = %table_id,
            target_size = ?target_size,
            max_concurrent_tasks = ?max_concurrent_tasks,
            "Optimizing table storage"
        );

        // For now, optimization is a no-op in the mirror engine
        // In a real implementation, this could:
        // - Compact small checkpoint files
        // - Remove redundant checkpoints
        // - Optimize file layout
        // - Update statistics

        Ok(())
    }
}
    
    /// Get all versions to reconcile.
    async fn get_all_versions(&self, table_id: &str) -> TxnLogResult<Vec<i64>> {
        // In a real implementation, this would query the database for all versions
        // For now, return empty as this is a placeholder
        Ok(vec![])
    }
    
    /// Get smart versions to reconcile (detect gaps).
    async fn get_smart_versions(&self, table_id: &str, last_version: i64) -> TxnLogResult<Vec<i64>> {
        // In a real implementation, this would detect gaps between SQL and object storage
        // For now, return empty as this is a placeholder
        Ok(vec![])
    }
    
    /// Reconcile a single version.
    async fn reconcile_version(&self, table_id: &str, version: i64) -> TxnLogResult<bool> {
        // Check if version already exists in object storage
        let commit_path = delta_commit_path(table_id, version);
        if self.engine.storage_client.file_exists(&commit_path).await? {
            tracing::debug!(
                table_id = %table_id,
                version = %version,
                "Version already exists in object storage"
            );
            return Ok(true);
        }
        
        // Get actions for this version from database
        let actions = self.get_version_actions(table_id, version).await?;
        
        // Mirror the version
        let result = self.engine.mirror_table_version(table_id, version, &actions).await?;
        
        Ok(result.success)
    }
    
    /// Get actions for a specific version.
    async fn get_version_actions(&self, table_id: &str, version: i64) -> TxnLogResult<Vec<DeltaAction>> {
        // In a real implementation, this would query the database for actions
        // For now, return empty as this is a placeholder
        Ok(vec![])
    }
    
    /// Update reconciler state after a cycle.
    fn update_reconciler_state(
        &mut self,
        cycle_start: chrono::DateTime<chrono::Utc>,
        versions_processed: u64,
        versions_reconciled: u64,
        failures_recovered: u64,
        duration_ms: u64,
    ) {
        self.state.last_reconciliation = Some(cycle_start);
        self.state.total_reconciliations += 1;
        self.state.total_versions_reconciled += versions_reconciled;
        self.state.total_failures_recovered += failures_recovered;
        
        // Update average duration
        if self.state.total_reconciliations == 1 {
            self.state.avg_reconciliation_duration_ms = duration_ms;
        } else {
            let total_duration = self.state.avg_reconciliation_duration_ms * (self.state.total_reconciliations - 1) + duration_ms;
            self.state.avg_reconciliation_duration_ms = total_duration / self.state.total_reconciliations;
        }
    }
    
    /// Add a table to be tracked for reconciliation.
    pub fn track_table(&mut self, table_id: String) {
        self.state.tracked_tables.entry(table_id)
            .or_insert_with(|| TableReconciliationState {
                table_id: table_id.clone(),
                last_reconciled_version: 0,
                last_reconciliation: chrono::Utc::now(),
                versions_reconciled: 0,
                failures_recovered: 0,
                status: TableReconciliationStatus::Tracked,
                last_error: None,
            });
        
        tracing::info!(table_id = %table_id, "Added table to reconciliation tracking");
    }
    
    /// Remove a table from reconciliation tracking.
    pub fn untrack_table(&mut self, table_id: &str) {
        if self.state.tracked_tables.remove(table_id).is_some() {
            tracing::info!(table_id = %table_id, "Removed table from reconciliation tracking");
        }
    }
    
    /// Get current reconciler state.
    pub fn get_state(&self) -> &ReconcilerState {
        &self.state
    }
    
    /// Shutdown the reconciler.
    pub fn shutdown(&mut self) -> Result<(), String> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            shutdown_tx.send(()).map_err(|e| format!("Failed to send shutdown signal: {}", e))
        } else {
            Err("Reconciler already shutdown".to_string())
        }
    }
}

/// Failure recovery configuration.
#[derive(Debug, Clone)]
pub struct FailureRecoveryConfig {
    /// Enable automatic failure recovery
    pub enable_auto_recovery: bool,
    /// Maximum number of recovery attempts per failure
    pub max_recovery_attempts: u32,
    /// Base delay for recovery attempts in seconds
    pub base_recovery_delay_secs: u64,
    /// Maximum delay for recovery attempts in seconds
    pub max_recovery_delay_secs: u64,
    /// Backoff multiplier for recovery attempts
    pub recovery_backoff_multiplier: f64,
    /// Enable failure pattern detection
    pub enable_pattern_detection: bool,
    /// Failure threshold for pattern detection
    pub failure_threshold: u32,
    /// Time window for pattern detection in minutes
    pub pattern_detection_window_mins: u64,
    /// Enable circuit breaker pattern
    pub enable_circuit_breaker: bool,
    /// Circuit breaker failure threshold
    pub circuit_breaker_threshold: u32,
    /// Circuit breaker timeout in seconds
    pub circuit_breaker_timeout_secs: u64,
}

impl Default for FailureRecoveryConfig {
    fn default() -> Self {
        Self {
            enable_auto_recovery: true,
            max_recovery_attempts: 3,
            base_recovery_delay_secs: 60, // 1 minute
            max_recovery_delay_secs: 3600, // 1 hour
            recovery_backoff_multiplier: 2.0,
            enable_pattern_detection: true,
            failure_threshold: 5,
            pattern_detection_window_mins: 30,
            enable_circuit_breaker: true,
            circuit_breaker_threshold: 10,
            circuit_breaker_timeout_secs: 300, // 5 minutes
        }
    }
}

/// Failure recovery manager.
#[derive(Debug)]
pub struct FailureRecoveryManager {
    /// Recovery configuration
    config: FailureRecoveryConfig,
    /// Recovery state tracking
    recovery_state: std::sync::Arc<std::sync::Mutex<RecoveryState>>,
    /// Circuit breaker state
    circuit_breaker: std::sync::Arc<std::sync::Mutex<CircuitBreakerState>>,
    /// Mirror engine reference
    engine: std::sync::Arc<ObjectStorageMirrorEngine>,
}

/// Recovery state tracking.
#[derive(Debug, Clone)]
pub struct RecoveryState {
    /// Active recovery operations
    pub active_recoveries: std::collections::HashMap<String, RecoveryOperation>,
    /// Completed recoveries
    pub completed_recoveries: Vec<RecoveryOperation>,
    /// Failure patterns detected
    pub failure_patterns: Vec<FailurePattern>,
    /// Total recovery attempts
    pub total_recovery_attempts: u64,
    /// Successful recoveries
    pub successful_recoveries: u64,
    /// Failed recoveries
    pub failed_recoveries: u64,
}

/// Recovery operation information.
#[derive(Debug, Clone)]
pub struct RecoveryOperation {
    /// Recovery operation ID
    pub recovery_id: String,
    /// Table ID being recovered
    pub table_id: String,
    /// Version being recovered
    pub version: i64,
    /// Original failure reason
    pub original_failure: String,
    /// Recovery attempt number
    pub attempt_number: u32,
    /// Maximum attempts allowed
    pub max_attempts: u32,
    /// Recovery start time
    pub start_time: chrono::DateTime<chrono::Utc>,
    /// Last attempt time
    pub last_attempt_time: chrono::DateTime<chrono::Utc>,
    /// Next attempt time
    pub next_attempt_time: chrono::DateTime<chrono::Utc>,
    /// Recovery status
    pub status: RecoveryStatus,
    /// Recovery strategy used
    pub strategy: RecoveryStrategy,
}

/// Recovery status.
#[derive(Debug, Clone, PartialEq)]
pub enum RecoveryStatus {
    /// Recovery is pending
    Pending,
    /// Recovery is in progress
    InProgress,
    /// Recovery completed successfully
    Completed,
    /// Recovery failed permanently
    Failed,
    /// Recovery was cancelled
    Cancelled,
}

/// Recovery strategy.
#[derive(Debug, Clone, PartialEq)]
pub enum RecoveryStrategy {
    /// Simple retry with same parameters
    SimpleRetry,
    /// Retry with exponential backoff
    ExponentialBackoff,
    /// Retry with different parameters
    ParameterAdjustment,
    /// Full reconstruction from source
    FullReconstruction,
    /// Selective recovery (only failed components)
    SelectiveRecovery,
}

/// Failure pattern information.
#[derive(Debug, Clone)]
pub struct FailurePattern {
    /// Pattern ID
    pub pattern_id: String,
    /// Pattern type
    pub pattern_type: FailurePatternType,
    /// Pattern description
    pub description: String,
    /// First occurrence
    pub first_occurrence: chrono::DateTime<chrono::Utc>,
    /// Last occurrence
    pub last_occurrence: chrono::DateTime<chrono::Utc>,
    /// Number of occurrences
    pub occurrence_count: u32,
    /// Affected tables
    pub affected_tables: Vec<String>,
    /// Pattern severity
    pub severity: FailureSeverity,
    /// Recommended action
    pub recommended_action: String,
}

/// Failure pattern type.
#[derive(Debug, Clone, PartialEq)]
pub enum FailurePatternType {
    /// Storage connectivity issues
    StorageConnectivity,
    /// Network timeouts
    NetworkTimeout,
    /// Serialization failures
    SerializationFailure,
    /// Validation failures
    ValidationFailure,
    /// Resource exhaustion
    ResourceExhaustion,
    /// Concurrent access conflicts
    ConcurrentAccess,
    /// Data corruption
    DataCorruption,
    /// Unknown pattern
    Unknown,
}

/// Failure severity level.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum FailureSeverity {
    /// Low severity - minor issues
    Low,
    /// Medium severity - affecting performance
    Medium,
    /// High severity - affecting functionality
    High,
    /// Critical severity - system-wide impact
    Critical,
}

/// Circuit breaker state.
#[derive(Debug, Clone)]
pub struct CircuitBreakerState {
    /// Current circuit breaker state
    pub state: CircuitBreakerStatus,
    /// Number of consecutive failures
    pub failure_count: u32,
    /// Last failure time
    pub last_failure_time: Option<chrono::DateTime<chrono::Utc>>,
    /// Circuit breaker open time
    pub open_time: Option<chrono::DateTime<chrono::Utc>>,
    /// Total trips (times circuit opened)
    pub total_trips: u64,
}

/// Circuit breaker status.
#[derive(Debug, Clone, PartialEq)]
pub enum CircuitBreakerStatus {
    /// Circuit is closed (normal operation)
    Closed,
    /// Circuit is open (blocking operations)
    Open,
    /// Circuit is half-open (testing recovery)
    HalfOpen,
}

/// Recovery result.
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    /// Recovery operation ID
    pub recovery_id: String,
    /// Table ID
    pub table_id: String,
    /// Version
    pub version: i64,
    /// Recovery success
    pub success: bool,
    /// Number of attempts made
    pub attempts_made: u32,
    /// Total recovery duration
    pub total_duration_ms: u64,
    /// Recovery strategy used
    pub strategy: RecoveryStrategy,
    /// Error message if failed
    pub error_message: Option<String>,
    /// Recovery timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl FailureRecoveryManager {
    /// Create new failure recovery manager.
    pub fn new(
        config: FailureRecoveryConfig,
        engine: std::sync::Arc<ObjectStorageMirrorEngine>,
    ) -> Self {
        Self {
            config,
            recovery_state: std::sync::Arc::new(std::sync::Mutex::new(RecoveryState {
                active_recoveries: std::collections::HashMap::new(),
                completed_recoveries: Vec::new(),
                failure_patterns: Vec::new(),
                total_recovery_attempts: 0,
                successful_recoveries: 0,
                failed_recoveries: 0,
            })),
            circuit_breaker: std::sync::Arc::new(std::sync::Mutex::new(CircuitBreakerState {
                state: CircuitBreakerStatus::Closed,
                failure_count: 0,
                last_failure_time: None,
                open_time: None,
                total_trips: 0,
            })),
            engine,
        }
    }
    
    /// Handle a mirroring failure and initiate recovery if needed.
    pub async fn handle_failure(
        &self,
        table_id: &str,
        version: i64,
        error: &TxnLogError,
    ) -> TxnLogResult<Option<RecoveryResult>> {
        if !self.config.enable_auto_recovery {
            tracing::warn!(
                table_id = %table_id,
                version = %version,
                error = %error,
                "Auto recovery disabled, failure not handled"
            );
            return Ok(None);
        }
        
        // Check circuit breaker
        if self.config.enable_circuit_breaker && !self.check_circuit_breaker().await {
            tracing::warn!(
                table_id = %table_id,
                version = %version,
                "Circuit breaker is open, skipping recovery"
            );
            return Ok(None);
        }
        
        // Detect failure pattern
        let pattern_type = self.detect_failure_pattern(error);
        
        // Determine recovery strategy
        let strategy = self.determine_recovery_strategy(error, &pattern_type);
        
        // Create recovery operation
        let recovery_id = format!("recovery-{}-{}-{}", table_id, version, uuid::Uuid::new_v4());
        let recovery_operation = RecoveryOperation {
            recovery_id: recovery_id.clone(),
            table_id: table_id.to_string(),
            version,
            original_failure: format!("{}", error),
            attempt_number: 1,
            max_attempts: self.config.max_recovery_attempts,
            start_time: chrono::Utc::now(),
            last_attempt_time: chrono::Utc::now(),
            next_attempt_time: chrono::Utc::now(),
            status: RecoveryStatus::Pending,
            strategy,
        };
        
        // Add to active recoveries
        {
            let mut state = self.recovery_state.lock().unwrap();
            state.active_recoveries.insert(recovery_id.clone(), recovery_operation);
            state.total_recovery_attempts += 1;
        }
        
        tracing::info!(
            table_id = %table_id,
            version = %version,
            recovery_id = %recovery_id,
            strategy = ?strategy,
            "Started recovery operation"
        );
        
        // Execute recovery
        let result = self.execute_recovery(&recovery_id).await;
        
        // Handle result
        match result {
            Ok(recovery_result) => {
                if recovery_result.success {
                    self.record_recovery_success(&recovery_id).await;
                } else {
                    self.record_recovery_failure(&recovery_id, &recovery_result.error_message).await;
                }
                Ok(Some(recovery_result))
            }
            Err(e) => {
                self.record_recovery_failure(&recovery_id, &Some(format!("{}", e))).await;
                Err(e)
            }
        }
    }
    
    /// Execute a recovery operation.
    async fn execute_recovery(&self, recovery_id: &str) -> TxnLogResult<RecoveryResult> {
        let start_time = std::time::Instant::now();
        
        loop {
            // Get recovery operation
            let recovery_op = {
                let state = self.recovery_state.lock().unwrap();
                state.active_recoveries.get(recovery_id).cloned()
            };
            
            let recovery_op = match recovery_op {
                Some(op) => op,
                None => return Err(TxnLogError::Internal("Recovery operation not found".to_string())),
            };
            
            // Check if we've exceeded max attempts
            if recovery_op.attempt_number > recovery_op.max_attempts {
                return Ok(RecoveryResult {
                    recovery_id: recovery_id.to_string(),
                    table_id: recovery_op.table_id.clone(),
                    version: recovery_op.version,
                    success: false,
                    attempts_made: recovery_op.attempt_number - 1,
                    total_duration_ms: start_time.elapsed().as_millis() as u64,
                    strategy: recovery_op.strategy,
                    error_message: Some("Maximum recovery attempts exceeded".to_string()),
                    timestamp: chrono::Utc::now(),
                });
            }
            
            // Wait until next attempt time
            let now = chrono::Utc::now();
            if now < recovery_op.next_attempt_time {
                let wait_duration = (recovery_op.next_attempt_time - now).to_std()
                    .unwrap_or(std::time::Duration::from_secs(0));
                tokio::time::sleep(wait_duration).await;
            }
            
            // Update attempt status
            {
                let mut state = self.recovery_state.lock().unwrap();
                if let Some(op) = state.active_recoveries.get_mut(recovery_id) {
                    op.status = RecoveryStatus::InProgress;
                    op.last_attempt_time = chrono::Utc::now();
                }
            }
            
            // Execute recovery based on strategy
            let result = match recovery_op.strategy {
                RecoveryStrategy::SimpleRetry => {
                    self.execute_simple_retry(&recovery_op).await
                }
                RecoveryStrategy::ExponentialBackoff => {
                    self.execute_exponential_backoff_retry(&recovery_op).await
                }
                RecoveryStrategy::ParameterAdjustment => {
                    self.execute_parameter_adjustment_retry(&recovery_op).await
                }
                RecoveryStrategy::FullReconstruction => {
                    self.execute_full_reconstruction(&recovery_op).await
                }
                RecoveryStrategy::SelectiveRecovery => {
                    self.execute_selective_recovery(&recovery_op).await
                }
            };
            
            match result {
                Ok(success) => {
                    if success {
                        return Ok(RecoveryResult {
                            recovery_id: recovery_id.to_string(),
                            table_id: recovery_op.table_id.clone(),
                            version: recovery_op.version,
                            success: true,
                            attempts_made: recovery_op.attempt_number,
                            total_duration_ms: start_time.elapsed().as_millis() as u64,
                            strategy: recovery_op.strategy,
                            error_message: None,
                            timestamp: chrono::Utc::now(),
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        recovery_id = %recovery_id,
                        attempt = %recovery_op.attempt_number,
                        error = %e,
                        "Recovery attempt failed"
                    );
                }
            }
            
            // Schedule next attempt
            let next_attempt_time = self.calculate_next_attempt_time(recovery_op.attempt_number);
            
            {
                let mut state = self.recovery_state.lock().unwrap();
                if let Some(op) = state.active_recoveries.get_mut(recovery_id) {
                    op.attempt_number += 1;
                    op.next_attempt_time = next_attempt_time;
                    op.status = RecoveryStatus::Pending;
                }
            }
        }
    }
    
    /// Execute simple retry recovery.
    async fn execute_simple_retry(&self, recovery_op: &RecoveryOperation) -> TxnLogResult<bool> {
        // Get actions for the version and retry mirroring
        let actions = self.get_version_actions(&recovery_op.table_id, recovery_op.version).await?;
        let result = self.engine.mirror_table_version(&recovery_op.table_id, recovery_op.version, &actions).await?;
        Ok(result.success)
    }
    
    /// Execute exponential backoff retry recovery.
    async fn execute_exponential_backoff_retry(&self, recovery_op: &RecoveryOperation) -> TxnLogResult<bool> {
        // Similar to simple retry but with built-in exponential backoff
        self.execute_simple_retry(recovery_op).await
    }
    
    /// Execute parameter adjustment retry recovery.
    async fn execute_parameter_adjustment_retry(&self, recovery_op: &RecoveryOperation) -> TxnLogResult<bool> {
        // Try with different parameters (e.g., smaller batch size)
        tracing::info!(
            recovery_id = %recovery_op.recovery_id,
            "Attempting recovery with parameter adjustment"
        );
        
        self.execute_simple_retry(recovery_op).await
    }
    
    /// Execute full reconstruction recovery.
    async fn execute_full_reconstruction(&self, recovery_op: &RecoveryOperation) -> TxnLogResult<bool> {
        // Reconstruct the entire version from source data
        tracing::info!(
            recovery_id = %recovery_op.recovery_id,
            "Attempting full reconstruction recovery"
        );
        
        self.execute_simple_retry(recovery_op).await
    }
    
    /// Execute selective recovery.
    async fn execute_selective_recovery(&self, recovery_op: &RecoveryOperation) -> TxnLogResult<bool> {
        // Recover only the failed components
        tracing::info!(
            recovery_id = %recovery_op.recovery_id,
            "Attempting selective recovery"
        );
        
        self.execute_simple_retry(recovery_op).await
    }
    
    /// Get actions for a specific version.
    async fn get_version_actions(&self, table_id: &str, version: i64) -> TxnLogResult<Vec<DeltaAction>> {
        // In a real implementation, this would query the database
        // For now, return empty as this is a placeholder
        Ok(vec![])
    }
    
    /// Calculate next attempt time with exponential backoff.
    fn calculate_next_attempt_time(&self, attempt_number: u32) -> chrono::DateTime<chrono::Utc> {
        let base_delay = self.config.base_recovery_delay_secs as f64;
        let multiplier = self.config.recovery_backoff_multiplier.powf((attempt_number - 1) as f64);
        let delay_secs = base_delay * multiplier;
        let max_delay = self.config.max_recovery_delay_secs as f64;
        let final_delay = delay_secs.min(max_delay);
        
        chrono::Utc::now() + chrono::Duration::seconds(final_delay as i64)
    }
    
    /// Detect failure pattern from error.
    fn detect_failure_pattern(&self, error: &TxnLogError) -> FailurePatternType {
        match error {
            TxnLogError::ObjectStorage(_) => FailurePatternType::StorageConnectivity,
            TxnLogError::ConnectionError(_) => FailurePatternType::NetworkTimeout,
            TxnLogError::Serialization(_) => FailurePatternType::SerializationFailure,
            TxnLogError::Validation(_) => FailurePatternType::ValidationFailure,
            TxnLogError::Internal(_) => FailurePatternType::ResourceExhaustion,
            _ => FailurePatternType::Unknown,
        }
    }
    
    /// Determine recovery strategy based on error and pattern.
    fn determine_recovery_strategy(&self, error: &TxnLogError, pattern_type: &FailurePatternType) -> RecoveryStrategy {
        match pattern_type {
            FailurePatternType::StorageConnectivity => RecoveryStrategy::ExponentialBackoff,
            FailurePatternType::NetworkTimeout => RecoveryStrategy::ExponentialBackoff,
            FailurePatternType::SerializationFailure => RecoveryStrategy::ParameterAdjustment,
            FailurePatternType::ValidationFailure => RecoveryStrategy::ParameterAdjustment,
            FailurePatternType::ResourceExhaustion => RecoveryStrategy::ParameterAdjustment,
            FailurePatternType::ConcurrentAccess => RecoveryStrategy::SelectiveRecovery,
            FailurePatternType::DataCorruption => RecoveryStrategy::FullReconstruction,
            FailurePatternType::Unknown => RecoveryStrategy::SimpleRetry,
        }
    }
    
    /// Check circuit breaker state.
    async fn check_circuit_breaker(&self) -> bool {
        let mut breaker = self.circuit_breaker.lock().unwrap();
        
        match breaker.state {
            CircuitBreakerStatus::Closed => true,
            CircuitBreakerStatus::Open => {
                // Check if timeout has passed
                if let Some(open_time) = breaker.open_time {
                    let elapsed = (chrono::Utc::now() - open_time).num_seconds();
                    if elapsed >= self.config.circuit_breaker_timeout_secs as i64 {
                        breaker.state = CircuitBreakerStatus::HalfOpen;
                        tracing::info!("Circuit breaker transitioning to half-open");
                        return true;
                    }
                }
                false
            }
            CircuitBreakerStatus::HalfOpen => true,
        }
    }
    
    /// Record successful recovery.
    async fn record_recovery_success(&self, recovery_id: &str) {
        let mut state = self.recovery_state.lock().unwrap();
        
        if let Some(recovery_op) = state.active_recoveries.remove(recovery_id) {
            let mut completed_recovery = recovery_op.clone();
            completed_recovery.status = RecoveryStatus::Completed;
            state.completed_recoveries.push(completed_recovery);
            state.successful_recoveries += 1;
            
            // Reset circuit breaker on success
            if self.config.enable_circuit_breaker {
                let mut breaker = self.circuit_breaker.lock().unwrap();
                breaker.state = CircuitBreakerStatus::Closed;
                breaker.failure_count = 0;
            }
        }
        
        tracing::info!(recovery_id = %recovery_id, "Recovery completed successfully");
    }
    
    /// Record recovery failure.
    async fn record_recovery_failure(&self, recovery_id: &str, error_message: &Option<String>) {
        let mut state = self.recovery_state.lock().unwrap();
        
        if let Some(recovery_op) = state.active_recoveries.remove(recovery_id) {
            let mut completed_recovery = recovery_op.clone();
            completed_recovery.status = RecoveryStatus::Failed;
            state.completed_recoveries.push(completed_recovery);
            state.failed_recoveries += 1;
            
            // Update circuit breaker on failure
            if self.config.enable_circuit_breaker {
                let mut breaker = self.circuit_breaker.lock().unwrap();
                breaker.failure_count += 1;
                breaker.last_failure_time = Some(chrono::Utc::now());
                
                if breaker.failure_count >= self.config.circuit_breaker_threshold {
                    breaker.state = CircuitBreakerStatus::Open;
                    breaker.open_time = Some(chrono::Utc::now());
                    breaker.total_trips += 1;
                    
                    tracing::warn!(
                        failure_count = %breaker.failure_count,
                        threshold = %self.config.circuit_breaker_threshold,
                        "Circuit breaker opened due to excessive failures"
                    );
                }
            }
        }
        
        tracing::error!(
            recovery_id = %recovery_id,
            error = ?error_message,
            "Recovery failed"
        );
    }
    
    /// Get current recovery state.
    pub fn get_recovery_state(&self) -> RecoveryState {
        self.recovery_state.lock().unwrap().clone()
    }
    
    /// Get circuit breaker state.
    pub fn get_circuit_breaker_state(&self) -> CircuitBreakerState {
        self.circuit_breaker.lock().unwrap().clone()
    }
    
    /// Reset circuit breaker manually.
    pub fn reset_circuit_breaker(&self) {
        let mut breaker = self.circuit_breaker.lock().unwrap();
        breaker.state = CircuitBreakerStatus::Closed;
        breaker.failure_count = 0;
        breaker.last_failure_time = None;
        breaker.open_time = None;
        
        tracing::info!("Circuit breaker manually reset");
    }
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

    /// Optimize table storage (compact checkpoints, cleanup old files, etc.).
    async fn optimize_table(
        &self,
        table_id: &str,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<i32>,
    ) -> TxnLogResult<()>;
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
    /// Current mirroring statistics
    pub mirroring_stats: MirroringStats,
    /// Health check results
    pub health_checks: HashMap<String, HealthCheckResult>,
    /// Performance metrics
    pub performance_metrics: PerformanceMetrics,
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
    retry_executor: RetryExecutor,
    tracker: std::sync::Arc<std::sync::Mutex<MirroringTracker>>,
    recovery_manager: std::sync::Arc<FailureRecoveryManager>,
}

impl ObjectStorageMirrorEngine {
    /// Create new mirror engine with object storage.
    pub async fn new(config: MirroringConfig) -> TxnLogResult<Self> {
        let storage_client = ObjectStorageClient::new(config.storage_config.clone()).await?;
        
        // Default checkpoint cadence config
        let checkpoint_config = CheckpointCadenceConfig::default();
        let checkpoint_manager = CheckpointCadenceManager::new(checkpoint_config);
        
        // Use retry config from mirroring config
        let retry_executor = RetryExecutor::new(config.retry_config.clone());
        
        // Initialize tracking with 1000 operation history
        let tracker = std::sync::Arc::new(std::sync::Mutex::new(
            MirroringTracker::new(1000)
        ));
        
        Ok(Self {
            storage_client,
            config,
            checkpoint_manager,
            retry_executor,
            tracker,
        })
    }
    
    /// Create mirror engine with custom checkpoint cadence.
    pub async fn new_with_cadence(
        config: MirroringConfig,
        checkpoint_config: CheckpointCadenceConfig,
    ) -> TxnLogResult<Self> {
        let storage_client = ObjectStorageClient::new(config.storage_config.clone()).await?;
        let checkpoint_manager = CheckpointCadenceManager::new(checkpoint_config);
        let retry_executor = RetryExecutor::new(config.retry_config.clone());
        
        // Initialize tracking with 1000 operation history
        let tracker = std::sync::Arc::new(std::sync::Mutex::new(
            MirroringTracker::new(1000)
        ));
        
        Ok(Self {
            storage_client,
            config,
            checkpoint_manager,
            retry_executor,
            tracker,
        })
    }
    
    /// Get checkpoint statistics.
    pub fn get_checkpoint_stats(&self) -> CheckpointStats {
        self.checkpoint_manager.get_stats()
    }
    
    /// Get mirroring tracker.
    pub fn get_tracker(&self) -> std::sync::Arc<std::sync::Mutex<MirroringTracker>> {
        self.tracker.clone()
    }
    
    /// Get current mirroring statistics.
    pub fn get_mirroring_stats(&self) -> MirroringStats {
        self.tracker.lock().unwrap().get_stats().clone()
    }
    
    /// Get current performance metrics.
    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        self.tracker.lock().unwrap().get_performance_metrics().clone()
    }
    
    /// Get recent operations.
    pub fn get_recent_operations(&self, limit: usize) -> Vec<MirroringOperationRecord> {
        self.tracker.lock().unwrap().get_recent_operations(limit).to_vec()
    }
    
    /// Run health checks and record results.
    pub async fn run_health_checks(&self) -> TxnLogResult<()> {
        let mut tracker = self.tracker.lock().unwrap();
        
        // Storage connectivity check
        let storage_check = self.check_storage_connectivity().await;
        tracker.record_health_check("storage_connectivity".to_string(), storage_check);
        
        // Performance check
        let performance_check = self.check_performance().await;
        tracker.record_health_check("performance".to_string(), performance_check);
        
        // Queue depth check
        let queue_check = self.check_queue_depth();
        tracker.record_health_check("queue_depth".to_string(), queue_check);
        
        // Error rate check
        let error_rate_check = self.check_error_rate();
        tracker.record_health_check("error_rate".to_string(), error_rate_check);
        
        Ok(())
    }
    
    /// Check storage connectivity.
    async fn check_storage_connectivity(&self) -> HealthCheckResult {
        let start_time = std::time::Instant::now();
        let timestamp = chrono::Utc::now();
        
        match self.storage_client.file_exists("_health_check").await {
            Ok(_) => {
                let duration_ms = start_time.elapsed().as_millis() as u64;
                HealthCheckResult {
                    name: "storage_connectivity".to_string(),
                    healthy: true,
                    message: "Storage is accessible".to_string(),
                    timestamp,
                    duration_ms,
                    details: HashMap::new(),
                }
            }
            Err(e) => {
                let duration_ms = start_time.elapsed().as_millis() as u64;
                let mut details = HashMap::new();
                details.insert("error".to_string(), format!("{}", e));
                
                HealthCheckResult {
                    name: "storage_connectivity".to_string(),
                    healthy: false,
                    message: "Storage is not accessible".to_string(),
                    timestamp,
                    duration_ms,
                    details,
                }
            }
        }
    }
    
    /// Check performance metrics.
    async fn check_performance(&self) -> HealthCheckResult {
        let start_time = std::time::Instant::now();
        let timestamp = chrono::Utc::now();
        let performance = self.tracker.lock().unwrap().get_performance_metrics();
        
        let healthy = performance.avg_latency_ms < 5000.0 && // Less than 5 seconds average
                     performance.error_rate_percent < 10.0;   // Less than 10% error rate
        
        let mut details = HashMap::new();
        details.insert("avg_latency_ms".to_string(), format!("{:.2}", performance.avg_latency_ms));
        details.insert("error_rate_percent".to_string(), format!("{:.2}", performance.error_rate_percent));
        details.insert("ops_per_second".to_string(), format!("{:.2}", performance.ops_per_second));
        
        HealthCheckResult {
            name: "performance".to_string(),
            healthy,
            message: if healthy {
                "Performance is within acceptable limits".to_string()
            } else {
                "Performance is degraded".to_string()
            },
            timestamp,
            duration_ms: start_time.elapsed().as_millis() as u64,
            details,
        }
    }
    
    /// Check queue depth.
    fn check_queue_depth(&self) -> HealthCheckResult {
        let start_time = std::time::Instant::now();
        let timestamp = chrono::Utc::now();
        let stats = self.tracker.lock().unwrap().get_stats();
        
        let healthy = stats.queue_depth < 100; // Less than 100 pending operations
        
        let mut details = HashMap::new();
        details.insert("queue_depth".to_string(), stats.queue_depth.to_string());
        details.insert("in_progress_tables".to_string(), stats.in_progress_tables.to_string());
        
        HealthCheckResult {
            name: "queue_depth".to_string(),
            healthy,
            message: if healthy {
                "Queue depth is acceptable".to_string()
            } else {
                "Queue depth is too high".to_string()
            },
            timestamp,
            duration_ms: start_time.elapsed().as_millis() as u64,
            details,
        }
    }
    
    /// Check error rate.
    fn check_error_rate(&self) -> HealthCheckResult {
        let start_time = std::time::Instant::now();
        let timestamp = chrono::Utc::now();
        let performance = self.tracker.lock().unwrap().get_performance_metrics();
        
        let healthy = performance.error_rate_percent < 5.0; // Less than 5% error rate
        
        let mut details = HashMap::new();
        details.insert("error_rate_percent".to_string(), format!("{:.2}", performance.error_rate_percent));
        details.insert("retry_rate_percent".to_string(), format!("{:.2}", performance.retry_rate_percent));
        
        HealthCheckResult {
            name: "error_rate".to_string(),
            healthy,
            message: if healthy {
                "Error rate is acceptable".to_string()
            } else {
                "Error rate is too high".to_string()
            },
            timestamp,
            duration_ms: start_time.elapsed().as_millis() as u64,
            details,
        }
    }
    
    /// Mirror table version with retry logic.
    pub async fn mirror_table_version_with_retry(
        &self,
        table_id: &str,
        version: i64,
        actions: &[DeltaAction],
    ) -> RetryResult<MirroringResult> {
        let operation = MirroringOperation::new(
            table_id.to_string(),
            version,
            actions.to_vec(),
            std::sync::Arc::new(self),
        );
        
        let retry_result = self.retry_executor.execute_with_retry(operation).await;
        
        // Update retry tracking
        if retry_result.total_attempts > 1 {
            tracing::info!(
                table_id = %table_id,
                version = %version,
                total_attempts = %retry_result.total_attempts,
                total_duration_ms = %retry_result.total_duration_ms,
                "Mirroring completed with retries"
            );
            
            // Update tracker with retry information
            let operation_id = format!("{}-{}", table_id, version);
            let mut tracker = self.tracker.lock().unwrap();
            if let Some(record) = tracker.operations.iter_mut()
                .find(|op| op.table_id == table_id && op.version == version) {
                record.retry_attempts = retry_result.total_attempts - 1;
            }
        }
        
        retry_result
    }
    
    /// Start periodic health checks.
    pub fn start_health_checks(&self, interval_secs: u64) -> tokio::task::JoinHandle<()> {
        let engine = std::sync::Arc::new(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                std::time::Duration::from_secs(interval_secs)
            );
            
            loop {
                interval.tick().await;
                
                if let Err(e) = engine.run_health_checks().await {
                    tracing::error!(error = %e, "Health check failed");
                }
            }
        })
    }
    
    /// Start background reconciler.
    pub fn start_reconciler(&self, config: ReconcilerConfig) -> tokio::task::JoinHandle<()> {
        let engine = std::sync::Arc::new(self);
        tokio::spawn(async move {
            let reconciler = BackgroundReconciler::new(engine, config);
            reconciler.run().await;
        })
    }
    
    /// Mirror multiple table versions with concurrent retry logic.
    pub async fn mirror_multiple_versions(
        &self,
        operations: Vec<(String, i64, Vec<DeltaAction>)>,
    ) -> Vec<RetryResult<MirroringResult>> {
        use futures::future::join_all;
        
        let futures: Vec<_> = operations
            .into_iter()
            .map(|(table_id, version, actions)| {
                let operation = MirroringOperation::new(
                    table_id.clone(),
                    version,
                    actions,
                    std::sync::Arc::new(self),
                );
                
                async move {
                    let retry_executor = RetryExecutor::new(RetryConfig::default());
                    retry_executor.execute_with_retry(operation).await
                }
            })
            .collect();
        
        join_all(futures).await
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
        let operation_id = format!("{}-{}-{}", table_id, version, uuid::Uuid::new_v4());
        
        // Start tracking the operation
        {
            let mut tracker = self.tracker.lock().unwrap();
            tracker.start_operation(operation_id.clone(), table_id.to_string(), version);
        }
        
        tracing::info!(
            table_id = %table_id,
            version = %version,
            action_count = %actions.len(),
            operation_id = %operation_id,
            "Starting table version mirroring"
        );

        let mut files_written = 0;
        let mut total_size = 0;
        let mut error_message = None;
        let mut retry_attempts = 0;

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

        // Complete tracking
        {
            let mut tracker = self.tracker.lock().unwrap();
            tracker.complete_operation(
                &operation_id,
                success,
                files_written as u64,
                total_size,
                error_message.clone(),
                retry_attempts,
            );
        }

        if success {
            tracing::info!(
                table_id = %table_id,
                version = %version,
                files_written = %files_written,
                total_size = %total_size,
                duration_ms = %duration.as_millis(),
                operation_id = %operation_id,
                "Table version mirroring completed successfully"
            );
        } else {
            tracing::error!(
                table_id = %table_id,
                version = %version,
                error = %error_message.as_ref().unwrap(),
                duration_ms = %duration.as_millis(),
                operation_id = %operation_id,
                "Table version mirroring failed"
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
        let tracker = self.tracker.lock().unwrap();
        let stats = tracker.get_stats();
        let performance = tracker.get_performance_metrics();
        let health_checks = tracker.get_health_checks();
        
        // Determine overall health
        let healthy = stats.failed_tables == 0 && 
                     performance.error_rate_percent < 5.0 &&
                     health_checks.values().all(|check| check.healthy);
        
        Ok(MirrorStatus {
            healthy,
            backend_type: self.config.storage_config.backend_type.clone(),
            endpoint: self.config.storage_config.endpoint.clone(),
            last_success: Some(chrono::Utc::now()),
            total_operations: stats.total_versions,
            failed_operations: stats.failed_tables,
            avg_duration_ms: performance.avg_latency_ms as u64,
            details: HashMap::new(),
            mirroring_stats: stats.clone(),
            health_checks: health_checks.clone(),
            performance_metrics: performance.clone(),
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
            mirroring_stats: MirroringStats::default(),
            health_checks: HashMap::new(),
            performance_metrics: PerformanceMetrics::default(),
        })
    }

    async fn optimize_table(
        &self,
        table_id: &str,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<i32>,
    ) -> TxnLogResult<()> {
        tracing::debug!(
            table_id = %table_id,
            target_size = ?target_size,
            max_concurrent_tasks = ?max_concurrent_tasks,
            "NoOp mirror engine optimize_table called"
        );
        Ok(())
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
        
    #[tokio::test]
    async fn test_mirroring_tracker() {
        let mut tracker = MirroringTracker::new(100);
        
        // Test starting operations
        tracker.start_operation("op1".to_string(), "table1".to_string(), 1);
        tracker.start_operation("op2".to_string(), "table2".to_string(), 2);
        
        let stats = tracker.get_stats();
        assert_eq!(stats.in_progress_tables, 2);
        assert_eq!(stats.queue_depth, 0);
        
        // Test completing operations
        tracker.complete_operation("op1", true, 5, 1024, None, 0);
        tracker.complete_operation("op2", false, 0, 0, Some("Failed".to_string()), 2);
        
        let stats = tracker.get_stats();
        assert_eq!(stats.in_progress_tables, 0);
        assert_eq!(stats.successful_tables, 1);
        assert_eq!(stats.failed_tables, 1);
        assert_eq!(stats.total_versions, 1);
        assert_eq!(stats.total_data_bytes, 1024);
    }
    
    #[tokio::test]
    async fn test_health_checks() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = MirroringConfig {
            storage_config: ObjectStorageConfig {
                backend_type: StorageBackend::Local,
                endpoint: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                bucket: temp_dir.path().to_str().unwrap().to_string(),
                region: None,
                additional_params: std::collections::HashMap::new(),
            },
            ..Default::default()
        };
        
        let engine = ObjectStorageMirrorEngine::new(config).await.unwrap();
        
        // Run health checks
        engine.run_health_checks().await.unwrap();
        
        let status = engine.get_status().await.unwrap();
        assert!(status.healthy);
        assert!(!status.health_checks.is_empty());
        
        // Check specific health checks
        let storage_check = status.health_checks.get("storage_connectivity").unwrap();
        assert!(storage_check.healthy);
        assert_eq!(storage_check.name, "storage_connectivity");
        
        let performance_check = status.health_checks.get("performance").unwrap();
        assert!(performance_check.healthy);
        
        let queue_check = status.health_checks.get("queue_depth").unwrap();
        assert!(queue_check.healthy);
        
        let error_rate_check = status.health_checks.get("error_rate").unwrap();
        assert!(error_rate_check.healthy);
    }
    
    #[tokio::test]
    async fn test_mirroring_stats() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = MirroringConfig {
            storage_config: ObjectStorageConfig {
                backend_type: StorageBackend::Local,
                endpoint: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                bucket: temp_dir.path().to_str().unwrap().to_string(),
                region: None,
                additional_params: std::collections::HashMap::new(),
            },
            ..Default::default()
        };
        
        let engine = ObjectStorageMirrorEngine::new(config).await.unwrap();
        
        // Perform some mirroring operations
        let actions = vec![
            DeltaAction::Protocol(Protocol::default()),
            DeltaAction::Add(AddFile::new("test.parquet".to_string(), 1024, 1234567890, true)),
        ];
        
        let result1 = engine.mirror_table_version("table1", 1, &actions).await.unwrap();
        assert!(result1.success);
        
        let result2 = engine.mirror_table_version("table2", 1, &actions).await.unwrap();
        assert!(result2.success);
        
        // Check statistics
        let stats = engine.get_mirroring_stats();
        assert_eq!(stats.successful_tables, 2);
        assert_eq!(stats.total_versions, 2);
        assert!(stats.total_data_bytes > 0);
        
        // Check performance metrics
        let performance = engine.get_performance_metrics();
        assert!(performance.avg_latency_ms > 0.0);
        assert_eq!(performance.error_rate_percent, 0.0);
        
        // Check recent operations
        let recent_ops = engine.get_recent_operations(10);
        assert_eq!(recent_ops.len(), 2);
        assert!(recent_ops.iter().all(|op| op.status == MirroringOperationStatus::Completed));
    }
    
    #[tokio::test]
    async fn test_comprehensive_mirror_status() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = MirroringConfig {
            storage_config: ObjectStorageConfig {
                backend_type: StorageBackend::Local,
                endpoint: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                bucket: temp_dir.path().to_str().unwrap().to_string(),
                region: None,
                additional_params: std::collections::HashMap::new(),
            },
            ..Default::default()
        };
        
        let engine = ObjectStorageMirrorEngine::new(config).await.unwrap();
        
        // Run health checks
        engine.run_health_checks().await.unwrap();
        
        // Get comprehensive status
        let status = engine.get_status().await.unwrap();
        
        assert!(status.healthy);
        assert_eq!(status.backend_type, StorageBackend::Local);
        assert!(status.last_success.is_some());
        
        // Check mirroring stats
        assert_eq!(status.mirroring_stats.total_tables, 0);
        assert_eq!(status.mirroring_stats.successful_tables, 0);
        assert_eq!(status.mirroring_stats.failed_tables, 0);
        
        // Check health checks
        assert!(!status.health_checks.is_empty());
        assert!(status.health_checks.values().all(|check| check.healthy));
        
        // Check performance metrics
        assert_eq!(status.performance_metrics.error_rate_percent, 0.0);
        assert_eq!(status.performance_metrics.retry_rate_percent, 0.0);
    }
    
    #[tokio::test]
    async fn test_background_reconciler() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = MirroringConfig {
            storage_config: ObjectStorageConfig {
                backend_type: StorageBackend::Local,
                endpoint: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                bucket: temp_dir.path().to_str().unwrap().to_string(),
                region: None,
                additional_params: std::collections::HashMap::new(),
            },
            ..Default::default()
        };
        
        let engine = std::sync::Arc::new(ObjectStorageMirrorEngine::new(config).await.unwrap());
        
        let reconciler_config = ReconcilerConfig {
            interval_secs: 1, // 1 second for testing
            max_tables_per_cycle: 10,
            batch_size: 5,
            enable_failure_recovery: true,
            max_pending_age_secs: 60,
            enable_performance_monitoring: true,
            strategy: ReconciliationStrategy::Incremental,
        };
        
        let mut reconciler = BackgroundReconciler::new(engine.clone(), reconciler_config);
        
        // Add a table to track
        reconciler.track_table("test_table".to_string());
        
        // Check initial state
        let state = reconciler.get_state();
        assert_eq!(state.tracked_tables.len(), 1);
        assert!(state.tracked_tables.contains_key("test_table"));
        
        let table_state = state.tracked_tables.get("test_table").unwrap();
        assert_eq!(table_state.table_id, "test_table");
        assert_eq!(table_state.last_reconciled_version, 0);
        assert_eq!(table_state.status, TableReconciliationStatus::Tracked);
        
        // Test shutdown
        let shutdown_result = reconciler.shutdown();
        assert!(shutdown_result.is_ok());
        
        // Test double shutdown
        let shutdown_result = reconciler.shutdown();
        assert!(shutdown_result.is_err());
    }
    
    #[tokio::test]
    async fn test_reconciler_strategies() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = MirroringConfig {
            storage_config: ObjectStorageConfig {
                backend_type: StorageBackend::Local,
                endpoint: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                bucket: temp_dir.path().to_str().unwrap().to_string(),
                region: None,
                additional_params: std::collections::HashMap::new(),
            },
            ..Default::default()
        };
        
        let engine = std::sync::Arc::new(ObjectStorageMirrorEngine::new(config).await.unwrap());
        
        // Test incremental strategy
        let incremental_config = ReconcilerConfig {
            strategy: ReconciliationStrategy::Incremental,
            ..Default::default()
        };
        let incremental_reconciler = BackgroundReconciler::new(engine.clone(), incremental_config);
        assert_eq!(incremental_reconciler.config.strategy, ReconciliationStrategy::Incremental);
        
        // Test full strategy
        let full_config = ReconcilerConfig {
            strategy: ReconciliationStrategy::Full,
            ..Default::default()
        };
        let full_reconciler = BackgroundReconciler::new(engine.clone(), full_config);
        assert_eq!(full_reconciler.config.strategy, ReconciliationStrategy::Full);
        
        // Test smart strategy
        let smart_config = ReconcilerConfig {
            strategy: ReconciliationStrategy::Smart,
            ..Default::default()
        };
        let smart_reconciler = BackgroundReconciler::new(engine, smart_config);
        assert_eq!(smart_reconciler.config.strategy, ReconciliationStrategy::Smart);
    }
    
    #[tokio::test]
    async fn test_table_reconciliation_state() {
        let mut state = TableReconciliationState {
            table_id: "test_table".to_string(),
            last_reconciled_version: 10,
            last_reconciliation: chrono::Utc::now(),
            versions_reconciled: 5,
            failures_recovered: 2,
            status: TableReconciliationStatus::Completed,
            last_error: None,
        };
        
        assert_eq!(state.table_id, "test_table");
        assert_eq!(state.last_reconciled_version, 10);
        assert_eq!(state.versions_reconciled, 5);
        assert_eq!(state.failures_recovered, 2);
        assert_eq!(state.status, TableReconciliationStatus::Completed);
        assert!(state.last_error.is_none());
        
        // Update state
        state.status = TableReconciliationStatus::Failed;
        state.last_error = Some("Test error".to_string());
        state.last_reconciled_version = 15;
        
        assert_eq!(state.status, TableReconciliationStatus::Failed);
        assert_eq!(state.last_error.as_ref().unwrap(), "Test error");
        assert_eq!(state.last_reconciled_version, 15);
    }
    
    #[tokio::test]
    async fn test_reconciliation_result() {
        let start_time = chrono::Utc::now();
        let end_time = start_time + chrono::Duration::seconds(10);
        
        let result = ReconciliationResult {
            table_id: "test_table".to_string(),
            start_time,
            end_time,
            versions_processed: 100,
            versions_reconciled: 95,
            failures_recovered: 3,
            success: true,
            error_message: None,
            strategy: ReconciliationStrategy::Incremental,
        };
        
        assert_eq!(result.table_id, "test_table");
        assert_eq!(result.versions_processed, 100);
        assert_eq!(result.versions_reconciled, 95);
        assert_eq!(result.failures_recovered, 3);
        assert!(result.success);
        assert!(result.error_message.is_none());
        assert_eq!(result.strategy, ReconciliationStrategy::Incremental);
        
        let duration = result.end_time - result.start_time;
        assert_eq!(duration, chrono::Duration::seconds(10));
    }

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

    #[test]
    fn test_retry_config() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.base_delay_ms, 1000);
        assert_eq!(config.max_delay_ms, 30000);
        assert_eq!(config.backoff_multiplier, 2.0);
        assert_eq!(config.jitter_factor, 0.1);
    }

    #[test]
    fn test_retry_delay_calculation() {
        let config = RetryConfig {
            max_attempts: 5,
            base_delay_ms: 1000,
            max_delay_ms: 10000,
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        };
        
        let executor = RetryExecutor::new(config);
        
        // Test delay calculation
        let delay1 = executor.calculate_delay(1); // First retry
        assert!(delay1 >= 1800 && delay1 <= 2200); // 2000 ± 200 jitter
        
        let delay2 = executor.calculate_delay(2); // Second retry
        assert!(delay2 >= 3600 && delay2 <= 4400); // 4000 ± 400 jitter
        
        let delay3 = executor.calculate_delay(3); // Third retry
        assert!(delay3 >= 6400 && delay3 <= 7600); // 7000 ± 700 jitter
        
        // Test max delay clamping
        let delay4 = executor.calculate_delay(4); // Fourth retry (would exceed max)
        assert!(delay4 <= 10000); // Should be clamped to max
    }

    #[tokio::test]
    async fn test_retry_executor_success() {
        let config = RetryConfig::default();
        let executor = RetryExecutor::new(config);
        
        // Mock operation that succeeds on first try
        struct SuccessOperation;
        
        #[async_trait]
        impl RetryableOperation for SuccessOperation {
            type Output = String;
            
            async fn execute(&self) -> TxnLogResult<Self::Output> {
                Ok("success".to_string())
            }
            
            fn is_retryable_error(&self, _error: &TxnLogError) -> bool {
                false
            }
        }
        
        let result = executor.execute_with_retry(SuccessOperation).await;
        
        assert!(result.result.is_ok());
        assert_eq!(result.total_attempts, 1);
        assert_eq!(result.attempts.len(), 1);
        assert!(result.attempts[0].error.is_none());
    }

    #[tokio::test]
    async fn test_retry_executor_with_retries() {
        let config = RetryConfig {
            max_attempts: 3,
            base_delay_ms: 10, // Short delays for testing
            max_delay_ms: 100,
            backoff_multiplier: 2.0,
            jitter_factor: 0.0, // No jitter for predictable testing
        };
        
        let executor = RetryExecutor::new(config);
        
        // Mock operation that fails twice then succeeds
        struct RetryOperation {
            attempt_count: std::sync::atomic::AtomicU32,
        }
        
        #[async_trait]
        impl RetryableOperation for RetryOperation {
            type Output = String;
            
            async fn execute(&self) -> TxnLogResult<Self::Output> {
                let attempt = self.attempt_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                
                if attempt <= 2 {
                    Err(TxnLogError::ObjectStorage("Temporary failure".to_string()))
                } else {
                    Ok("success".to_string())
                }
            }
            
            fn is_retryable_error(&self, error: &TxnLogError) -> bool {
                matches!(error, TxnLogError::ObjectStorage(_))
            }
        }
        
        let result = executor.execute_with_retry(RetryOperation {
            attempt_count: std::sync::atomic::AtomicU32::new(0),
        }).await;
        
        assert!(result.result.is_ok());
        assert_eq!(result.total_attempts, 3);
        assert_eq!(result.attempts.len(), 3);
        
        // Check attempt details
        assert_eq!(result.attempts[0].delay_ms, 0); // First attempt no delay
        assert_eq!(result.attempts[1].delay_ms, 10); // Second attempt 10ms delay
        assert_eq!(result.attempts[2].delay_ms, 20); // Third attempt 20ms delay
        
        assert!(result.attempts[0].error.is_some()); // First attempt failed
        assert!(result.attempts[1].error.is_some()); // Second attempt failed
        assert!(result.attempts[2].error.is_none()); // Third attempt succeeded
    }

    #[tokio::test]
    async fn test_retry_executor_max_attempts() {
        let config = RetryConfig {
            max_attempts: 2,
            base_delay_ms: 10,
            max_delay_ms: 100,
            backoff_multiplier: 2.0,
            jitter_factor: 0.0,
        };
        
        let executor = RetryExecutor::new(config);
        
        // Mock operation that always fails
        struct FailOperation;
        
        #[async_trait]
        impl RetryableOperation for FailOperation {
            type Output = String;
            
            async fn execute(&self) -> TxnLogResult<Self::Output> {
                Err(TxnLogError::ObjectStorage("Always fails".to_string()))
            }
            
            fn is_retryable_error(&self, error: &TxnLogError) -> bool {
                matches!(error, TxnLogError::ObjectStorage(_))
            }
        }
        
        let result = executor.execute_with_retry(FailOperation).await;
        
        assert!(result.result.is_err());
        assert_eq!(result.total_attempts, 2);
        assert_eq!(result.attempts.len(), 2);
        
        // Both attempts should have failed
        assert!(result.attempts[0].error.is_some());
        assert!(result.attempts[1].error.is_some());
    }

    #[tokio::test]
    async fn test_mirroring_operation_retryable() {
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
            retry_config: RetryConfig::default(),
            storage_config,
        };
        
        let engine = ObjectStorageMirrorEngine::new(mirroring_config).await.unwrap();
        let actions = vec![
            DeltaAction::Protocol(Protocol::default()),
            DeltaAction::Add(AddFile::new("test.parquet".to_string(), 1024, 1234567890, true)),
        ];
        
        let operation = MirroringOperation::new(
            "test_table".to_string(),
            1,
            actions,
            std::sync::Arc::new(engine),
        );
        
        // Test retryable error detection
        let storage_error = TxnLogError::ObjectStorage("Connection failed".to_string());
        assert!(operation.is_retryable_error(&storage_error));
        
        let validation_error = TxnLogError::Validation("Invalid data".to_string());
        assert!(!operation.is_retryable_error(&validation_error));
    }

    #[tokio::test]
    async fn test_mirror_engine_with_retry() {
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
            retry_config: RetryConfig::default(),
            storage_config,
        };
        
        let engine = ObjectStorageMirrorEngine::new(mirroring_config).await.unwrap();
        let actions = vec![
            DeltaAction::Protocol(Protocol::default()),
            DeltaAction::Add(AddFile::new("test.parquet".to_string(), 1024, 1234567890, true)),
        ];
        
        // Test retry-enabled mirroring
        let retry_result = engine.mirror_table_version_with_retry("test_table", 1, &actions).await;
        
        assert!(retry_result.result.is_ok());
        assert!(retry_result.result.unwrap().success);
        assert_eq!(retry_result.total_attempts, 1); // Should succeed on first attempt
    }
}