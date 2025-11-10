//! Mirroring task definitions and utilities

use crate::error::MirrorResult;
use crate::generators::DeltaFile;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use std::time::Duration;

/// Detailed mirroring task definition with comprehensive tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorTaskDetail {
    /// Unique task identifier
    pub task_id: Uuid,
    /// Task type
    pub task_type: TaskType,
    /// Table identifier
    pub table_id: Uuid,
    /// Table path for logging
    pub table_path: String,
    /// Task priority
    pub priority: TaskPriority,
    /// Task creation time
    pub created_at: DateTime<Utc>,
    /// Task start time
    pub started_at: Option<DateTime<Utc>>,
    /// Task completion time
    pub completed_at: Option<DateTime<Utc>>,
    /// Current task status
    pub status: TaskStatus,
    /// Progress percentage (0.0 to 1.0)
    pub progress: f64,
    /// Error message if failed
    pub error_message: Option<String>,
    /// Number of files processed
    pub files_processed: usize,
    /// Total number of files to process
    pub total_files: usize,
    /// Number of bytes written
    pub bytes_written: u64,
    /// Task configuration
    pub config: TaskConfig,
    /// Retry attempts made
    pub retry_count: u32,
    /// Maximum retry attempts allowed
    pub max_retries: u32,
    /// Estimated completion time
    pub estimated_completion: Option<DateTime<Utc>>,
    /// Dependencies (other task IDs that must complete first)
    pub dependencies: Vec<Uuid>,
    /// Tags for task categorization
    pub tags: HashMap<String, String>,
}

/// Task type enumeration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TaskType {
    /// Mirror a specific commit
    CommitMirroring {
        /// Commit version
        version: i64,
        /// Commit actions included
        actions: Vec<String>,
    },
    /// Mirror all commits for a table
    TableMirroring {
        /// Starting version (inclusive)
        start_version: Option<i64>,
        /// Ending version (inclusive)
        end_version: Option<i64>,
        /// Whether to include tombstones
        include_tombstones: bool,
    },
    /// Generate checkpoint for a table
    CheckpointGeneration {
        /// Target version for checkpoint
        version: Option<i64>,
        /// Whether to generate compact checkpoint
        compact: bool,
        /// Checkpoint retention policy
        retention: CheckpointRetentionPolicy,
    },
    /// Batch processing of multiple tasks
    BatchProcessing {
        /// Subtasks included in batch
        subtasks: Vec<Uuid>,
        /// Whether to process subtasks sequentially
        sequential: bool,
    },
    /// Schema validation task
    SchemaValidation {
        /// Version to validate
        version: i64,
        /// Validation level
        level: ValidationLevel,
    },
    /// Cleanup and maintenance task
    Maintenance {
        /// Maintenance operation type
        operation: MaintenanceOperation,
        /// Target versions or ranges
        targets: Vec<String>,
    },
}

/// Task priority levels
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TaskPriority {
    /// Low priority
    Low = 1,
    /// Normal priority
    Normal = 2,
    /// High priority
    High = 3,
    /// Critical priority
    Critical = 4,
}

/// Checkpoint retention policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRetentionPolicy {
    /// Maximum number of checkpoints to retain
    pub max_count: usize,
    /// Maximum age in seconds
    pub max_age_secs: u64,
    /// Retain based on version
    retain_by_version: bool,
    /// Versions to always retain
    always_retain: Vec<i64>,
}

/// Validation levels for schema validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationLevel {
    /// Basic validation
    Basic,
    /// Standard validation with checks
    Standard,
    /// Strict validation with comprehensive checks
    Strict,
    /// Delta specification compliance
    DeltaSpec,
}

/// Maintenance operation types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MaintenanceOperation {
    /// Remove old log files
    CleanupOldLogs {
        /// Retention period in seconds
        retention_secs: u64,
        /// Whether to keep minimum versions
        keep_min_versions: usize,
    },
    /// Remove orphaned files
    RemoveOrphanedFiles,
    /// Optimize file layout
    OptimizeLayout,
    /// Update table statistics
    UpdateStats,
}

/// Task configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    /// Whether to enable compression
    pub compression_enabled: bool,
    /// Compression algorithm
    pub compression_algorithm: String,
    /// Whether to validate generated content
    pub validate_content: bool,
    /// Timeout in seconds
    pub timeout_secs: u64,
    /// Batch size for bulk operations
    pub batch_size: usize,
    /// Whether to include statistics
    pub include_statistics: bool,
    /// Retry configuration
    retry_config: RetryConfig,
    /// Performance tuning
    performance: TaskPerformanceConfig,
}

/// Retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Initial delay in milliseconds
    pub initial_delay_ms: u64,
    /// Maximum delay in milliseconds
    pub max_delay_ms: u64,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// Jitter factor
    pub jitter_factor: f64,
    /// Whether to use exponential backoff
    pub exponential_backoff: bool,
}

/// Task performance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskPerformanceConfig {
    /// Maximum memory usage in MB
    pub max_memory_mb: u64,
    /// Maximum concurrent files to process
    pub max_concurrent_files: usize,
    /// Buffer size for file operations
    pub buffer_size_kb: usize,
    /// Whether to use streaming for large files
    pub stream_large_files: bool,
    /// Large file threshold in MB
    pub large_file_threshold_mb: u64,
}

/// Task status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Task is queued for processing
    Queued,
    /// Task is currently being processed
    Running,
    /// Task completed successfully
    Completed,
    /// Task failed
    Failed,
    /// Task was cancelled
    Cancelled,
    /// Task is paused
    Paused,
    /// Task is waiting for dependencies
    Waiting,
}

/// Task executor interface
pub trait TaskExecutor: Send + Sync {
    /// Execute a mirroring task
    async fn execute_task(&self, task: MirrorTaskDetail) -> MirrorResult<TaskResult>;

    /// Check if a task can be executed (dependencies satisfied)
    async fn can_execute(&self, task: &MirrorTaskDetail) -> bool;

    /// Get estimated execution time for a task
    async fn estimate_duration(&self, task: &MirrorTaskDetail) -> Duration;
}

/// Result of task execution
#[derive(Debug, Clone)]
pub struct TaskResult {
    /// Task completion status
    pub status: TaskStatus,
    /// Generated files
    pub files: Vec<DeltaFile>,
    /// Processing statistics
    pub stats: TaskStats,
    /// Execution duration
    pub duration: Duration,
    /// Error details if failed
    pub error: Option<TaskError>,
}

/// Task execution statistics
#[derive(Debug, Clone)]
pub struct TaskStats {
    /// Number of files processed
    pub files_processed: usize,
    /// Total bytes processed
    pub bytes_processed: u64,
    /// Number of files written
    pub files_written: usize,
    /// Total bytes written
    pub bytes_written: u64,
    /// Number of files read
    pub files_read: usize,
    /// Total bytes read
    pub bytes_read: u64,
    /// Number of validation errors
    pub validation_errors: usize,
    /// Number of retries
    pub retries: u32,
    /// Peak memory usage in MB
    pub peak_memory_mb: u64,
    /// CPU time used in milliseconds
    pub cpu_time_ms: u64,
}

/// Task error details
#[derive(Debug, Clone)]
pub struct TaskError {
    /// Error message
    pub message: String,
    /// Error type
    pub error_type: TaskErrorType,
    /// Error timestamp
    pub timestamp: DateTime<Utc>,
    /// Stack trace if available
    pub stack_trace: Option<String>,
    /// Retry count when error occurred
    pub retry_count: u32,
}

/// Task error types
#[derive(Debug, Clone, PartialEq)]
pub enum TaskErrorType {
    /// Storage error
    Storage,
    /// Generation error
    Generation,
    /// Validation error
    Validation,
    /// Configuration error
    Configuration,
    /// Timeout error
    Timeout,
    /// Resource error (memory, disk, etc.)
    Resource,
    /// Network error
    Network,
    /// Unknown error
    Unknown,
}

/// Task queue management
pub struct TaskQueue {
    tasks: VecDeque<MirrorTaskDetail>,
    prioritized: HashMap<TaskPriority, VecDeque<MirrorTaskDetail>>>,
    dependencies: HashMap<Uuid, Vec<Uuid>>,
    completion_callbacks: HashMap<Uuid, Box<dyn TaskCompletionCallback>>,
}

/// Callback for task completion events
pub trait TaskCompletionCallback: Send + Sync {
    /// Called when a task completes
    fn on_task_complete(&self, task: &MirrorTaskDetail, result: &TaskResult);
}

impl MirrorTaskDetail {
    /// Create a new commit mirroring task
    pub fn commit_mirroring(
        table_id: Uuid,
        table_path: String,
        version: i64,
        actions: Vec<String>,
    ) -> Self {
        Self {
            task_id: Uuid::new_v4(),
            task_type: TaskType::CommitMirroring { version, actions },
            table_id,
            table_path,
            priority: TaskPriority::Normal,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            status: TaskStatus::Queued,
            progress: 0.0,
            error_message: None,
            files_processed: 0,
            total_files: 1,
            bytes_written: 0,
            config: TaskConfig::default(),
            retry_count: 0,
            max_retries: 3,
            estimated_completion: None,
            dependencies: Vec::new(),
            tags: HashMap::new(),
        }
    }

    /// Create a table mirroring task
    pub fn table_mirroring(
        table_id: Uuid,
        table_path: String,
        start_version: Option<i64>,
        end_version: Option<i64>,
    ) -> Self {
        Self {
            task_id: Uuid::new_v4(),
            task_type: TaskType::TableMirroring {
                start_version,
                end_version,
                include_tombstones: true,
            },
            table_id,
            table_path,
            priority: TaskPriority::Normal,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            status: TaskStatus::Queued,
            progress: 0.0,
            error_message: None,
            files_processed: 0,
            total_files: 0, // Will be determined by scanning
            bytes_written: 0,
            config: TaskConfig::default(),
            retry_count: 0,
            max_retries: 3,
            estimated_completion: None,
            dependencies: Vec::new(),
            tags: HashMap::new(),
        }
    }

    /// Create a checkpoint generation task
    pub fn checkpoint_generation(
        table_id: Uuid,
        table_path: String,
        version: Option<i64>,
        compact: bool,
    ) -> Self {
        Self {
            task_id: Uuid::new_v4(),
            task_type: TaskType::CheckpointGeneration {
                version,
                compact,
                retention: CheckpointRetentionPolicy::default(),
            },
            table_id,
            table_path,
            priority: TaskPriority::High, // Checkpoints are high priority
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            status: TaskStatus::Queued,
            progress: 0.0,
            error_message: None,
            files_processed: 0,
            total_files: 1,
            bytes_written: 0,
            config: TaskConfig::default(),
            retry_count: 0,
            max_retries: 3,
            estimated_completion: None,
            dependencies: Vec::new(),
            tags: HashMap::new(),
        }
    }

    /// Create a batch processing task
    pub fn batch_processing(
        table_id: Uuid,
        table_path: String,
        subtasks: Vec<MirrorTaskDetail>,
    ) -> Self {
        let subtask_ids: Vec<Uuid> = subtasks.iter().map(|t| t.task_id).collect();

        Self {
            task_id: Uuid::new_v4(),
            task_type: TaskType::BatchProcessing {
                subtasks: subtask_ids,
                sequential: false,
            },
            table_id,
            table_path,
            priority: TaskPriority::Normal,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            status: TaskStatus::Queued,
            progress: 0.0,
            error_message: None,
            files_processed: 0,
            total_files: 0,
            bytes_written: 0,
            config: TaskConfig::default(),
            retry_count: 0,
            max_retries: 1, // Batch tasks have fewer retries
            estimated_completion: None,
            dependencies: Vec::new(),
            tags: HashMap::new(),
        }
    }

    /// Update task status
    pub fn update_status(&mut self, status: TaskStatus) {
        self.status = status.clone();

        match status {
            TaskStatus::Running => {
                self.started_at = Some(Utc::now());
            }
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled => {
                self.completed_at = Some(Utc::now());
                self.progress = 1.0;
            }
            _ => {}
        }
    }

    /// Update task progress
    pub fn update_progress(&mut self, progress: f64) {
        self.progress = progress.clamp(0.0, 1.0);
    }

    /// Set error message
    pub fn set_error(&mut self, error: String) {
        self.error_message = Some(error);
    }

    /// Increment retry count
    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }

    /// Add a dependency
    pub fn add_dependency(&mut self, dependency: Uuid) {
        if !self.dependencies.contains(&dependency) {
            self.dependencies.push(dependency);
        }
    }

    /// Add a tag
    pub fn add_tag(&mut self, key: String, value: String) {
        self.tags.insert(key, value);
    }

    /// Check if task can be retried
    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries &&
            matches!(self.status, TaskStatus::Failed | TaskStatus::Queued)
    }

    /// Calculate task duration
    pub fn duration(&self) -> Option<Duration> {
        match (self.started_at, self.completed_at) {
            (Some(started), Some(completed)) => {
                Some(completed.signed_duration_since(started).abs())
            }
            _ => None,
        }
    }

    /// Check if task is in progress
    pub fn is_in_progress(&self) -> bool {
        matches!(self.status, TaskStatus::Running | TaskStatus::Queued | TaskStatus::Waiting)
    }

    /// Check if task is completed (either successfully or with failure)
    pub fn is_completed(&self) -> bool {
        matches!(self.status, TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled)
    }

    /// Check if task succeeded
    pub fn is_success(&self) -> bool {
        matches!(self.status, TaskStatus::Completed)
    }

    /// Check if task failed
    pub fn is_failed(&self) -> bool {
        matches!(self.status, TaskStatus::Failed)
    }
}

impl Default for TaskConfig {
    fn default() -> Self {
        Self {
            compression_enabled: true,
            compression_algorithm: "gzip".to_string(),
            validate_content: true,
            timeout_secs: 300,
            batch_size: 100,
            include_statistics: true,
            retry_config: RetryConfig {
                initial_delay_ms: 1000,
                max_delay_ms: 30000,
                backoff_multiplier: 2.0,
                jitter_factor: 0.1,
                exponential_backoff: true,
            },
            performance: TaskPerformanceConfig {
                max_memory_mb: 512,
                max_concurrent_files: 10,
                buffer_size_kb: 64,
                stream_large_files: true,
                large_file_threshold_mb: 100,
            },
        }
    }
}

impl Default for CheckpointRetentionPolicy {
    fn default() -> Self {
        Self {
            max_count: 5,
            max_age_secs: 86400, // 24 hours
            retain_by_version: true,
            always_retain: vec![0], // Always retain version 0
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
            exponential_backoff: true,
        }
    }
}

impl Default for TaskPerformanceConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: 512,
            max_concurrent_files: 10,
            buffer_size_kb: 64,
            stream_large_files: true,
            large_file_threshold_mb: 100,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_creation() {
        let table_id = Uuid::new_v4();
        let task = MirrorTaskDetail::commit_mirroring(
            table_id,
            "/test/table".to_string(),
            1,
            vec!["action1".to_string(), "action2".to_string()],
        );

        assert_eq!(task.table_id, table_id);
        assert_eq!(task.table_path, "/test/table");
        assert!(task.tags.is_empty());
        assert_eq!(task.retry_count, 0);
    }

    #[test]
    fn test_task_status_transitions() {
        let mut task = MirrorTaskDetail::commit_mirroring(
            Uuid::new_v4(),
            "/test/table".to_string(),
            1,
            vec!["action1".to_string()],
        );

        // Initial status
        assert_eq!(task.status, TaskStatus::Queued);
        assert!(task.started_at.is_none());

        // Start processing
        task.update_status(TaskStatus::Running);
        assert_eq!(task.status, TaskStatus::Running);
        assert!(task.started_at.is_some());

        // Complete task
        task.update_status(TaskStatus::Completed);
        assert_eq!(task.status, TaskStatus::Completed);
        assert!(task.completed_at.is_some());
        assert_eq!(task.progress, 1.0);
    }

    #[test]
    fn test_task_retry_logic() {
        let mut task = MirrorTaskDetail::commit_mirroring(
            Uuid::new_v4(),
            "/test/table".to_string(),
            1,
            vec!["action1".to_string()],
        );

        assert!(task.can_retry());
        assert_eq!(task.retry_count, 0);

        task.increment_retry();
        assert_eq!(task.retry_count, 1);
        assert!(task.can_retry());

        // Exceed max retries
        task.retry_count = 3;
        assert!(!task.can_retry());
    }

    #[test]
    fn test_task_dependencies() {
        let task_id = Uuid::new_v4();
        let dependency_id = Uuid::new_v4();

        let mut task = MirrorTaskDetail::commit_mirroring(
            task_id,
            "/test/table".to_string(),
            1,
            vec!["action1".to_string()],
        );

        task.add_dependency(dependency_id);
        assert_eq!(task.dependencies.len(), 1);
        assert!(task.dependencies.contains(&dependency_id));

        // Adding same dependency shouldn't duplicate
        task.add_dependency(dependency_id);
        assert_eq!(task.dependencies.len(), 1);
    }

    #[test]
    fn test_task_completion() {
        let mut task = MirrorTaskDetail::commit_mirroring(
            Uuid::new_v4(),
            "/test/table".to_string(),
            1,
            vec!["action1".to_string()],
        );

        assert!(!task.is_in_progress());
        assert!(!task.is_completed());

        task.update_status(TaskStatus::Running);
        assert!(task.is_in_progress());
        assert!(!task.is_completed());

        task.update_status(TaskStatus::Completed);
        assert!(!task.is_in_progress());
        assert!(task.is_completed());
        assert!(task.is_success());
        assert!(!task.is_failed());

        task.update_status(TaskStatus::Failed);
        assert!(!task.is_in_progress());
        assert!(task.is_completed());
        assert!(!task.is_success());
        assert!(task.is_failed());
    }
}