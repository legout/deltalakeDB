//! Error types for Delta Lake log mirroring operations

use thiserror::Error;
use std::sync::Arc;

/// Result type for mirroring operations
pub type MirrorResult<T> = Result<T, MirrorError>;

/// Errors that can occur during Delta Lake log mirroring
#[derive(Error, Debug)]
pub enum MirrorError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Storage error
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),

    /// JSON generation error
    #[error("JSON generation error: {0}")]
    JsonGenerationError(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// Parquet generation error
    #[error("Parquet generation error: {0}")]
    ParquetGenerationError(String),

    /// SQL adapter error
    #[error("SQL adapter error: {0}")]
    SqlAdapterError(String),

    /// Timeout error
    #[error("Operation timed out after {0} seconds")]
    TimeoutError(u64),

    /// Validation error
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// Resource not found
    #[error("Resource not found: {0}")]
    NotFound(String),

    /// Permission error
    #[error("Permission denied: {0}")]
    PermissionError(String),

    /// Quota exceeded
    #[error("Quota exceeded: {0}")]
    QuotaExceeded(String),

    /// Retry exhausted
    #[error("Operation failed after {0} retry attempts: {1}")]
    RetryExhausted(u32, String),

    /// Task queue full
    #[error("Task queue is full (size: {0})")]
    TaskQueueFull(usize),

    /// Task cancelled
    #[error("Task was cancelled: {0}")]
    TaskCancelled(String),

    /// Engine error
    #[error("Mirror engine error: {0}")]
    EngineError(String),

    /// Pipeline error
    #[error("Pipeline error: {0}")]
    PipelineError(String),

    /// Monitoring error
    #[error("Monitoring error: {0}")]
    MonitoringError(String),

    /// Generic error
    #[error("Error: {0}")]
    Generic(String),

    /// Multiple errors
    #[error("Multiple errors occurred: {0:?}")]
    MultipleErrors(Vec<Arc<MirrorError>>),
}

/// Storage-specific errors
#[derive(Error, Debug)]
pub enum StorageError {
    /// Connection error
    #[error("Storage connection error: {0}")]
    ConnectionError(String),

    /// Authentication error
    #[error("Storage authentication error: {0}")]
    AuthenticationError(String),

    /// Upload error
    #[error("File upload error: {path}: {error}")]
    UploadError {
        /// File path
        path: String,
        /// Error message
        error: String,
    },

    /// Download error
    #[error("File download error: {path}: {error}")]
    DownloadError {
        /// File path
        path: String,
        /// Error message
        error: String,
    },

    /// Delete error
    #[error("File delete error: {path}: {error}")]
    DeleteError {
        /// File path
        path: String,
        /// Error message
        error: String,
    },

    /// List error
    #[error("Directory listing error: {path}: {error}")]
    ListError {
        /// Directory path
        path: String,
        /// Error message
        error: String,
    },

    /// Invalid path
    #[error("Invalid storage path: {0}")]
    InvalidPath(String),

    /// Storage backend error
    #[error("Storage backend error ({backend}): {error}")]
    BackendError {
        /// Backend name
        backend: String,
        /// Error message
        error: String,
    },

    /// IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Object store error
    #[error("Object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    /// HTTP error
    #[error("HTTP error: {0}")]
    HttpError(String),

    /// Rate limited
    #[error("Rate limited: retry after {0} seconds")]
    RateLimited(u64),
}

/// Validation error details
#[derive(Error, Debug)]
pub enum ValidationError {
    /// Invalid table path
    #[error("Invalid table path: {0}")]
    InvalidTablePath(String),

    /// Invalid commit version
    #[error("Invalid commit version: {0}")]
    InvalidCommitVersion(i64),

    /// Missing required field
    #[error("Missing required field: {0}")]
    MissingRequiredField(String),

    /// Invalid action format
    #[error("Invalid action format: {0}")]
    InvalidActionFormat(String),

    /// Invalid Delta JSON structure
    #[error("Invalid Delta JSON structure: {0}")]
    InvalidDeltaJson(String),

    /// Checkpoint version conflict
    #[error("Checkpoint version conflict: expected {expected}, found {found}")]
    CheckpointVersionConflict {
        /// Expected version
        expected: i64,
        /// Found version
        found: i64,
    },

    /// File size exceeds limit
    #[error("File size {size} exceeds limit {limit}")]
    FileSizeExceeded {
        /// Actual file size
        size: u64,
        /// Maximum allowed size
        limit: u64,
    },

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
}

impl MirrorError {
    /// Create a configuration error
    pub fn config_error(message: impl Into<String>) -> Self {
        Self::ConfigError(message.into())
    }

    /// Create a JSON generation error
    pub fn json_generation_error(message: impl Into<String>) -> Self {
        Self::JsonGenerationError(message.into())
    }

    /// Create a Parquet generation error
    pub fn parquet_generation_error(message: impl Into<String>) -> Self {
        Self::ParquetGenerationError(message.into())
    }

    /// Create a SQL adapter error
    pub fn sql_adapter_error(message: impl Into<String>) -> Self {
        Self::SqlAdapterError(message.into())
    }

    /// Create a timeout error
    pub fn timeout_error(seconds: u64) -> Self {
        Self::TimeoutError(seconds)
    }

    /// Create a validation error
    pub fn validation_error(message: impl Into<String>) -> Self {
        Self::ValidationError(message.into())
    }

    /// Create a not found error
    pub fn not_found(resource: impl Into<String>) -> Self {
        Self::NotFound(resource.into())
    }

    /// Create a permission error
    pub fn permission_error(message: impl Into<String>) -> Self {
        Self::PermissionError(message.into())
    }

    /// Create a quota exceeded error
    pub fn quota_exceeded(message: impl Into<String>) -> Self {
        Self::QuotaExceeded(message.into())
    }

    /// Create a retry exhausted error
    pub fn retry_exhausted(attempts: u32, message: impl Into<String>) -> Self {
        Self::RetryExhausted(attempts, message.into())
    }

    /// Create a task queue full error
    pub fn task_queue_full(size: usize) -> Self {
        Self::TaskQueueFull(size)
    }

    /// Create a task cancelled error
    pub fn task_cancelled(message: impl Into<String>) -> Self {
        Self::TaskCancelled(message.into())
    }

    /// Create an engine error
    pub fn engine_error(message: impl Into<String>) -> Self {
        Self::EngineError(message.into())
    }

    /// Create a pipeline error
    pub fn pipeline_error(message: impl Into<String>) -> Self {
        Self::PipelineError(message.into())
    }

    /// Create a monitoring error
    pub fn monitoring_error(message: impl Into<String>) -> Self {
        Self::MonitoringError(message.into())
    }

    /// Create a generic error
    pub fn generic(message: impl Into<String>) -> Self {
        Self::Generic(message.into())
    }

    /// Create a multiple errors error
    pub fn multiple_errors(errors: Vec<MirrorError>) -> Self {
        let arc_errors: Vec<Arc<MirrorError>> = errors
            .into_iter()
            .map(Arc::new)
            .collect();
        Self::MultipleErrors(arc_errors)
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            MirrorError::StorageError(storage_err) => storage_err.is_retryable(),
            MirrorError::TimeoutError(_) => true,
            MirrorError::RetryExhausted(_, _) => false,
            MirrorError::ValidationError(_) => false,
            MirrorError::ConfigError(_) => false,
            MirrorError::PermissionError(_) => false,
            MirrorError::NotFound(_) => false,
            MirrorError::TaskCancelled(_) => false,
            _ => true,
        }
    }

    /// Check if this error indicates a temporary failure
    pub fn is_temporary(&self) -> bool {
        self.is_retryable()
    }

    /// Get error category for metrics
    pub fn error_category(&self) -> &'static str {
        match self {
            MirrorError::StorageError(_) => "storage",
            MirrorError::JsonGenerationError(_) => "json_generation",
            MirrorError::ParquetGenerationError(_) => "parquet_generation",
            MirrorError::SerializationError(_) => "serialization",
            MirrorError::SqlAdapterError(_) => "sql_adapter",
            MirrorError::TimeoutError(_) => "timeout",
            MirrorError::ValidationError(_) => "validation",
            MirrorError::NotFound(_) => "not_found",
            MirrorError::PermissionError(_) => "permission",
            MirrorError::QuotaExceeded(_) => "quota",
            MirrorError::RetryExhausted(_, _) => "retry_exhausted",
            MirrorError::TaskQueueFull(_) => "task_queue",
            MirrorError::TaskCancelled(_) => "task_cancelled",
            MirrorError::EngineError(_) => "engine",
            MirrorError::PipelineError(_) => "pipeline",
            MirrorError::MonitoringError(_) => "monitoring",
            MirrorError::ConfigError(_) => "config",
            MirrorError::Generic(_) => "generic",
            MirrorError::MultipleErrors(_) => "multiple",
        }
    }
}

impl StorageError {
    /// Create a connection error
    pub fn connection_error(message: impl Into<String>) -> Self {
        Self::ConnectionError(message.into())
    }

    /// Create an authentication error
    pub fn authentication_error(message: impl Into<String>) -> Self {
        Self::AuthenticationError(message.into())
    }

    /// Create an upload error
    pub fn upload_error(path: impl Into<String>, error: impl Into<String>) -> Self {
        Self::UploadError {
            path: path.into(),
            error: error.into(),
        }
    }

    /// Create a download error
    pub fn download_error(path: impl Into<String>, error: impl Into<String>) -> Self {
        Self::DownloadError {
            path: path.into(),
            error: error.into(),
        }
    }

    /// Create a delete error
    pub fn delete_error(path: impl Into<String>, error: impl Into<String>) -> Self {
        Self::DeleteError {
            path: path.into(),
            error: error.into(),
        }
    }

    /// Create a list error
    pub fn list_error(path: impl Into<String>, error: impl Into<String>) -> Self {
        Self::ListError {
            path: path.into(),
            error: error.into(),
        }
    }

    /// Create an invalid path error
    pub fn invalid_path(path: impl Into<String>) -> Self {
        Self::InvalidPath(path.into())
    }

    /// Create a backend error
    pub fn backend_error(backend: impl Into<String>, error: impl Into<String>) -> Self {
        Self::BackendError {
            backend: backend.into(),
            error: error.into(),
        }
    }

    /// Create an HTTP error
    pub fn http_error(message: impl Into<String>) -> Self {
        Self::HttpError(message.into())
    }

    /// Create a rate limited error
    pub fn rate_limited(retry_after: u64) -> Self {
        Self::RateLimited(retry_after)
    }

    /// Check if this storage error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            StorageError::ConnectionError(_) => true,
            StorageError::UploadError { .. } => true,
            StorageError::DownloadError { .. } => true,
            StorageError::ListError { .. } => true,
            StorageError::RateLimited(_) => true,
            StorageError::ObjectStoreError(_) => true,
            StorageError::HttpError(_) => true,
            StorageError::IoError(_) => false, // IO errors are typically not retryable
            StorageError::AuthenticationError(_) => false,
            StorageError::InvalidPath(_) => false,
            StorageError::DeleteError { .. } => false,
            StorageError::BackendError { .. } => false,
        }
    }
}

impl ValidationError {
    /// Create an invalid table path error
    pub fn invalid_table_path(path: impl Into<String>) -> Self {
        Self::InvalidTablePath(path.into())
    }

    /// Create an invalid commit version error
    pub fn invalid_commit_version(version: i64) -> Self {
        Self::InvalidCommitVersion(version)
    }

    /// Create a missing required field error
    pub fn missing_required_field(field: impl Into<String>) -> Self {
        Self::MissingRequiredField(field.into())
    }

    /// Create an invalid action format error
    pub fn invalid_action_format(format: impl Into<String>) -> Self {
        Self::InvalidActionFormat(format.into())
    }

    /// Create an invalid Delta JSON structure error
    pub fn invalid_delta_json(structure: impl Into<String>) -> Self {
        Self::InvalidDeltaJson(structure.into())
    }

    /// Create a checkpoint version conflict error
    pub fn checkpoint_version_conflict(expected: i64, found: i64) -> Self {
        Self::CheckpointVersionConflict { expected, found }
    }

    /// Create a file size exceeded error
    pub fn file_size_exceeded(size: u64, limit: u64) -> Self {
        Self::FileSizeExceeded { size, limit }
    }

    /// Create an invalid configuration error
    pub fn invalid_configuration(message: impl Into<String>) -> Self {
        Self::InvalidConfiguration(message.into())
    }
}

/// Convert multiple errors into a single error
pub fn combine_errors(errors: Vec<MirrorError>) -> MirrorError {
    if errors.len() == 1 {
        errors.into_iter().next().unwrap()
    } else {
        MirrorError::multiple_errors(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mirror_error_creation() {
        let err = MirrorError::config_error("Invalid configuration");
        assert!(matches!(err, MirrorError::ConfigError(_)));
        assert_eq!(err.error_category(), "config");
    }

    #[test]
    fn test_storage_error_creation() {
        let err = StorageError::upload_error("test.txt", "Network error");
        assert!(matches!(err, StorageError::UploadError { .. }));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_validation_error_creation() {
        let err = ValidationError::invalid_table_path("invalid/path");
        assert!(matches!(err, ValidationError::InvalidTablePath(_)));
    }

    #[test]
    fn test_error_retryable() {
        let retryable_err = MirrorError::timeout_error(30);
        assert!(retryable_err.is_retryable());

        let non_retryable_err = MirrorError::validation_error("Invalid data");
        assert!(!non_retryable_err.is_retryable());
    }

    #[test]
    fn test_error_category() {
        let storage_err = MirrorError::StorageError(StorageError::connection_error("Failed"));
        assert_eq!(storage_err.error_category(), "storage");

        let validation_err = MirrorError::validation_error("Invalid");
        assert_eq!(validation_err.error_category(), "validation");
    }

    #[test]
    fn test_combine_errors() {
        let errors = vec![
            MirrorError::config_error("Bad config"),
            MirrorError::validation_error("Bad data"),
        ];

        let combined = combine_errors(errors);
        assert!(matches!(combined, MirrorError::MultipleErrors(_)));
    }

    #[test]
    fn test_retry_exhausted_error() {
        let err = MirrorError::retry_exhausted(3, "Network failed");
        assert!(matches!(err, MirrorError::RetryExhausted(3, _)));
        assert!(!err.is_retryable());
        assert_eq!(err.error_category(), "retry_exhausted");
    }
}