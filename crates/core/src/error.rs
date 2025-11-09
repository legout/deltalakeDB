//! Error types for Delta domain models.

use serde_json::Error as JsonError;

/// Main error type for Delta operations.
#[derive(Debug, thiserror::Error)]
pub enum DeltaError {
    /// Protocol-related errors
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// Validation errors
    #[error("Validation error: {0}")]
    Validation(#[from] ValidationError),

    /// JSON serialization/deserialization errors
    #[error("JSON error: {0}")]
    Json(#[from] JsonError),

    /// IO errors (for future use)
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Generic error with custom message
    #[error("{0}")]
    Message(String),
}

/// Errors related to Delta protocol versioning and compatibility.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// Unsupported minimum reader version
    #[error("Unsupported minimum reader version: {version}. Maximum supported: {max_supported}")]
    UnsupportedReaderVersion { version: i32, max_supported: i32 },

    /// Unsupported minimum writer version
    #[error("Unsupported minimum writer version: {version}. Maximum supported: {max_supported}")]
    UnsupportedWriterVersion { version: i32, max_supported: i32 },

    /// Protocol version mismatch
    #[error("Protocol version mismatch: expected {expected}, found {found}")]
    VersionMismatch { expected: i32, found: i32 },

    /// Incompatible protocol features
    #[error("Protocol contains unsupported features: {features:?}")]
    UnsupportedFeatures { features: Vec<String> },
}

/// Errors that occur during domain model validation.
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    /// Invalid table identifier
    #[error("Invalid table identifier: {id}")]
    InvalidTableId { id: String },

    /// Empty table name
    #[error("Table name cannot be empty")]
    EmptyTableName,

    /// Invalid table location
    #[error("Invalid table location: {location}")]
    InvalidTableLocation { location: String },

    /// Invalid file path
    #[error("Invalid file path: {path}")]
    InvalidFilePath { path: String },

    /// Negative file size
    #[error("File size cannot be negative: {size}")]
    NegativeFileSize { size: i64 },

    /// Invalid timestamp
    #[error("Invalid timestamp: {timestamp}")]
    InvalidTimestamp { timestamp: i64 },

    /// Empty commit actions
    #[error("Commit cannot have empty actions list")]
    EmptyCommitActions,

    /// Invalid commit version
    #[error("Commit version cannot be negative: {version}")]
    InvalidCommitVersion { version: i64 },

    /// Invalid schema string
    #[error("Invalid schema: {schema}")]
    InvalidSchema { schema: String },

    /// Missing required field
    #[error("Missing required field: {field}")]
    MissingField { field: String },

    /// Invalid partition value
    #[error("Invalid partition value for column '{column}': {value}")]
    InvalidPartitionValue { column: String, value: String },

    /// Invalid statistics JSON
    #[error("Invalid statistics JSON: {error}")]
    InvalidStatistics { error: String },
}

// Note: thiserror::Error automatically implements Display for these error types

impl From<ProtocolError> for ValidationError {
    fn from(error: ProtocolError) -> Self {
        match error {
            ProtocolError::UnsupportedReaderVersion { version, max_supported } => {
                ValidationError::InvalidTableId {
                    id: format!("Unsupported reader version {} > {}", version, max_supported),
                }
            }
            ProtocolError::UnsupportedWriterVersion { version, max_supported } => {
                ValidationError::InvalidTableId {
                    id: format!("Unsupported writer version {} > {}", version, max_supported),
                }
            }
            ProtocolError::VersionMismatch { expected, found } => {
                ValidationError::InvalidTableId {
                    id: format!("Protocol version mismatch: expected {}, found {}", expected, found),
                }
            }
            ProtocolError::UnsupportedFeatures { features } => {
                ValidationError::InvalidTableId {
                    id: format!("Unsupported features: {:?}", features),
                }
            }
        }
    }
}