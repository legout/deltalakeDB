//! Error types for mirror engine operations.

use thiserror::Error;

/// Errors that can occur during mirror operations.
#[derive(Error, Debug)]
pub enum MirrorError {
    /// Failed to serialize action to JSON
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Failed to write to object store
    #[error("Object store error: {0}")]
    ObjectStoreError(String),

    /// Mirror conflict - different content for same version
    #[error("Mirror conflict for version: {reason}")]
    ConflictError { reason: String },

    /// Parquet checkpoint generation failed
    #[error("Checkpoint generation failed: {0}")]
    CheckpointError(String),

    /// Database error retrieving actions
    #[error("Database error: {0}")]
    DatabaseError(String),

    /// Action validation error
    #[error("Invalid action: {0}")]
    ValidationError(String),

    /// Mirror status tracking error
    #[error("Status tracking error: {0}")]
    StatusTrackingError(String),

    /// Generic IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Generic error
    #[error("{0}")]
    Other(String),
}

/// Result type for mirror operations.
pub type MirrorResult<T> = Result<T, MirrorError>;
