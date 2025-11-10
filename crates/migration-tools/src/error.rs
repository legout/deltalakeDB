//! Error types for migration operations.

use thiserror::Error;

/// Errors that can occur during migration operations.
#[derive(Error, Debug)]
pub enum MigrationError {
    /// Failed to read Delta log artifacts
    #[error("Failed to read Delta log: {0}")]
    DeltaLogError(String),

    /// Failed to parse Delta action or JSON
    #[error("Failed to parse Delta action: {0}")]
    ParseError(String),

    /// Database operation failed
    #[error("Database error: {0}")]
    DatabaseError(String),

    /// Validation failed
    #[error("Validation failed: {0}")]
    ValidationError(String),

    /// Configuration or input error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Object store error
    #[error("Object store error: {0}")]
    ObjectStoreError(String),

    /// IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Generic error
    #[error("{0}")]
    Other(String),
}

/// Result type for migration operations.
pub type MigrationResult<T> = Result<T, MigrationError>;
