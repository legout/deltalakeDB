//! Error types for transaction log operations.

use thiserror::Error;

/// Result type for transaction log operations.
pub type TxnLogResult<T> = Result<T, TxnLogError>;

/// Errors that can occur during transaction log operations.
#[derive(Error, Debug)]
pub enum TxnLogError {
    /// I/O error when reading or writing files.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Delta protocol error.
    #[error("Delta protocol error: {message}")]
    Protocol { message: String },

    /// Version conflict error for optimistic concurrency.
    #[error("Version conflict: expected {expected}, found {actual}")]
    VersionConflict { expected: i64, actual: i64 },

    /// Table not found error.
    #[error("Table not found: {table_id}")]
    TableNotFound { table_id: String },

    /// Invalid version error.
    #[error("Invalid version: {version}")]
    InvalidVersion { version: i64 },

    /// Invalid timestamp error.
    #[error("Invalid timestamp: {timestamp}")]
    InvalidTimestamp { timestamp: i64 },

    /// Database connection error.
    #[error("Database error: {message}")]
    Database { message: String },

    /// Object storage error.
    #[error("Object storage error: {message}")]
    ObjectStorage { message: String },

    /// Configuration error.
    #[error("Configuration error: {message}")]
    Configuration { message: String },

    /// Validation error.
    #[error("Validation error: {message}")]
    Validation { message: String },

    /// Internal error.
    #[error("Internal error: {message}")]
    Internal { message: String },
}

impl TxnLogError {
    /// Create a new protocol error.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }

    /// Create a new version conflict error.
    pub fn version_conflict(expected: i64, actual: i64) -> Self {
        Self::VersionConflict { expected, actual }
    }

    /// Create a new table not found error.
    pub fn table_not_found(table_id: impl Into<String>) -> Self {
        Self::TableNotFound {
            table_id: table_id.into(),
        }
    }

    /// Create a new invalid version error.
    pub fn invalid_version(version: i64) -> Self {
        Self::InvalidVersion { version }
    }

    /// Create a new invalid timestamp error.
    pub fn invalid_timestamp(timestamp: i64) -> Self {
        Self::InvalidTimestamp { timestamp }
    }

    /// Create a new database error.
    pub fn database(message: impl Into<String>) -> Self {
        Self::Database {
            message: message.into(),
        }
    }

    /// Create a new object storage error.
    pub fn object_storage(message: impl Into<String>) -> Self {
        Self::ObjectStorage {
            message: message.into(),
        }
    }

    /// Create a new configuration error.
    pub fn configuration(message: impl Into<String>) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    /// Create a new validation error.
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
        }
    }

    /// Create a new internal error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}