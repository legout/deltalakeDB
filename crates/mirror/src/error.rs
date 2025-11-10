use thiserror::Error;

/// Errors surfaced by the mirror subsystem.
#[derive(Debug, Error)]
pub enum MirrorError {
    /// Failed to serialize commit actions to JSON.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// I/O failure while writing to the backing object store.
    #[error("object store error: {0}")]
    ObjectStore(String),
    /// Database interaction error.
    #[error("database error: {0}")]
    Database(String),
    /// Invalid table state prevented mirroring.
    #[error("invalid mirror state: {0}")]
    InvalidState(String),
    /// Failed to parse a table location URI/path.
    #[error("invalid table location '{location}': {message}")]
    InvalidLocation {
        /// The provided table location string.
        location: String,
        /// Error message.
        message: String,
    },
}

impl From<sqlx::Error> for MirrorError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value.to_string())
    }
}
