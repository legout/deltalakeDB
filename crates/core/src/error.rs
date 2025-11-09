//! Error types for transaction log operations.

use std::io;
use thiserror::Error;

/// Errors that can occur during transaction log operations.
#[derive(Error, Debug)]
pub enum TxnLogError {
    /// Version conflict: expected version doesn't match actual version
    #[error("Version conflict: expected {expected}, got {actual}")]
    VersionConflict {
        /// Expected version number
        expected: i64,
        /// Actual version number
        actual: i64,
    },

    /// Table was not found
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// IO error from underlying storage
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Generic error
    #[error("{0}")]
    Other(String),
}

impl From<serde_json::Error> for TxnLogError {
    fn from(err: serde_json::Error) -> Self {
        TxnLogError::SerializationError(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_conflict_error() {
        let err = TxnLogError::VersionConflict {
            expected: 5,
            actual: 6,
        };
        assert_eq!(err.to_string(), "Version conflict: expected 5, got 6");
    }

    #[test]
    fn test_table_not_found_error() {
        let err = TxnLogError::TableNotFound("my_table".to_string());
        assert_eq!(err.to_string(), "Table not found: my_table");
    }

    #[test]
    fn test_serialization_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let err: TxnLogError = json_err.into();
        assert!(err.to_string().contains("Serialization error"));
    }
}
