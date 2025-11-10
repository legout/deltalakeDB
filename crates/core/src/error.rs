//! Error types for transaction log operations.
//!
//! This module defines a hierarchical error system for all deltalakedb operations:
//!
//! **Error Hierarchy:**
//! - `DeltaLakeError` (base trait for all errors)
//!   - `TxnLogError` (transaction log operations)
//!   - `TransactionError` (multi-table transactions) - implemented in transaction.rs
//!   - `TableError` (table operations) - implemented in table.rs
//!   - `UriError` (URI parsing) - implemented in uri.rs
//!
//! **Error Handling Strategy:**
//! - All error types implement `std::error::Error` for trait interoperability
//! - `From` trait implemented for automatic error conversion
//! - Error messages are user-friendly and actionable
//! - Errors preserve context (table IDs, versions, etc.)

use std::io;
use thiserror::Error;

/// Errors that can occur during transaction log operations.
///
/// This is a core error type covering metadata, storage, and serialization issues.
/// For transaction-specific errors, see `TransactionError`.
/// For table operations, see `TableError`.
#[derive(Error, Debug)]
pub enum TxnLogError {
    /// Version conflict: expected version doesn't match actual version
    #[error("Version conflict: expected version {expected}, but found {actual}")]
    VersionConflict {
        /// Expected version number
        expected: i64,
        /// Actual version number
        actual: i64,
    },

    /// Table was not found in the metadata store
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// IO error from underlying storage
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    /// Serialization/deserialization error (typically JSON)
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Generic error for other metadata operations
    #[error("{0}")]
    Other(String),
}

/// Automatic conversion from JSON errors to TxnLogError
impl From<serde_json::Error> for TxnLogError {
    fn from(err: serde_json::Error) -> Self {
        TxnLogError::SerializationError(err.to_string())
    }
}

/// Implement Display for better error formatting
impl std::fmt::Display for TxnLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxnLogError::VersionConflict { expected, actual } => {
                write!(f, "Version conflict: expected {}, but found {}", expected, actual)
            }
            TxnLogError::TableNotFound(name) => {
                write!(f, "Table not found: {}", name)
            }
            TxnLogError::IoError(err) => {
                write!(f, "IO error: {}", err)
            }
            TxnLogError::SerializationError(msg) => {
                write!(f, "Serialization error: {}", msg)
            }
            TxnLogError::Other(msg) => {
                write!(f, "{}", msg)
            }
        }
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
