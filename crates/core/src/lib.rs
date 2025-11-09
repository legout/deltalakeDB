//! deltalakedb-core
//!
//! Core domain models and actions for the SQL-backed Delta Lake metadata plane.
//! Provides pluggable abstractions for transaction log operations supporting both
//! file-based and SQL-backed implementations.

#![warn(missing_docs)]

pub mod actions;
pub mod error;
pub mod file;
pub mod reader;
pub mod writer;

// Re-export key types for convenience
pub use actions::*;
pub use error::{TxnLogError, TxnLogResult};
pub use file::{FileTxnLogReader, FileTxnLogWriter};
pub use reader::{TableSnapshot, TxnLogReader, TxnLogReaderExt};
pub use writer::{Transaction, TxnLogWriter, TxnLogWriterExt};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod integration_tests;
