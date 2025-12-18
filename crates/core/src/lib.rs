//! deltalakedb-core
//!
//! Core domain models and actions for the SQL-backed Delta Lake metadata plane.

#![warn(missing_docs)]

/// Shared Delta JSON action definitions.
pub mod delta;
/// Transaction log abstractions shared across metadata implementations.
pub mod txn_log;

// Re-export key types for pyo3 bindings and public API
pub use delta::{
    AddPayload, CommitInfo, DeltaAction, MetaDataPayload, ProtocolPayload, RemovePayload,
};
pub use txn_log::{
    ActiveFile, Protocol, RemovedFile, TableMetadata, TxnLogReader, TxnLogWriter, Version,
};
