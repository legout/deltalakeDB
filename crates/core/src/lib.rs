//! deltalakedb-core
//!
//! Core domain models and traits for the SQL-backed Delta Lake metadata plane.
//!
//! This crate provides the foundational abstractions and data types for building Delta Lake
//! metadata systems. The key components are:
//!
//! - **Traits**: Abstract interfaces for reading and writing transaction logs
//!   - `TxnLogReader`: Read-only access to table metadata and snapshots
//!   - `TxnLogWriter`: Write access with optimistic concurrency control
//!
//! - **Types**: Domain models for Delta Lake operations
//!   - `AddFile`: Files added to a table
//!   - `RemoveFile`: Files removed from a table
//!   - `Action`: Union type for all transaction log actions
//!   - `Snapshot`: Complete table state at a specific version
//!   - `CommitHandle`: Opaque handle for in-progress transactions
//!
//! - **Error Handling**: Custom error types for transaction log operations
//!   - `TxnLogError`: Comprehensive error variants
//!
//! # Example
//!
//! ```ignore
//! use deltalakedb_core::traits::{TxnLogReader, TxnLogWriter};
//! use deltalakedb_core::mocks::MockTxnLogReader;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let reader = MockTxnLogReader::new();
//!     let version = reader.get_latest_version().await?;
//!     println!("Latest version: {}", version);
//!     Ok(())
//! }
//! ```

#![warn(missing_docs)]

pub mod error;
pub mod mocks;
pub mod traits;
pub mod types;

pub use error::TxnLogError;
pub use traits::{TxnLogReader, TxnLogWriter};
pub use types::{
    Action, AddFile, CommitHandle, MetadataUpdate, ProtocolUpdate, RemoveFile, Snapshot, TxnAction,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crate_exports() {
        // Verify that key types are properly exported
        let _: CommitHandle = CommitHandle::new("test".to_string());
    }
}
