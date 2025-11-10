//! deltalakedb-mirror
//!
//! Mirror engine for writing Delta Lake artifacts to object storage.
//!
//! This crate provides the mirror engine that writes canonical `_delta_log` artifacts
//! (JSON commits and Parquet checkpoints) to object storage after each SQL commit.
//! This ensures full Delta Lake ecosystem compatibility - external tools like Spark,
//! DuckDB, and Polars can read SQL-backed tables without modifications.
//!
//! # Architecture
//!
//! The mirror engine operates in phases:
//! 1. **JSON Serialization**: Convert committed actions to canonical Delta JSON format
//! 2. **Checkpoint Generation**: Create Parquet checkpoints at configured intervals
//! 3. **Idempotent Writes**: Safely retry failed mirrors without corruption
//! 4. **Status Tracking**: Record mirror progress per version for reconciliation
//! 5. **Reconciliation**: Background process to repair mirror gaps
//!
//! # Example
//!
//! ```ignore
//! use deltalakedb_mirror::MirrorEngine;
//! use object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! let store = Arc::new(InMemory::new());
//! let engine = MirrorEngine::new(store, 10);  // Checkpoint every 10 commits
//!
//! // After SQL commit succeeds
//! engine.mirror_version(table_location, version, &actions).await?;
//! ```

#![warn(missing_docs)]

pub mod checkpoint;
pub mod engine;
pub mod error;
pub mod reconciler;
pub mod serializer;

pub use checkpoint::CheckpointWriter;
pub use engine::MirrorEngine;
pub use error::{MirrorError, MirrorResult};
pub use reconciler::{MirrorReconciler, ReconciliationConfig};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mirror_exports() {
        // Verify key types are exported
        let _: MirrorResult<()> = Ok(());
    }
}
