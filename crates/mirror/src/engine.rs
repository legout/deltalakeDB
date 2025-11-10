//! Mirror engine for writing Delta artifacts to object storage.
//!
//! Coordinates the mirroring of committed SQL versions to canonical `_delta_log` artifacts
//! (JSON commits and Parquet checkpoints) for Delta Lake ecosystem compatibility.

use crate::checkpoint::CheckpointWriter;
use crate::error::{MirrorError, MirrorResult};
use crate::serializer::serialize_actions;
use bytes::Bytes;
use deltalakedb_core::types::{Action, Snapshot};
use object_store::ObjectStore;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Mirror engine that writes Delta artifacts to object storage.
///
/// Responsible for:
/// - Serializing committed actions to canonical JSON
/// - Writing JSON commits as `_delta_log/NNNNNNNNNN.json`
/// - Generating Parquet checkpoints at configured intervals
/// - Idempotent write operations (safe for retries)
///
/// # Example
///
/// ```ignore
/// let mirror = MirrorEngine::new(object_store, checkpoint_interval);
/// mirror.mirror_version(table_id, version, &snapshot).await?;
/// ```
pub struct MirrorEngine {
    /// Object store client (S3, GCS, Azure, local, etc.)
    object_store: Arc<dyn ObjectStore>,
    /// Checkpoint interval (default: 10 commits)
    checkpoint_interval: u64,
}

impl MirrorEngine {
    /// Create a new mirror engine.
    ///
    /// # Arguments
    ///
    /// * `object_store` - Object store client for writing artifacts
    /// * `checkpoint_interval` - Generate checkpoint every N commits (default: 10)
    pub fn new(object_store: Arc<dyn ObjectStore>, checkpoint_interval: u64) -> Self {
        MirrorEngine {
            object_store,
            checkpoint_interval,
        }
    }

    /// Mirror a committed version to object storage.
    ///
    /// # Arguments
    ///
    /// * `table_location` - Table's object storage location (e.g., `s3://bucket/path`)
    /// * `version` - Version number to mirror
    /// * `actions` - Actions committed in this version
    /// * `snapshot` - Table snapshot (for checkpoint generation)
    ///
    /// # Returns
    ///
    /// Ok if successful, Err with details if mirroring failed
    ///
    /// # Failures
    ///
    /// Mirror failures do NOT fail the commit - they're retried by the reconciler.
    /// This method is best-effort.
    pub async fn mirror_version(
        &self,
        table_location: &str,
        version: i64,
        actions: &[Action],
        snapshot: &Snapshot,
    ) -> MirrorResult<()> {
        info!("Mirroring version {} to {}", version, table_location);

        // Write JSON commit
        self.write_json_commit(table_location, version, actions).await?;

        // Generate checkpoint if interval reached
        if self.should_checkpoint(version) {
            match self.write_checkpoint(table_location, version, snapshot).await {
                Ok(_) => info!("Checkpoint written for version {}", version),
                Err(e) => warn!("Checkpoint generation failed for version {}: {}", version, e),
                // Continue even if checkpoint fails - JSON commit is authoritative
            }
        }

        Ok(())
    }

    /// Write JSON commit to `_delta_log/NNNNNNNNNN.json`.
    async fn write_json_commit(
        &self,
        table_location: &str,
        version: i64,
        actions: &[Action],
    ) -> MirrorResult<()> {
        // Serialize actions to canonical JSON
        let json_content = serialize_actions(actions)?;

        // Construct path: _delta_log/NNNNNNNNNN.json (zero-padded version)
        let key = format!("{}/_delta_log/{:020}.json", table_location, version);

        debug!("Writing JSON commit to: {}", key);

        // Convert JSON to bytes
        let bytes = Bytes::from(json_content);

        // Write with idempotent semantics
        self.put_idempotent(&key, bytes, &json_content).await?;

        info!("JSON commit written: {}", key);
        Ok(())
    }

    /// Write Parquet checkpoint to `_delta_log/NNNNNNNNNN.checkpoint.parquet`.
    async fn write_checkpoint(
        &self,
        table_location: &str,
        version: i64,
        snapshot: &Snapshot,
    ) -> MirrorResult<()> {
        let checkpoint_path = CheckpointWriter::checkpoint_path(table_location, version);
        debug!("Generating checkpoint at: {}", checkpoint_path);

        // Note: This is a simplified implementation for local testing.
        // Production would write to object storage, not local filesystem.
        // The checkpoint path is constructed but not written in this phase.
        // Phase 3 will integrate with actual object store writes.

        let _bytes_written = CheckpointWriter::write_checkpoint(snapshot, "/tmp/checkpoint.parquet")?;

        info!("Checkpoint generated for version {}", version);
        Ok(())
    }

    /// Check if a checkpoint should be generated at this version.
    fn should_checkpoint(&self, version: i64) -> bool {
        // Generate checkpoint at intervals and at version 0
        version == 0 || version % self.checkpoint_interval as i64 == 0
    }

    /// Idempotently write content to object store.
    ///
    /// Attempts conditional put-if-absent. On conflict, verifies that
    /// existing content matches the new content (idempotent success).
    async fn put_idempotent(
        &self,
        key: &str,
        bytes: Bytes,
        content_str: &str,
    ) -> MirrorResult<()> {
        match self.object_store.put_if_absent(key.into(), bytes.clone()).await {
            Ok(_) => {
                debug!("Wrote new object: {}", key);
                Ok(())
            }
            Err(object_store::Error::Conflict) => {
                // Object exists - verify content matches for idempotent semantics
                debug!("Object exists, verifying content: {}", key);

                match self.object_store.get(&key.into()).await {
                    Ok(existing_bytes) => {
                        let existing_str = String::from_utf8(existing_bytes.to_vec())
                            .map_err(|e| {
                                MirrorError::ConflictError {
                                    reason: format!("Invalid UTF-8 in existing object: {}", e),
                                }
                            })?;

                        if existing_str == content_str {
                            // Content matches - idempotent retry succeeded
                            debug!("Content matches - idempotent retry successful: {}", key);
                            Ok(())
                        } else {
                            // Content differs - actual conflict
                            Err(MirrorError::ConflictError {
                                reason: format!(
                                    "Different content for key: {}. Expected {} bytes, got {} bytes",
                                    key,
                                    content_str.len(),
                                    existing_str.len()
                                ),
                            })
                        }
                    }
                    Err(e) => Err(MirrorError::ObjectStoreError(format!(
                        "Failed to read existing object {}: {}",
                        key, e
                    ))),
                }
            }
            Err(e) => Err(MirrorError::ObjectStoreError(format!(
                "Failed to write object {}: {}",
                key, e
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_interval() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let engine = MirrorEngine::new(store, 10);

        // Checkpoint at 0
        assert!(engine.should_checkpoint(0));

        // Checkpoint at 10, 20, 30, etc.
        assert!(engine.should_checkpoint(10));
        assert!(engine.should_checkpoint(20));
        assert!(engine.should_checkpoint(30));

        // No checkpoint at other versions
        assert!(!engine.should_checkpoint(1));
        assert!(!engine.should_checkpoint(9));
        assert!(!engine.should_checkpoint(11));
        assert!(!engine.should_checkpoint(19));
    }

    #[test]
    fn test_custom_checkpoint_interval() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let engine = MirrorEngine::new(store, 5);

        // Checkpoint at 0, 5, 10, 15, etc.
        assert!(engine.should_checkpoint(0));
        assert!(engine.should_checkpoint(5));
        assert!(engine.should_checkpoint(10));

        // No checkpoint at other versions
        assert!(!engine.should_checkpoint(1));
        assert!(!engine.should_checkpoint(6));
    }

    #[tokio::test]
    async fn test_mirror_engine_creation() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let engine = MirrorEngine::new(store, 10);

        // Engine should be created successfully
        assert_eq!(engine.checkpoint_interval, 10);
    }
}
