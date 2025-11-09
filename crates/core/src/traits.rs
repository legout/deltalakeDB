//! Abstractions for reading and writing Delta transaction logs.
//!
//! This module provides traits that allow different implementations (file-based, SQL-backed)
//! to provide transaction log operations behind a common interface.

use crate::error::TxnLogError;
use crate::types::{Action, AddFile, CommitHandle, Snapshot};
use async_trait::async_trait;

/// Reader abstraction for Delta transaction logs.
///
/// Implementations of this trait provide read-only access to Delta table metadata
/// stored in transaction logs. Implementations must support:
/// - Reading the latest version
/// - Reading snapshots at specific versions
/// - Time travel to earlier versions
/// - Listing active files
///
/// All operations are async and non-blocking.
#[async_trait]
pub trait TxnLogReader: Send + Sync {
    /// Get the latest version of the transaction log.
    ///
    /// # Scenarios
    ///
    /// **Success case:** Returns the current version number without listing all commits.
    ///
    /// **Table not found:** Returns `TxnLogError::TableNotFound` if the table doesn't exist.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let version = reader.get_latest_version().await?;
    /// println!("Latest version: {}", version);
    /// ```
    async fn get_latest_version(&self) -> Result<i64, TxnLogError>;

    /// Read a snapshot of the table at a specific version.
    ///
    /// A snapshot includes:
    /// - The version number
    /// - All currently active files
    /// - Table schema and metadata
    /// - Protocol requirements
    ///
    /// # Arguments
    ///
    /// * `version` - Specific version to read. If `None`, returns the latest snapshot.
    ///
    /// # Scenarios
    ///
    /// **Success case:** Returns complete snapshot including active files, schema, and metadata.
    ///
    /// **Version not found:** Returns `TxnLogError::Other` if the requested version doesn't exist.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snapshot = reader.read_snapshot(Some(5)).await?;
    /// println!("Files at version 5: {}", snapshot.files.len());
    /// ```
    async fn read_snapshot(&self, version: Option<i64>) -> Result<Snapshot, TxnLogError>;

    /// Get all active files at a specific version.
    ///
    /// This is a convenience method that returns only the file list from a snapshot,
    /// useful for scanning operations.
    ///
    /// # Arguments
    ///
    /// * `version` - The version to read files from.
    ///
    /// # Scenarios
    ///
    /// **Success case:** Returns all `AddFile` entries active at the given version.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let files = reader.get_files(5).await?;
    /// for file in files {
    ///     println!("File: {}", file.path);
    /// }
    /// ```
    async fn get_files(&self, version: i64) -> Result<Vec<AddFile>, TxnLogError>;

    /// Time travel to a specific timestamp.
    ///
    /// Returns the snapshot corresponding to the latest commit that occurred
    /// at or before the given timestamp.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - Unix timestamp in milliseconds since epoch.
    ///
    /// # Scenarios
    ///
    /// **Success case:** Returns the snapshot corresponding to the latest commit before or at that timestamp.
    ///
    /// **No commits before timestamp:** Returns an error if there are no commits before the given timestamp.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snapshot = reader.time_travel(1234567890).await?;
    /// println!("Snapshot version at that time: {}", snapshot.version);
    /// ```
    async fn time_travel(&self, timestamp: i64) -> Result<Snapshot, TxnLogError>;
}

/// Writer abstraction for Delta transaction logs.
///
/// Implementations of this trait provide write access to Delta transaction logs
/// with optimistic concurrency control. A write follows this sequence:
/// 1. Begin commit - validates current version
/// 2. Write actions - stages actions for the new version
/// 3. Finalize commit - atomically commits and increments version
///
/// Implementations must guarantee:
/// - Atomicity: all actions in a commit succeed or fail together
/// - Version uniqueness: each version is written exactly once
/// - Conflict detection: detects concurrent writes to same version
#[async_trait]
pub trait TxnLogWriter: Send + Sync {
    /// Begin a new commit transaction.
    ///
    /// This method validates the current version and prepares for optimistic concurrency.
    /// It returns a handle that must be used for subsequent operations in this transaction.
    ///
    /// # Arguments
    ///
    /// * `table_id` - The unique identifier for the table being written.
    ///
    /// # Scenarios
    ///
    /// **Success case:** Validates current version and returns a transaction handle.
    ///
    /// **Table not found:** Returns `TxnLogError::TableNotFound` if table doesn't exist.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let handle = writer.begin_commit("my_table").await?;
    /// ```
    async fn begin_commit(&mut self, table_id: &str) -> Result<CommitHandle, TxnLogError>;

    /// Write actions to the in-progress commit.
    ///
    /// Actions are staged and will be atomically committed in `finalize_commit()`.
    /// Multiple calls to this method can be made before finalization.
    ///
    /// # Arguments
    ///
    /// * `handle` - The commit handle from `begin_commit()`.
    /// * `actions` - Actions to write (add/remove files, metadata updates, etc).
    ///
    /// # Scenarios
    ///
    /// **Success case:** Validates and stages the actions for commit.
    ///
    /// **Invalid action:** Returns an error if action is malformed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let actions = vec![Action::Add(file1), Action::Add(file2)];
    /// writer.write_actions(&handle, actions).await?;
    /// ```
    async fn write_actions(
        &mut self,
        handle: &CommitHandle,
        actions: Vec<Action>,
    ) -> Result<(), TxnLogError>;

    /// Finalize the in-progress commit.
    ///
    /// This atomically commits all staged actions and increments the version by exactly one.
    /// The returned version is the new current version after the commit.
    ///
    /// # Arguments
    ///
    /// * `handle` - The commit handle from `begin_commit()`.
    ///
    /// # Scenarios
    ///
    /// **Success case:** Commits atomically and returns the new version number.
    ///
    /// **Concurrent write conflict:** Returns `TxnLogError::VersionConflict` if another writer
    /// committed while this transaction was in progress.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let new_version = writer.finalize_commit(handle).await?;
    /// println!("Committed as version: {}", new_version);
    /// ```
    async fn finalize_commit(&mut self, handle: CommitHandle) -> Result<i64, TxnLogError>;

    /// Validate that the expected version matches the actual current version.
    ///
    /// This is a utility method for implementing optimistic concurrency control.
    /// Implementations typically call this during `begin_commit()`.
    ///
    /// # Arguments
    ///
    /// * `expected` - The version we expect to commit after.
    ///
    /// # Scenarios
    ///
    /// **Success case:** Expected version matches current version.
    ///
    /// **Version mismatch:** Returns `TxnLogError::VersionConflict` with expected and actual versions.
    ///
    /// # Example
    ///
    /// ```ignore
    /// writer.validate_version(current_version).await?;
    /// ```
    async fn validate_version(&self, expected: i64) -> Result<(), TxnLogError>;
}

#[cfg(test)]
mod tests {
    // Traits are tested via implementations, not directly
    // See mocks.rs for concrete test implementations
}
