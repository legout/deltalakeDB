//! Mock implementations of traits for testing.

use crate::error::TxnLogError;
use crate::traits::{TxnLogReader, TxnLogWriter};
use crate::types::{Action, AddFile, CommitHandle, MetadataUpdate, ProtocolUpdate, Snapshot};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Mock transaction log reader for testing.
///
/// Stores snapshots in memory keyed by version.
#[derive(Clone)]
pub struct MockTxnLogReader {
    snapshots: Arc<Mutex<HashMap<i64, Snapshot>>>,
}

impl MockTxnLogReader {
    /// Create a new mock reader.
    pub fn new() -> Self {
        MockTxnLogReader {
            snapshots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Add a snapshot to the mock reader.
    pub fn add_snapshot(&self, version: i64, snapshot: Snapshot) {
        let mut snapshots = self.snapshots.lock().unwrap();
        snapshots.insert(version, snapshot);
    }

    /// Add a default snapshot for testing.
    pub fn add_default_snapshot(&self, version: i64) {
        let snapshot = Snapshot {
            version,
            timestamp: 1000000 + version * 1000,
            files: vec![AddFile {
                path: format!("s3://bucket/file-v{}.parquet", version),
                size: 1024,
                modification_time: 1000000 + version * 1000,
                data_change_version: version,
                stats: None,
                stats_truncated: None,
                tags: None,
            }],
            metadata: MetadataUpdate {
                description: Some("Mock table".to_string()),
                schema: None,
                partition_columns: None,
                created_time: Some(1000000),
                configuration: None,
            },
            protocol: ProtocolUpdate {
                min_reader_version: Some(1),
                min_writer_version: Some(2),
            },
        };
        self.add_snapshot(version, snapshot);
    }
}

impl Default for MockTxnLogReader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl TxnLogReader for MockTxnLogReader {
    async fn get_latest_version(&self) -> Result<i64, TxnLogError> {
        let snapshots = self.snapshots.lock().unwrap();
        snapshots
            .keys()
            .max()
            .copied()
            .ok_or_else(|| TxnLogError::TableNotFound("mock_table".to_string()))
    }

    async fn read_snapshot(&self, version: Option<i64>) -> Result<Snapshot, TxnLogError> {
        let snapshots = self.snapshots.lock().unwrap();
        let v = match version {
            Some(v) => v,
            None => snapshots
                .keys()
                .max()
                .copied()
                .ok_or_else(|| TxnLogError::TableNotFound("mock_table".to_string()))?,
        };

        snapshots
            .get(&v)
            .cloned()
            .ok_or_else(|| TxnLogError::Other(format!("Version {} not found", v)))
    }

    async fn get_files(&self, version: i64) -> Result<Vec<AddFile>, TxnLogError> {
        self.read_snapshot(Some(version))
            .await
            .map(|snapshot| snapshot.files)
    }

    async fn time_travel(&self, timestamp: i64) -> Result<Snapshot, TxnLogError> {
        let snapshots = self.snapshots.lock().unwrap();
        snapshots
            .values()
            .filter(|s| s.timestamp <= timestamp)
            .max_by_key(|s| s.timestamp)
            .cloned()
            .ok_or_else(|| TxnLogError::Other("No commits before timestamp".to_string()))
    }
}

/// Mock transaction log writer for testing.
///
/// Tracks commits in memory with version validation.
pub struct MockTxnLogWriter {
    current_version: i64,
    commits: Vec<Vec<Action>>,
    pending_actions: HashMap<String, Vec<Action>>,
}

impl MockTxnLogWriter {
    /// Create a new mock writer starting at version 0.
    pub fn new() -> Self {
        MockTxnLogWriter {
            current_version: 0,
            commits: vec![],
            pending_actions: HashMap::new(),
        }
    }

    /// Get the current version.
    pub fn current_version(&self) -> i64 {
        self.current_version
    }

    /// Get all recorded commits.
    pub fn commits(&self) -> &Vec<Vec<Action>> {
        &self.commits
    }
}

impl Default for MockTxnLogWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl TxnLogWriter for MockTxnLogWriter {
    async fn begin_commit(&mut self, table_id: &str) -> Result<CommitHandle, TxnLogError> {
        let handle = CommitHandle::new(format!("txn-{}-v{}", table_id, self.current_version + 1));
        self.pending_actions
            .insert(handle.txn_id().to_string(), vec![]);
        Ok(handle)
    }

    async fn write_actions(
        &mut self,
        handle: &CommitHandle,
        actions: Vec<Action>,
    ) -> Result<(), TxnLogError> {
        if let Some(pending) = self.pending_actions.get_mut(handle.txn_id()) {
            pending.extend(actions);
            Ok(())
        } else {
            Err(TxnLogError::Other("Handle not found".to_string()))
        }
    }

    async fn finalize_commit(&mut self, handle: CommitHandle) -> Result<i64, TxnLogError> {
        let actions = self
            .pending_actions
            .remove(handle.txn_id())
            .ok_or_else(|| TxnLogError::Other("Handle not found".to_string()))?;

        self.commits.push(actions);
        self.current_version += 1;
        Ok(self.current_version)
    }

    async fn validate_version(&self, expected: i64) -> Result<(), TxnLogError> {
        if expected == self.current_version {
            Ok(())
        } else {
            Err(TxnLogError::VersionConflict {
                expected,
                actual: self.current_version,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_reader_latest_version() {
        let reader = MockTxnLogReader::new();
        reader.add_default_snapshot(1);
        reader.add_default_snapshot(2);

        let version = reader.get_latest_version().await.unwrap();
        assert_eq!(version, 2);
    }

    #[tokio::test]
    async fn test_mock_reader_snapshot() {
        let reader = MockTxnLogReader::new();
        reader.add_default_snapshot(5);

        let snapshot = reader.read_snapshot(Some(5)).await.unwrap();
        assert_eq!(snapshot.version, 5);
        assert_eq!(snapshot.files.len(), 1);
    }

    #[tokio::test]
    async fn test_mock_reader_latest_snapshot() {
        let reader = MockTxnLogReader::new();
        reader.add_default_snapshot(1);
        reader.add_default_snapshot(3);

        let snapshot = reader.read_snapshot(None).await.unwrap();
        assert_eq!(snapshot.version, 3);
    }

    #[tokio::test]
    async fn test_mock_reader_time_travel() {
        let reader = MockTxnLogReader::new();
        reader.add_default_snapshot(1); // timestamp: 1001000
        reader.add_default_snapshot(2); // timestamp: 1002000
        reader.add_default_snapshot(3); // timestamp: 1003000

        let snapshot = reader.time_travel(1001500).await.unwrap();
        assert_eq!(snapshot.version, 1);

        let snapshot = reader.time_travel(1002000).await.unwrap();
        assert_eq!(snapshot.version, 2);
    }

    #[tokio::test]
    async fn test_mock_reader_get_files() {
        let reader = MockTxnLogReader::new();
        reader.add_default_snapshot(1);

        let files = reader.get_files(1).await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].path.contains("file-v1"));
    }

    #[tokio::test]
    async fn test_mock_writer_commit() {
        let mut writer = MockTxnLogWriter::new();
        assert_eq!(writer.current_version(), 0);

        let handle = writer.begin_commit("test_table").await.unwrap();
        let action = Action::Add(AddFile {
            path: "s3://bucket/file.parquet".to_string(),
            size: 1024,
            modification_time: 1000000,
            data_change_version: 1,
            stats: None,
            stats_truncated: None,
            tags: None,
        });

        writer.write_actions(&handle, vec![action]).await.unwrap();
        let version = writer.finalize_commit(handle).await.unwrap();

        assert_eq!(version, 1);
        assert_eq!(writer.current_version(), 1);
        assert_eq!(writer.commits().len(), 1);
    }

    #[tokio::test]
    async fn test_mock_writer_multiple_commits() {
        let mut writer = MockTxnLogWriter::new();

        for i in 1..=3 {
            let handle = writer.begin_commit("test").await.unwrap();
            writer.finalize_commit(handle).await.unwrap();
            assert_eq!(writer.current_version(), i);
        }

        assert_eq!(writer.commits().len(), 3);
    }

    #[tokio::test]
    async fn test_mock_writer_version_conflict() {
        let writer = MockTxnLogWriter::new();

        let err = writer.validate_version(5).await;
        assert!(matches!(err, Err(TxnLogError::VersionConflict { .. })));
    }

    #[tokio::test]
    async fn test_mock_writer_version_validate() {
        let writer = MockTxnLogWriter::new();

        let result = writer.validate_version(0).await;
        assert!(result.is_ok());
    }
}
