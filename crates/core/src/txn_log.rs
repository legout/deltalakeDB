use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Monotonically increasing table version.
pub type Version = i64;

/// Version to reference before the first commit lands.
pub const INITIAL_VERSION: Version = -1;

const LOG_DIR_NAME: &str = "_delta_log";

/// Protocol requirements of a Delta table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Protocol {
    /// Minimum supported reader protocol version.
    pub min_reader_version: u32,
    /// Minimum supported writer protocol version.
    pub min_writer_version: u32,
}

/// Table metadata as recorded in the transaction log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableMetadata {
    /// Serialized schema JSON string.
    pub schema_json: String,
    /// Partition columns.
    pub partition_columns: Vec<String>,
    /// Table configuration entries.
    pub configuration: HashMap<String, String>,
}

impl TableMetadata {
    /// Creates new metadata with the provided schema JSON and partitions.
    pub fn new(
        schema_json: impl Into<String>,
        partition_columns: Vec<String>,
        configuration: HashMap<String, String>,
    ) -> Self {
        Self {
            schema_json: schema_json.into(),
            partition_columns,
            configuration,
        }
    }
}

/// Active data file tracked by the transaction log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActiveFile {
    /// File path relative to the table root.
    pub path: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Optional millisecond timestamp from when the file was added.
    pub modification_time: i64,
    /// Partition column values for the file.
    pub partition_values: HashMap<String, String>,
}

impl ActiveFile {
    /// Creates a new active file entry.
    pub fn new(
        path: impl Into<String>,
        size_bytes: u64,
        modification_time: i64,
        partition_values: HashMap<String, String>,
    ) -> Self {
        Self {
            path: path.into(),
            size_bytes,
            modification_time,
            partition_values,
        }
    }
}

/// Removal marker for a data file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemovedFile {
    /// File path relative to the table root.
    pub path: String,
    /// Millisecond timestamp when removal occurred.
    pub deletion_timestamp: Option<i64>,
}

impl RemovedFile {
    /// Creates a new removal entry.
    pub fn new(path: impl Into<String>, deletion_timestamp: Option<i64>) -> Self {
        Self {
            path: path.into(),
            deletion_timestamp,
        }
    }
}

/// Snapshot of the table at a specific version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSnapshot {
    /// Version represented by this snapshot.
    pub version: Version,
    /// Millisecond timestamp of the commit that produced this snapshot.
    pub timestamp_millis: i64,
    /// Latest metadata entry.
    pub metadata: TableMetadata,
    /// Active protocol entry.
    pub protocol: Protocol,
    /// Current table properties.
    pub properties: HashMap<String, String>,
    /// Active file list.
    pub files: Vec<ActiveFile>,
}

/// Data included in a commit request.
#[derive(Debug, Clone)]
pub struct CommitRequest {
    /// Expected current version enforcing optimistic concurrency (`current == expected_version`).
    pub expected_version: Version,
    /// Optional protocol update.
    pub protocol: Option<Protocol>,
    /// Optional metadata update.
    pub metadata: Option<TableMetadata>,
    /// Property updates merged into the current configuration.
    pub set_properties: HashMap<String, String>,
    /// Files to add during the commit.
    pub add_actions: Vec<ActiveFile>,
    /// Files to remove during the commit.
    pub remove_actions: Vec<RemovedFile>,
}

impl CommitRequest {
    /// Creates an empty commit request targeting the provided version.
    pub fn new(expected_version: Version) -> Self {
        Self {
            expected_version,
            protocol: None,
            metadata: None,
            set_properties: HashMap::new(),
            add_actions: Vec::new(),
            remove_actions: Vec::new(),
        }
    }

    fn into_entries(self) -> Result<Vec<CommitEntry>, TxnLogError> {
        let mut entries = Vec::new();
        if let Some(protocol) = self.protocol {
            entries.push(CommitEntry::Protocol(protocol));
        }
        if let Some(metadata) = self.metadata {
            entries.push(CommitEntry::Metadata(metadata));
        }
        if !self.set_properties.is_empty() {
            entries.push(CommitEntry::SetProperties {
                properties: self.set_properties,
            });
        }
        entries.extend(self.add_actions.into_iter().map(|file| CommitEntry::Add { file }));
        entries.extend(
            self.remove_actions
                .into_iter()
                .map(|file| CommitEntry::Remove { file }),
        );

        if entries.is_empty() {
            return Err(TxnLogError::Invalid(
                "commit must include at least one action".into(),
            ));
        }

        Ok(entries)
    }
}

impl Default for CommitRequest {
    fn default() -> Self {
        Self::new(INITIAL_VERSION)
    }
}

/// Result of a successful commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitResult {
    /// Version assigned to the commit.
    pub version: Version,
    /// Commit timestamp in milliseconds.
    pub timestamp_millis: i64,
    /// Number of actions persisted in the log file.
    pub action_count: usize,
}

/// Errors returned by transaction log operations.
#[derive(Debug, Error)]
pub enum TxnLogError {
    /// Filesystem error while accessing the log.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// JSON serialization/deserialization failure.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// Missing metadata entry when building a snapshot.
    #[error("metadata entry missing before the requested version")]
    MissingMetadata,
    /// Missing protocol entry when building a snapshot.
    #[error("protocol entry missing before the requested version")]
    MissingProtocol,
    /// Log contains no commits yet.
    #[error("transaction log is empty")]
    EmptyLog,
    /// Missing commit file for the requested version.
    #[error("commit version {0} not found")]
    MissingVersion(Version),
    /// Optimistic concurrency violation.
    #[error("concurrency conflict: expected version {expected}, current version {actual}")]
    Concurrency {
        /// Version supplied by the caller.
        expected: Version,
        /// Version observed on disk.
        actual: Version,
    },
    /// Invalid request/operation.
    #[error("{0}")]
    Invalid(String),
}

/// Common interface for reading transaction log state.
pub trait TxnLogReader {
    /// Returns the table URI the reader was created with.
    fn table_uri(&self) -> &Path;
    /// Returns the latest available version or [`INITIAL_VERSION`] when empty.
    fn current_version(&self) -> Result<Version, TxnLogError>;
    /// Builds a snapshot for the provided version. Passing `None` reads the current version.
    fn snapshot_at_version(
        &self,
        version: Option<Version>,
    ) -> Result<TableSnapshot, TxnLogError>;
    /// Builds a snapshot for the latest version committed at or before the timestamp.
    fn snapshot_by_timestamp(&self, timestamp_millis: i64) -> Result<TableSnapshot, TxnLogError>;
}

/// Common interface for writing transaction log state.
pub trait TxnLogWriter {
    /// Returns the table URI the writer was created with.
    fn table_uri(&self) -> &Path;
    /// Commits a batch of actions, enforcing optimistic concurrency.
    fn commit(&self, request: CommitRequest) -> Result<CommitResult, TxnLogError>;
}

/// File-backed transaction log reader.
#[derive(Debug, Clone)]
pub struct FileTxnLogReader {
    location: LogLocation,
}

impl FileTxnLogReader {
    /// Creates a reader rooted at the provided table URI/path.
    pub fn new(table_uri: impl Into<PathBuf>) -> Self {
        Self {
            location: LogLocation::new(table_uri),
        }
    }
}

impl TxnLogReader for FileTxnLogReader {
    fn table_uri(&self) -> &Path {
        self.location.table_uri()
    }

    fn current_version(&self) -> Result<Version, TxnLogError> {
        self.location.current_version()
    }

    fn snapshot_at_version(
        &self,
        version: Option<Version>,
    ) -> Result<TableSnapshot, TxnLogError> {
        let target = match version {
            Some(v) => v,
            None => self.location.current_version()?,
        };
        if target < 0 {
            return Err(TxnLogError::EmptyLog);
        }
        let mut builder = SnapshotBuilder::default();
        for v in 0..=target {
            let commit = self.location.read_commit(v)?;
            builder.apply(&commit);
        }
        builder.build()
    }

    fn snapshot_by_timestamp(&self, timestamp_millis: i64) -> Result<TableSnapshot, TxnLogError> {
        let versions = self.location.ordered_versions()?;
        if versions.is_empty() {
            return Err(TxnLogError::EmptyLog);
        }

        let mut builder = SnapshotBuilder::default();
        let mut applied_any = false;
        for version in versions {
            let commit = self.location.read_commit(version)?;
            if commit.timestamp_millis <= timestamp_millis {
                builder.apply(&commit);
                applied_any = true;
            } else {
                break;
            }
        }

        if !applied_any {
            return Err(TxnLogError::Invalid(
                "no commits exist at or before the requested timestamp".into(),
            ));
        }

        builder.build()
    }
}

/// File-backed transaction log writer.
#[derive(Debug, Clone)]
pub struct FileTxnLogWriter {
    location: LogLocation,
}

impl FileTxnLogWriter {
    /// Creates a writer rooted at the provided table URI/path.
    pub fn new(table_uri: impl Into<PathBuf>) -> Self {
        Self {
            location: LogLocation::new(table_uri),
        }
    }
}

impl TxnLogWriter for FileTxnLogWriter {
    fn table_uri(&self) -> &Path {
        self.location.table_uri()
    }

    fn commit(&self, request: CommitRequest) -> Result<CommitResult, TxnLogError> {
        let current = self.location.current_version()?;
        if current != request.expected_version {
            return Err(TxnLogError::Concurrency {
                expected: request.expected_version,
                actual: current,
            });
        }

        let new_version = current + 1;
        let entries = request.into_entries()?;
        let timestamp = current_timestamp_millis();
        let commit = CommitFile {
            version: new_version,
            timestamp_millis: timestamp,
            entries,
        };
        let encoded = serde_json::to_vec_pretty(&commit)?;

        self.location.ensure_log_dir()?;
        let final_path = self.location.commit_path(new_version)?;
        let temp_path = final_path.with_extension("json.tmp");
        fs::write(&temp_path, encoded)?;
        fs::rename(&temp_path, &final_path)?;

        Ok(CommitResult {
            version: new_version,
            timestamp_millis: timestamp,
            action_count: commit.entries.len(),
        })
    }
}

#[derive(Debug, Clone)]
struct LogLocation {
    table_uri: PathBuf,
}

impl LogLocation {
    fn new(table_uri: impl Into<PathBuf>) -> Self {
        Self {
            table_uri: table_uri.into(),
        }
    }

    fn table_uri(&self) -> &Path {
        self.table_uri.as_path()
    }

    fn log_dir(&self) -> PathBuf {
        self.table_uri.join(LOG_DIR_NAME)
    }

    fn ensure_log_dir(&self) -> Result<(), TxnLogError> {
        fs::create_dir_all(self.log_dir()).map_err(TxnLogError::from)
    }

    fn commit_path(&self, version: Version) -> Result<PathBuf, TxnLogError> {
        if version < 0 {
            return Err(TxnLogError::Invalid(format!(
                "version must be non-negative, got {version}"
            )));
        }
        Ok(self
            .log_dir()
            .join(format!("{version:020}.json")))
    }

    fn ordered_versions(&self) -> Result<Vec<Version>, TxnLogError> {
        match fs::read_dir(self.log_dir()) {
            Ok(entries) => {
                let mut versions = Vec::new();
                for entry in entries {
                    let entry = entry?;
                    if let Some(version) = filename_to_version(entry.file_name().to_string_lossy().as_ref()) {
                        versions.push(version);
                    }
                }
                versions.sort_unstable();
                Ok(versions)
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(err) => Err(err.into()),
        }
    }

    fn current_version(&self) -> Result<Version, TxnLogError> {
        let versions = self.ordered_versions()?;
        Ok(versions.last().copied().unwrap_or(INITIAL_VERSION))
    }

    fn read_commit(&self, version: Version) -> Result<CommitFile, TxnLogError> {
        let path = self.commit_path(version)?;
        let data = fs::read(&path).map_err(|err| {
            if err.kind() == io::ErrorKind::NotFound {
                TxnLogError::MissingVersion(version)
            } else {
                TxnLogError::Io(err)
            }
        })?;
        let commit: CommitFile = serde_json::from_slice(&data)?;
        Ok(commit)
    }
}

fn filename_to_version(name: &str) -> Option<Version> {
    if !name.ends_with(".json") {
        return None;
    }
    let (numeric, _) = name.split_at(name.len().saturating_sub(5));
    numeric.parse().ok()
}

fn current_timestamp_millis() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
    duration.as_millis() as i64
}

#[derive(Debug, Serialize, Deserialize)]
struct CommitFile {
    version: Version,
    timestamp_millis: i64,
    entries: Vec<CommitEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CommitEntry {
    Metadata(TableMetadata),
    Protocol(Protocol),
    SetProperties { properties: HashMap<String, String> },
    Add { file: ActiveFile },
    Remove { file: RemovedFile },
}

#[derive(Default)]
struct SnapshotBuilder {
    version: Version,
    timestamp_millis: i64,
    metadata: Option<TableMetadata>,
    protocol: Option<Protocol>,
    properties: HashMap<String, String>,
    files: HashMap<String, ActiveFile>,
}

impl SnapshotBuilder {
    fn apply(&mut self, commit: &CommitFile) {
        self.version = commit.version;
        self.timestamp_millis = commit.timestamp_millis;
        for entry in &commit.entries {
            match entry {
                CommitEntry::Metadata(metadata) => {
                    self.metadata = Some(metadata.clone());
                }
                CommitEntry::Protocol(protocol) => {
                    self.protocol = Some(protocol.clone());
                }
                CommitEntry::SetProperties { properties } => {
                    for (key, value) in properties {
                        self.properties.insert(key.clone(), value.clone());
                    }
                }
                CommitEntry::Add { file } => {
                    self.files.insert(file.path.clone(), file.clone());
                }
                CommitEntry::Remove { file } => {
                    self.files.remove(&file.path);
                }
            }
        }
    }

    fn build(self) -> Result<TableSnapshot, TxnLogError> {
        let metadata = self.metadata.ok_or(TxnLogError::MissingMetadata)?;
        let protocol = self.protocol.ok_or(TxnLogError::MissingProtocol)?;
        let mut files: Vec<ActiveFile> = self.files.into_values().collect();
        files.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(TableSnapshot {
            version: self.version,
            timestamp_millis: self.timestamp_millis,
            metadata,
            protocol,
            properties: self.properties,
            files,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

    struct TempTable {
        path: PathBuf,
    }

    impl TempTable {
        fn new() -> Self {
            let mut base = std::env::temp_dir();
            base.push(format!(
                "deltalakedb-txnlog-{}-{}-{}",
                std::process::id(),
                current_timestamp_millis(),
                NEXT_ID.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&base).expect("create temp dir");
            Self { path: base }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn sample_metadata() -> TableMetadata {
        let mut config = HashMap::new();
        config.insert("delta.appendOnly".into(), "false".into());
        TableMetadata::new(
            r#"{"type":"struct","fields":[{"name":"id","type":"long"}]}"#,
            vec!["id".into()],
            config,
        )
    }

    fn sample_protocol() -> Protocol {
        Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        }
    }

    fn sample_file(path: &str) -> ActiveFile {
        ActiveFile::new(
            path,
            1024,
            current_timestamp_millis(),
            HashMap::from([("id".into(), "0".into())]),
        )
    }

    #[test]
    fn file_round_trip_snapshot() {
        let dir = TempTable::new();
        let writer = FileTxnLogWriter::new(dir.path());
        let mut request = CommitRequest::new(INITIAL_VERSION);
        request.metadata = Some(sample_metadata());
        request.protocol = Some(sample_protocol());
        request
            .set_properties
            .insert("delta.enableChangeDataFeed".into(), "false".into());
        request.add_actions.push(sample_file("part-000.parquet"));

        let result = writer.commit(request).expect("first commit");
        assert_eq!(result.version, 0);

        let reader = FileTxnLogReader::new(dir.path());
        assert_eq!(reader.current_version().unwrap(), 0);

        let snapshot = reader.snapshot_at_version(None).expect("snapshot");
        assert_eq!(snapshot.version, 0);
        assert_eq!(snapshot.files.len(), 1);
        assert_eq!(snapshot.metadata.partition_columns, vec!["id"]);
        assert_eq!(
            snapshot
                .properties
                .get("delta.enableChangeDataFeed")
                .map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn writer_detects_concurrency_conflict() {
        let dir = TempTable::new();
        let writer = FileTxnLogWriter::new(dir.path());
        let mut first = CommitRequest::new(INITIAL_VERSION);
        first.metadata = Some(sample_metadata());
        first.protocol = Some(sample_protocol());
        first.add_actions.push(sample_file("a.parquet"));
        writer.commit(first).expect("initial commit");

        let mut stale = CommitRequest::new(INITIAL_VERSION);
        stale.add_actions.push(sample_file("b.parquet"));
        let err = writer.commit(stale).expect_err("should see conflict");
        assert!(matches!(err, TxnLogError::Concurrency { .. }));
    }

    #[test]
    fn snapshot_by_timestamp_picks_earlier_version() {
        let dir = TempTable::new();
        let writer = FileTxnLogWriter::new(dir.path());

        let mut first = CommitRequest::new(INITIAL_VERSION);
        first.metadata = Some(sample_metadata());
        first.protocol = Some(sample_protocol());
        first.add_actions.push(sample_file("a.parquet"));
        let first_result = writer.commit(first).expect("first commit");

        thread::sleep(Duration::from_millis(5));

        let mut second = CommitRequest::new(first_result.version);
        second.add_actions.push(sample_file("b.parquet"));
        let second_result = writer.commit(second).expect("second commit");

        let reader = FileTxnLogReader::new(dir.path());
        let older = reader
            .snapshot_by_timestamp(first_result.timestamp_millis)
            .expect("older snapshot");
        assert_eq!(older.version, 0);

        let latest = reader
            .snapshot_by_timestamp(second_result.timestamp_millis)
            .expect("latest snapshot");
        assert_eq!(latest.version, 1);
        assert_eq!(latest.files.len(), 2);
    }
}
