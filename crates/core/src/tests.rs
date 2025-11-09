//! Unit tests for transaction log abstractions.

use crate::{
    AddFile, DeltaAction, FileTxnLogReader, FileTxnLogWriter, Metadata, Protocol,
    RemoveFile, TableMetadata, TxnLogReader, TxnLogReaderExt, TxnLogWriter,
    TxnLogWriterExt,
};
use chrono::Utc;
use std::collections::HashMap;
use tempfile::TempDir;
use tokio::fs;

#[tokio::test]
async fn test_file_reader_creation() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();
    let log_path = table_path.join("_delta_log");

    // Create log directory
    fs::create_dir_all(&log_path).await.unwrap();

    let reader = FileTxnLogReader::new(table_path).unwrap();
    assert_eq!(reader.table_location(), table_path);
    assert_eq!(reader.log_path(), log_path);
}

#[tokio::test]
async fn test_file_writer_creation() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();

    let writer = FileTxnLogWriter::new(table_path).unwrap();
    assert_eq!(writer.table_location(), table_path);
    assert!(writer.log_path().exists());
}

#[tokio::test]
async fn test_add_file_action() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();
    let table_id = "test_table";

    let writer = FileTxnLogWriter::new(table_path).unwrap();
    let reader = FileTxnLogReader::new(table_path).unwrap();

    // Create add file action
    let add_file = AddFile::new(
        "part-00000.parquet".to_string(),
        1024,
        Utc::now().timestamp_millis(),
        true,
    )
    .with_partition_value("year".to_string(), "2023".to_string())
    .with_stats("{\"numRecords\": 100}".to_string());

    // Commit the action
    let version = writer
        .add_files(table_id, vec![add_file])
        .await
        .unwrap();

    assert_eq!(version, 0);

    // Read back the action
    let actions = reader.get_version_actions(table_id, version).await.unwrap();
    assert_eq!(actions.len(), 1);

    match &actions[0] {
        DeltaAction::Add(add) => {
            assert_eq!(add.path, "part-00000.parquet");
            assert_eq!(add.size, 1024);
            assert_eq!(add.data_change, true);
            assert_eq!(add.partition_values.get("year"), Some(&"2023".to_string()));
            assert_eq!(add.stats, Some("{\"numRecords\": 100}".to_string()));
        }
        _ => panic!("Expected Add action"),
    }
}

#[tokio::test]
async fn test_remove_file_action() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();
    let table_id = "test_table";

    let writer = FileTxnLogWriter::new(table_path).unwrap();
    let reader = FileTxnLogReader::new(table_path).unwrap();

    // First add a file
    let add_file = AddFile::new(
        "part-00000.parquet".to_string(),
        1024,
        Utc::now().timestamp_millis(),
        true,
    );

    writer.add_files(table_id, vec![add_file]).await.unwrap();

    // Then remove it
    let remove_file = RemoveFile::new("part-00000.parquet".to_string(), true)
        .with_deletion_timestamp(Utc::now().timestamp_millis());

    let version = writer
        .remove_files(table_id, vec![remove_file])
        .await
        .unwrap();

    assert_eq!(version, 1);

    // Check active files - should be empty
    let active_files = reader.list_active_files(table_id).await.unwrap();
    assert_eq!(active_files.len(), 0);
}

#[tokio::test]
async fn test_metadata_action() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();
    let table_id = "test_table";

    let writer = FileTxnLogWriter::new(table_path).unwrap();
    let reader = FileTxnLogReader::new(table_path).unwrap();

    // Create metadata
    let format = crate::Format::new("parquet".to_string());
    let metadata = Metadata::new(
        "test-metadata".to_string(),
        "{\"type\": \"struct\", \"fields\": []}".to_string(),
        format,
    )
    .with_partition_column("year".to_string());

    // Commit metadata
    let version = writer.update_metadata(table_id, metadata.clone()).await.unwrap();
    assert_eq!(version, 0);

    // Read back metadata
    let table_metadata = reader.get_table_metadata(table_id).await.unwrap();
    assert_eq!(table_metadata.metadata.schema_string, metadata.schema_string);
    assert_eq!(table_metadata.metadata.partition_columns, vec!["year"]);
}

#[tokio::test]
async fn test_protocol_action() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();
    let table_id = "test_table";

    let writer = FileTxnLogWriter::new(table_path).unwrap();
    let reader = FileTxnLogReader::new(table_path).unwrap();

    // Create metadata first
    let format = crate::Format::new("parquet".to_string());
    let metadata = Metadata::new(
        "test-metadata".to_string(),
        "{\"type\": \"struct\", \"fields\": []}".to_string(),
        format,
    );
    writer.update_metadata(table_id, metadata.clone()).await.unwrap();

    // Create protocol
    let protocol = Protocol::new(2, 3);

    // Commit protocol
    let version = writer.update_protocol(table_id, protocol.clone()).await.unwrap();
    assert_eq!(version, 1);

    // Read back protocol
    let table_metadata = reader.get_table_metadata(table_id).await.unwrap();
    assert_eq!(table_metadata.protocol.min_reader_version, 2);
    assert_eq!(table_metadata.protocol.min_writer_version, 3);
}

#[tokio::test]
async fn test_version_sequence() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();
    let table_id = "test_table";

    let writer = FileTxnLogWriter::new(table_path).unwrap();
    let reader = FileTxnLogReader::new(table_path).unwrap();

    // Commit multiple versions
    let add_file1 = AddFile::new(
        "part-00000.parquet".to_string(),
        1024,
        Utc::now().timestamp_millis(),
        true,
    );

    let add_file2 = AddFile::new(
        "part-00001.parquet".to_string(),
        2048,
        Utc::now().timestamp_millis(),
        true,
    );

    let version1 = writer.add_files(table_id, vec![add_file1]).await.unwrap();
    let version2 = writer.add_files(table_id, vec![add_file2]).await.unwrap();

    assert_eq!(version1, 0);
    assert_eq!(version2, 1);

    // Check latest version
    let latest_version = reader.get_latest_version(table_id).await.unwrap();
    assert_eq!(latest_version, 1);

    // Check version history
    let versions = reader.list_table_versions(table_id).await.unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].version, 0);
    assert_eq!(versions[1].version, 1);
}

#[tokio::test]
async fn test_active_files_calculation() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();
    let table_id = "test_table";

    let writer = FileTxnLogWriter::new(table_path).unwrap();
    let reader = FileTxnLogReader::new(table_path).unwrap();

    // Add two files
    let add_file1 = AddFile::new(
        "part-00000.parquet".to_string(),
        1024,
        Utc::now().timestamp_millis(),
        true,
    );

    let add_file2 = AddFile::new(
        "part-00001.parquet".to_string(),
        2048,
        Utc::now().timestamp_millis(),
        true,
    );

    writer.add_files(table_id, vec![add_file1]).await.unwrap();
    writer.add_files(table_id, vec![add_file2]).await.unwrap();

    // Remove first file
    let remove_file = RemoveFile::new("part-00000.parquet".to_string(), true);
    writer.remove_files(table_id, vec![remove_file]).await.unwrap();

    // Check active files - should only have the second file
    let active_files = reader.list_active_files(table_id).await.unwrap();
    assert_eq!(active_files.len(), 1);
    assert_eq!(active_files[0].path, "part-00001.parquet");
    assert_eq!(active_files[0].size, 2048);
}

#[tokio::test]
async fn test_table_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();
    let table_id = "test_table";

    let writer = FileTxnLogWriter::new(table_path).unwrap();
    let reader = FileTxnLogReader::new(table_path).unwrap();

    // Add files and metadata
    let add_file = AddFile::new(
        "part-00000.parquet".to_string(),
        1024,
        Utc::now().timestamp_millis(),
        true,
    );

    let format = crate::Format::new("parquet".to_string());
    let metadata = Metadata::new(
        "test-metadata".to_string(),
        "{\"type\": \"struct\", \"fields\": []}".to_string(),
        format,
    );

    writer.add_files(table_id, vec![add_file]).await.unwrap();
    writer.update_metadata(table_id, metadata).await.unwrap();

    // Get snapshot
    let snapshot = reader.get_latest_snapshot(table_id).await.unwrap();
    assert_eq!(snapshot.version, 1); // metadata update creates version 1
    assert_eq!(snapshot.file_count(), 1);
    assert_eq!(snapshot.total_size(), 1024);
    assert_eq!(snapshot.table_id(), table_id);
}

#[tokio::test]
async fn test_version_conflict() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();
    let table_id = "test_table";

    let writer = FileTxnLogWriter::new(table_path).unwrap();

    // Add a file normally
    let add_file = AddFile::new(
        "part-00000.parquet".to_string(),
        1024,
        Utc::now().timestamp_millis(),
        true,
    );

    writer.add_files(table_id, vec![add_file]).await.unwrap();

    // Try to commit with wrong version
    let actions = vec![DeltaAction::Protocol(Protocol::new(1, 1))];
    let result = writer
        .commit_actions_with_version(table_id, 5, actions, None, None)
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        crate::TxnLogError::VersionConflict { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 5);
        }
        _ => panic!("Expected VersionConflict error"),
    }
}