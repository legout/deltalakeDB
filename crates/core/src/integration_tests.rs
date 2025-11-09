//! Integration tests for transaction log abstractions
//! 
//! These tests verify that our implementation works correctly with the actual API.

use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;
use tokio::fs;
use chrono::Utc;

use crate::{
    FileTxnLogReader, FileTxnLogWriter, TxnLogReader, TxnLogWriter,
    TxnLogWriterExt,
    DeltaAction, AddFile, RemoveFile, Metadata, Protocol,
    Format, TxnLogError, TxnLogResult
};

/// Creates a temporary directory for testing
async fn setup_test_dir() -> TxnLogResult<(TempDir, PathBuf)> {
    let temp_dir = TempDir::new().map_err(|e| TxnLogError::Internal {
        message: format!("Failed to create temp dir: {}", e)
    })?;
    let path = temp_dir.path().to_path_buf();
    Ok((temp_dir, path))
}

/// Creates a basic Delta Lake metadata action
fn create_test_metadata() -> Metadata {
    Metadata::new(
        uuid::Uuid::new_v4().to_string(),
        r#"{"type": "struct", "fields": []}"#.to_string(),
        Format::new("parquet".to_string()),
    )
}

/// Creates a basic Delta Lake protocol action
fn create_test_protocol() -> Protocol {
    Protocol::new(1, 2)
}

/// Creates a test AddFile action
fn create_test_add_file(path: &str, size: i64) -> AddFile {
    AddFile::new(
        path.to_string(),
        size,
        Utc::now().timestamp_millis(),
        true,
    )
}

/// Creates a test RemoveFile action
fn create_test_remove_file(path: &str) -> RemoveFile {
    RemoveFile::new(path.to_string(), true)
}

#[tokio::test]
async fn test_basic_file_operations() -> TxnLogResult<()> {
    let (_temp_dir, table_path) = setup_test_dir().await?;
    
    // Create the _delta_log directory
    fs::create_dir_all(table_path.join("_delta_log")).await?;
    
    // Create writer and reader
    let writer = FileTxnLogWriter::new(&table_path)?;
    let reader = FileTxnLogReader::new(&table_path)?;
    
    let table_id = "test_table";
    
    // Test initial state - log directory exists but no files yet
    // Note: table_exists checks if log directory exists, not specific table files
    
    // Create table with metadata and protocol
    let metadata = create_test_metadata();
    let protocol = create_test_protocol();
    
    let actions = vec![
        DeltaAction::Metadata(metadata),
        DeltaAction::Protocol(protocol),
    ];
    
    let version = writer.commit_actions(
        table_id,
        actions,
        Some("CREATE TABLE".to_string()),
        None,
    ).await?;
    
    assert_eq!(version, 0);
    
    // Verify table exists
    assert!(reader.table_exists(table_id).await?);
    
    // Get latest version
    let latest_version = reader.get_latest_version(table_id).await?;
    assert_eq!(latest_version, 0);
    
    // Get table metadata
    let table_metadata = reader.get_table_metadata(table_id).await?;
    assert_eq!(table_metadata.version, 0);
    
    // Get version actions
    let actions = reader.get_version_actions(table_id, 0).await?;
    assert_eq!(actions.len(), 2);
    
    Ok(())
}

#[tokio::test]
async fn test_file_add_remove_workflow() -> TxnLogResult<()> {
    let (_temp_dir, table_path) = setup_test_dir().await?;
    
    // Create the _delta_log directory
    fs::create_dir_all(table_path.join("_delta_log")).await?;
    
    let writer = FileTxnLogWriter::new(&table_path)?;
    let reader = FileTxnLogReader::new(&table_path)?;
    
    let table_id = "file_workflow_test";
    
    // Initialize table
    let init_actions = vec![
        DeltaAction::Metadata(create_test_metadata()),
        DeltaAction::Protocol(create_test_protocol()),
    ];
    
    writer.commit_actions(
        table_id,
        init_actions,
        Some("INIT".to_string()),
        None,
    ).await?;
    
    // Add files
    let add_file1 = create_test_add_file("part-00000.parquet", 1024);
    let add_file2 = create_test_add_file("part-00001.parquet", 2048);
    
    let version_1 = writer.add_files(table_id, vec![add_file1, add_file2]).await?;
    assert_eq!(version_1, 1);
    
    // Check active files
    let active_files = reader.list_active_files(table_id).await?;
    assert_eq!(active_files.len(), 2);
    
    let file_paths: Vec<String> = active_files.iter().map(|f| f.path.clone()).collect();
    assert!(file_paths.contains(&"part-00000.parquet".to_string()));
    assert!(file_paths.contains(&"part-00001.parquet".to_string()));
    
    // Remove one file
    let remove_file1 = create_test_remove_file("part-00000.parquet");
    let version_2 = writer.remove_files(table_id, vec![remove_file1]).await?;
    assert_eq!(version_2, 2);
    
    // Check active files again
    let active_files_v2 = reader.list_active_files(table_id).await?;
    assert_eq!(active_files_v2.len(), 1);
    assert_eq!(active_files_v2[0].path, "part-00001.parquet");
    
    Ok(())
}

#[tokio::test]
async fn test_version_sequence() -> TxnLogResult<()> {
    let (_temp_dir, table_path) = setup_test_dir().await?;
    
    // Create the _delta_log directory
    fs::create_dir_all(table_path.join("_delta_log")).await?;
    
    let writer = FileTxnLogWriter::new(&table_path)?;
    let reader = FileTxnLogReader::new(&table_path)?;
    
    let table_id = "version_test";
    
    // Initialize table
    let init_actions = vec![
        DeltaAction::Metadata(create_test_metadata()),
        DeltaAction::Protocol(create_test_protocol()),
    ];
    
    writer.commit_actions(
        table_id,
        init_actions,
        Some("INIT".to_string()),
        None,
    ).await?;
    
    // Create multiple versions
    for i in 1..=5 {
        let add_file = create_test_add_file(&format!("file-{}.parquet", i), i * 1024);
        let version = writer.add_files(table_id, vec![add_file]).await?;
        assert_eq!(version, i);
    }
    
    // Verify final version
    let latest_version = reader.get_latest_version(table_id).await?;
    assert_eq!(latest_version, 5);
    
    // Check version history
    let versions = reader.list_table_versions(table_id).await?;
    assert_eq!(versions.len(), 6); // versions 0 through 5
    
    // Verify specific versions
    for i in 0..=5 {
        let version_info = reader.get_table_version(table_id, i).await?;
        assert_eq!(version_info.version, i);
    }
    
    Ok(())
}

#[tokio::test]
async fn test_error_handling() -> TxnLogResult<()> {
    let (_temp_dir, table_path) = setup_test_dir().await?;
    
    // Create the _delta_log directory
    fs::create_dir_all(table_path.join("_delta_log")).await?;
    
    let reader = FileTxnLogReader::new(&table_path)?;
    
    // Test reading non-existent version when no files exist
    let result = reader.get_version_actions("non_existent", 0).await;
    assert!(matches!(result, Err(TxnLogError::InvalidVersion { .. })));
    
    // Test reading invalid version for existing table
    let writer = FileTxnLogWriter::new(&table_path)?;
    let table_id = "error_test";
    
    // Create table
    let init_actions = vec![
        DeltaAction::Metadata(create_test_metadata()),
        DeltaAction::Protocol(create_test_protocol()),
    ];
    
    writer.commit_actions(
        table_id,
        init_actions,
        Some("INIT".to_string()),
        None,
    ).await?;
    
    // Try to read non-existent version
    let result = reader.get_version_actions(table_id, 999).await;
    assert!(matches!(result, Err(TxnLogError::InvalidVersion { .. })));
    
    Ok(())
}

#[tokio::test]
async fn test_file_persistence() -> TxnLogResult<()> {
    let (_temp_dir, table_path) = setup_test_dir().await?;
    
    let table_id = "persistence_test";
    
    // Create initial data
    {
        // Create the _delta_log directory
        fs::create_dir_all(table_path.join("_delta_log")).await?;
        
        let writer = FileTxnLogWriter::new(&table_path)?;
        
        let actions = vec![
            DeltaAction::Metadata(create_test_metadata()),
            DeltaAction::Protocol(create_test_protocol()),
            DeltaAction::Add(create_test_add_file("data.parquet", 1024)),
        ];
        
        writer.commit_actions(
            table_id,
            actions,
            Some("INIT".to_string()),
            None,
        ).await?;
    }
    
    // Verify files were created
    let log_dir = table_path.join("_delta_log");
    assert!(log_dir.exists());
    
    let mut entries = fs::read_dir(&log_dir).await?;
    let mut json_files = Vec::new();
    
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            json_files.push(path);
        }
    }
    
    assert_eq!(json_files.len(), 1);
    assert!(json_files[0].to_string_lossy().contains("00000"));
    
    // Verify file contents can be read by new reader instance
    let reader = FileTxnLogReader::new(&table_path)?;
    assert!(reader.table_exists(table_id).await?);
    
    let actions = reader.get_version_actions(table_id, 0).await?;
    assert_eq!(actions.len(), 3); // metadata + protocol + add file
    
    Ok(())
}

#[tokio::test]
async fn test_transaction_workflow() -> TxnLogResult<()> {
    let (_temp_dir, table_path) = setup_test_dir().await?;
    
    // Create the _delta_log directory
    fs::create_dir_all(table_path.join("_delta_log")).await?;
    
    let writer = FileTxnLogWriter::new(&table_path)?;
    let reader = FileTxnLogReader::new(&table_path)?;
    
    let table_id = "transaction_test";
    
    // Initialize table
    let init_actions = vec![
        DeltaAction::Metadata(create_test_metadata()),
        DeltaAction::Protocol(create_test_protocol()),
    ];
    
    writer.commit_actions(
        table_id,
        init_actions,
        Some("INIT".to_string()),
        None,
    ).await?;
    
    // Begin a transaction
    let mut txn = writer.begin_transaction(table_id).await?;
    
    // Stage some actions
    txn.stage_action(DeltaAction::Add(create_test_add_file("file1.parquet", 1024)));
    txn.stage_action(DeltaAction::Add(create_test_add_file("file2.parquet", 2048)));
    
    // Commit the transaction
    let version = writer.commit_actions(
        table_id,
        txn.staged_actions.clone(),
        Some("TRANSACTION COMMIT".to_string()),
        None,
    ).await?;
    
    assert_eq!(version, 1);
    
    // Verify the files were added
    let active_files = reader.list_active_files(table_id).await?;
    assert_eq!(active_files.len(), 2);
    
    Ok(())
}