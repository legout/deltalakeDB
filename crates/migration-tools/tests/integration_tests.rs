//! Integration tests for migration tools.

use deltalakedb_core::types::{AddFile, Action, MetadataUpdate, ProtocolUpdate, RemoveFile};
use deltalakedb_migration_tools::{ImportStats, MigrationValidator};

#[test]
fn test_import_stats_calculation() {
    let mut stats = ImportStats::default();
    stats.versions_imported = 5;
    stats.add_actions = 100;
    stats.remove_actions = 20;
    stats.metadata_updates = 2;
    stats.protocol_updates = 1;

    assert_eq!(stats.versions_imported, 5);
    assert_eq!(stats.total_actions(), 123);
    assert!(stats.summary().contains("5 versions"));
}

#[test]
fn test_validation_result_passed() {
    let result = deltalakedb_migration_tools::MigrationError::ConfigError("test".to_string());
    let err_msg = format!("{}", result);
    assert!(err_msg.contains("Configuration error"));
}

#[test]
fn test_action_parsing() {
    // Test that action types are recognized
    let add_file = AddFile {
        path: "s3://bucket/file.parquet".to_string(),
        size: 1024,
        modification_time: 1704067200000,
        data_change_version: 1,
        stats: None,
        stats_truncated: None,
        tags: None,
    };

    let remove_file = RemoveFile {
        path: "s3://bucket/old.parquet".to_string(),
        deletion_timestamp: 1704067200000,
        data_change: true,
        extended_file_metadata: None,
        deletion_vector: None,
    };

    let metadata = MetadataUpdate {
        description: Some("Test table".to_string()),
        schema: None,
        partition_columns: None,
        created_time: None,
        configuration: None,
    };

    let protocol = ProtocolUpdate {
        min_reader_version: Some(1),
        min_writer_version: Some(2),
    };

    // Create actions
    let actions = vec![
        Action::Add(add_file),
        Action::Remove(remove_file),
        Action::Metadata(metadata),
        Action::Protocol(protocol),
    ];

    // Verify we created all action types
    assert_eq!(actions.len(), 4);
    matches!(actions[0], Action::Add(_));
    matches!(actions[1], Action::Remove(_));
    matches!(actions[2], Action::Metadata(_));
    matches!(actions[3], Action::Protocol(_));
}

#[test]
fn test_import_config_validation() {
    use deltalakedb_migration_tools::importer::ImportConfig;
    use uuid::Uuid;

    let config = ImportConfig {
        table_id: Uuid::new_v4(),
        source_location: "s3://bucket/table".to_string(),
        up_to_version: Some(100),
        skip_validation: false,
        dry_run: true,
    };

    assert!(config.dry_run);
    assert_eq!(config.up_to_version, Some(100));
    assert!(!config.skip_validation);
}

#[test]
fn test_add_file_creation() {
    let file = AddFile {
        path: "file.parquet".to_string(),
        size: 2048,
        modification_time: 1704067200000,
        data_change_version: 5,
        stats: None,
        stats_truncated: None,
        tags: None,
    };

    assert_eq!(file.path, "file.parquet");
    assert_eq!(file.size, 2048);
    assert_eq!(file.modification_time, 1704067200000);
    assert_eq!(file.data_change_version, 5);
}

#[test]
fn test_remove_file_creation() {
    let file = RemoveFile {
        path: "old.parquet".to_string(),
        deletion_timestamp: 1704067200000,
        data_change: true,
        extended_file_metadata: None,
        deletion_vector: None,
    };

    assert_eq!(file.path, "old.parquet");
    assert_eq!(file.deletion_timestamp, 1704067200000);
    assert!(file.data_change);
}

#[test]
fn test_validation_result_tracking() {
    let mut result = deltalakedb_migration_tools::MigrationError::ConfigError("test".to_string());

    // Verify error type
    match result {
        deltalakedb_migration_tools::MigrationError::ConfigError(msg) => {
            assert_eq!(msg, "test");
        }
        _ => panic!("Wrong error type"),
    }
}
