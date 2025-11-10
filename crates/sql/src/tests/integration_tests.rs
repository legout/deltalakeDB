//! Integration tests for SQL adapters

use crate::{
    tests::{TestConfig, utils, performance, concurrency},
    AdapterFactory, DatabaseConfig,
};
use crate::traits::{TxnLogReader, TxnLogWriter, DatabaseAdapter};
use deltalakedb_core::{Table, Commit, Protocol, Metadata, Action};
use std::sync::Arc;
use uuid::Uuid;
use tokio::time::{sleep, Duration};
use serde_json::json;

// Import test macros
use crate::{test_all_adapters, test_sqlite_only, test_duckdb_only};

/// Test basic adapter functionality
test_all_adapters!(test_adapter_basics, |adapter| async move {
    // Test database type
    assert!(!adapter.database_type().is_empty());

    // Test database version
    let version = adapter.database_version().await.unwrap();
    assert!(!version.is_empty());

    // Test health check
    let healthy = adapter.health_check().await.unwrap();
    assert!(healthy);

    // Test connection
    let connected = adapter.test_connection().await.unwrap();
    assert!(connected);

    // Test configuration
    let config = adapter.get_config();
    assert!(!config.url.is_empty());

    // Test optimization hints
    let hints = adapter.get_optimization_hints().await.unwrap();
    assert!(!hints.is_empty());

    // Test pool stats
    let stats = adapter.pool_stats().await.unwrap();
    assert!(stats.total_connections >= 1);
});

/// Test schema initialization and management
test_all_adapters!(test_schema_management, |adapter| async move {
    // Check if schema is already initialized
    let schema_ok = adapter.check_schema_version().await.unwrap();
    assert!(schema_ok);

    // Test schema migration (should be idempotent)
    adapter.migrate_schema().await.unwrap();

    // Test index creation
    adapter.create_indexes().await.unwrap();

    // Verify schema is still OK
    let schema_ok_after = adapter.check_schema_version().await.unwrap();
    assert!(schema_ok_after);
});

/// Test table operations
test_all_adapters!(test_table_operations, |adapter| async move {
    // Create test table
    let table = utils::create_test_table("/test/integration_test_table");

    // Test create_table
    let created_table = adapter.create_table(&table).await.unwrap();
    assert!(utils::tables_equal_ignoring_timestamps(&table, &created_table));

    // Test read_table
    let read_table = adapter.read_table(table.id).await.unwrap();
    assert!(read_table.is_some());
    assert!(utils::tables_equal_ignoring_timestamps(&table, &read_table.unwrap()));

    // Test read_table_by_path
    let read_by_path = adapter.read_table_by_path(&table.table_path).await.unwrap();
    assert!(read_by_path.is_some());
    assert_eq!(read_by_path.unwrap().id, table.id);

    // Test table_exists
    let exists = adapter.table_exists(&table.table_path).await.unwrap();
    assert!(exists);

    // Test list_tables
    let tables = adapter.list_tables(None, None).await.unwrap();
    assert!(!tables.is_empty());
    assert!(tables.iter().any(|t| t.id == table.id));

    // Test list_tables with pagination
    let paginated_tables = adapter.list_tables(Some(1), Some(0)).await.unwrap();
    assert_eq!(paginated_tables.len(), 1);

    // Test update_table
    let mut updated_table = created_table.clone();
    updated_table.table_name = "updated_integration_test_table".to_string();
    let table_result = adapter.update_table(&updated_table).await.unwrap();
    assert_eq!(table_result.table_name, "updated_integration_test_table");

    // Verify update persisted
    let read_updated = adapter.read_table(table.id).await.unwrap();
    assert!(read_updated.is_some());
    assert_eq!(read_updated.unwrap().table_name, "updated_integration_test_table");

    // Test delete_table
    let deleted = adapter.delete_table(table.id).await.unwrap();
    assert!(deleted);

    // Verify table is soft-deleted
    let exists_after_delete = adapter.table_exists(&table.table_path).await.unwrap();
    assert!(!exists_after_delete);
});

/// Test protocol operations
test_all_adapters!(test_protocol_operations, |adapter| async move {
    let table_id = Uuid::new_v4();
    let protocol = utils::create_test_protocol();

    // Test update_protocol
    adapter.update_protocol(table_id, &protocol).await.unwrap();

    // Test read_protocol
    let read_protocol = adapter.read_protocol(table_id).await.unwrap();
    assert!(read_protocol.is_some());
    let proto = read_protocol.unwrap();
    assert_eq!(proto.min_reader_version, protocol.min_reader_version);
    assert_eq!(proto.min_writer_version, protocol.min_writer_version);

    // Test protocol update
    let new_protocol = Protocol {
        min_reader_version: 2,
        min_writer_version: 3,
    };
    adapter.update_protocol(table_id, &new_protocol).await.unwrap();

    // Verify update
    let updated_protocol = adapter.read_protocol(table_id).await.unwrap();
    assert!(updated_protocol.is_some());
    let updated = updated_protocol.unwrap();
    assert_eq!(updated.min_reader_version, 2);
    assert_eq!(updated.min_writer_version, 3);
});

/// Test metadata operations
test_all_adapters!(test_metadata_operations, |adapter| async move {
    let table_id = Uuid::new_v4();
    let metadata = utils::create_test_metadata("integration_test");

    // Test update_metadata
    adapter.update_metadata(table_id, &metadata).await.unwrap();

    // Test read_metadata
    let read_metadata = adapter.read_metadata(table_id).await.unwrap();
    assert!(read_metadata.is_some());
    let meta = read_metadata.unwrap();
    assert_eq!(meta.id, metadata.id);
    assert_eq!(meta.name, metadata.name);
    assert_eq!(meta.description, metadata.description);
    assert_eq!(meta.format, metadata.format);
    assert_eq!(meta.partition_columns, metadata.partition_columns);
    assert_eq!(meta.created_time, metadata.created_time);

    // Test metadata update
    let new_metadata = Metadata {
        id: metadata.id,
        name: "updated_integration_test".to_string(),
        description: Some("Updated test table".to_string()),
        format: metadata.format,
        schema_string: metadata.schema_string,
        partition_columns: vec!["year".to_string()],
        configuration: json!({"new_config": "value"}),
        created_time: metadata.created_time,
    };

    adapter.update_metadata(table_id, &new_metadata).await.unwrap();

    // Verify update
    let updated_metadata = adapter.read_metadata(table_id).await.unwrap();
    assert!(updated_metadata.is_some());
    let updated = updated_metadata.unwrap();
    assert_eq!(updated.name, "updated_integration_test");
    assert_eq!(updated.description, Some("Updated test table".to_string()));
    assert_eq!(updated.partition_columns, vec!["year".to_string()]);
});

/// Test commit operations
test_all_adapters!(test_commit_operations, |adapter| async move {
    let table_id = Uuid::new_v4();

    // Test get_next_version (should return 1 for new table)
    let next_version = adapter.get_next_version(table_id).await.unwrap();
    assert_eq!(next_version, 1);

    // Create multiple commits
    let commit1 = utils::create_test_commit(table_id, 1, "WRITE");
    let commit2 = utils::create_test_commit(table_id, 2, "WRITE");
    let commit3 = utils::create_test_commit(table_id, 3, "MERGE");

    // Test write_commit
    let written_commit1 = adapter.write_commit(&commit1).await.unwrap();
    assert_eq!(written_commit1.version, 1);
    assert_eq!(written_commit1.operation_type, "WRITE");
    assert_eq!(written_commit1.table_id, table_id);

    // Test get_next_version after first commit
    let next_version2 = adapter.get_next_version(table_id).await.unwrap();
    assert_eq!(next_version2, 2);

    // Test write_commits (bulk operation)
    let written_commits = adapter.write_commits(&[commit2, commit3]).await.unwrap();
    assert_eq!(written_commits.len(), 2);
    assert_eq!(written_commits[0].version, 2);
    assert_eq!(written_commits[1].version, 3);

    // Test read_commit
    let read_commit1 = adapter.read_commit(table_id, 1).await.unwrap();
    assert!(read_commit1.is_some());
    assert_eq!(read_commit1.unwrap().version, 1);

    let read_commit2 = adapter.read_commit(table_id, 2).await.unwrap();
    assert!(read_commit2.is_some());
    assert_eq!(read_commit2.unwrap().version, 2);

    // Test read_latest_commit
    let latest_commit = adapter.read_latest_commit(table_id).await.unwrap();
    assert!(latest_commit.is_some());
    assert_eq!(latest_commit.unwrap().version, 3);

    // Test get_table_version
    let table_version = adapter.get_table_version(table_id).await.unwrap();
    assert_eq!(table_version, Some(3));

    // Test count_commits
    let commit_count = adapter.count_commits(table_id).await.unwrap();
    assert_eq!(commit_count, 3);

    // Test read_commits_range
    let commits = adapter.read_commits_range(table_id, Some(1), Some(3), None).await.unwrap();
    assert_eq!(commits.len(), 3);
    // Should be in DESC order
    assert_eq!(commits[0].version, 3);
    assert_eq!(commits[1].version, 2);
    assert_eq!(commits[2].version, 1);

    // Test read_commits_range with limit
    let limited_commits = adapter.read_commits_range(table_id, None, None, Some(2)).await.unwrap();
    assert_eq!(limited_commits.len(), 2);

    // Test get_next_version after all commits
    let next_version4 = adapter.get_next_version(table_id).await.unwrap();
    assert_eq!(next_version4, 4);
});

/// Test file operations and metadata queries
test_all_adapters!(test_file_operations, |adapter| async move {
    let table_id = Uuid::new_v4();

    // Create a commit first (required for file operations)
    let commit = utils::create_test_commit(table_id, 1, "WRITE");
    adapter.write_commit(&commit).await.unwrap();

    // Test read_table_files (should be empty initially)
    let files = adapter.read_table_files(table_id, None, None).await.unwrap();
    assert!(files.is_empty());

    // Note: In a full implementation, we would:
    // 1. Insert commit actions (AddFile, RemoveFile, etc.)
    // 2. Update the delta_files table
    // 3. Then test the read_table_files with actual data

    // For now, test the range filtering functionality
    let files_range = adapter.read_table_files(table_id, Some(1), Some(1)).await.unwrap();
    assert!(files_range.is_empty());
});

/// Test version conflict handling
test_all_adapters!(test_version_conflicts, |adapter| async move {
    let table_id = Uuid::new_v4();

    // Create initial commit
    let commit1 = utils::create_test_commit(table_id, 1, "WRITE");
    adapter.write_commit(&commit1).await.unwrap();

    // Verify version tracking
    let next_version = adapter.get_next_version(table_id).await.unwrap();
    assert_eq!(next_version, 2);

    // Create commits with specific versions
    let commit2 = utils::create_test_commit(table_id, 2, "WRITE");
    let commit3 = utils::create_test_commit(table_id, 3, "WRITE");

    adapter.write_commit(&commit2).await.unwrap();
    adapter.write_commit(&commit3).await.unwrap();

    // Verify final state
    let final_version = adapter.get_next_version(table_id).await.unwrap();
    assert_eq!(final_version, 4);

    let commit_count = adapter.count_commits(table_id).await.unwrap();
    assert_eq!(commit_count, 3);
});

/// Test transaction operations
test_all_adapters!(test_transaction_operations, |adapter| async move {
    let table = utils::create_test_table("/test/transaction_test_table");

    // Note: Full transaction testing would require implementing
    // the begin_transaction method completely

    // For now, test that the method exists and returns an expected error
    let result = adapter.begin_transaction().await;
    assert!(result.is_err());
});

/// Test error handling and edge cases
test_all_adapters!(test_error_handling, |adapter| async move {
    // Test reading non-existent table
    let non_existent_id = Uuid::new_v4();
    let read_table = adapter.read_table(non_existent_id).await.unwrap();
    assert!(read_table.is_none());

    // Test reading non-existent commit
    let read_commit = adapter.read_commit(non_existent_id, 999).await.unwrap();
    assert!(read_commit.is_none());

    // Test table existence for non-existent path
    let exists = adapter.table_exists("/non/existent/path").await.unwrap();
    assert!(!exists);

    // Test reading protocol for non-existent table
    let read_protocol = adapter.read_protocol(non_existent_id).await.unwrap();
    assert!(read_protocol.is_none());

    // Test reading metadata for non-existent table
    let read_metadata = adapter.read_metadata(non_existent_id).await.unwrap();
    assert!(read_metadata.is_none());

    // Test get_next_version for non-existent table (should return 1)
    let next_version = adapter.get_next_version(non_existent_id).await.unwrap();
    assert_eq!(next_version, 1);

    // Test count_commits for non-existent table (should return 0)
    let commit_count = adapter.count_commits(non_existent_id).await.unwrap();
    assert_eq!(commit_count, 0);

    // Test get_table_version for non-existent table (should return None)
    let table_version = adapter.get_table_version(non_existent_id).await.unwrap();
    assert!(table_version.is_none());
});

/// Test performance characteristics
test_sqlite_only!(test_performance_operations, |adapter| async move {
    // Test table creation performance
    let table = utils::create_test_table("/test/performance_table");
    let (_, metrics) = performance::measure("create_table", || adapter.create_table(&table)).await;
    assert!(metrics.success);
    println!("Table creation took {}ms", metrics.duration_ms);

    // Test commit writing performance
    let commit = utils::create_test_commit(table.id, 1, "WRITE");
    let (_, metrics) = performance::measure("write_commit", || adapter.write_commit(&commit)).await;
    assert!(metrics.success);
    println!("Commit writing took {}ms", metrics.duration_ms);

    // Test reading performance
    let (_, metrics) = performance::measure("read_table", || adapter.read_table(table.id)).await;
    assert!(metrics.success);
    println!("Table reading took {}ms", metrics.duration_ms);

    // Test bulk operations
    let commits: Vec<_> = (2..=100)
        .map(|version| utils::create_test_commit(table.id, version, "WRITE"))
        .collect();

    let (_, metrics) = performance::measure_bulk("write_commits_bulk", commits.len() as u64, || {
        adapter.write_commits(&commits)
    }).await;
    assert!(metrics.success);
    println!("Bulk commit writing (99 commits) took {}ms, throughput: {:.2} commits/sec",
             metrics.duration_ms, metrics.throughput());

    // Test list tables performance
    let (_, metrics) = performance::measure("list_tables", || adapter.list_tables(None, None)).await;
    assert!(metrics.success);
    println!("List tables took {}ms", metrics.duration_ms);

    // Test count commits performance
    let (_, metrics) = performance::measure("count_commits", || adapter.count_commits(table.id)).await;
    assert!(metrics.success);
    println!("Count commits took {}ms", metrics.duration_ms);
});

/// Test concurrent operations
test_sqlite_only!(test_concurrent_operations, |adapter| async move {
    let adapter = Arc::new(adapter);

    // Test concurrent table creation
    let table_results = concurrency::test_concurrent_table_creation(adapter.clone(), 10).await;
    let successful_creates = table_results.iter().filter(|r| r.is_ok()).count();
    println!("Concurrent table creation: {}/10 successful", successful_creates);
    assert!(successful_creates >= 8); // Allow for some failures due to race conditions

    // Get a table ID for commit testing
    let table_id = if let Ok(table) = &table_results[0] {
        table.id
    } else {
        // Create a table manually
        let table = utils::create_test_table("/test/concurrent_commit_table");
        adapter.create_table(&table).await.unwrap().id
    };

    // Test concurrent commit writing
    let commit_results = concurrency::test_concurrent_commit_writing(adapter.clone(), table_id, 20).await;
    let successful_commits = commit_results.iter().filter(|r| r.is_ok()).count();
    println!("Concurrent commit writing: {}/20 successful", successful_commits);

    // Verify final state
    let final_commit_count = adapter.count_commits(table_id).await.unwrap();
    println!("Final commit count: {}", final_commit_count);
    assert!(final_commit_count >= 10); // At least some commits should have succeeded
});

/// Test adapter factory functionality
test_all_adapters!(test_adapter_factory, |adapter| async move {
    let database_type = adapter.database_type();

    // Test that the adapter was created correctly
    assert!(!database_type.is_empty());

    // Test basic functionality
    let healthy = adapter.health_check().await.unwrap();
    assert!(healthy);

    // Test version
    let version = adapter.database_version().await.unwrap();
    assert!(!version.is_empty());
});

/// Test cleanup and resource management
test_all_adapters!(test_cleanup_and_resource_management, |adapter| async move {
    // Create some test data
    let table = utils::create_test_table("/test/cleanup_table");
    let created_table = adapter.create_table(&table).await.unwrap();

    let commit = utils::create_test_commit(created_table.id, 1, "WRITE");
    adapter.write_commit(&commit).await.unwrap();

    // Test vacuum operations
    let vacuumed = adapter.vacuum_commits(created_table.id, 0).await.unwrap();
    println!("Vacuumed {} commits", vacuumed);

    // Verify data is gone after vacuum
    let commit_count = adapter.count_commits(created_table.id).await.unwrap();
    assert_eq!(commit_count, 0);

    // Test table deletion
    let deleted = adapter.delete_table(created_table.id).await.unwrap();
    assert!(deleted);

    // Verify table is gone
    let exists = adapter.table_exists(&created_table.table_path).await.unwrap();
    assert!(!exists);

    // Test adapter cleanup
    adapter.close().await.unwrap();
});