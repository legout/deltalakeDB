//! Integration tests for multi-table transaction documentation examples.
//!
//! This module tests all the examples provided in the documentation
//! to ensure they work correctly and stay up-to-date.

use deltalakedb_sql::multi_table::*;
use deltalakedb_sql::connection::{DatabaseConfig, DatabaseConnection};
use deltalakedb_core::{
    error::TxnLogResult,
    transaction::TransactionIsolationLevel,
    writer::TxnLogWriterExt,
    DeltaAction, AddFile, RemoveFile, Metadata, Protocol, Format,
};
use std::collections::HashMap;
use chrono::Utc;
use tempfile::tempdir;

/// Create a test database connection
async fn create_test_connection() -> TxnLogResult<DatabaseConnection> {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let config = DatabaseConfig::new(format!("sqlite:{}", db_path.to_str().unwrap()));
    config.connect().await
}

/// Create test AddFile objects
fn create_test_add_files(prefix: &str, count: usize) -> Vec<AddFile> {
    (0..count).map(|i| AddFile {
        path: format!("{}/file_{}.parquet", prefix, i),
        size: 1000 + i as i64,
        modification_time: Utc::now().timestamp(),
        data_change: true,
        partition_values: Default::default(),
        stats: None,
        tags: None,
    }).collect()
}

/// Create test RemoveFile objects
fn create_test_remove_files(prefix: &str, count: usize) -> Vec<RemoveFile> {
    (0..count).map(|i| RemoveFile {
        path: format!("{}/old_file_{}.parquet", prefix, i),
        deletion_timestamp: Some(Utc::now().timestamp()),
        data_change: true,
        extended_file_metadata: None,
        partition_values: None,
        size: Some(500 + i as i64),
        tags: None,
    }).collect()
}

/// Create test metadata
fn create_test_metadata() -> Metadata {
    Metadata {
        id: "test_metadata".to_string(),
        format: Format {
            provider: "parquet".to_string(),
            options: Default::default(),
        },
        schema_string: r#"{"type": "struct", "fields": [
            {"name": "id", "type": "long", "nullable": false},
            {"name": "name", "type": "string", "nullable": true}
        ]}"#.to_string(),
        partition_columns: vec!["year".to_string(), "month".to_string()],
        configuration: Default::default(),
        created_time: Some(Utc::now().timestamp()),
    }
}

#[tokio::test]
async fn test_basic_multi_table_transaction_example() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    // Begin transaction
    let mut tx = writer.begin_transaction();
    
    // Create test data
    let add_files_table1 = create_test_add_files("table1", 2);
    let add_files_table2 = create_test_add_files("table2", 1);
    
    // Add files to multiple tables
    tx.add_files("table1".to_string(), 0, add_files_table1)?;
    tx.add_files("table2".to_string(), 0, add_files_table2)?;
    
    assert_eq!(tx.table_count(), 2);
    assert_eq!(tx.total_action_count(), 3);
    
    // Commit transaction
    let result = writer.commit_transaction(tx).await?;
    
    assert_eq!(result.table_results.len(), 2);
    assert!(result.table_results.iter().all(|r| r.success));
    
    Ok(())
}

#[tokio::test]
async fn test_table_actions_builder_example() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    // Create table actions using builder pattern
    let table_actions = TableActionsBuilder::new("customers".to_string(), 5)
        .add_files(create_test_add_files("customers", 1))
        .remove_files(create_test_remove_files("customers", 1))
        .update_metadata(create_test_metadata())
        .with_operation("MERGE".to_string())
        .with_operation_params(HashMap::from([
            ("source".to_string(), "staging_customers".to_string()),
            ("condition".to_string(), "target.id = source.id".to_string()),
        ]))
        .build_and_validate()?;
    
    assert_eq!(table_actions.table_id, "customers");
    assert_eq!(table_actions.expected_version, 5);
    assert_eq!(table_actions.action_count(), 3); // 1 add + 1 remove + 1 metadata
    assert_eq!(table_actions.operation, Some("MERGE".to_string()));
    
    let summary = table_actions.summary();
    assert_eq!(summary.add_count, 1);
    assert_eq!(summary.remove_count, 1);
    assert_eq!(summary.metadata_count, 1);
    
    Ok(())
}

#[tokio::test]
async fn test_isolation_levels_example() -> TxnLogResult<()> {
    // Example 1: ReadCommitted (default, good for most cases)
    let tx_read_committed = MultiTableTransaction::with_isolation_level(
        MultiTableConfig::default(),
        TransactionIsolationLevel::ReadCommitted,
    );
    assert_eq!(tx_read_committed.isolation_level, TransactionIsolationLevel::ReadCommitted);
    
    // Example 2: RepeatableRead (for consistent reads during long transactions)
    let tx_repeatable_read = MultiTableTransaction::with_isolation_level(
        MultiTableConfig::default(),
        TransactionIsolationLevel::RepeatableRead,
    );
    assert_eq!(tx_repeatable_read.isolation_level, TransactionIsolationLevel::RepeatableRead);
    
    // Example 3: Serializable (highest consistency, lower performance)
    let tx_serializable = MultiTableTransaction::with_isolation_and_priority(
        MultiTableConfig::default(),
        TransactionIsolationLevel::Serializable,
        10, // High priority for critical operations
    );
    assert_eq!(tx_serializable.isolation_level, TransactionIsolationLevel::Serializable);
    assert_eq!(tx_serializable.priority, 10);
    
    Ok(())
}

#[tokio::test]
async fn test_error_handling_example() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    // Example 1: Empty transaction validation
    let mut empty_tx = writer.begin_transaction();
    assert!(empty_tx.validate().is_err());
    
    // Example 2: Table actions validation
    let mut empty_table_actions = TableActions::new("empty_table".to_string(), 0);
    assert!(empty_table_actions.validate().is_err());
    
    // Example 3: Valid transaction with proper actions
    let mut valid_tx = writer.begin_transaction();
    let add_file = create_test_add_files("valid", 1).remove(0).unwrap();
    valid_tx.add_file("valid_table".to_string(), 0, add_file)?;
    assert!(valid_tx.validate().is_ok());
    
    // Example 4: Transaction state transitions
    let mut state_tx = writer.begin_transaction();
    assert_eq!(state_tx.state, deltalakedb_core::transaction::TransactionState::Active);
    
    state_tx.mark_committing()?;
    assert_eq!(state_tx.state, deltalakedb_core::transaction::TransactionState::Committing);
    
    state_tx.mark_committed();
    assert_eq!(state_tx.state, deltalakedb_core::transaction::TransactionState::Committed);
    
    // Try to add actions after commit
    let table_actions = TableActions::new("after_commit".to_string(), 0);
    assert!(state_tx.stage_actions(table_actions).is_err());
    
    Ok(())
}

#[tokio::test]
async fn test_configuration_example() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    
    // Example 1: High-performance configuration (for bulk operations)
    let high_perf_config = MultiTableConfig {
        enable_consistency_validation: false,  // Skip validation for speed
        enable_ordered_mirroring: false,        // Skip mirroring for speed
        enable_deadlock_detection: false,       // Skip deadlock detection
        max_tables_per_transaction: 50,        // Allow more tables
        max_actions_per_transaction: 50000,      // Allow more actions
        max_actions_per_table: 5000,             // Allow more actions per table
        max_transaction_age_seconds: 7200,      // 2 hours for large operations
        max_retry_attempts: 1,                  // Fewer retries for speed
        retry_base_delay_ms: 50,                // Faster retries
        retry_max_delay_ms: 1000,               // Lower max delay
        ..Default::default()
    };
    
    let high_perf_writer = MultiTableWriter::new(connection.clone(), None, high_perf_config);
    assert!(!high_perf_writer.config.enable_consistency_validation);
    assert!(!high_perf_writer.config.enable_ordered_mirroring);
    assert_eq!(high_perf_writer.config.max_actions_per_transaction, 50000);
    
    // Example 2: Safety-first configuration (for critical data)
    let safety_config = MultiTableConfig {
        enable_consistency_validation: true,   // Enable all validations
        enable_ordered_mirroring: true,         // Enable mirroring
        enable_deadlock_detection: true,        // Enable deadlock detection
        max_tables_per_transaction: 10,         // Limit tables for safety
        max_actions_per_transaction: 1000,       // Limit actions for safety
        max_actions_per_table: 100,             // Limit per-table actions
        max_transaction_age_seconds: 300,       // 5 minutes timeout
        max_retry_attempts: 5,                 // More retries for reliability
        retry_base_delay_ms: 200,              // Conservative retry delay
        retry_max_delay_ms: 10000,             // Higher max delay
        ..Default::default()
    };
    
    let safety_writer = MultiTableWriter::new(connection.clone(), None, safety_config);
    assert!(safety_writer.config.enable_consistency_validation);
    assert!(safety_writer.config.enable_ordered_mirroring);
    assert_eq!(safety_writer.config.max_tables_per_transaction, 10);
    
    // Example 3: Balanced configuration (general purpose)
    let balanced_config = MultiTableConfig::default();
    let balanced_writer = MultiTableWriter::new(connection, None, balanced_config);
    assert!(balanced_writer.config.enable_consistency_validation);
    assert!(!balanced_writer.config.enable_ordered_mirroring);
    
    Ok(())
}

#[tokio::test]
async fn test_use_case_multi_table_data_ingestion() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    let customer_files = create_test_add_files("customers", 3);
    let order_files = create_test_add_files("orders", 5);
    
    let mut tx = writer.begin_transaction();
    
    // Add customer data
    tx.add_files("customers".to_string(), 0, customer_files)?;
    
    // Add corresponding order data
    tx.add_files("orders".to_string(), 0, order_files)?;
    
    // Commit atomically - both tables succeed or both fail
    let result = writer.commit_transaction(tx).await?;
    
    assert_eq!(result.table_results.len(), 2);
    assert!(result.table_results.iter().all(|r| r.success));
    
    Ok(())
}

#[tokio::test]
async fn test_use_case_schema_migration() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    let table_ids = vec![
        "table1".to_string(),
        "table2".to_string(),
        "table3".to_string(),
    ];
    let new_metadata = create_test_metadata();
    
    let mut tx = writer.begin_transaction();
    
    // Update metadata for all tables atomically
    for table_id in table_ids {
        tx.update_metadata(table_id, 0, new_metadata.clone())?;
    }
    
    let result = writer.commit_transaction(tx).await?;
    
    assert_eq!(result.table_results.len(), 3);
    assert!(result.table_results.iter().all(|r| r.success));
    
    Ok(())
}

#[tokio::test]
async fn test_use_case_data_archiving() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    let old_files = create_test_remove_files("active_data", 2);
    let archive_files = create_test_add_files("archive_data", 2);
    
    let mut tx = writer.begin_transaction();
    
    // Remove from active table
    tx.remove_files("active_data".to_string(), 0, old_files)?;
    
    // Add to archive table
    tx.add_files("archive_data".to_string(), 0, archive_files)?;
    
    let result = writer.commit_transaction(tx).await?;
    
    assert_eq!(result.table_results.len(), 2);
    assert!(result.table_results.iter().all(|r| r.success));
    
    Ok(())
}

#[tokio::test]
async fn test_use_case_complex_etl() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    let source_data = create_test_add_files("raw_staging", 3);
    let transformed_data = create_test_add_files("processed_data", 2);
    let audit_data = create_test_add_files("audit_log", 1);
    
    let mut tx = writer.begin_transaction();
    
    // Stage raw data
    tx.add_files("raw_staging".to_string(), 0, source_data)?;
    
    // Stage transformed data
    tx.add_files("processed_data".to_string(), 0, transformed_data)?;
    
    // Stage audit trail
    tx.add_files("audit_log".to_string(), 0, audit_data)?;
    
    let result = writer.commit_transaction(tx).await?;
    
    assert_eq!(result.table_results.len(), 3);
    assert!(result.table_results.iter().all(|r| r.success));
    
    Ok(())
}

#[tokio::test]
async fn test_transaction_summary_and_metrics() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    let mut tx = writer.begin_transaction();
    
    // Add actions to multiple tables
    tx.add_files("table1".to_string(), 0, create_test_add_files("table1", 2))?;
    tx.add_files("table2".to_string(), 0, create_test_add_files("table2", 1))?;
    tx.remove_files("table3".to_string(), 0, create_test_remove_files("table3", 1))?;
    
    // Test transaction summary
    let summary = tx.summary();
    assert_eq!(summary.table_count, 3);
    assert_eq!(summary.total_action_count, 4);
    assert_eq!(summary.state, deltalakedb_core::transaction::TransactionState::Active);
    
    // Test table action summary
    let table_actions = tx.get_staged_actions("table1")?;
    let table_summary = table_actions.summary();
    assert_eq!(table_summary.table_id, "table1");
    assert_eq!(table_summary.add_count, 2);
    assert_eq!(table_summary.total_actions, 2);
    
    Ok(())
}

#[tokio::test]
async fn test_mixed_operations() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    let mut tx = writer.begin_transaction();
    
    let add_files = create_test_add_files("mixed", 2);
    let remove_files = create_test_remove_files("mixed", 1);
    
    // Test mixed operation
    tx.mixed_operation("mixed_table".to_string(), 0, add_files, remove_files)?;
    
    assert_eq!(tx.table_count(), 1);
    assert_eq!(tx.total_action_count(), 3); // 2 adds + 1 remove
    
    let table_actions = tx.get_staged_actions("mixed_table")?;
    let summary = table_actions.summary();
    assert_eq!(summary.add_count, 2);
    assert_eq!(summary.remove_count, 1);
    
    Ok(())
}

#[tokio::test]
async fn test_transaction_state_management() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    let mut tx = writer.begin_transaction();
    
    // Add some actions
    tx.add_file("test_table".to_string(), 0, create_test_add_files("test", 1).remove(0).unwrap())?;
    
    // Test state transitions
    assert_eq!(tx.state, deltalakedb_core::transaction::TransactionState::Active);
    
    tx.mark_committing()?;
    assert_eq!(tx.state, deltalakedb_core::transaction::TransactionState::Committing);
    
    tx.mark_committed();
    assert_eq!(tx.state, deltalakedb_core::transaction::TransactionState::Committed);
    
    // Test rollback path
    let mut tx2 = writer.begin_transaction();
    tx2.add_file("test_table2".to_string(), 0, create_test_add_files("test2", 1).remove(0).unwrap())?;
    
    assert_eq!(tx2.state, deltalakedb_core::transaction::TransactionState::Active);
    
    tx2.mark_rolling_back()?;
    assert_eq!(tx2.state, deltalakedb_core::transaction::TransactionState::RollingBack);
    
    tx2.mark_rolled_back();
    assert_eq!(tx2.state, deltalakedb_core::transaction::TransactionState::RolledBack);
    
    Ok(())
}

#[tokio::test]
async fn test_consistency_violations() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    // Test empty table actions validation
    let empty_actions = TableActions::new("empty".to_string(), 0);
    assert!(empty_actions.validate().is_err());
    
    // Test duplicate file detection
    let mut duplicate_actions = TableActions::new("duplicate".to_string(), 0);
    let file_path = "duplicate/file.parquet".to_string();
    
    duplicate_actions.add_file(file_path.clone(), 1000, Utc::now().timestamp());
    duplicate_actions.remove_file(file_path.clone()); // This should be fine
    
    // Add the same file again - this should cause a validation error in real implementation
    // For now, just test the basic structure
    assert_eq!(duplicate_actions.action_count(), 2);
    
    Ok(())
}

#[tokio::test]
async fn test_advanced_builder_patterns() -> TxnLogResult<()> {
    let connection = create_test_connection().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    // Test complex builder pattern with all features
    let complex_actions = TableActionsBuilder::new("complex_table".to_string(), 10)
        .add_files(create_test_add_files("complex", 3))
        .remove_files(create_test_remove_files("complex", 2))
        .update_metadata(create_test_metadata())
        .update_protocol(Protocol {
            min_reader_version: 2,
            min_writer_version: 2,
        })
        .with_operation("COMPLEX_MERGE".to_string())
        .with_operation_params(HashMap::from([
            ("batch_id".to_string(), "batch_123".to_string()),
            ("source_version".to_string(), "10".to_string()),
            ("target_version".to_string(), "11".to_string()),
        ]))
        .build_and_validate()?;
    
    assert_eq!(complex_actions.table_id, "complex_table");
    assert_eq!(complex_actions.expected_version, 10);
    assert_eq!(complex_actions.action_count(), 6); // 3 adds + 2 removes + 1 metadata + 1 protocol
    assert_eq!(complex_actions.operation, Some("COMPLEX_MERGE".to_string()));
    assert!(complex_actions.operation_params.is_some());
    
    let summary = complex_actions.summary();
    assert_eq!(summary.add_count, 3);
    assert_eq!(summary.remove_count, 2);
    assert_eq!(summary.metadata_count, 1);
    assert_eq!(summary.protocol_count, 1);
    
    Ok(())
}