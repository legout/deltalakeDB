//! Examples for multi-table transactions in deltalakedb-sql.
//!
//! This module contains practical examples demonstrating how to use
//! the multi-table transaction functionality for various scenarios.

use deltalakedb_sql::multi_table::{
    MultiTableWriter, MultiTableConfig, MultiTableTransaction, TableActions,
    TableActionsBuilder, ConsistencyViolation,
};
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

/// Example 1: Basic multi-table transaction
/// 
/// Demonstrates creating a simple transaction that adds files to multiple tables.
pub async fn basic_multi_table_transaction() -> TxnLogResult<()> {
    println!("=== Basic Multi-Table Transaction Example ===");
    
    // Setup database connection
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("example.db");
    let config = DatabaseConfig::new(format!("sqlite:{}", db_path.to_str().unwrap()));
    let connection = config.connect().await?;
    
    // Create multi-table writer
    let multi_config = MultiTableConfig::default();
    let writer = MultiTableWriter::new(connection, None, multi_config);
    
    // Begin transaction
    let mut tx = writer.begin_transaction();
    println!("Started transaction: {}", tx.transaction_id);
    
    // Create test data
    let add_files_table1 = vec![
        AddFile {
            path: "table1/file1.parquet".to_string(),
            size: 1000,
            modification_time: Utc::now().timestamp(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        },
        AddFile {
            path: "table1/file2.parquet".to_string(),
            size: 1500,
            modification_time: Utc::now().timestamp(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        },
    ];
    
    let add_files_table2 = vec![
        AddFile {
            path: "table2/file1.parquet".to_string(),
            size: 800,
            modification_time: Utc::now().timestamp(),
            data_change: true,
            partition_values: Default::default(),
            stats: None,
            tags: None,
        },
    ];
    
    // Add files to multiple tables
    tx.add_files("table1".to_string(), 0, add_files_table1)?;
    tx.add_files("table2".to_string(), 0, add_files_table2)?;
    
    println!("Staged actions for {} tables", tx.table_count());
    println!("Total actions: {}", tx.total_action_count());
    
    // Commit transaction
    let result = writer.commit_transaction(tx).await?;
    
    println!("Transaction committed successfully!");
    println!("Tables committed: {}", result.table_results.len());
    
    for table_result in &result.table_results {
        println!("  Table {}: {} actions, version {}", 
                table_result.table_id, 
                table_result.action_count, 
                table_result.version);
    }
    
    Ok(())
}

/// Example 2: Using the TableActions builder pattern
/// 
/// Demonstrates the fluent API for building complex table actions.
pub async fn table_actions_builder_example() -> TxnLogResult<()> {
    println!("\n=== TableActions Builder Example ===");
    
    // Setup (same as above)
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("example.db");
    let config = DatabaseConfig::new(format!("sqlite:{}", db_path.to_str().unwrap()));
    let connection = config.connect().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    // Create table actions using builder pattern
    let table_actions = TableActionsBuilder::new("customers".to_string(), 5)
        .add_files(vec![
            AddFile {
                path: "customers/2024/01/part-00000.parquet".to_string(),
                size: 2000,
                modification_time: Utc::now().timestamp(),
                data_change: true,
                partition_values: HashMap::from([
                    ("year".to_string(), "2024".to_string()),
                    ("month".to_string(), "01".to_string()),
                ]),
                stats: None,
                tags: None,
            },
        ])
        .remove_files(vec![
            RemoveFile {
                path: "customers/old_data.parquet".to_string(),
                deletion_timestamp: Some(Utc::now().timestamp()),
                data_change: true,
                extended_file_metadata: None,
                partition_values: None,
                size: Some(1500),
                tags: None,
            },
        ])
        .update_metadata(Metadata {
            id: "customers_metadata".to_string(),
            format: Format {
                provider: "parquet".to_string(),
                options: Default::default(),
            },
            schema_string: r#"{"type": "struct", "fields": [
                {"name": "id", "type": "long", "nullable": false},
                {"name": "name", "type": "string", "nullable": true},
                {"name": "email", "type": "string", "nullable": true}
            ]}"#.to_string(),
            partition_columns: vec!["year".to_string(), "month".to_string()],
            configuration: Default::default(),
            created_time: Some(Utc::now().timestamp()),
        })
        .with_operation("MERGE".to_string())
        .with_operation_params(HashMap::from([
            ("source".to_string(), "staging_customers".to_string()),
            ("condition".to_string(), "target.id = source.id".to_string()),
        ]))
        .build_and_validate()?;
    
    println!("Built table actions:");
    println!("  Table: {}", table_actions.table_id);
    println!("  Expected version: {}", table_actions.expected_version);
    println!("  Total actions: {}", table_actions.action_count());
    println!("  Operation: {:?}", table_actions.operation);
    
    let summary = table_actions.summary();
    println!("  Summary: {} adds, {} removes, {} metadata updates", 
            summary.add_count, summary.remove_count, summary.metadata_count);
    
    Ok(())
}

/// Example 3: Transaction with different isolation levels
/// 
/// Shows how to use different isolation levels for various consistency requirements.
pub async fn isolation_levels_example() -> TxnLogResult<()> {
    println!("\n=== Isolation Levels Example ===");
    
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("example.db");
    let config = DatabaseConfig::new(format!("sqlite:{}", db_path.to_str().unwrap()));
    let connection = config.connect().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    // Example 1: ReadCommitted (default, good for most cases)
    let tx_read_committed = MultiTableTransaction::with_isolation_level(
        MultiTableConfig::default(),
        TransactionIsolationLevel::ReadCommitted,
    );
    println!("ReadCommitted transaction: {}", tx_read_committed.transaction_id);
    
    // Example 2: RepeatableRead (for consistent reads during long transactions)
    let tx_repeatable_read = MultiTableTransaction::with_isolation_level(
        MultiTableConfig::default(),
        TransactionIsolationLevel::RepeatableRead,
    );
    println!("RepeatableRead transaction: {}", tx_repeatable_read.transaction_id);
    
    // Example 3: Serializable (highest consistency, lower performance)
    let tx_serializable = MultiTableTransaction::with_isolation_and_priority(
        MultiTableConfig::default(),
        TransactionIsolationLevel::Serializable,
        10, // High priority for critical operations
    );
    println!("Serializable transaction: {} (priority: {})", 
            tx_serializable.transaction_id, tx_serializable.priority);
    
    println!("Isolation levels demonstrate different consistency vs performance trade-offs:");
    println!("- ReadCommitted: Best performance, reads see committed data");
    println!("- RepeatableRead: Consistent snapshot throughout transaction");
    println!("- Serializable: Full serializability with conflict detection");
    
    Ok(())
}

/// Example 4: Error handling and validation
/// 
/// Demonstrates comprehensive error handling and validation scenarios.
pub async fn error_handling_example() -> TxnLogResult<()> {
    println!("\n=== Error Handling and Validation Example ===");
    
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("example.db");
    let config = DatabaseConfig::new(format!("sqlite:{}", db_path.to_str().unwrap()));
    let connection = config.connect().await?;
    let writer = MultiTableWriter::new(connection, None, MultiTableConfig::default());
    
    // Example 1: Empty transaction validation
    let mut empty_tx = writer.begin_transaction();
    match empty_tx.validate() {
        Ok(_) => println!("Unexpected: empty transaction validated"),
        Err(e) => println!("Expected error for empty transaction: {}", e),
    }
    
    // Example 2: Table actions validation
    let mut empty_table_actions = TableActions::new("empty_table".to_string(), 0);
    match empty_table_actions.validate() {
        Ok(_) => println!("Unexpected: empty table actions validated"),
        Err(e) => println!("Expected error for empty table actions: {}", e),
    }
    
    // Example 3: Valid transaction with proper actions
    let mut valid_tx = writer.begin_transaction();
    let add_file = AddFile {
        path: "valid/data.parquet".to_string(),
        size: 1000,
        modification_time: Utc::now().timestamp(),
        data_change: true,
        partition_values: Default::default(),
        stats: None,
        tags: None,
    };
    
    valid_tx.add_file("valid_table".to_string(), 0, add_file)?;
    
    match valid_tx.validate() {
        Ok(_) => println!("Valid transaction passed validation"),
        Err(e) => println!("Unexpected error for valid transaction: {}", e),
    }
    
    // Example 4: Transaction state transitions
    let mut state_tx = writer.begin_transaction();
    println!("Initial state: {:?}", state_tx.state);
    
    state_tx.mark_committing()?;
    println!("After mark_committing: {:?}", state_tx.state);
    
    state_tx.mark_committed();
    println!("After mark_committed: {:?}", state_tx.state);
    
    // Try to add actions after commit
    let table_actions = TableActions::new("after_commit".to_string(), 0);
    match state_tx.stage_actions(table_actions) {
        Ok(_) => println!("Unexpected: staged actions after commit"),
        Err(e) => println!("Expected error for staging after commit: {}", e),
    }
    
    Ok(())
}

/// Example 5: Configuration and performance tuning
/// 
/// Shows how to configure the multi-table transaction system for different use cases.
pub async fn configuration_example() -> TxnLogResult<()> {
    println!("\n=== Configuration and Performance Tuning Example ===");
    
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("example.db");
    let config = DatabaseConfig::new(format!("sqlite:{}", db_path.to_str().unwrap()));
    let connection = config.connect().await?;
    
    // Example 1: High-performance configuration (for bulk operations)
    let high_perf_config = MultiTableConfig {
        enable_consistency_validation: false, // Skip validation for speed
        enable_ordered_mirroring: false,      // Skip mirroring for speed
        enable_deadlock_detection: false,     // Skip deadlock detection
        max_tables_per_transaction: 50,       // Allow more tables
        max_actions_per_transaction: 50000,    // Allow more actions
        max_actions_per_table: 5000,           // Allow more actions per table
        max_transaction_age_seconds: 7200,     // 2 hours for large operations
        max_retry_attempts: 1,                // Fewer retries for speed
        retry_base_delay_ms: 50,              // Faster retries
        retry_max_delay_ms: 1000,             // Lower max delay
        ..Default::default()
    };
    
    let high_perf_writer = MultiTableWriter::new(connection.clone(), None, high_perf_config);
    println!("High-performance writer configured for bulk operations");
    
    // Example 2: Safety-first configuration (for critical data)
    let safety_config = MultiTableConfig {
        enable_consistency_validation: true,  // Enable all validations
        enable_ordered_mirroring: true,       // Enable mirroring
        enable_deadlock_detection: true,       // Enable deadlock detection
        max_tables_per_transaction: 10,        // Limit tables for safety
        max_actions_per_transaction: 1000,     // Limit actions for safety
        max_actions_per_table: 100,            // Limit per-table actions
        max_transaction_age_seconds: 300,     // 5 minutes timeout
        max_retry_attempts: 5,                 // More retries for reliability
        retry_base_delay_ms: 200,              // Conservative retry delay
        retry_max_delay_ms: 10000,             // Higher max delay
        ..Default::default()
    };
    
    let safety_writer = MultiTableWriter::new(connection, None, safety_config);
    println!("Safety-first writer configured for critical operations");
    
    // Example 3: Balanced configuration (general purpose)
    let balanced_config = MultiTableConfig::default();
    let balanced_writer = MultiTableWriter::new(connection, None, balanced_config);
    println!("Balanced writer configured for general use");
    
    println!("\nConfiguration trade-offs:");
    println!("- High-performance: Maximum throughput, minimal safety checks");
    println!("- Safety-first: Maximum reliability, comprehensive validation");
    println!("- Balanced: Good compromise between performance and safety");
    
    Ok(())
}

/// Example 6: Concurrent transactions and deadlock handling
/// 
/// Demonstrates how the system handles concurrent operations and potential deadlocks.
pub async fn concurrent_transactions_example() -> TxnLogResult<()> {
    println!("\n=== Concurrent Transactions Example ===");
    
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("example.db");
    let config = DatabaseConfig::new(format!("sqlite:{}", db_path.to_str().unwrap()));
    let connection = config.connect().await?;
    
    // Enable deadlock detection
    let deadlock_config = MultiTableConfig {
        enable_deadlock_detection: true,
        ..Default::default()
    };
    
    let writer = MultiTableWriter::new(connection, None, deadlock_config);
    
    // Simulate concurrent transactions
    let mut tx1 = writer.begin_transaction();
    let mut tx2 = writer.begin_transaction();
    
    // Transaction 1: High priority, accesses tables A -> B
    let tx1 = MultiTableTransaction::with_isolation_and_priority(
        MultiTableConfig::default(),
        TransactionIsolationLevel::Serializable,
        10, // High priority
    );
    
    // Transaction 2: Lower priority, accesses tables B -> A (potential deadlock)
    let tx2 = MultiTableTransaction::with_isolation_and_priority(
        MultiTableConfig::default(),
        TransactionIsolationLevel::Serializable,
        5, // Lower priority
    );
    
    println!("Transaction 1 (high priority): {}", tx1.transaction_id);
    println!("Transaction 2 (lower priority): {}", tx2.transaction_id);
    
    // In a real scenario, the deadlock detection would:
    // 1. Detect the circular dependency (tx1 waits for tx2, tx2 waits for tx1)
    // 2. Resolve by priority (tx1 wins, tx2 is rolled back)
    // 3. Log the deadlock for monitoring
    
    println!("Deadlock detection would resolve conflicts by priority");
    println!("Higher priority transactions win, lower priority ones are rolled back");
    
    Ok(())
}

/// Example 7: Mirroring and replication
/// 
/// Shows how to use the ordered mirroring feature for data replication.
pub async fn mirroring_example() -> TxnLogResult<()> {
    println!("\n=== Mirroring and Replication Example ===");
    
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("example.db");
    let config = DatabaseConfig::new(format!("sqlite:{}", db_path.to_str().unwrap()));
    let connection = config.connect().await?;
    
    // Enable ordered mirroring
    let mirror_config = MultiTableConfig {
        enable_ordered_mirroring: true,
        ..Default::default()
    };
    
    let writer = MultiTableWriter::new(connection, None, mirror_config);
    
    // Create a transaction that will trigger mirroring
    let mut tx = writer.begin_transaction();
    
    // Add files to multiple tables
    tx.add_file("source_table1".to_string(), 0, AddFile {
        path: "data/table1/batch1.parquet".to_string(),
        size: 5000,
        modification_time: Utc::now().timestamp(),
        data_change: true,
        partition_values: Default::default(),
        stats: None,
        tags: None,
    })?;
    
    tx.add_file("source_table2".to_string(), 0, AddFile {
        path: "data/table2/batch1.parquet".to_string(),
        size: 3000,
        modification_time: Utc::now().timestamp(),
        data_change: true,
        partition_values: Default::default(),
        stats: None,
        tags: None,
    })?;
    
    println!("Transaction prepared with mirroring enabled");
    println!("When committed, changes will be mirrored to configured storage systems");
    
    // In a real implementation, this would:
    // 1. Commit the transaction to the primary storage
    // 2. Trigger ordered mirroring to secondary storage systems
    // 3. Monitor mirroring progress and handle failures
    // 4. Provide detailed mirroring status and error reporting
    
    println!("Mirroring features:");
    println!("- Ordered: Ensures consistent replication order across tables");
    println!("- Retry logic: Automatic retry for transient failures");
    println!("- Monitoring: Detailed status tracking and alerting");
    println!("- Failure handling: Partial failure recovery and escalation");
    
    Ok(())
}

/// Run all examples
pub async fn run_all_examples() -> TxnLogResult<()> {
    println!("Running Multi-Table Transaction Examples\n");
    
    basic_multi_table_transaction().await?;
    table_actions_builder_example().await?;
    isolation_levels_example().await?;
    error_handling_example().await?;
    configuration_example().await?;
    concurrent_transactions_example().await?;
    mirroring_example().await?;
    
    println!("\n=== All Examples Completed Successfully ===");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_basic_example() {
        basic_multi_table_transaction().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_builder_example() {
        table_actions_builder_example().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_error_handling_example() {
        error_handling_example().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_configuration_example() {
        configuration_example().await.unwrap();
    }
}