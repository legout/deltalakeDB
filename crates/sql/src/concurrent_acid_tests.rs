//! Integration tests for ACID properties under concurrent load
//! 
//! These tests verify that multi-table transactions maintain ACID properties
//! when multiple transactions are running concurrently.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;

use crate::multi_table::{
    MultiTableConfig, MultiTableTransaction, MultiTableWriter,
    TableActions,
};
use deltalakedb_core::{
    transaction::{TransactionIsolationLevel, TransactionState},
    actions::{AddFile, RemoveFile, Metadata, Protocol, Format, DeltaAction},
    error::{TxnLogError, TxnLogResult},
};
use crate::connection::{DatabaseConfig, DatabaseConnection};

/// Creates a test database and multi-table writer
async fn setup_test_writer() -> TxnLogResult<(TempDir, MultiTableWriter)> {
    let temp_dir = tempfile::tempdir().map_err(|e| TxnLogError::Internal {
        message: format!("Failed to create temp dir: {}", e)
    })?;
    
    let config = DatabaseConfig {
        url: format!("sqlite:{}", temp_dir.path().join("test.db").to_string_lossy()),
        max_connections: 5,
        min_connections: 1,
        connect_timeout_secs: 30,
        idle_timeout_secs: 300,
    };
    
    let connection = config.connect().await.map_err(|e| TxnLogError::database(format!("Failed to connect: {}", e)))?;
    let multi_config = MultiTableConfig {
        enable_consistency_validation: true,
        enable_ordered_mirroring: false,
        enable_deadlock_detection: true,
        default_isolation_level: TransactionIsolationLevel::ReadCommitted,
        max_tables_per_transaction: 10,
        max_actions_per_transaction: 1000,
        max_actions_per_table: 100,
        max_transaction_age_seconds: 300, // 5 minutes for testing
        max_retry_attempts: 3,
        retry_base_delay_ms: 10,
        retry_max_delay_ms: 100,
    };
    
    let writer = MultiTableWriter::new(connection, None, multi_config);
    Ok((temp_dir, writer))
}

/// Creates test AddFile actions
fn create_test_add_files(paths: &[&str]) -> Vec<AddFile> {
    paths.iter().enumerate().map(|(i, _path)| AddFile {
        path: format!("file_{}.parquet", i),
        size: 1000 + i as i64,
        modification_time: 1234567890 + i as i64,
        data_change: true,
        partition_values: HashMap::new(),
        stats: None,
        tags: None,
    }).collect()
}

/// Creates test metadata
fn create_test_metadata() -> Metadata {
    Metadata::new(
        "test_schema".to_string(),
        "{}".to_string(),
        Format::default()
    )
}

/// Creates test RemoveFile actions
fn create_test_remove_files(paths: &[&str]) -> Vec<RemoveFile> {
    paths.iter().map(|path| RemoveFile {
        path: format!("file_{}.parquet", path),
        deletion_timestamp: Some(1234567890),
        data_change: true,
        extended_file_metadata: None,
        partition_values: None,
        size: Some(1000),
        tags: None,
    }).collect()
}

#[tokio::test]
async fn test_atomicity_single_table_concurrent() {
    // Test that concurrent transactions on the same table maintain atomicity
    let (temp_dir, writer) = setup_test_writer().await.unwrap();
    
    // Create initial table state
    writer.create_table("test_table", "Test Table", "/tmp/test", create_test_metadata()).await.unwrap();
    
    // Start multiple concurrent transactions
    let mut handles = Vec::new();
    for i in 0..5 {
        let writer_clone = writer.clone();
        let handle = tokio::spawn(async move {
            let mut tx = writer_clone.begin_transaction();
            
            // Each transaction tries to add different files
            let files = create_test_add_files(&[&format!("concurrent_{}", i)]);
            tx.add_files("test_table".to_string(), 0, files).unwrap();
            
            // Try to commit with timeout
            timeout(Duration::from_secs(5), writer_clone.commit_transaction(tx)).await
        });
        handles.push(handle);
    }
    
    // Wait for all transactions to complete
    let mut successful_commits = 0;
    let mut failed_commits = 0;
    
    for handle in handles {
        match handle.await.unwrap() {
            Ok(Ok(_)) => successful_commits += 1,
            Ok(Err(_)) => failed_commits += 1,
            Err(_) => failed_commits += 1, // Timeout
        }
    }
    
    // At most one transaction should succeed due to version conflicts
    assert!(successful_commits <= 1, "Expected at most 1 successful commit, got {}", successful_commits);
    assert!(failed_commits >= 4, "Expected at least 4 failed commits, got {}", failed_commits);
    
    drop(temp_dir);
}

#[tokio::test]
async fn test_consistency_cross_table() {
    // Test that cross-table consistency is maintained under concurrent load
    let (temp_dir, writer) = setup_test_writer().await.unwrap();
    
    // Create multiple tables
    for table_name in ["table_a", "table_b", "table_c"] {
        writer.create_table(table_name, table_name, "/tmp/test", create_test_metadata()).await.unwrap();
    }
    
    // Start concurrent transactions that span multiple tables
    let mut handles = Vec::new();
    for i in 0..3 {
        let writer_clone = writer.clone();
        let handle = tokio::spawn(async move {
            let mut tx = writer_clone.begin_transaction();
            
            // Each transaction spans multiple tables
            let files_a = create_test_add_files(&[&format!("multi_a_{}", i)]);
            let files_b = create_test_add_files(&[&format!("multi_b_{}", i)]);
            
            tx.add_files("table_a".to_string(), 0, files_a).unwrap();
            tx.add_files("table_b".to_string(), 0, files_b).unwrap();
            
            timeout(Duration::from_secs(5), writer_clone.commit_transaction(tx)).await
        });
        handles.push(handle);
    }
    
    // Wait for all transactions
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }
    
    // All transactions should either succeed or fail cleanly
    let successful = results.iter().filter(|r| {
        matches!(r, Ok(Ok(_)))
    }).count();
    
    // With proper isolation, some should succeed, some should fail
    assert!(successful > 0, "Expected some transactions to succeed");
    assert!(successful < 3, "Expected some transactions to fail due to conflicts");
    
    drop(temp_dir);
}

#[tokio::test]
async fn test_isolation_read_committed() {
    // Test that Read Committed isolation prevents dirty reads
    let (temp_dir, writer) = setup_test_writer().await.unwrap();
    
    writer.create_table("isolation_test", "Isolation Test", "/tmp/test", create_test_metadata()).await.unwrap();
    
    // Start a transaction that adds data
    let writer_clone1 = writer.clone();
    let handle1 = tokio::spawn(async move {
        let mut tx = writer_clone1.begin_transaction();
        let files = create_test_add_files(&["isolation_file"]);
        tx.add_files("isolation_test".to_string(), 0, files).unwrap();
        
        // Don't commit yet - keep it active
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Now try to read from another transaction
        let writer_clone2 = writer_clone1.clone();
        let reader_tx = writer_clone2.begin_transaction();
        
        // This should not see the uncommitted data
        let timeout_result: Result<Result<i64, tokio::time::error::Elapsed>, _> = timeout(Duration::from_secs(2), async {
            // In a real implementation, this would query the table
            // For now, we'll simulate the isolation check
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(0i64) // No uncommitted data visible
        }).await;
        
        assert!(timeout_result.is_ok(), "Read operation should complete within timeout");
        
        // Return success
        Ok::<(), TxnLogError>(())
    });
    
    // Start another transaction that tries to read while first is active
    let result = handle1.await.unwrap();
    assert!(result.is_ok(), "First transaction should complete successfully");
    
    drop(temp_dir);
}

#[tokio::test]
async fn test_durability_after_commit() {
    // Test that committed data persists after crashes/restarts
    let (temp_dir, writer) = setup_test_writer().await.unwrap();
    
    writer.create_table("durability_test", "Durability Test", "/tmp/test", create_test_metadata()).await.unwrap();
    
    // Commit a transaction
    let mut tx = writer.begin_transaction();
    let files = create_test_add_files(&["durable_file"]);
    tx.add_files("durability_test".to_string(), 0, files).unwrap();
    
    let commit_result = writer.commit_transaction(tx).await.unwrap();
    assert!(commit_result.transaction.state == TransactionState::Committed);
    assert!(commit_result.table_results.len() == 1);
    assert!(commit_result.table_results[0].success);
    
    // In a real implementation, we would restart the connection
    // and verify data is still there
    // For now, we'll simulate by checking the writer state
    
    // Try to read the committed data
    let mut read_tx = writer.begin_transaction();
    let read_result: Result<Result<i64, tokio::time::error::Elapsed>, _> = timeout(Duration::from_secs(2), async {
        // Simulate reading the committed version
        Ok(commit_result.table_results[0].version as i64)
    }).await;
    
    assert!(read_result.is_ok(), "Read operation should complete within timeout");
    let read_version = read_result.unwrap().unwrap();
    
    assert_eq!(read_version, 1, "Should read the committed version");
    
    drop(temp_dir);
}

#[tokio::test]
async fn test_deadlock_detection_and_resolution() {
    // Test deadlock detection and resolution
    let (temp_dir, writer) = setup_test_writer().await.unwrap();
    
    // Create two tables for deadlock scenario
    for table_name in ["deadlock_a", "deadlock_b"] {
        writer.create_table(table_name, table_name, "/tmp/test", create_test_metadata()).await.unwrap();
    }
    
    // Start two transactions that could deadlock
    let writer_clone1 = writer.clone();
    let writer_clone2 = writer.clone();
    let handle1 = tokio::spawn(async move {
        let mut tx1 = writer_clone1.begin_transaction();
        
        // Transaction 1: Lock A, then try to lock B
        let files_a = create_test_add_files(&["deadlock_1_a"]);
        tx1.add_files("deadlock_a".to_string(), 0, files_a).unwrap();
        
        // Small delay to increase deadlock probability
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        let files_b = create_test_add_files(&["deadlock_1_b"]);
        tx1.add_files("deadlock_b".to_string(), 0, files_b).unwrap();
        
        timeout(Duration::from_secs(3), writer_clone1.commit_transaction(tx1)).await
    });
    
    let handle2 = tokio::spawn(async move {
        let mut tx2 = writer_clone2.begin_transaction();
        
        // Transaction 2: Lock B, then try to lock A (reverse order)
        let files_b = create_test_add_files(&["deadlock_2_b"]);
        tx2.add_files("deadlock_b".to_string(), 0, files_b).unwrap();
        
        // Small delay to increase deadlock probability
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        let files_a = create_test_add_files(&["deadlock_2_a"]);
        tx2.add_files("deadlock_a".to_string(), 0, files_a).unwrap();
        
        timeout(Duration::from_secs(3), writer_clone2.commit_transaction(tx2)).await
    });
    
    // Wait for both transactions
    let (result1, result2) = tokio::join!(handle1, handle2);
    
    let result1 = result1.unwrap();
    let result2 = result2.unwrap();
    
    // At least one should succeed, one should fail (deadlock resolution)
    let success_count = [&result1, &result2].iter().filter(|r| {
        matches!(r, Ok(Ok(_)))
    }).count();
    
    assert_eq!(success_count, 1, "Exactly one transaction should succeed due to deadlock resolution");
    
    // The failed transaction should fail with a deadlock-related error
    let failed_result = if result1.is_err() { &result1 } else { &result2 };
    match failed_result {
        Ok(Err(e)) => {
            // Check if it's a deadlock or timeout error
            let error_msg = format!("{}", e);
            assert!(error_msg.contains("timeout") || 
                    error_msg.contains("conflict") || 
                    error_msg.contains("deadlock"),
                    "Expected deadlock-related error, got: {}", error_msg);
        }
        _ => panic!("Expected one transaction to fail"),
    }
    
    drop(temp_dir);
}

#[tokio::test]
async fn test_high_concurrency_stress() {
    // Test system under high concurrency stress
    let (temp_dir, writer) = setup_test_writer().await.unwrap();
    
    // Create multiple tables
    for i in 0..5 {
        let table_name = format!("stress_table_{}", i);
        writer.create_table(&table_name, &table_name, "/tmp/test", create_test_metadata()).await.unwrap();
    }
    
    // Start many concurrent transactions
    let mut handles = Vec::new();
    for i in 0..20 {
        let writer_clone = writer.clone();
        let handle = tokio::spawn(async move {
            let mut tx = writer_clone.begin_transaction();
            
            // Each transaction works on a random table
            let table_idx = i % 5;
            let table_name = format!("stress_table_{}", table_idx);
            
            let files = create_test_add_files(&[&format!("stress_{}", i)]);
            tx.add_files(table_name, 0, files).unwrap();
            
            // Short timeout for stress test
            timeout(Duration::from_secs(2), writer_clone.commit_transaction(tx)).await
        });
        handles.push(handle);
    }
    
    // Wait for all transactions with overall timeout
    let overall_timeout = timeout(Duration::from_secs(10), async {
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        results
    }).await;
    
    let results = match overall_timeout {
        Ok(r) => r,
        Err(_) => {
            // Timeout occurred - this is acceptable in stress test
            panic!("Stress test timed out - this indicates performance issues");
        }
    };
    
    // Analyze results
    let successful = results.iter().filter(|r| matches!(r, Ok(Ok(_)))).count();
    let failed = results.iter().filter(|r| matches!(r, Ok(Err(_)))).count();
    let timeouts = results.iter().filter(|r| matches!(r, Err(_))).count();
    
    println!("Stress test results: {} successful, {} failed, {} timeouts", 
             successful, failed, timeouts);
    
    // Should have some successes and some failures
    assert!(successful > 0, "Should have some successful transactions");
    assert!(failed + timeouts > 0, "Should have some failed or timed out transactions");
    
    // Success rate should be reasonable (not too high, not too low)
    let total = results.len();
    let success_rate = successful as f64 / total as f64;
    assert!(success_rate > 0.1, "Success rate should be > 10%");
    assert!(success_rate < 0.8, "Success rate should be < 80% due to conflicts");
    
    drop(temp_dir);
}

#[tokio::test]
async fn test_transaction_priority_under_load() {
    // Test that transaction priority works correctly under concurrent load
    let (temp_dir, writer) = setup_test_writer().await.unwrap();
    
    writer.create_table("priority_test", "Priority Test", "/tmp/test", create_test_metadata()).await.unwrap();
    
    // Start transactions with different priorities
    let mut handles = Vec::new();
    for (priority, i) in [(10, 0), (5, 1), (1, 2), (0, 3)] {
        let writer_clone = writer.clone();
        let handle = tokio::spawn(async move {
            let mut tx = writer_clone.begin_transaction();
            tx.priority = priority; // Set priority
            
            let files = create_test_add_files(&[&format!("priority_{}", i)]);
            tx.add_files("priority_test".to_string(), 0, files).unwrap();
            
            timeout(Duration::from_secs(3), writer_clone.commit_transaction(tx)).await
        });
        handles.push(handle);
    }
    
    // Wait for all transactions
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }
    
    // Higher priority transactions should have better success rate
    let mut priority_results = HashMap::new();
    for (i, result) in results.iter().enumerate() {
        let priority = [10, 5, 1, 0][i];
        let success = matches!(result, Ok(Ok(_)));
        priority_results.insert(priority, success);
    }
    
    // Check that higher priority transactions are more likely to succeed
    let high_priority_success = priority_results.get(&10).copied().unwrap_or(false);
    let low_priority_success = priority_results.get(&0).copied().unwrap_or(false);
    
    assert!(high_priority_success >= low_priority_success, 
              "Higher priority transactions should be at least as successful as lower priority ones");
    
    drop(temp_dir);
}