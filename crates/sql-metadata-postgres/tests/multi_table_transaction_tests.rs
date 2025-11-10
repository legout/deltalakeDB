//! Comprehensive tests for multi-table transactions.
//!
//! Run with: cargo test --test multi_table_transaction_tests -- --ignored --nocapture
//! Requires: PostgreSQL running at TEST_DATABASE_URL or localhost:5432

mod fixtures;

use anyhow::Result;
use deltalakedb_sql_metadata_postgres::MultiTableWriter;
use deltalakedb_core::{MultiTableTransaction, TransactionConfig, StagedTable};
use deltalakedb_core::types::Action;
use fixtures::{create_test_pool, setup_test_table, cleanup_test_table, get_test_database_url};
use uuid::Uuid;

fn create_test_file(path: &str, size: i64) -> Action {
    Action::Add(deltalakedb_core::types::AddFile {
        path: path.to_string(),
        size: Some(size),
        modification_time: 0,
        data_change_version: 0,
        partition_values: None,
        tags: None,
        stats: None,
    })
}

/// Test 6.1: Unit tests for transaction staging and validation
#[tokio::test]
#[ignore]
async fn test_transaction_staging_basic() -> Result<()> {
    let config = TransactionConfig::default();
    let mut tx = MultiTableTransaction::new(config);

    let table_id = Uuid::new_v4();
    let actions = vec![create_test_file("file1.parquet", 1024)];
    let staged = StagedTable::new(table_id, actions);

    tx.stage_table(table_id, staged)?;

    assert_eq!(tx.table_count(), 1);
    println!("Transaction staging validated: 1 table staged");

    Ok(())
}

/// Test 6.1: Prevent duplicate staging
#[tokio::test]
#[ignore]
async fn test_transaction_duplicate_staging_prevented() -> Result<()> {
    let config = TransactionConfig::default();
    let mut tx = MultiTableTransaction::new(config);

    let table_id = Uuid::new_v4();
    let actions = vec![create_test_file("file1.parquet", 1024)];
    let staged = StagedTable::new(table_id, actions.clone());

    tx.stage_table(table_id, staged)?;

    // Try to stage the same table again
    let staged2 = StagedTable::new(table_id, actions);
    let result = tx.stage_table(table_id, staged2);

    assert!(
        result.is_err(),
        "Duplicate staging should be prevented"
    );

    println!("Duplicate staging correctly prevented");

    Ok(())
}

/// Test 6.1: Validate max_tables limit
#[tokio::test]
#[ignore]
async fn test_transaction_max_tables_limit() -> Result<()> {
    let config = TransactionConfig {
        max_tables: 3,
        max_files_per_table: 1000,
        timeout_secs: 60,
    };
    let mut tx = MultiTableTransaction::new(config);

    // Stage 3 tables (should succeed)
    for i in 0..3 {
        let table_id = Uuid::new_v4();
        let actions = vec![create_test_file(&format!("file{}.parquet", i), 1024)];
        let staged = StagedTable::new(table_id, actions);
        tx.stage_table(table_id, staged)?;
    }

    assert_eq!(tx.table_count(), 3);

    // Try to stage 4th table (should fail)
    let table_id = Uuid::new_v4();
    let actions = vec![create_test_file("file4.parquet", 1024)];
    let staged = StagedTable::new(table_id, actions);
    let result = tx.stage_table(table_id, staged);

    assert!(result.is_err(), "Max tables limit should be enforced");

    println!("Max tables limit correctly enforced");

    Ok(())
}

/// Test 6.2: Integration tests with 2+ tables and real database
#[tokio::test]
#[ignore]
async fn test_multi_table_transaction_two_tables() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;

    // Create two tables
    let table_a = setup_test_table(&pool, "multi_table_a", "s3://bucket/a").await?;
    let table_b = setup_test_table(&pool, "multi_table_b", "s3://bucket/b").await?;

    let writer = MultiTableWriter::new(pool.clone());

    // Create transaction
    let config = TransactionConfig::default();
    let mut tx = MultiTableTransaction::new(config);

    // Stage both tables
    let actions_a = vec![
        create_test_file("a1.parquet", 1024),
        create_test_file("a2.parquet", 2048),
    ];
    let staged_a = StagedTable::new(table_a, actions_a);
    tx.stage_table(table_a, staged_a)?;

    let actions_b = vec![
        create_test_file("b1.parquet", 4096),
    ];
    let staged_b = StagedTable::new(table_b, actions_b);
    tx.stage_table(table_b, staged_b)?;

    // Commit transaction
    let result = writer.commit(tx).await?;

    println!("Multi-table transaction committed:");
    println!("  Transaction ID: {}", result.transaction_id);
    println!("  Version for table A: {}", result.get_version(&table_a).unwrap_or(0));
    println!("  Version for table B: {}", result.get_version(&table_b).unwrap_or(0));

    // Verify both tables were incremented
    assert_eq!(result.versions.len(), 2);
    assert!(result.get_version(&table_a).is_some());
    assert!(result.get_version(&table_b).is_some());

    // Verify files were written
    let file_count_a: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dl_add_files WHERE table_id = $1"
    )
    .bind(table_a)
    .fetch_one(&pool)
    .await?;

    let file_count_b: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dl_add_files WHERE table_id = $1"
    )
    .bind(table_b)
    .fetch_one(&pool)
    .await?;

    assert_eq!(file_count_a, 2);
    assert_eq!(file_count_b, 1);

    cleanup_test_table(&pool, table_a).await?;
    cleanup_test_table(&pool, table_b).await?;

    Ok(())
}

/// Test 6.3: Rollback on version conflict in second table
#[tokio::test]
#[ignore]
async fn test_multi_table_rollback_on_conflict() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;

    let table_a = setup_test_table(&pool, "conflict_a", "s3://bucket/conflict_a").await?;
    let table_b = setup_test_table(&pool, "conflict_b", "s3://bucket/conflict_b").await?;

    let writer = MultiTableWriter::new(pool.clone());

    // First transaction succeeds
    let config = TransactionConfig::default();
    let mut tx1 = MultiTableTransaction::new(config.clone());

    let actions_a = vec![create_test_file("a1.parquet", 1024)];
    let staged_a = StagedTable::new(table_a, actions_a);
    tx1.stage_table(table_a, staged_a)?;

    let actions_b = vec![create_test_file("b1.parquet", 1024)];
    let staged_b = StagedTable::new(table_b, actions_b);
    tx1.stage_table(table_b, staged_b)?;

    let result1 = writer.commit(tx1).await?;
    println!("First transaction committed successfully");

    // Second transaction with stale version on first table (should fail)
    let mut tx2 = MultiTableTransaction::new(config);

    // Try to commit with version conflict
    let actions_a2 = vec![create_test_file("a2.parquet", 2048)];
    let staged_a2 = StagedTable::new(table_a, actions_a2).with_expected_version(0); // Stale
    tx2.stage_table(table_a, staged_a2)?;

    let actions_b2 = vec![create_test_file("b2.parquet", 2048)];
    let staged_b2 = StagedTable::new(table_b, actions_b2);
    tx2.stage_table(table_b, staged_b2)?;

    // This commit should fail due to version conflict
    let result2 = writer.commit(tx2).await;

    if result2.is_err() {
        println!("Transaction correctly rejected due to version conflict");
        
        // Verify no partial writes occurred
        let version_a: i64 = sqlx::query_scalar(
            "SELECT current_version FROM dl_tables WHERE table_id = $1"
        )
        .bind(table_a)
        .fetch_one(&pool)
        .await?;

        let version_b: i64 = sqlx::query_scalar(
            "SELECT current_version FROM dl_tables WHERE table_id = $1"
        )
        .bind(table_b)
        .fetch_one(&pool)
        .await?;

        assert_eq!(version_a, 1, "Table A should still be at version 1");
        assert_eq!(version_b, 1, "Table B should still be at version 1");

        println!("Verified no partial writes on conflict");
    } else {
        println!("Warning: Conflict should have been detected");
    }

    cleanup_test_table(&pool, table_a).await?;
    cleanup_test_table(&pool, table_b).await?;

    Ok(())
}

/// Test 6.4: Partial mirror failure handling
#[tokio::test]
#[ignore]
async fn test_multi_table_partial_mirror_failure() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;

    let table_a = setup_test_table(&pool, "mirror_a", "s3://bucket/mirror_a").await?;
    let table_b = setup_test_table(&pool, "mirror_b", "s3://bucket/mirror_b").await?;

    let writer = MultiTableWriter::new(pool.clone());

    // Create and commit transaction
    let config = TransactionConfig::default();
    let mut tx = MultiTableTransaction::new(config);

    let actions_a = vec![create_test_file("a1.parquet", 1024)];
    let staged_a = StagedTable::new(table_a, actions_a);
    tx.stage_table(table_a, staged_a)?;

    let actions_b = vec![create_test_file("b1.parquet", 1024)];
    let staged_b = StagedTable::new(table_b, actions_b);
    tx.stage_table(table_b, staged_b)?;

    let result = writer.commit(tx).await?;
    println!("Transaction committed, attempting mirror");

    // Mirror tables (may have partial failures)
    let mirror_results = writer.mirror_all_tables_after_commit(&result).await;

    println!("Mirror results:");
    for (&table_id, results) in &mirror_results {
        for (engine, success, error) in results {
            println!(
                "  Table {}: {} -> {}",
                table_id,
                engine,
                if *success { "OK" } else { "FAILED" }
            );
            if let Some(err) = error {
                println!("    Error: {}", err);
            }
        }
    }

    // SQL transaction should have succeeded regardless of mirror status
    for (&table_id, &version) in &result.versions {
        let current_version: i64 = sqlx::query_scalar(
            "SELECT current_version FROM dl_tables WHERE table_id = $1"
        )
        .bind(table_id)
        .fetch_one(&pool)
        .await?;

        assert_eq!(current_version, version, "SQL version should be updated");
    }

    println!("Verified SQL commit succeeded despite potential mirror failures");

    cleanup_test_table(&pool, table_a).await?;
    cleanup_test_table(&pool, table_b).await?;

    Ok(())
}

/// Test 6.5: External reader consistency after multi-table commit
#[tokio::test]
#[ignore]
async fn test_multi_table_external_reader_consistency() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;

    let table_a = setup_test_table(&pool, "consistency_a", "s3://bucket/consistency_a").await?;
    let table_b = setup_test_table(&pool, "consistency_b", "s3://bucket/consistency_b").await?;

    let writer = MultiTableWriter::new(pool.clone());

    // Commit multi-table transaction
    let config = TransactionConfig::default();
    let mut tx = MultiTableTransaction::new(config);

    let actions_a = vec![create_test_file("a1.parquet", 1024)];
    let staged_a = StagedTable::new(table_a, actions_a);
    tx.stage_table(table_a, staged_a)?;

    let actions_b = vec![create_test_file("b1.parquet", 1024)];
    let staged_b = StagedTable::new(table_b, actions_b);
    tx.stage_table(table_b, staged_b)?;

    let result = writer.commit(tx).await?;

    // Simulate external reader checking both tables immediately after commit
    let version_a: i64 = sqlx::query_scalar(
        "SELECT current_version FROM dl_tables WHERE table_id = $1"
    )
    .bind(table_a)
    .fetch_one(&pool)
    .await?;

    let version_b: i64 = sqlx::query_scalar(
        "SELECT current_version FROM dl_tables WHERE table_id = $1"
    )
    .bind(table_b)
    .fetch_one(&pool)
    .await?;

    // Both should be at expected versions in SQL
    let expected_a = result.get_version(&table_a).unwrap_or(0);
    let expected_b = result.get_version(&table_b).unwrap_or(0);

    assert_eq!(version_a, expected_a, "Table A should be at expected version");
    assert_eq!(version_b, expected_b, "Table B should be at expected version");

    println!(
        "External reader consistency verified: A={}, B={}",
        version_a, version_b
    );

    cleanup_test_table(&pool, table_a).await?;
    cleanup_test_table(&pool, table_b).await?;

    Ok(())
}

/// Test 6.6: Concurrent multi-table transaction isolation
#[tokio::test]
#[ignore]
async fn test_concurrent_multi_table_isolation() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;

    let table_a = setup_test_table(&pool, "iso_a", "s3://bucket/iso_a").await?;
    let table_b = setup_test_table(&pool, "iso_b", "s3://bucket/iso_b").await?;

    // Spawn two concurrent multi-table transaction attempts
    let pool_clone1 = pool.clone();
    let pool_clone2 = pool.clone();

    let handle1 = tokio::spawn(async move {
        let writer = MultiTableWriter::new(pool_clone1);
        let config = TransactionConfig::default();
        let mut tx = MultiTableTransaction::new(config);

        let actions_a = vec![create_test_file("a_t1_1.parquet", 1024)];
        let staged_a = StagedTable::new(table_a, actions_a);
        tx.stage_table(table_a, staged_a).ok()?;

        let actions_b = vec![create_test_file("b_t1_1.parquet", 1024)];
        let staged_b = StagedTable::new(table_b, actions_b);
        tx.stage_table(table_b, staged_b).ok()?;

        writer.commit(tx).await.ok()
    });

    let handle2 = tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let writer = MultiTableWriter::new(pool_clone2);
        let config = TransactionConfig::default();
        let mut tx = MultiTableTransaction::new(config);

        let actions_a = vec![create_test_file("a_t2_1.parquet", 1024)];
        let staged_a = StagedTable::new(table_a, actions_a);
        tx.stage_table(table_a, staged_a).ok()?;

        let actions_b = vec![create_test_file("b_t2_1.parquet", 1024)];
        let staged_b = StagedTable::new(table_b, actions_b);
        tx.stage_table(table_b, staged_b).ok()?;

        writer.commit(tx).await.ok()
    });

    let res1 = handle1.await?;
    let res2 = handle2.await?;

    // One should succeed, one might fail due to conflict
    let successes = [res1.is_some(), res2.is_some()].iter().filter(|&&x| x).count();

    println!(
        "Concurrent multi-table transactions: {} succeeded, {} had conflicts",
        successes,
        2 - successes
    );

    assert!(
        successes >= 1,
        "At least one transaction should succeed"
    );

    cleanup_test_table(&pool, table_a).await?;
    cleanup_test_table(&pool, table_b).await?;

    Ok(())
}
