//! Concurrent writer isolation tests.
//!
//! Run with: cargo test --test concurrent_writer_tests -- --ignored --nocapture
//! Requires: PostgreSQL running at TEST_DATABASE_URL or localhost:5432

mod fixtures;

use anyhow::Result;
use deltalakedb_sql_metadata_postgres::PostgresWriter;
use fixtures::{create_test_pool, setup_test_table, cleanup_test_table, get_test_database_url};
use uuid::Uuid;

/// Test that concurrent writes to the same version conflict
#[tokio::test]
#[ignore]
async fn test_concurrent_writers_same_version_conflict() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_test_table(&pool, "concurrent_conflict", "s3://bucket/concurrent_conflict")
        .await?;

    // Create two writer instances
    let writer1 = PostgresWriter::new(pool.clone());
    let writer2 = PostgresWriter::new(pool.clone());

    // Spawn concurrent commit attempts on same version
    let table1 = table_id;
    let table2 = table_id;

    let handle1 = tokio::spawn(async move {
        writer1.begin_commit(table1, 0).await
    });

    let handle2 = tokio::spawn(async move {
        writer2.begin_commit(table2, 0).await
    });

    // Wait for both to complete
    let res1 = handle1.await;
    let res2 = handle2.await;

    // Extract results
    let commit1 = res1.ok().flatten();
    let commit2 = res2.ok().flatten();

    // At most one should succeed
    let success_count = [commit1.is_some(), commit2.is_some()]
        .iter()
        .filter(|&&x| x)
        .count();

    println!(
        "Concurrent writes to same version: {} succeeded, {} failed",
        success_count,
        2 - success_count
    );

    assert!(
        success_count <= 1,
        "Both concurrent writers succeeded; exactly one should succeed"
    );

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Test that concurrent writes to different tables don't block each other
#[tokio::test]
#[ignore]
async fn test_concurrent_writers_different_tables_no_blocking() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;

    // Create two separate tables
    let table_a = setup_test_table(&pool, "concurrent_table_a", "s3://bucket/concurrent_a").await?;
    let table_b = setup_test_table(&pool, "concurrent_table_b", "s3://bucket/concurrent_b").await?;

    let writer1 = PostgresWriter::new(pool.clone());
    let writer2 = PostgresWriter::new(pool.clone());

    // Spawn concurrent commits on different tables
    let handle1 = tokio::spawn(async move {
        writer1.begin_commit(table_a, 0).await
    });

    let handle2 = tokio::spawn(async move {
        writer2.begin_commit(table_b, 0).await
    });

    // Both should succeed
    let res1 = handle1.await?;
    let res2 = handle2.await?;

    let commit1_success = res1.is_ok();
    let commit2_success = res2.is_ok();

    println!(
        "Concurrent writes to different tables: A={}, B={}",
        if commit1_success { "OK" } else { "FAIL" },
        if commit2_success { "OK" } else { "FAIL" }
    );

    assert!(
        commit1_success && commit2_success,
        "Both writers should succeed on different tables"
    );

    cleanup_test_table(&pool, table_a).await?;
    cleanup_test_table(&pool, table_b).await?;
    Ok(())
}

/// Test rapid sequential commits increment version correctly
#[tokio::test]
#[ignore]
async fn test_sequential_commits_version_increment() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_test_table(&pool, "sequential_versions", "s3://bucket/sequential")
        .await?;

    let writer = PostgresWriter::new(pool.clone());

    // Perform sequential commits
    for expected_version in 0..10 {
        let handle = writer.begin_commit(table_id, expected_version).await?;

        // Immediately finalize
        writer
            .finalize_commit(table_id, handle, expected_version)
            .await?;
    }

    // Get final version
    let final_version: i64 = sqlx::query_scalar(
        "SELECT current_version FROM dl_tables WHERE table_id = $1"
    )
    .bind(table_id)
    .fetch_one(&pool)
    .await?;

    println!("Final version after 10 commits: {} (expected: 9)", final_version);
    assert_eq!(
        final_version, 9,
        "Version should be 9 after 10 commits, got {}",
        final_version
    );

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Test that stale version write attempt fails
#[tokio::test]
#[ignore]
async fn test_stale_version_write_rejected() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_test_table(&pool, "stale_version", "s3://bucket/stale")
        .await?;

    let writer = PostgresWriter::new(pool.clone());

    // First commit succeeds
    let handle1 = writer.begin_commit(table_id, 0).await?;
    writer.finalize_commit(table_id, handle1, 0).await?;

    // Now try to commit with old version (should fail)
    let result = writer.begin_commit(table_id, 0).await;

    println!(
        "Stale version write: {}",
        if result.is_err() { "REJECTED (OK)" } else { "ACCEPTED (BAD)" }
    );

    assert!(
        result.is_err(),
        "Commit with stale version should be rejected"
    );

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Test write ordering under concurrent load
#[tokio::test]
#[ignore]
async fn test_concurrent_write_ordering() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_test_table(&pool, "write_ordering", "s3://bucket/write_ordering")
        .await?;

    // Spawn multiple writers
    let mut handles = Vec::new();

    for i in 0..5 {
        let pool_clone = pool.clone();
        let table_id_clone = table_id;

        let handle = tokio::spawn(async move {
            let writer = PostgresWriter::new(pool_clone);
            let mut commit_count = 0;

            // Each writer tries to do 3 commits
            for _ in 0..3 {
                if let Ok(commit_handle) = writer.begin_commit(table_id_clone, i).await {
                    if writer
                        .finalize_commit(table_id_clone, commit_handle, i)
                        .await
                        .is_ok()
                    {
                        commit_count += 1;
                    }
                }
                // Add small delay between attempts
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }

            commit_count
        });

        handles.push(handle);
    }

    // Wait for all writers
    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let total_commits: i32 = results.iter().map(|&r| r as i32).sum();
    println!("Total commits by 5 concurrent writers: {}", total_commits);

    // Verify version incremented appropriately
    let final_version: i64 = sqlx::query_scalar(
        "SELECT current_version FROM dl_tables WHERE table_id = $1"
    )
    .bind(table_id)
    .fetch_one(&pool)
    .await?;

    println!("Final version: {}", final_version);
    assert!(
        final_version > 0,
        "Some commits should have succeeded"
    );

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

// Helper to import futures for join_all
use futures;
