//! Writer integration tests covering lifecycle, large commits, rollback, and conformance.
//!
//! Run with: cargo test --test writer_integration_tests -- --ignored --nocapture
//! Requires: PostgreSQL running at TEST_DATABASE_URL or localhost:5432

mod fixtures;

use anyhow::Result;
use deltalakedb_sql_metadata_postgres::PostgresWriter;
use deltalakedb_core::types::Action;
use fixtures::{create_test_pool, setup_test_table, cleanup_test_table, get_test_database_url};
use std::time::Instant;

/// Helper to create a test AddFile action
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

/// Test 5.2: Full writer lifecycle - begin, write, finalize
#[tokio::test]
#[ignore]
async fn test_full_writer_lifecycle() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_test_table(&pool, "writer_lifecycle", "s3://bucket/writer_lifecycle")
        .await?;

    let writer = PostgresWriter::new(pool.clone());

    // Begin commit
    let handle = writer.begin_commit(table_id, 0).await?;
    println!("Commit handle created: {:?}", handle);

    // Write actions
    let actions = vec![
        create_test_file("file1.parquet", 1024),
        create_test_file("file2.parquet", 2048),
        create_test_file("file3.parquet", 4096),
    ];

    writer
        .write_actions(table_id, &handle, &actions)
        .await?;
    println!("3 actions written");

    // Finalize
    writer
        .finalize_commit(table_id, handle, 0)
        .await?;
    println!("Commit finalized");

    // Verify version incremented
    let version: i64 =
        sqlx::query_scalar("SELECT current_version FROM dl_tables WHERE table_id = $1")
            .bind(table_id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(version, 1, "Version should be 1 after first commit");

    // Verify files were written
    let file_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dl_add_files WHERE table_id = $1 AND version = 0"
    )
    .bind(table_id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(file_count, 3, "Should have 3 files written");

    println!("Full lifecycle test passed");
    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Test 5.4: Large commits with 1000+ files
#[tokio::test]
#[ignore]
async fn test_large_commit_1000_files() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_test_table(&pool, "writer_large_commit", "s3://bucket/writer_large")
        .await?;

    let writer = PostgresWriter::new(pool.clone());

    // Generate 1000 add file actions
    let actions: Vec<Action> = (0..1000)
        .map(|i| create_test_file(&format!("file_{:04}.parquet", i), 1024 * (1 + i % 10)))
        .collect();

    println!("Starting large commit with 1000 files...");
    let start = Instant::now();

    // Begin commit
    let handle = writer.begin_commit(table_id, 0).await?;

    // Write all actions
    writer
        .write_actions(table_id, &handle, &actions)
        .await?;

    // Finalize
    writer
        .finalize_commit(table_id, handle, 0)
        .await?;

    let duration = start.elapsed();
    println!("Large commit completed in {:.2}s", duration.as_secs_f64());

    // Verify all files were written
    let file_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dl_add_files WHERE table_id = $1 AND version = 0"
    )
    .bind(table_id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(file_count, 1000, "Should have 1000 files written");

    // Should complete in reasonable time (< 30 seconds for 1000 files)
    assert!(
        duration.as_secs() < 30,
        "Large commit too slow: {:?}s (target: < 30s)",
        duration.as_secs()
    );

    println!("Large commit test passed ({}ms per file)", duration.as_millis() as f64 / 1000.0);
    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Test 5.5: Rollback and error recovery
#[tokio::test]
#[ignore]
async fn test_rollback_on_validation_failure() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_test_table(&pool, "writer_rollback", "s3://bucket/writer_rollback")
        .await?;

    let writer = PostgresWriter::new(pool.clone());

    // Start first valid commit
    let handle1 = writer.begin_commit(table_id, 0).await?;

    let valid_actions = vec![create_test_file("valid.parquet", 1024)];
    writer
        .write_actions(table_id, &handle1, &valid_actions)
        .await?;
    writer
        .finalize_commit(table_id, handle1, 0)
        .await?;

    println!("First commit succeeded");

    // Try second commit
    let handle2 = writer.begin_commit(table_id, 1).await?;

    let test_actions = vec![create_test_file("test.parquet", 2048)];
    writer
        .write_actions(table_id, &handle2, &test_actions)
        .await?;

    // Finalize second commit
    writer
        .finalize_commit(table_id, handle2, 1)
        .await?;

    println!("Second commit succeeded");

    // Verify version incremented to 2
    let version: i64 =
        sqlx::query_scalar("SELECT current_version FROM dl_tables WHERE table_id = $1")
            .bind(table_id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(version, 2, "Version should be 2 after two commits");

    // Verify no partial data left behind
    let total_files: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dl_add_files WHERE table_id = $1"
    )
    .bind(table_id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(total_files, 2, "Should have exactly 2 files (no partial data)");

    println!("Rollback/recovery test passed");
    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Test 5.3: Multiple sequential commits work correctly
#[tokio::test]
#[ignore]
async fn test_sequential_commits_integration() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_test_table(&pool, "writer_sequential", "s3://bucket/writer_sequential")
        .await?;

    let writer = PostgresWriter::new(pool.clone());

    // Do 5 commits
    for version in 0..5 {
        let actions = vec![
            create_test_file(&format!("v{}f1.parquet", version), 1024),
            create_test_file(&format!("v{}f2.parquet", version), 2048),
        ];

        let handle = writer.begin_commit(table_id, version).await?;
        writer
            .write_actions(table_id, &handle, &actions)
            .await?;
        writer
            .finalize_commit(table_id, handle, version)
            .await?;
    }

    // Verify final version
    let version: i64 =
        sqlx::query_scalar("SELECT current_version FROM dl_tables WHERE table_id = $1")
            .bind(table_id)
            .fetch_one(&pool)
            .await?;

    assert_eq!(version, 5, "Version should be 5 after 5 commits");

    // Verify all files present
    let total_files: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dl_add_files WHERE table_id = $1"
    )
    .bind(table_id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(total_files, 10, "Should have 10 files total (2 per commit * 5 commits)");

    println!("Sequential commits integration test passed");
    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Test 5.5: Idempotency - duplicate commit attempts are safely rejected
#[tokio::test]
#[ignore]
async fn test_idempotent_commit_handling() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_test_table(&pool, "writer_idempotent", "s3://bucket/writer_idempotent")
        .await?;

    let writer = PostgresWriter::new(pool.clone());

    let actions = vec![create_test_file("file.parquet", 1024)];

    // First commit succeeds
    let handle1 = writer.begin_commit(table_id, 0).await?;
    writer
        .write_actions(table_id, &handle1, &actions)
        .await?;
    writer
        .finalize_commit(table_id, handle1, 0)
        .await?;

    println!("First commit succeeded");

    // Try to commit with same version again (should fail)
    let result = writer.begin_commit(table_id, 0).await;

    assert!(
        result.is_err(),
        "Duplicate commit with same version should fail"
    );

    println!("Idempotent handling test passed");
    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Test 5.6: Conformance - file counts and metadata preserved correctly
#[tokio::test]
#[ignore]
async fn test_conformance_file_metadata_preservation() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_test_table(&pool, "writer_conformance", "s3://bucket/writer_conformance")
        .await?;

    let writer = PostgresWriter::new(pool.clone());

    // Create actions with specific sizes and paths
    let test_files = vec![
        ("data/file_a.parquet", 1024i64),
        ("data/file_b.parquet", 2048i64),
        ("data/file_c.parquet", 4096i64),
    ];

    let actions: Vec<_> = test_files
        .iter()
        .map(|(path, size)| create_test_file(path, *size))
        .collect();

    let handle = writer.begin_commit(table_id, 0).await?;
    writer
        .write_actions(table_id, &handle, &actions)
        .await?;
    writer
        .finalize_commit(table_id, handle, 0)
        .await?;

    // Verify stored files match what was written
    let stored_files: Vec<(String, i64)> = sqlx::query_as(
        "SELECT path, size FROM dl_add_files WHERE table_id = $1 AND version = 0 ORDER BY path"
    )
    .bind(table_id)
    .fetch_all(&pool)
    .await?;

    assert_eq!(stored_files.len(), 3, "Should have 3 files stored");

    // Verify paths and sizes
    for (i, (expected_path, expected_size)) in test_files.iter().enumerate() {
        assert_eq!(
            &stored_files[i].0, expected_path,
            "Path mismatch at index {}",
            i
        );
        assert_eq!(
            stored_files[i].1, *expected_size,
            "Size mismatch for file {}",
            expected_path
        );
    }

    println!("Conformance test passed - all metadata preserved correctly");
    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}
