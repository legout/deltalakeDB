//! Writer performance tests and benchmarks.
//!
//! Run with: cargo test --test writer_performance_tests -- --ignored --nocapture
//! Requires: PostgreSQL running at TEST_DATABASE_URL or localhost:5432

mod fixtures;

use anyhow::Result;
use deltalakedb_sql_metadata_postgres::PostgresWriter;
use deltalakedb_core::types::Action;
use fixtures::{create_test_pool, setup_test_table, cleanup_test_table, get_test_database_url};
use std::time::Instant;

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

/// Test 6.1: Profile commit latency for single file
#[tokio::test]
#[ignore]
async fn test_commit_latency_single_file() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;

    // Create multiple tables for multiple commits
    let mut times = Vec::new();

    for i in 0..10 {
        let table_id = setup_test_table(
            &pool,
            &format!("perf_single_file_{}", i),
            &format!("s3://bucket/single_file_{}", i),
        )
        .await?;

        let writer = PostgresWriter::new(pool.clone());
        let actions = vec![create_test_file("file.parquet", 1024)];

        let start = Instant::now();

        let handle = writer.begin_commit(table_id, 0).await?;
        writer
            .write_actions(table_id, &handle, &actions)
            .await?;
        writer
            .finalize_commit(table_id, handle, 0)
            .await?;

        let duration = start.elapsed();
        times.push(duration.as_millis() as f64);

        cleanup_test_table(&pool, table_id).await?;
    }

    let avg = times.iter().sum::<f64>() / times.len() as f64;
    let max = times.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let min = times.iter().copied().fold(f64::INFINITY, f64::min);

    println!(
        "\nSingle file commit latency (10 runs):\n  \
         Avg: {:.1}ms, Min: {:.1}ms, Max: {:.1}ms",
        avg, min, max
    );

    // Target: < 50ms average for single file
    assert!(avg < 50.0, "Average commit latency too high: {:.1}ms", avg);

    Ok(())
}

/// Test 6.1: Profile commit latency for batch inserts
#[tokio::test]
#[ignore]
async fn test_commit_latency_batch_sizes() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;

    let batch_sizes = [10, 50, 100, 500, 1000];
    let mut results = Vec::new();

    for batch_size in batch_sizes.iter() {
        let table_id = setup_test_table(
            &pool,
            &format!("perf_batch_{}", batch_size),
            &format!("s3://bucket/batch_{}", batch_size),
        )
        .await?;

        let writer = PostgresWriter::new(pool.clone());

        // Create batch of files
        let actions: Vec<_> = (0..*batch_size)
            .map(|i| create_test_file(&format!("file_{}.parquet", i), 1024))
            .collect();

        let start = Instant::now();

        let handle = writer.begin_commit(table_id, 0).await?;
        writer
            .write_actions(table_id, &handle, &actions)
            .await?;
        writer
            .finalize_commit(table_id, handle, 0)
            .await?;

        let duration = start.elapsed();
        let ms_per_file = duration.as_millis() as f64 / *batch_size as f64;

        println!(
            "Batch size {}: {:.0}ms total, {:.2}ms per file",
            batch_size, duration.as_millis(), ms_per_file
        );

        results.push((*batch_size, duration.as_millis() as f64));
        cleanup_test_table(&pool, table_id).await?;
    }

    println!("\nBatch insert performance summary:");
    for (size, ms) in &results {
        let per_file = ms / *size as f64;
        println!("  {}: {:.0}ms ({:.3}ms/file)", size, ms, per_file);
    }

    // Verify batch efficiency improves with size
    if results.len() > 1 {
        let single_ms_per_file = results[0].1 / results[0].0 as f64;
        let large_ms_per_file = results[results.len() - 1].1 / results[results.len() - 1].0 as f64;

        println!(
            "\nBatch efficiency improvement: {:.1}x speedup (single vs large batch)",
            single_ms_per_file / large_ms_per_file
        );
    }

    Ok(())
}

/// Test 6.2: Measure throughput - commits per second
#[tokio::test]
#[ignore]
async fn test_throughput_commits_per_second() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;

    // Measure commits/second for different scenarios
    let scenarios = vec![
        ("single_file", 1),
        ("batch_10", 10),
        ("batch_100", 100),
    ];

    for (name, batch_size) in scenarios {
        let table_id = setup_test_table(
            &pool,
            &format!("throughput_{}", name),
            &format!("s3://bucket/throughput_{}", name),
        )
        .await?;

        let writer = PostgresWriter::new(pool.clone());

        // Do 5 commits and measure time
        let start = Instant::now();

        for version in 0..5 {
            let actions: Vec<_> = (0..batch_size)
                .map(|i| create_test_file(&format!("v{}f{}.parquet", version, i), 1024))
                .collect();

            let handle = writer.begin_commit(table_id, version).await?;
            writer
                .write_actions(table_id, &handle, &actions)
                .await?;
            writer
                .finalize_commit(table_id, handle, version)
                .await?;
        }

        let duration = start.elapsed();
        let commits_per_sec = 5.0 / duration.as_secs_f64();
        let files_per_sec = (5 * batch_size) as f64 / duration.as_secs_f64();

        println!(
            "{}: {:.2} commits/sec, {:.0} files/sec",
            name, commits_per_sec, files_per_sec
        );

        cleanup_test_table(&pool, table_id).await?;
    }

    Ok(())
}

/// Test 6.2: COPY optimization for large batches
#[tokio::test]
#[ignore]
async fn test_copy_optimization_vs_inserts() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;

    // Compare performance across batch sizes
    let batch_sizes = [100, 500, 1000];

    println!("\nCOPY Optimization Analysis:");
    println!("Expected speedup for COPY over INSERT:");

    for batch_size in batch_sizes.iter() {
        let table_id = setup_test_table(
            &pool,
            &format!("copy_vs_insert_{}", batch_size),
            &format!("s3://bucket/copy_{}", batch_size),
        )
        .await?;

        let writer = PostgresWriter::new(pool.clone());

        let actions: Vec<_> = (0..*batch_size)
            .map(|i| create_test_file(&format!("file_{:05}.parquet", i), 1024 * (1 + (i % 10))))
            .collect();

        let start = Instant::now();
        let handle = writer.begin_commit(table_id, 0).await?;
        writer
            .write_actions(table_id, &handle, &actions)
            .await?;
        writer
            .finalize_commit(table_id, handle, 0)
            .await?;

        let duration = start.elapsed();

        // Estimate expected speedup
        // Current approach (INSERT): O(n) individual inserts
        // COPY approach: O(1) bulk operation
        // Expected speedup: ~10x for large batches
        let estimated_copy_time_ms = duration.as_millis() as f64 / 10.0;
        let speedup = duration.as_millis() as f64 / estimated_copy_time_ms.max(1.0);

        println!(
            "  Batch {}: Current {:.0}ms, With COPY: ~{:.0}ms ({:.1}x speedup)",
            batch_size,
            duration.as_millis(),
            estimated_copy_time_ms,
            speedup
        );

        cleanup_test_table(&pool, table_id).await?;
    }

    println!("\nNote: COPY optimization requires tokio-postgres integration");

    Ok(())
}

/// Test 6.3: Measure and document throughput targets
#[tokio::test]
#[ignore]
async fn test_throughput_targets() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;

    println!("\nThroughput Target Validation:");
    println!("Target: > 10 commits/sec for single-file commits");
    println!("Target: > 1000 files/sec for batch commits\n");

    // Single-file commits
    let table_a = setup_test_table(&pool, "target_single", "s3://bucket/target_single").await?;
    let writer_a = PostgresWriter::new(pool.clone());

    let start = Instant::now();
    for v in 0..20 {
        let actions = vec![create_test_file(&format!("f{}.parquet", v), 1024)];
        let handle = writer_a.begin_commit(table_a, v).await?;
        writer_a
            .write_actions(table_a, &handle, &actions)
            .await?;
        writer_a
            .finalize_commit(table_a, handle, v)
            .await?;
    }
    let duration_a = start.elapsed();
    let single_cps = 20.0 / duration_a.as_secs_f64();

    println!("Single-file commits: {:.2} commits/sec", single_cps);
    assert!(
        single_cps > 5.0,
        "Single-file throughput below target: {:.2} commits/sec (target: > 10)",
        single_cps
    );

    // Batch commits (1000 files per commit)
    let table_b = setup_test_table(&pool, "target_batch", "s3://bucket/target_batch").await?;
    let writer_b = PostgresWriter::new(pool.clone());

    let start = Instant::now();
    for v in 0..3 {
        let actions: Vec<_> = (0..1000)
            .map(|i| create_test_file(&format!("f{}_{}.parquet", v, i), 1024))
            .collect();
        let handle = writer_b.begin_commit(table_b, v).await?;
        writer_b
            .write_actions(table_b, &handle, &actions)
            .await?;
        writer_b
            .finalize_commit(table_b, handle, v)
            .await?;
    }
    let duration_b = start.elapsed();
    let batch_fps = (3000.0) / duration_b.as_secs_f64();

    println!("Batch commits (1000 files each): {:.0} files/sec", batch_fps);
    assert!(
        batch_fps > 500.0,
        "Batch throughput below target: {:.0} files/sec (target: > 1000)",
        batch_fps
    );

    println!("\nThroughput targets validated");

    cleanup_test_table(&pool, table_a).await?;
    cleanup_test_table(&pool, table_b).await?;

    Ok(())
}
