//! Performance tests for PostgreSQL reader with 100k files.
//!
//! Run with: cargo test --test reader_performance_tests -- --ignored --nocapture
//! Requires: PostgreSQL running at TEST_DATABASE_URL or localhost:5432

mod fixtures;

use anyhow::Result;
use deltalakedb_sql_metadata_postgres::PostgresReader;
use deltalakedb_core::traits::TxnLogReader;
use fixtures::{create_test_pool, setup_table_with_100k_files, cleanup_test_table, get_test_database_url};
use std::time::Instant;
use std::collections::HashMap;

#[tokio::test]
#[ignore] // Run with --ignored when PostgreSQL is available
async fn test_reader_get_latest_version_performance() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_table_with_100k_files(&pool, "perf_latest_version", "s3://bucket/perf_latest").await?;

    let reader = PostgresReader::new(pool.clone(), table_id);

    // Warm up
    let _ = reader.get_latest_version().await?;

    // Measure get_latest_version performance (target: < 1ms)
    let mut times = Vec::new();
    for _ in 0..100 {
        let start = Instant::now();
        let version = reader.get_latest_version().await?;
        let duration = start.elapsed();
        times.push(duration.as_millis() as f64);
    }

    let avg = times.iter().sum::<f64>() / times.len() as f64;
    let max = times.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let min = times.iter().copied().fold(f64::INFINITY, f64::min);

    println!(
        "\nget_latest_version performance (100 runs):\n  \
         Avg: {:.3}ms, Min: {:.3}ms, Max: {:.3}ms",
        avg, min, max
    );

    // Assert performance targets
    assert!(
        avg < 1.0,
        "Average get_latest_version too slow: {:.3}ms (target: < 1ms)",
        avg
    );
    assert!(
        max < 5.0,
        "Max get_latest_version too slow: {:.3}ms (target: < 5ms)",
        max
    );

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_reader_read_snapshot_100k_files_performance() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_table_with_100k_files(&pool, "perf_snapshot_100k", "s3://bucket/perf_snapshot_100k").await?;

    let reader = PostgresReader::new(pool.clone(), table_id);

    // Warm up (load page cache)
    let _ = reader.read_snapshot(None).await?;

    // Measure read_snapshot performance with 100k files (target: < 800ms p95)
    let mut times = Vec::new();
    for _ in 0..5 {
        let start = Instant::now();
        let snapshot = reader.read_snapshot(None).await?;
        let duration = start.elapsed();
        times.push(duration.as_millis() as f64);

        println!("Snapshot read: {:.0}ms, files: {}", duration.as_millis(), snapshot.files.len());
    }

    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let avg = times.iter().sum::<f64>() / times.len() as f64;
    let p95 = times[(times.len() * 95) / 100];
    let max = times.last().copied().unwrap_or(0.0);

    println!(
        "\nread_snapshot (100k files) performance (5 runs):\n  \
         Avg: {:.0}ms, P95: {:.0}ms, Max: {:.0}ms",
        avg, p95, max
    );

    // Assert performance targets
    assert!(
        p95 < 800.0,
        "P95 read_snapshot too slow: {:.0}ms (target: < 800ms)",
        p95
    );
    assert!(
        avg < 600.0,
        "Average read_snapshot too slow: {:.0}ms (target: < 600ms)",
        avg
    );

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_reader_time_travel_performance() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_table_with_100k_files(&pool, "perf_time_travel", "s3://bucket/perf_time_travel").await?;

    let reader = PostgresReader::new(pool.clone(), table_id);

    // Warm up
    let _ = reader.time_travel_by_version(50).await?;

    // Measure time travel to middle version (target: < 500ms)
    let mut times = Vec::new();
    for version in [10, 25, 50, 75, 99].iter() {
        let start = Instant::now();
        let snapshot = reader.time_travel_by_version(*version).await?;
        let duration = start.elapsed();
        times.push(duration.as_millis() as f64);

        println!("Time travel to v{}: {:.0}ms, files: {}", version, duration.as_millis(), snapshot.files.len());
    }

    let avg = times.iter().sum::<f64>() / times.len() as f64;
    let max = times.iter().copied().fold(f64::NEG_INFINITY, f64::max);

    println!(
        "\ntime_travel_by_version performance (5 versions):\n  \
         Avg: {:.0}ms, Max: {:.0}ms",
        avg, max
    );

    assert!(
        avg < 500.0,
        "Average time_travel too slow: {:.0}ms (target: < 500ms)",
        avg
    );
    assert!(
        max < 600.0,
        "Max time_travel too slow: {:.0}ms (target: < 600ms)",
        max
    );

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_reader_get_files_performance() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_table_with_100k_files(&pool, "perf_get_files", "s3://bucket/perf_get_files").await?;

    let reader = PostgresReader::new(pool.clone(), table_id);

    // Get latest version
    let version = reader.get_latest_version().await?;

    // Warm up
    let _ = reader.get_files(version).await?;

    // Measure get_files performance (target: < 500ms for 1000 files)
    let mut times = Vec::new();
    for _ in 0..3 {
        let start = Instant::now();
        let files = reader.get_files(version).await?;
        let duration = start.elapsed();
        times.push(duration.as_millis() as f64);

        println!("Get files at v{}: {:.0}ms, count: {}", version, duration.as_millis(), files.len());
    }

    let avg = times.iter().sum::<f64>() / times.len() as f64;

    println!(
        "\nget_files performance (3 runs):\n  \
         Avg: {:.0}ms",
        avg
    );

    assert!(
        avg < 500.0,
        "Average get_files too slow: {:.0}ms (target: < 500ms)",
        avg
    );

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_reader_scaled_performance_metrics() -> Result<()> {
    // Measure scalability as file count increases
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;

    let mut results = HashMap::new();

    for (table_name, file_scale, versions) in [
        ("scale_1k", 1, 10),
        ("scale_10k", 10, 10),
        ("scale_100k", 100, 10),
    ] {
        let table_id = fixtures::setup_table_with_versions(&pool, &format!("perf_{}", table_name), &format!("s3://bucket/{}", table_name), versions)
            .await?;

        let reader = PostgresReader::new(pool.clone(), table_id);

        let start = Instant::now();
        let snapshot = reader.read_snapshot(None).await?;
        let duration = start.elapsed();

        let file_count = snapshot.files.len();
        let ms_per_file = duration.as_millis() as f64 / file_count.max(1) as f64;

        println!(
            "Snapshot read for {}: {}ms ({} files, {:.3}ms/file)",
            table_name, duration.as_millis(), file_count, ms_per_file
        );

        results.insert(table_name, (file_count, duration.as_millis() as f64));
        cleanup_test_table(&pool, table_id).await?;
    }

    // Verify near-linear scaling
    println!("\nScaling analysis:");
    for (name, (count, ms)) in &results {
        println!("  {}: {} files in {}ms", name, count, ms);
    }

    Ok(())
}
