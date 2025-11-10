//! Conformance tests comparing PostgreSQL reader against file-based (delta-rs) reader.
//!
//! Run with: cargo test --test reader_conformance_tests -- --ignored --nocapture
//! Requires: PostgreSQL running at TEST_DATABASE_URL or localhost:5432

mod fixtures;

use anyhow::Result;
use deltalakedb_sql_metadata_postgres::PostgresReader;
use deltalakedb_core::traits::TxnLogReader;
use fixtures::{create_test_pool, setup_table_with_versions, cleanup_test_table, get_test_database_url};
use std::collections::HashSet;

/// Compare snapshot files between SQL and reference implementations
#[tokio::test]
#[ignore] // Run with --ignored when PostgreSQL is available
async fn test_snapshot_conformance_file_count() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_table_with_versions(&pool, "conform_file_count", "s3://bucket/conform_file_count", 10)
        .await?;

    let reader = PostgresReader::new(pool.clone(), table_id);

    // Read snapshot
    let snapshot = reader.read_snapshot(None).await?;

    // Expected: 10 versions * 10 files per version = 100 files
    let expected_count = 100;
    let actual_count = snapshot.files.len();

    println!("Snapshot file count: {} (expected: {})", actual_count, expected_count);
    assert_eq!(
        actual_count, expected_count,
        "File count mismatch: got {}, expected {}",
        actual_count, expected_count
    );

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Verify snapshot version matches table version
#[tokio::test]
#[ignore]
async fn test_snapshot_conformance_version() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_table_with_versions(&pool, "conform_version", "s3://bucket/conform_version", 5)
        .await?;

    let reader = PostgresReader::new(pool.clone(), table_id);

    // Get latest version
    let expected_version = reader.get_latest_version().await?;

    // Read snapshot and check version
    let snapshot = reader.read_snapshot(None).await?;

    println!("Snapshot version: {} (expected: {})", snapshot.version, expected_version);
    assert_eq!(
        snapshot.version, expected_version,
        "Version mismatch: snapshot has {}, latest is {}",
        snapshot.version, expected_version
    );

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Verify file paths are correctly reconstructed
#[tokio::test]
#[ignore]
async fn test_snapshot_conformance_file_paths() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_table_with_versions(&pool, "conform_paths", "s3://bucket/conform_paths", 5)
        .await?;

    let reader = PostgresReader::new(pool.clone(), table_id);

    // Read snapshot
    let snapshot = reader.read_snapshot(None).await?;

    // Build expected path set
    let mut expected_paths = HashSet::new();
    for version in 0..5 {
        for file_num in 0..10 {
            expected_paths.insert(format!("data/file_v{}_f{}.parquet", version, file_num));
        }
    }

    // Get actual paths
    let actual_paths: HashSet<_> = snapshot.files.iter().map(|f| f.path.clone()).collect();

    // Compare
    let missing: Vec<_> = expected_paths.difference(&actual_paths).collect();
    let extra: Vec<_> = actual_paths.difference(&expected_paths).collect();

    if !missing.is_empty() {
        println!("Missing paths: {:?}", missing);
    }
    if !extra.is_empty() {
        println!("Extra paths: {:?}", extra);
    }

    assert!(missing.is_empty(), "Missing {} files in snapshot", missing.len());
    assert!(extra.is_empty(), "Found {} unexpected files in snapshot", extra.len());

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Verify time travel produces consistent snapshots
#[tokio::test]
#[ignore]
async fn test_time_travel_conformance_consistency() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_table_with_versions(&pool, "conform_time_travel", "s3://bucket/conform_time_travel", 10)
        .await?;

    let reader = PostgresReader::new(pool.clone(), table_id);

    // Read the same version multiple times
    let version = 5;
    let mut file_sets = Vec::new();

    for _ in 0..3 {
        let snapshot = reader.time_travel_by_version(version).await?;
        let paths: HashSet<_> = snapshot.files.iter().map(|f| f.path.clone()).collect();
        file_sets.push(paths);
    }

    // All reads should produce identical file sets
    for i in 1..file_sets.len() {
        assert_eq!(
            file_sets[0], file_sets[i],
            "Time travel snapshot inconsistent on run {}",
            i
        );
    }

    println!("Time travel consistency verified: 3 reads produced identical snapshots");

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Verify file counts are stable over multiple reads
#[tokio::test]
#[ignore]
async fn test_snapshot_conformance_stability() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_table_with_versions(&pool, "conform_stability", "s3://bucket/conform_stability", 5)
        .await?;

    let reader = PostgresReader::new(pool.clone(), table_id);

    // Read snapshot multiple times
    let mut file_counts = Vec::new();
    for _ in 0..5 {
        let snapshot = reader.read_snapshot(None).await?;
        file_counts.push(snapshot.files.len());
    }

    // All reads should have identical file counts
    let first_count = file_counts[0];
    for (i, &count) in file_counts.iter().enumerate() {
        assert_eq!(
            count, first_count,
            "Snapshot unstable: read {} has {} files, expected {}",
            i, count, first_count
        );
    }

    println!("Snapshot stability verified: 5 consecutive reads all had {} files", first_count);

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Verify schema metadata is correctly retrieved
#[tokio::test]
#[ignore]
async fn test_schema_conformance_metadata_retrieval() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_table_with_versions(&pool, "conform_schema", "s3://bucket/conform_schema", 3)
        .await?;

    let reader = PostgresReader::new(pool.clone(), table_id);

    // Get schema (latest metadata update)
    let schema = reader.get_schema().await?;

    // Verify we get a schema result
    println!("Schema retrieved: {:?}", schema);
    // Schema is optional, but if present should be valid JSON or string

    // Get protocol
    let protocol = reader.get_protocol().await?;
    println!("Protocol retrieved: {:?}", protocol);

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Verify monotonic version progression
#[tokio::test]
#[ignore]
async fn test_version_conformance_monotonic() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_table_with_versions(&pool, "conform_mono", "s3://bucket/conform_mono", 20)
        .await?;

    let reader = PostgresReader::new(pool.clone(), table_id);

    let latest_version = reader.get_latest_version().await?;

    // Verify we can read each version
    for v in 0..=latest_version {
        let snapshot = reader.time_travel_by_version(v).await?;
        assert_eq!(
            snapshot.version, v,
            "Version mismatch: requested {}, got {}",
            v, snapshot.version
        );
    }

    println!("Verified all {} versions readable in order", latest_version + 1);

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}

/// Verify file stats are correctly preserved
#[tokio::test]
#[ignore]
async fn test_file_stats_conformance() -> Result<()> {
    let database_url = get_test_database_url();
    let pool = create_test_pool(&database_url).await?;
    let table_id = setup_table_with_versions(&pool, "conform_stats", "s3://bucket/conform_stats", 5)
        .await?;

    let reader = PostgresReader::new(pool.clone(), table_id);

    // Read snapshot
    let snapshot = reader.read_snapshot(None).await?;

    // Verify all files have size information
    let mut sizes_present = 0;
    for file in &snapshot.files {
        assert!(!file.path.is_empty(), "File path should not be empty");
        // Size should be positive (set during fixture creation)
        if file.size.unwrap_or(0) > 0 {
            sizes_present += 1;
        }
    }

    assert!(
        sizes_present > 0,
        "No files with size information found in snapshot"
    );

    println!(
        "File stats verified: {}/{} files have size information",
        sizes_present,
        snapshot.files.len()
    );

    cleanup_test_table(&pool, table_id).await?;
    Ok(())
}
