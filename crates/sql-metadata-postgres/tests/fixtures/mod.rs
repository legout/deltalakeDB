//! Test fixtures for PostgreSQL reader and writer tests.

use anyhow::Result;
use sqlx::postgres::{PgPool, PgPoolOptions};
use uuid::Uuid;
use std::time::{SystemTime, UNIX_EPOCH};

/// Create a test database pool
pub async fn create_test_pool(database_url: &str) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    // Run migrations
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await?;

    Ok(pool)
}

/// Setup a test Delta table with given name and location
pub async fn setup_test_table(pool: &PgPool, table_name: &str, location: &str) -> Result<Uuid> {
    let table_id = Uuid::new_v4();

    sqlx::query!(
        r#"
        INSERT INTO dl_tables (table_id, table_name, location, current_version, min_reader_version, min_writer_version)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
        table_id,
        table_name,
        location,
        0i64,
        1i32,
        2i32
    )
    .execute(pool)
    .await?;

    Ok(table_id)
}

/// Populate a test table with 100k files across 100 versions (1000 files per version)
pub async fn setup_table_with_100k_files(pool: &PgPool, table_name: &str, location: &str) -> Result<Uuid> {
    let table_id = setup_test_table(pool, table_name, location).await?;

    // Batch insert 100k files across 100 versions
    for version in 0..100 {
        let mut tx = pool.begin().await?;

        // Create version record
        let timestamp = (version as i64) * 1000; // 1 second per version
        sqlx::query!(
            r#"
            INSERT INTO dl_table_versions (table_id, version, commit_timestamp, operation_type, num_actions)
            VALUES ($1, $2, $3, $4, $5)
            "#,
            table_id,
            version as i64,
            timestamp,
            "AddFile",
            1000i32
        )
        .execute(&mut *tx)
        .await?;

        // Insert 1000 files for this version
        for file_num in 0..1000 {
            let path = format!("data/v{:03}/file_{:06}.parquet", version, file_num);
            let size = 1024i64 * (1 + (file_num % 100) as i64); // Vary sizes
            let modification_time = timestamp * 1000;
            let data_change_version = version as i64;

            sqlx::query!(
                r#"
                INSERT INTO dl_add_files (table_id, version, path, size, modification_time, data_change_version)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
                table_id,
                version as i64,
                path,
                size,
                modification_time,
                data_change_version
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
    }

    // Update table version to 100
    sqlx::query!(
        "UPDATE dl_tables SET current_version = 100 WHERE table_id = $1",
        table_id
    )
    .execute(pool)
    .await?;

    Ok(table_id)
}

/// Create a test table with specific versions and actions
pub async fn setup_table_with_versions(
    pool: &PgPool,
    table_name: &str,
    location: &str,
    versions_count: usize,
) -> Result<Uuid> {
    let table_id = setup_test_table(pool, table_name, location).await?;

    for version in 0..versions_count {
        let mut tx = pool.begin().await?;

        let timestamp = (version as i64) * 1000;
        sqlx::query!(
            r#"
            INSERT INTO dl_table_versions (table_id, version, commit_timestamp, operation_type, num_actions)
            VALUES ($1, $2, $3, $4, $5)
            "#,
            table_id,
            version as i64,
            timestamp,
            "AddFile",
            10i32
        )
        .execute(&mut *tx)
        .await?;

        // Add 10 files per version
        for file_num in 0..10 {
            let path = format!("data/file_v{}_f{}.parquet", version, file_num);
            sqlx::query!(
                r#"
                INSERT INTO dl_add_files (table_id, version, path, size, modification_time, data_change_version)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
                table_id,
                version as i64,
                path,
                1024i64,
                timestamp * 1000,
                version as i64
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
    }

    // Update table version
    sqlx::query!(
        "UPDATE dl_tables SET current_version = $1 WHERE table_id = $2",
        versions_count as i64 - 1,
        table_id
    )
    .execute(pool)
    .await?;

    Ok(table_id)
}

/// Clean up test data (optional, for cleanup)
pub async fn cleanup_test_table(pool: &PgPool, table_id: Uuid) -> Result<()> {
    // Delete all test data
    sqlx::query!("DELETE FROM dl_add_files WHERE table_id = $1", table_id)
        .execute(pool)
        .await?;

    sqlx::query!("DELETE FROM dl_remove_files WHERE table_id = $1", table_id)
        .execute(pool)
        .await?;

    sqlx::query!("DELETE FROM dl_metadata_updates WHERE table_id = $1", table_id)
        .execute(pool)
        .await?;

    sqlx::query!("DELETE FROM dl_protocol_updates WHERE table_id = $1", table_id)
        .execute(pool)
        .await?;

    sqlx::query!("DELETE FROM dl_txn_actions WHERE table_id = $1", table_id)
        .execute(pool)
        .await?;

    sqlx::query!("DELETE FROM dl_table_versions WHERE table_id = $1", table_id)
        .execute(pool)
        .await?;

    sqlx::query!("DELETE FROM dl_tables WHERE table_id = $1", table_id)
        .execute(pool)
        .await?;

    Ok(())
}

/// Get database URL from environment or default
/// 
/// IMPORTANT: TEST_DATABASE_URL environment variable must be set to run tests.
/// Default is a placeholder that will fail. Set to your test database connection string.
/// 
/// Example usage:
/// Set environment variable with your test database credentials before running tests.
/// Supports postgresql:// connection strings.
pub fn get_test_database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| {
            // Must be set via environment variable for tests to run
            "postgresql://localhost/deltalakedb_test".to_string()
        })
}
