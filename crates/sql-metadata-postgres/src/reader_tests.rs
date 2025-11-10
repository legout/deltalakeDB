//! Integration tests for PostgreSQL reader.
//!
//! Note: These tests require a running PostgreSQL instance and the schema migrations to be applied.
//! Run with: SQLX_OFFLINE=true cargo test (if offline mode is enabled)
//! Or with a local PostgreSQL: cargo test

#[cfg(test)]
mod tests {
    use deltalakedb_core::traits::TxnLogReader;
    use deltalakedb_core::types::AddFile;
    use sqlx::postgres::PgPoolOptions;
    use uuid::Uuid;

    use crate::reader::PostgresReader;

    /// Helper to create a test database pool (requires DATABASE_URL env var or local postgres)
    #[tokio::test]
    #[ignore] // Only run with `cargo test -- --ignored` when PostgreSQL is available
    async fn test_postgres_reader_latest_version() {
        // Setup
        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost/delta_test".to_string());

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("Failed to connect to test database");

        // Run migrations
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        // Insert test data
        let table_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO dl_tables (table_id, table_name, location, current_version, min_reader_version, min_writer_version)
             VALUES ($1, $2, $3, $4, $5, $6)"
        )
        .bind(table_id)
        .bind("test_table")
        .bind("s3://bucket/path")
        .bind(5i64)
        .bind(1i32)
        .bind(2i32)
        .execute(&pool)
        .await
        .expect("Failed to insert test table");

        // Create reader and test
        let reader = PostgresReader::new(pool.clone(), table_id);
        let version = reader.get_latest_version().await.expect("Failed to get version");
        assert_eq!(version, 5);

        // Cleanup
        sqlx::query("DELETE FROM dl_tables WHERE table_id = $1")
            .bind(table_id)
            .execute(&pool)
            .await
            .expect("Failed to cleanup");
    }

    #[tokio::test]
    #[ignore]
    async fn test_postgres_reader_not_found() {
        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost/delta_test".to_string());

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("Failed to connect to test database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let non_existent_id = Uuid::new_v4();
        let reader = PostgresReader::new(pool, non_existent_id);

        let result = reader.get_latest_version().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_reader_creation() {
        let table_id = Uuid::new_v4();
        let pool = sqlx::postgres::PgPool::connect_lazy("postgresql://localhost").unwrap();
        let reader = PostgresReader::new(pool, table_id);

        assert_eq!(reader.table_id(), table_id);
    }
}
