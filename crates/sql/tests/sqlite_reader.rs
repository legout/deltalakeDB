use chrono::Utc;
use deltalakedb_catalog::{schema_ddl, Dialect};
use deltalakedb_core::txn_log::TxnLogReader;
use deltalakedb_sql::SqliteTxnLogReader;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use tempfile::NamedTempFile;
use tokio::runtime::Builder;
use uuid::Uuid;

#[test]
fn sqlite_reader_returns_latest_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    run_with_sqlite(|dsn, table_id| {
        let reader = SqliteTxnLogReader::connect("file:///tmp/sqlite", table_id, &dsn)?;
        assert_eq!(reader.current_version()?, 1);
        let snapshot = reader.snapshot_at_version(None)?;
        assert_eq!(snapshot.version, 1);
        assert_eq!(snapshot.files.len(), 1);
        assert_eq!(snapshot.files[0].path, "part-001");
        Ok(())
    })
}

#[test]
fn sqlite_reader_time_travel() -> Result<(), Box<dyn std::error::Error>> {
    run_with_sqlite(|dsn, table_id| {
        let reader = SqliteTxnLogReader::connect("file:///tmp/sqlite", table_id, &dsn)?;
        let snapshot = reader.snapshot_at_version(Some(0))?;
        assert_eq!(snapshot.version, 0);
        assert_eq!(snapshot.files.len(), 1);
        assert_eq!(snapshot.files[0].path, "part-000");
        Ok(())
    })
}

fn run_with_sqlite<F>(f: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(String, Uuid) -> Result<(), Box<dyn std::error::Error>>,
{
    let tmp = NamedTempFile::new()?;
    let dsn = format!("sqlite://{}", tmp.path().display());
    let table_id = Uuid::new_v4();
    let runtime = Builder::new_current_thread().enable_all().build()?;
    runtime
        .block_on(async {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&dsn)
                .await?;
            bootstrap_sqlite(&pool, table_id).await?;
            Ok::<_, sqlx::Error>(())
        })
        .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;
    f(dsn, table_id)
}

async fn bootstrap_sqlite(pool: &SqlitePool, table_id: Uuid) -> Result<(), sqlx::Error> {
    apply_schema(pool).await?;
    let iso = Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO dl_table_heads(table_id, current_version, updated_at) VALUES (?, -1, ?)",
    )
    .bind(table_id)
    .bind(&iso)
    .execute(pool)
    .await?;

    insert_commit(pool, table_id, 0, "part-000").await?;
    insert_commit(pool, table_id, 1, "part-001").await?;
    sqlx::query(
        "INSERT INTO dl_remove_files(table_id, version, path, deletion_timestamp, data_change) VALUES (?, 1, 'part-000', 0, 1)",
    )
    .bind(table_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn apply_schema(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    for stmt in schema_ddl(Dialect::Sqlite) {
        sqlx::query(stmt).execute(pool).await?;
    }
    Ok(())
}

async fn insert_commit(
    pool: &SqlitePool,
    table_id: Uuid,
    version: i64,
    path: &str,
) -> Result<(), sqlx::Error> {
    let iso = Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO dl_table_versions(table_id, version, committed_at, committer, operation, operation_params) VALUES (?, ?, ?, 'test', 'WRITE', '{}')",
    )
    .bind(table_id)
    .bind(version)
    .bind(&iso)
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO dl_metadata_updates(table_id, version, schema_json, partition_columns, table_properties) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(table_id)
    .bind(version)
    .bind(r#"{"type":"struct","fields":[{"name":"id","type":"long"}]}"#)
    .bind("[\"id\"]")
    .bind("{}")
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO dl_protocol_updates(table_id, version, min_reader_version, min_writer_version) VALUES (?, ?, 2, 5)",
    )
    .bind(table_id)
    .bind(version)
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO dl_add_files(table_id, version, path, size_bytes, partition_values, stats, data_change, modification_time) VALUES (?, ?, ?, 128, ?, NULL, 1, 0)",
    )
    .bind(table_id)
    .bind(version)
    .bind(path)
    .bind(r#"{"id":"0"}"#)
    .execute(pool)
    .await?;

    Ok(())
}
