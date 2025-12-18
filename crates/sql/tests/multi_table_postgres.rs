use chrono::Utc;
use deltalakedb_core::txn_log::{
    ActiveFile, CommitRequest, Protocol, TableMetadata, TxnLogError, Version, INITIAL_VERSION,
};
#[path = "common.rs"]
mod common;

use deltalakedb_sql::PostgresTxnLogWriter;
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use tokio::runtime::Builder;
use uuid::Uuid;

#[test]
#[serial]
fn multi_table_commit_advances_both_tables() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = match std::env::var("PG_TEST_DSN") {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };

    let runtime = Builder::new_current_thread().enable_all().build()?;
    let table_a = Uuid::new_v4();
    let table_b = Uuid::new_v4();
    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        common::reset_catalog(&pool).await?;
        bootstrap_table(&pool, table_a, "file:///tmp/table-a").await?;
        bootstrap_table(&pool, table_b, "file:///tmp/table-b").await?;
        Ok::<_, sqlx::Error>(())
    })?;

    let writer_a = PostgresTxnLogWriter::connect("file:///tmp/table-a", table_a, &dsn)?;
    let writer_b = PostgresTxnLogWriter::connect("file:///tmp/table-b", table_b, &dsn)?;

    let req_a = sample_request();
    let mut req_b = sample_request();
    req_b.add_actions[0].path = "part-b".into();

    let builder = writer_a
        .multi_table()
        .stage_from_writer(&writer_a, req_a)
        .stage_from_writer(&writer_b, req_b);
    let results = builder.commit()?;
    assert_eq!(results.len(), 2);

    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let head_a: Version =
            sqlx::query_scalar("SELECT current_version FROM dl_table_heads WHERE table_id = $1")
                .bind(table_a)
                .fetch_one(&pool)
                .await?;
        let head_b: Version =
            sqlx::query_scalar("SELECT current_version FROM dl_table_heads WHERE table_id = $1")
                .bind(table_b)
                .fetch_one(&pool)
                .await?;
        assert_eq!(head_a, 0);
        assert_eq!(head_b, 0);

        let mirror_rows: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM dl_mirror_status WHERE table_id in ($1,$2)")
                .bind(table_a)
                .bind(table_b)
                .fetch_one(&pool)
                .await?;
        assert_eq!(mirror_rows.0, 2);
        Ok::<_, sqlx::Error>(())
    })?;

    Ok(())
}

#[test]
#[serial]
fn multi_table_conflict_aborts_entire_transaction() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = match std::env::var("PG_TEST_DSN") {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };

    let runtime = Builder::new_current_thread().enable_all().build()?;
    let table_a = Uuid::new_v4();
    let table_b = Uuid::new_v4();
    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        common::reset_catalog(&pool).await?;
        bootstrap_table(&pool, table_a, "file:///tmp/table-a").await?;
        bootstrap_table(&pool, table_b, "file:///tmp/table-b").await?;
        Ok::<_, sqlx::Error>(())
    })?;

    let writer_a = PostgresTxnLogWriter::connect("file:///tmp/table-a", table_a, &dsn)?;
    let writer_b = PostgresTxnLogWriter::connect("file:///tmp/table-b", table_b, &dsn)?;

    let req_a = sample_request();
    let builder = writer_a
        .multi_table()
        .stage_from_writer(&writer_a, req_a.clone());
    let _ = builder.commit()?;

    let mut stale_req_a = sample_request();
    stale_req_a.expected_version = INITIAL_VERSION;
    let mut req_b = sample_request();
    req_b.add_actions[0].path = "part-b".into();

    let builder = writer_a
        .multi_table()
        .stage_from_writer(&writer_a, stale_req_a)
        .stage_from_writer(&writer_b, req_b);
    let err = builder.commit().expect_err("should fail due to conflict");
    match err {
        TxnLogError::Concurrency { .. } => {}
        other => panic!("unexpected error {other:?}"),
    }

    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let head_b: Version =
            sqlx::query_scalar("SELECT current_version FROM dl_table_heads WHERE table_id = $1")
                .bind(table_b)
                .fetch_one(&pool)
                .await?;
        assert_eq!(head_b, INITIAL_VERSION);
        Ok::<_, sqlx::Error>(())
    })?;

    Ok(())
}
async fn bootstrap_table(
    pool: &sqlx::PgPool,
    table_id: Uuid,
    location: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO dl_tables(table_id, name, location, protocol_min_reader, protocol_min_writer)
            VALUES ($1, 'demo', $2, 2, 3)"#,
    )
    .bind(table_id)
    .bind(location)
    .execute(pool)
    .await?;

    sqlx::query("INSERT INTO dl_table_heads(table_id, current_version) VALUES ($1, -1)")
        .bind(table_id)
        .execute(pool)
        .await?;
    Ok(())
}

fn sample_request() -> CommitRequest {
    let mut request = CommitRequest::new(INITIAL_VERSION);
    request.metadata = Some(sample_metadata());
    request.protocol = Some(sample_protocol());
    request.add_actions.push(sample_file("part-000"));
    request
}

fn sample_metadata() -> TableMetadata {
    TableMetadata::new(
        r#"{"type":"struct","fields":[{"name":"id","type":"long"}]}"#,
        vec!["id".into()],
        HashMap::new(),
    )
}

fn sample_protocol() -> Protocol {
    Protocol {
        min_reader_version: 2,
        min_writer_version: 5,
    }
}

fn sample_file(path: &str) -> ActiveFile {
    ActiveFile::new(path, 128, Utc::now().timestamp_millis(), HashMap::new())
}
