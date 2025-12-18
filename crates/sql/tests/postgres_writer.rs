#[path = "common.rs"]
mod common;

use chrono::Utc;
use deltalakedb_core::txn_log::{
    ActiveFile, AppTransaction, CommitRequest, Protocol, TableMetadata, TxnLogError, TxnLogWriter,
    INITIAL_VERSION,
};
use deltalakedb_sql::PostgresTxnLogWriter;
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use tokio::runtime::Builder;
use uuid::Uuid;

#[test]
#[serial]
fn postgres_writer_persists_actions() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = match std::env::var("PG_TEST_DSN") {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };

    let runtime = Builder::new_current_thread().enable_all().build()?;
    let table_id = Uuid::new_v4();
    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        common::reset_catalog(&pool).await?;
        bootstrap_table(&pool, table_id).await?;
        Ok::<_, sqlx::Error>(())
    })?;

    let table_uri = format!("deltasql://postgres/{table_id}");
    let writer = PostgresTxnLogWriter::connect(&table_uri, table_id, &dsn)?;

    let mut request = CommitRequest::new(INITIAL_VERSION);
    request.protocol = Some(Protocol {
        min_reader_version: 2,
        min_writer_version: 5,
    });
    request.metadata = Some(sample_metadata());
    request.add_actions.push(sample_file("part-000"));

    let result = writer.commit(request)?;
    assert_eq!(result.version, 0);
    assert_eq!(result.action_count, 3);

    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let head: i64 =
            sqlx::query_scalar("SELECT current_version FROM dl_table_heads WHERE table_id = $1")
                .bind(table_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(head, 0);

        let add_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dl_add_files WHERE table_id = $1")
                .bind(table_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(add_count, 1);

        let mirror_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM dl_mirror_status WHERE table_id = $1 AND status = 'PENDING'",
        )
        .bind(table_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(mirror_count, 1);
        Ok::<_, sqlx::Error>(())
    })?;

    Ok(())
}

#[test]
#[serial]
fn postgres_writer_detects_concurrency_conflict() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = match std::env::var("PG_TEST_DSN") {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };

    let runtime = Builder::new_current_thread().enable_all().build()?;
    let table_id = Uuid::new_v4();
    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        common::reset_catalog(&pool).await?;
        bootstrap_table(&pool, table_id).await?;
        Ok::<_, sqlx::Error>(())
    })?;

    let writer =
        PostgresTxnLogWriter::connect(format!("deltasql://postgres/{table_id}"), table_id, &dsn)?;

    let mut first = CommitRequest::new(INITIAL_VERSION);
    first.protocol = Some(Protocol {
        min_reader_version: 1,
        min_writer_version: 2,
    });
    first.metadata = Some(sample_metadata());
    first.add_actions.push(sample_file("a"));
    writer.commit(first)?;

    let mut stale = CommitRequest::new(INITIAL_VERSION);
    stale.add_actions.push(sample_file("b"));
    let err = writer
        .commit(stale)
        .expect_err("expected concurrency error");
    assert!(matches!(err, TxnLogError::Concurrency { .. }));
    Ok(())
}

#[test]
#[serial]
fn postgres_writer_idempotent_app_transaction() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = match std::env::var("PG_TEST_DSN") {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };

    let runtime = Builder::new_current_thread().enable_all().build()?;
    let table_id = Uuid::new_v4();
    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        common::reset_catalog(&pool).await?;
        bootstrap_table(&pool, table_id).await?;
        Ok::<_, sqlx::Error>(())
    })?;

    let writer =
        PostgresTxnLogWriter::connect(format!("deltasql://postgres/{table_id}"), table_id, &dsn)?;

    let marker = AppTransaction::new("job", 42);

    let mut first = CommitRequest::new(INITIAL_VERSION);
    first.protocol = Some(Protocol {
        min_reader_version: 1,
        min_writer_version: 2,
    });
    first.metadata = Some(sample_metadata());
    first.add_actions.push(sample_file("file"));
    first.app_transaction = Some(marker.clone());
    let initial = writer.commit(first)?;
    assert_eq!(initial.version, 0);

    let mut retry = CommitRequest::new(INITIAL_VERSION);
    retry.add_actions.push(sample_file("file"));
    retry.app_transaction = Some(marker.clone());
    let replay = writer.commit(retry)?;
    assert_eq!(replay.version, 0);

    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let add_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dl_add_files WHERE table_id = $1")
                .bind(table_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(add_count, 1);

        let txn_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dl_txn_actions WHERE table_id = $1")
                .bind(table_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(txn_count, 1);
        Ok::<_, sqlx::Error>(())
    })?;

    Ok(())
}

fn sample_metadata() -> TableMetadata {
    TableMetadata::new(
        r#"{"type":"struct","fields":[{"name":"id","type":"long"}]}"#,
        vec!["id".to_string()],
        std::collections::HashMap::new(),
    )
}

fn sample_file(name: &str) -> ActiveFile {
    ActiveFile::new(
        format!("{name}.parquet"),
        128,
        Utc::now().timestamp_millis(),
        std::collections::HashMap::from([(String::from("id"), String::from("0"))]),
    )
}

async fn bootstrap_table(pool: &sqlx::PgPool, table_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO dl_tables(table_id, name, location, protocol_min_reader, protocol_min_writer)
            VALUES ($1, 'demo', 's3://demo', 1, 2)"#,
    )
    .bind(table_id)
    .execute(pool)
    .await?;
    Ok(())
}
