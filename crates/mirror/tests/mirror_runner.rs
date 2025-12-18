#[path = "../../sql/tests/common.rs"]
mod schema;

use arrow_array::{StringArray, StructArray};
use chrono::{DateTime, Duration, Utc};
use deltalakedb_mirror::{
    AlertSink, LagAlert, LagSeverity, LocalFsObjectStore, MirrorRunner, MirrorService,
    MirrorStatus, ObjectStore,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::runtime::Builder;
use uuid::Uuid;

#[derive(Clone)]
struct FailStore;

#[async_trait::async_trait]
impl ObjectStore for FailStore {
    async fn put_file(
        &self,
        _table_location: &str,
        _file_name: &str,
        _bytes: &[u8],
    ) -> Result<(), deltalakedb_mirror::MirrorError> {
        Err(deltalakedb_mirror::MirrorError::ObjectStore(
            "simulated failure".into(),
        ))
    }
}

#[derive(Clone, Default)]
struct RecordingAlertSink {
    events: Arc<Mutex<Vec<LagAlert>>>,
}

impl AlertSink for RecordingAlertSink {
    fn emit(&self, alert: LagAlert) {
        self.events.lock().unwrap().push(alert);
    }
}

#[test]
#[serial]
fn mirror_writes_json_and_updates_status() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = match std::env::var("PG_TEST_DSN") {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };

    let runtime = Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async move {
        let pool = PgPoolOptions::new().max_connections(1).connect(&dsn).await?;
        schema::reset_catalog(&pool).await?;

        let table_id = Uuid::new_v4();
        let dir = TempDir::new()?;
        bootstrap_table(&pool, table_id, dir.path().to_string_lossy().as_ref()).await?;
        insert_commit(&pool, table_id, 0).await?;
        insert_add_action(&pool, table_id, 0, "part-000").await?;
        insert_mirror_status(&pool, table_id, 0, "PENDING").await?;

        let runner = MirrorRunner::new(pool.clone(), LocalFsObjectStore::default());
        let outcome = runner.process_next().await?.expect("job processed");
        assert_eq!(outcome.status, MirrorStatus::Succeeded);

        let log_file = dir.path().join("_delta_log").join("00000000000000000000.json");
        assert!(log_file.exists(), "expected mirrored JSON file");
        let content = std::fs::read_to_string(&log_file)?;
        let lines: Vec<_> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 4);
        let commit_info: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(commit_info["commitInfo"]["operation"], "WRITE");
        let protocol: serde_json::Value = serde_json::from_str(lines[1])?;
        assert_eq!(protocol["protocol"]["minReaderVersion"], 2);

        let status: (String, Option<String>, i32) = sqlx::query_as(
            "SELECT status, digest, attempts FROM dl_mirror_status WHERE table_id = $1 AND version = $2",
        )
        .bind(table_id)
        .bind(0_i64)
        .fetch_one(&pool)
        .await?;
        assert_eq!(status.0, "SUCCEEDED");
        assert!(status.1.is_some());
        assert_eq!(status.2, 1);

        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[test]
#[serial]
fn mirror_retries_on_failure() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = match std::env::var("PG_TEST_DSN") {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };

    let runtime = Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async move {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        schema::reset_catalog(&pool).await?;

        let table_id = Uuid::new_v4();
        let dir = TempDir::new()?;
        bootstrap_table(&pool, table_id, dir.path().to_string_lossy().as_ref()).await?;
        insert_commit(&pool, table_id, 0).await?;
        insert_add_action(&pool, table_id, 0, "part-000").await?;
        insert_mirror_status(&pool, table_id, 0, "FAILED").await?;

        let failing = MirrorRunner::new(pool.clone(), FailStore);
        let outcome = failing.process_next().await?.expect("job processed");
        assert_eq!(outcome.status, MirrorStatus::Failed);

        let status: (String, i32) = sqlx::query_as(
            "SELECT status, attempts FROM dl_mirror_status WHERE table_id = $1 AND version = $2",
        )
        .bind(table_id)
        .bind(0_i64)
        .fetch_one(&pool)
        .await?;
        assert_eq!(status.0, "FAILED");
        assert_eq!(status.1, 1);

        let runner = MirrorRunner::new(pool.clone(), LocalFsObjectStore::default());
        let outcome = runner.process_next().await?.expect("job processed");
        assert_eq!(outcome.status, MirrorStatus::Succeeded);

        let status: (String, i32) = sqlx::query_as(
            "SELECT status, attempts FROM dl_mirror_status WHERE table_id = $1 AND version = $2",
        )
        .bind(table_id)
        .bind(0_i64)
        .fetch_one(&pool)
        .await?;
        assert_eq!(status.0, "SUCCEEDED");
        assert_eq!(status.1, 2);

        let log_file = dir
            .path()
            .join("_delta_log")
            .join("00000000000000000000.json");
        assert!(log_file.exists());

        Ok::<_, Box<dyn std::error::Error>>(())
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

async fn insert_commit(
    pool: &sqlx::PgPool,
    table_id: Uuid,
    version: i64,
) -> Result<(), sqlx::Error> {
    insert_commit_with_time(pool, table_id, version, Utc::now()).await
}

async fn insert_commit_with_time(
    pool: &sqlx::PgPool,
    table_id: Uuid,
    version: i64,
    committed_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO dl_table_versions(table_id, version, committed_at, committer, operation, operation_params)
            VALUES ($1, $2, $3, 'mirror-test', 'WRITE', $4)"#,
    )
    .bind(table_id)
    .bind(version)
    .bind(committed_at)
    .bind(serde_json::json!({"mode": "Append"}))
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO dl_protocol_updates(table_id, version, min_reader_version, min_writer_version) VALUES ($1, $2, 2, 5)",
    )
    .bind(table_id)
    .bind(version)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"INSERT INTO dl_metadata_updates(table_id, version, schema_json, partition_columns, table_properties)
            VALUES ($1, $2, $3, $4, $5)"#,
    )
    .bind(table_id)
    .bind(version)
    .bind(serde_json::json!({
        "type": "struct",
        "fields": [{"name": "id", "type": "long"}]
    }))
    .bind(vec!["date".to_string()])
    .bind(serde_json::json!({"delta.appendOnly": "false"}))
    .execute(pool)
    .await?;

    Ok(())
}

async fn insert_add_action(
    pool: &sqlx::PgPool,
    table_id: Uuid,
    version: i64,
    path: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO dl_add_files(table_id, version, path, size_bytes, partition_values, stats, data_change, modification_time)
            VALUES ($1, $2, $3, 128, $4, NULL, TRUE, 123)"#,
    )
    .bind(table_id)
    .bind(version)
    .bind(path)
    .bind(serde_json::json!({"date": "2024-01-01"}))
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_mirror_status(
    pool: &sqlx::PgPool,
    table_id: Uuid,
    version: i64,
    status: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO dl_mirror_status(table_id, version, status, attempts)
            VALUES ($1, $2, $3, 0)"#,
    )
    .bind(table_id)
    .bind(version)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

#[test]
#[serial]
fn mirror_lag_alerts_trigger_thresholds() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = match std::env::var("PG_TEST_DSN") {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };

    let runtime = Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async move {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        schema::reset_catalog(&pool).await?;

        let table_id = Uuid::new_v4();
        let dir = TempDir::new()?;
        bootstrap_table(&pool, table_id, dir.path().to_string_lossy().as_ref()).await?;
        let old_time = Utc::now() - Duration::seconds(400);
        insert_commit_with_time(&pool, table_id, 0, old_time).await?;
        insert_add_action(&pool, table_id, 0, "part-000").await?;
        insert_mirror_status(&pool, table_id, 0, "PENDING").await?;

        let runner = MirrorRunner::new(pool.clone(), LocalFsObjectStore::default());
        let sink = RecordingAlertSink::default();
        let service = MirrorService::new(runner, sink.clone()).with_lag_thresholds(60, 300);
        let alerts = service.check_lag().await?;
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].severity, LagSeverity::Critical);
        assert!(alerts[0].lag_seconds >= 300);
        assert_eq!(alerts[0].table_id, table_id);

        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}

#[test]
#[serial]
fn mirror_emits_checkpoint_on_interval() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = match std::env::var("PG_TEST_DSN") {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };

    let runtime = Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async move {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        schema::reset_catalog(&pool).await?;

        let table_id = Uuid::new_v4();
        let dir = TempDir::new()?;
        bootstrap_table(&pool, table_id, dir.path().to_string_lossy().as_ref()).await?;
        insert_commit(&pool, table_id, 0).await?;
        insert_add_action(&pool, table_id, 0, "part-000").await?;
        insert_mirror_status(&pool, table_id, 0, "PENDING").await?;

        let runner = MirrorRunner::new(pool.clone(), LocalFsObjectStore::default());
        let service = MirrorService::new(runner, RecordingAlertSink::default())
            .with_checkpoint_interval(1)
            .with_lag_thresholds(600, 1200);
        service.run_once().await?;

        let checkpoint = dir
            .path()
            .join("_delta_log")
            .join("00000000000000000000.checkpoint.parquet");
        assert!(checkpoint.exists(), "checkpoint file not written");

        let file = std::fs::File::open(&checkpoint)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;
        let batch = reader.next().transpose()?.expect("batch");
        let add_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("add struct");
        let path_array = add_array
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("path array");
        let has_part = path_array.iter().flatten().any(|value| value == "part-000");
        assert!(has_part, "checkpoint missing expected file path");

        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
