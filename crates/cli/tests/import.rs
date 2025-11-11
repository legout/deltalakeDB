use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::Result;
use deltalakedb_cli::{run_import, ImportConfig};
use deltalakedb_core::txn_log::{
    ActiveFile, CommitRequest, FileTxnLogReader, FileTxnLogWriter, Protocol, RemovedFile,
    TableMetadata, TableSnapshot, TxnLogReader, TxnLogWriter, INITIAL_VERSION,
};
use deltalakedb_mirror::CheckpointSerializer;
use deltalakedb_sql::SqliteTxnLogReader;
use serde_json::{json, Value as JsonValue};
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use tempfile::{tempdir, NamedTempFile};
use tokio::runtime::Runtime;

#[test]
fn import_without_checkpoint() -> Result<()> {
    run_import_scenario(false)
}

#[test]
fn import_with_checkpoint() -> Result<()> {
    run_import_scenario(true)
}

fn run_import_scenario(with_checkpoint: bool) -> Result<()> {
    let table_dir = tempdir()?;
    build_sample_log(table_dir.path())?;
    let expected_snapshot = FileTxnLogReader::new(table_dir.path()).snapshot_at_version(None)?;
    if with_checkpoint {
        write_checkpoint(table_dir.path())?;
    }

    let db = NamedTempFile::new()?;
    let dsn = format!("sqlite://{}", db.path().display());
    let summary = {
        let rt = Runtime::new().expect("runtime");
        rt.block_on(async {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&dsn)
                .await?;
            apply_sqlite_schema(&pool).await?;
            drop(pool);
            run_import(ImportConfig {
                table_path: table_dir.path().to_path_buf(),
                dsn: dsn.clone(),
                schema: None,
                table: Some("demo_table".into()),
            })
            .await
        })?
    };

    let table_uri = format!("file://{}", table_dir.path().display());
    let reader = SqliteTxnLogReader::connect(table_uri, summary.table_id, &dsn)?;
    let sql_snapshot = reader.snapshot_at_version(None)?;

    assert_snapshots_match(&sql_snapshot, &expected_snapshot);
    assert_eq!(summary.current_version, expected_snapshot.version);
    Ok(())
}

fn build_sample_log(table_path: &Path) -> Result<()> {
    let writer = FileTxnLogWriter::new(table_path);
    let mut request = CommitRequest::new(INITIAL_VERSION);
    request.metadata = Some(sample_metadata());
    request.protocol = Some(sample_protocol());
    request
        .add_actions
        .push(sample_file("part-000.parquet", "2024-01-01"));
    writer.commit(request)?;

    let mut request = CommitRequest::new(0);
    request
        .add_actions
        .push(sample_file("part-001.parquet", "2024-01-02"));
    request
        .remove_actions
        .push(RemovedFile::new("part-000.parquet", Some(2)));
    writer.commit(request)?;
    Ok(())
}

fn write_checkpoint(table_path: &Path) -> Result<()> {
    let reader = FileTxnLogReader::new(table_path);
    let snapshot = reader.snapshot_at_version(None)?;
    let bytes = CheckpointSerializer::serialize(
        &snapshot.protocol,
        &snapshot.metadata,
        &snapshot.files,
        &snapshot.properties,
    )?;
    let log_dir = table_path.join("_delta_log");
    fs::write(
        log_dir.join(format!("{:020}.checkpoint.parquet", snapshot.version)),
        bytes,
    )?;
    let pointer = json!({
        "version": snapshot.version,
        "size": 1,
        "parts": 1,
    });
    fs::write(log_dir.join("_last_checkpoint"), pointer.to_string())?;
    Ok(())
}

fn sample_metadata() -> TableMetadata {
    let mut config = HashMap::new();
    config.insert("delta.appendOnly".into(), "false".into());
    TableMetadata::new(
        r#"{"type":"struct","fields":[{"name":"id","type":"long"}]}"#,
        vec!["date".into()],
        config,
    )
}

fn sample_protocol() -> Protocol {
    Protocol {
        min_reader_version: 2,
        min_writer_version: 5,
    }
}

fn sample_file(path: &str, date: &str) -> ActiveFile {
    ActiveFile::new(
        path,
        128,
        0,
        HashMap::from([(String::from("date"), date.to_string())]),
    )
}

async fn apply_sqlite_schema(pool: &SqlitePool) -> Result<()> {
    let stmts = [
        r#"CREATE TABLE IF NOT EXISTS dl_tables (
                table_id TEXT PRIMARY KEY,
                name TEXT,
                location TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                protocol_min_reader INTEGER NOT NULL,
                protocol_min_writer INTEGER NOT NULL,
                properties TEXT NOT NULL DEFAULT '{}'
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_table_heads (
                table_id TEXT PRIMARY KEY,
                current_version INTEGER NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_table_versions (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                committed_at TEXT NOT NULL,
                committer TEXT,
                operation TEXT,
                operation_params TEXT,
                PRIMARY KEY (table_id, version)
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_add_files (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                path TEXT NOT NULL,
                size_bytes INTEGER,
                partition_values TEXT,
                stats TEXT,
                data_change INTEGER,
                modification_time INTEGER,
                PRIMARY KEY (table_id, version, path)
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_remove_files (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                path TEXT NOT NULL,
                deletion_timestamp INTEGER,
                data_change INTEGER,
                PRIMARY KEY (table_id, version, path)
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_metadata_updates (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                schema_json TEXT NOT NULL,
                partition_columns TEXT,
                table_properties TEXT,
                PRIMARY KEY (table_id, version)
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_protocol_updates (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                min_reader_version INTEGER NOT NULL,
                min_writer_version INTEGER NOT NULL,
                PRIMARY KEY (table_id, version)
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_txn_actions (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                app_id TEXT NOT NULL,
                last_update INTEGER NOT NULL,
                PRIMARY KEY (table_id, version, app_id)
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_mirror_status (
                table_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                digest TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (table_id, version)
            )"#,
    ];

    for stmt in stmts {
        sqlx::query(stmt).execute(pool).await?;
    }
    Ok(())
}

fn assert_snapshots_match(actual: &TableSnapshot, expected: &TableSnapshot) {
    let actual_schema: JsonValue =
        serde_json::from_str(&actual.metadata.schema_json).expect("actual schema json");
    let expected_schema: JsonValue =
        serde_json::from_str(&expected.metadata.schema_json).expect("expected schema json");
    assert_eq!(actual.version, expected.version, "version mismatch");
    assert_eq!(actual.files, expected.files, "files mismatch");
    assert_eq!(
        actual.metadata.partition_columns, expected.metadata.partition_columns,
        "partition columns mismatch"
    );
    assert_eq!(actual_schema, expected_schema, "schema mismatch");
    assert_eq!(
        actual.metadata.configuration, expected.metadata.configuration,
        "metadata config mismatch"
    );
    assert_eq!(actual.protocol, expected.protocol, "protocol mismatch");
}
