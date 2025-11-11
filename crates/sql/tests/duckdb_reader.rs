use chrono::{Duration, Utc};
use deltalakedb_core::txn_log::TxnLogReader;
use deltalakedb_sql::DuckdbTxnLogReader;
use duckdb::Connection;
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn duckdb_reader_returns_latest_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    run_with_duckdb(|path, table_id| {
        let reader = DuckdbTxnLogReader::connect("deltasql://duckdb/test", table_id, path)?;
        assert_eq!(reader.current_version()?, 1);
        let snapshot = reader.snapshot_at_version(None)?;
        assert_eq!(snapshot.version, 1);
        assert_eq!(snapshot.files.len(), 1);
        assert_eq!(snapshot.files[0].path, "part-001");
        Ok(())
    })
}

#[test]
fn duckdb_reader_time_travel() -> Result<(), Box<dyn std::error::Error>> {
    run_with_duckdb(|path, table_id| {
        let reader = DuckdbTxnLogReader::connect("deltasql://duckdb/test", table_id, path)?;
        let snapshot = reader.snapshot_at_version(Some(0))?;
        assert_eq!(snapshot.version, 0);
        assert_eq!(snapshot.files.len(), 1);
        assert_eq!(snapshot.files[0].path, "part-000");
        Ok(())
    })
}

fn run_with_duckdb<F>(f: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(&str, Uuid) -> Result<(), Box<dyn std::error::Error>>,
{
    let dir = tempdir()?;
    let db_path = dir.path().join("catalog.duckdb");
    let path = db_path.to_string_lossy().to_string();
    let table_id = Uuid::new_v4();
    bootstrap_duckdb(&path, table_id)?;
    f(&path, table_id)
}

fn bootstrap_duckdb(path: &str, table_id: Uuid) -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open(path)?;
    apply_schema(&conn)?;
    let earlier = Utc::now() - Duration::seconds(5);
    let later = Utc::now();

    conn.execute(
        "INSERT INTO dl_table_heads(table_id, current_version, updated_at) VALUES (?, 1, CURRENT_TIMESTAMP)",
        duckdb::params![table_id.to_string()],
    )?;

    insert_commit(&conn, table_id, 0, "part-000", earlier)?;
    insert_commit(&conn, table_id, 1, "part-001", later)?;
    conn.execute(
        "INSERT INTO dl_remove_files(table_id, version, path, deletion_timestamp, data_change) VALUES (?, 1, 'part-000', 0, TRUE)",
        duckdb::params![table_id.to_string()],
    )?;
    Ok(())
}

fn apply_schema(conn: &Connection) -> duckdb::Result<()> {
    let stmts = [
        r#"CREATE TABLE IF NOT EXISTS dl_table_heads (
                table_id UUID PRIMARY KEY,
                current_version BIGINT NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_table_versions (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                committed_at TIMESTAMP NOT NULL,
                committer VARCHAR,
                operation VARCHAR,
                PRIMARY KEY (table_id, version)
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_metadata_updates (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                schema_json JSON NOT NULL,
                partition_columns VARCHAR[],
                table_properties JSON,
                PRIMARY KEY (table_id, version)
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_protocol_updates (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                min_reader_version INTEGER NOT NULL,
                min_writer_version INTEGER NOT NULL,
                PRIMARY KEY (table_id, version)
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_add_files (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                path VARCHAR NOT NULL,
                size_bytes BIGINT,
                partition_values JSON,
                modification_time BIGINT,
                PRIMARY KEY (table_id, version, path)
            )"#,
        r#"CREATE TABLE IF NOT EXISTS dl_remove_files (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                path VARCHAR NOT NULL,
                deletion_timestamp BIGINT,
                data_change BOOLEAN,
                PRIMARY KEY (table_id, version, path)
            )"#,
    ];

    for stmt in stmts {
        conn.execute(stmt, [])?;
    }
    Ok(())
}

fn insert_commit(
    conn: &Connection,
    table_id: Uuid,
    version: i64,
    path: &str,
    committed_at: chrono::DateTime<Utc>,
) -> duckdb::Result<()> {
    let committed_at = committed_at.to_rfc3339();
    conn.execute(
        "INSERT INTO dl_table_versions(table_id, version, committed_at, committer, operation) VALUES (?, ?, CAST(? AS TIMESTAMP), 'test', 'WRITE')",
        duckdb::params![table_id.to_string(), version, committed_at],
    )?;

    conn.execute(
        "INSERT INTO dl_metadata_updates(table_id, version, schema_json, partition_columns, table_properties) VALUES (?, ?, '{\"type\":\"struct\"}', NULL, '{}')",
        duckdb::params![table_id.to_string(), version],
    )?;

    conn.execute(
        "INSERT INTO dl_protocol_updates(table_id, version, min_reader_version, min_writer_version) VALUES (?, ?, 2, 5)",
        duckdb::params![table_id.to_string(), version],
    )?;

    conn.execute(
        "INSERT INTO dl_add_files(table_id, version, path, size_bytes, partition_values, modification_time) VALUES (?, ?, ?, 128, '{\"date\":\"2024-01-01\"}', 1)",
        duckdb::params![table_id.to_string(), version, path],
    )?;

    Ok(())
}
