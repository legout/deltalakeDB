#[path = "common.rs"]
mod common;

use chrono::{Duration, Utc};
use deltalakedb_core::txn_log::TxnLogReader;
use deltalakedb_sql::PostgresTxnLogReader;
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[test]
#[serial]
fn postgres_reader_smoke() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = match std::env::var("PG_TEST_DSN") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("skipping postgres_reader_smoke; set PG_TEST_DSN to run");
            return Ok(());
        }
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let table_id = Uuid::new_v4();
    let earlier = Utc::now() - Duration::seconds(5);
    let later = Utc::now();

    runtime.block_on(async {
        let pool = PgPoolOptions::new().max_connections(1).connect(&dsn).await?;

        common::reset_catalog(&pool).await?;

        sqlx::query(
            r#"INSERT INTO dl_tables(table_id, name, location, protocol_min_reader, protocol_min_writer)
            VALUES ($1, 'demo', 's3://demo', 2, 5)"#,
        )
        .bind(table_id)
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT INTO dl_table_heads(table_id, current_version) VALUES ($1, 1)",
        )
        .bind(table_id)
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT INTO dl_table_versions(table_id, version, committed_at, committer, operation) VALUES ($1, 0, $2, 'user', 'WRITE')",
        )
        .bind(table_id)
        .bind(earlier)
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT INTO dl_table_versions(table_id, version, committed_at, committer, operation) VALUES ($1, 1, $2, 'user', 'WRITE')",
        )
        .bind(table_id)
        .bind(later)
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT INTO dl_metadata_updates(table_id, version, schema_json, partition_columns, table_properties) VALUES ($1, 0, $2, $3, $4)",
        )
        .bind(table_id)
        .bind(serde_json::json!({
            "type": "struct",
            "fields": [{"name": "id", "type": "long"}]
        }))
        .bind(vec!["date".to_string()])
        .bind(serde_json::json!({"delta.appendOnly": "false"}))
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT INTO dl_protocol_updates(table_id, version, min_reader_version, min_writer_version) VALUES ($1, 0, 2, 5)",
        )
        .bind(table_id)
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT INTO dl_add_files(table_id, version, path, size_bytes, partition_values, modification_time) VALUES ($1, 0, 'part-000', 100, $2, 1)",
        )
        .bind(table_id)
        .bind(serde_json::json!({"date": "2024-01-01"}))
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT INTO dl_add_files(table_id, version, path, size_bytes, partition_values, modification_time) VALUES ($1, 1, 'part-001', 200, $2, 2)",
        )
        .bind(table_id)
        .bind(serde_json::json!({"date": "2024-01-02"}))
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT INTO dl_remove_files(table_id, version, path, deletion_timestamp) VALUES ($1, 1, 'part-000', 2)",
        )
        .bind(table_id)
        .execute(&pool)
        .await?;

        Ok::<_, sqlx::Error>(())
    })?;

    let table_uri = format!("deltasql://postgres/{}", table_id);
    let reader = PostgresTxnLogReader::connect(table_uri, table_id, dsn)?;

    assert_eq!(reader.current_version()?, 1);

    let latest = reader.snapshot_at_version(None)?;
    assert_eq!(latest.version, 1);
    assert_eq!(latest.files.len(), 1);
    assert_eq!(latest.files[0].path, "part-001");
    assert_eq!(
        latest.files[0].partition_values.get("date"),
        Some(&"2024-01-02".to_string())
    );
    assert_eq!(latest.metadata.partition_columns, vec!["date".to_string()]);
    assert_eq!(latest.protocol.min_reader_version, 2);
    assert_eq!(
        latest.properties.get("delta.appendOnly"),
        Some(&"false".to_string())
    );

    let version_zero = reader.snapshot_at_version(Some(0))?;
    assert_eq!(version_zero.version, 0);
    assert_eq!(version_zero.files.len(), 1);
    assert_eq!(version_zero.files[0].path, "part-000");

    let ts = earlier.timestamp_millis();
    let via_ts = reader.snapshot_by_timestamp(ts)?;
    assert_eq!(via_ts.version, 0);

    Ok(())
}
