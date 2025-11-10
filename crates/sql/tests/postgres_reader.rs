use chrono::{Duration, Utc};
use deltalakedb_core::txn_log::TxnLogReader;
use deltalakedb_sql::PostgresTxnLogReader;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[test]
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

        // Best-effort cleanup so the test can be re-run.
        let drops = [
            "DROP TABLE IF EXISTS dl_mirror_status",
            "DROP TABLE IF EXISTS dl_txn_actions",
            "DROP TABLE IF EXISTS dl_protocol_updates",
            "DROP TABLE IF EXISTS dl_metadata_updates",
            "DROP TABLE IF EXISTS dl_remove_files",
            "DROP TABLE IF EXISTS dl_add_files",
            "DROP TABLE IF EXISTS dl_table_versions",
            "DROP TABLE IF EXISTS dl_table_heads",
            "DROP TABLE IF EXISTS dl_tables",
        ];
        for stmt in drops {
            sqlx::query(stmt).execute(&pool).await?;
        }

        let creates = [
            r#"CREATE TABLE dl_tables (
                table_id UUID PRIMARY KEY,
                name TEXT,
                location TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                protocol_min_reader INT NOT NULL,
                protocol_min_writer INT NOT NULL,
                properties JSONB NOT NULL DEFAULT '{}'::jsonb
            )"#,
            r#"CREATE TABLE dl_table_heads (
                table_id UUID PRIMARY KEY,
                current_version BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
            r#"CREATE TABLE dl_table_versions (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                committed_at TIMESTAMPTZ NOT NULL,
                committer TEXT,
                operation TEXT,
                operation_params JSONB,
                PRIMARY KEY (table_id, version)
            )"#,
            r#"CREATE TABLE dl_add_files (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                path TEXT NOT NULL,
                size_bytes BIGINT,
                partition_values JSONB,
                stats JSONB,
                data_change BOOLEAN DEFAULT TRUE,
                modification_time BIGINT,
                PRIMARY KEY (table_id, version, path)
            )"#,
            r#"CREATE TABLE dl_remove_files (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                path TEXT NOT NULL,
                deletion_timestamp BIGINT,
                data_change BOOLEAN DEFAULT TRUE,
                PRIMARY KEY (table_id, version, path)
            )"#,
            r#"CREATE TABLE dl_metadata_updates (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                schema_json JSONB NOT NULL,
                partition_columns TEXT[],
                table_properties JSONB,
                PRIMARY KEY (table_id, version)
            )"#,
            r#"CREATE TABLE dl_protocol_updates (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                min_reader_version INT NOT NULL,
                min_writer_version INT NOT NULL,
                PRIMARY KEY (table_id, version)
            )"#,
        ];
        for stmt in creates {
            sqlx::query(stmt).execute(&pool).await?;
        }

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
