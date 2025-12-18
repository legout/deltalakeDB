use sqlx::PgPool;

pub async fn reset_catalog(pool: &PgPool) -> Result<(), sqlx::Error> {
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
        sqlx::query(stmt).execute(pool).await?;
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
                table_id UUID PRIMARY KEY REFERENCES dl_tables(table_id) ON DELETE CASCADE,
                current_version BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
        r#"CREATE TABLE dl_table_versions (
                table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
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
        r#"CREATE TABLE dl_txn_actions (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                app_id TEXT NOT NULL,
                last_update BIGINT NOT NULL,
                PRIMARY KEY (table_id, version, app_id)
            )"#,
        r#"CREATE TABLE dl_mirror_status (
                table_id UUID NOT NULL,
                version BIGINT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                attempts INT NOT NULL DEFAULT 0,
                last_error TEXT,
                digest TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (table_id, version)
            )"#,
    ];

    for stmt in creates {
        sqlx::query(stmt).execute(pool).await?;
    }

    Ok(())
}
