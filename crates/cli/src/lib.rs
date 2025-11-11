use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use arrow_array::{Array, Int32Array, Int64Array, ListArray, StringArray, StructArray};
use chrono::{DateTime, TimeZone, Utc};
use deltalakedb_core::delta::{json_value_to_string, DeltaAction};
use deltalakedb_core::txn_log::{ActiveFile, Protocol, RemovedFile, TableMetadata, Version};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;
use serde_json::{json, Map as JsonMap, Value};
use sqlx::{postgres::PgPoolOptions, sqlite::SqlitePoolOptions, PgPool, SqlitePool};
use uuid::Uuid;

/// Configuration for running the `dl import` command.
pub struct ImportConfig {
    pub table_path: PathBuf,
    pub dsn: String,
    pub schema: Option<String>,
    pub table: Option<String>,
}

/// Result of a successful import run.
pub struct ImportSummary {
    pub table_id: Uuid,
    pub current_version: Version,
    pub commits: usize,
}

/// Executes the import workflow end-to-end.
pub async fn run_import(config: ImportConfig) -> Result<ImportSummary> {
    let table_root = config.table_path.canonicalize().with_context(|| {
        format!(
            "unable to resolve table path {}",
            config.table_path.display()
        )
    })?;
    let log_dir = table_root.join("_delta_log");
    if !log_dir.exists() {
        bail!(
            "missing _delta_log directory under {}",
            table_root.display()
        );
    }

    let commits = build_timeline(&log_dir)?;
    if commits.is_empty() {
        bail!("no commits discovered under {}", log_dir.display());
    }

    let (final_protocol, final_metadata) = latest_state(&commits)?;
    let location = table_root.to_string_lossy().to_string();
    let table_name = config
        .table
        .clone()
        .or_else(|| {
            table_root
                .file_name()
                .and_then(|os| os.to_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "delta_table".to_string());
    let schema = config
        .schema
        .clone()
        .unwrap_or_else(|| "public".to_string());
    let full_name = format!("{}.{}", schema, table_name);

    let catalog = CatalogConnection::connect(&config.dsn).await?;
    catalog
        .bootstrap(
            &full_name,
            &location,
            &final_protocol,
            &final_metadata,
            &commits,
        )
        .await
}

/// Represents all actions captured for a particular version.
#[derive(Debug, Clone)]
struct VersionActions {
    version: Version,
    timestamp_millis: i64,
    protocol: Option<Protocol>,
    metadata: Option<TableMetadata>,
    add_files: Vec<ActiveFile>,
    remove_files: Vec<RemovedFile>,
    operation: String,
    operation_params: Option<Value>,
}

impl VersionActions {
    fn action_count(&self) -> usize {
        let mut count = self.add_files.len() + self.remove_files.len();
        if self.metadata.is_some() {
            count += 1;
        }
        if self.protocol.is_some() {
            count += 1;
        }
        count
    }
}

#[derive(Clone)]
enum CatalogConnection {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl CatalogConnection {
    async fn connect(dsn: &str) -> Result<Self> {
        if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect(dsn)
                .await
                .with_context(|| "failed to connect to Postgres")?;
            Ok(CatalogConnection::Postgres(pool))
        } else if dsn.starts_with("sqlite://") {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(dsn)
                .await
                .with_context(|| "failed to connect to SQLite")?;
            Ok(CatalogConnection::Sqlite(pool))
        } else {
            bail!("unsupported DSN: {}", dsn);
        }
    }

    async fn bootstrap(
        &self,
        name: &str,
        location: &str,
        protocol: &Protocol,
        metadata: &TableMetadata,
        commits: &[VersionActions],
    ) -> Result<ImportSummary> {
        match self {
            CatalogConnection::Postgres(pool) => {
                import_into_postgres(pool, name, location, protocol, metadata, commits).await
            }
            CatalogConnection::Sqlite(pool) => {
                import_into_sqlite(pool, name, location, protocol, metadata, commits).await
            }
        }
    }
}

async fn import_into_postgres(
    pool: &PgPool,
    name: &str,
    location: &str,
    protocol: &Protocol,
    metadata: &TableMetadata,
    commits: &[VersionActions],
) -> Result<ImportSummary> {
    let existing: Option<Uuid> =
        sqlx::query_scalar("SELECT table_id FROM dl_tables WHERE location = $1")
            .bind(location)
            .fetch_optional(pool)
            .await
            .context("failed to query dl_tables")?;
    let table_id = existing.unwrap_or_else(Uuid::new_v4);

    let props = Value::Object(map_to_json_map(&metadata.configuration));
    sqlx::query(
        r#"
        INSERT INTO dl_tables(table_id, name, location, protocol_min_reader, protocol_min_writer, properties)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (table_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            location = EXCLUDED.location,
            protocol_min_reader = EXCLUDED.protocol_min_reader,
            protocol_min_writer = EXCLUDED.protocol_min_writer,
            properties = EXCLUDED.properties
        "#,
    )
    .bind(table_id)
    .bind(name)
    .bind(location)
    .bind(protocol.min_reader_version as i32)
    .bind(protocol.min_writer_version as i32)
    .bind(&props)
    .execute(pool)
    .await
    .context("failed to upsert dl_tables")?;

    clear_postgres_tables(pool, table_id).await?;
    ensure_head_row_postgres(pool, table_id).await?;

    for commit in commits {
        insert_postgres_commit(pool, table_id, commit).await?;
    }

    if let Some(last) = commits.last() {
        update_postgres_head(pool, table_id, last.version).await?;
        return Ok(ImportSummary {
            table_id,
            current_version: last.version,
            commits: commits.len(),
        });
    }

    bail!("no commits processed for table {}", name)
}

async fn clear_postgres_tables(pool: &PgPool, table_id: Uuid) -> Result<()> {
    let tables = [
        "dl_add_files",
        "dl_remove_files",
        "dl_metadata_updates",
        "dl_protocol_updates",
        "dl_txn_actions",
        "dl_mirror_status",
        "dl_table_versions",
        "dl_table_heads",
    ];

    for table in tables {
        let query = format!("DELETE FROM {table} WHERE table_id = $1");
        sqlx::query(&query)
            .bind(table_id)
            .execute(pool)
            .await
            .with_context(|| format!("failed clearing {table}"))?;
    }

    Ok(())
}

async fn ensure_head_row_postgres(pool: &PgPool, table_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO dl_table_heads(table_id, current_version)
        VALUES ($1, $2)
        ON CONFLICT (table_id) DO NOTHING
        "#,
    )
    .bind(table_id)
    .bind(-1_i64)
    .execute(pool)
    .await
    .context("failed to create table head")?;
    Ok(())
}

async fn insert_postgres_commit(
    pool: &PgPool,
    table_id: Uuid,
    commit: &VersionActions,
) -> Result<()> {
    let committed_at = millis_to_datetime(commit.timestamp_millis);
    let params = build_operation_params(
        &commit.operation,
        commit.action_count(),
        &commit.operation_params,
    );

    sqlx::query(
        r#"
        INSERT INTO dl_table_versions
            (table_id, version, committed_at, committer, operation, operation_params)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(table_id)
    .bind(commit.version)
    .bind(committed_at)
    .bind("dl-import")
    .bind(&commit.operation)
    .bind(params)
    .execute(pool)
    .await
    .context("failed to insert table version")?;

    if let Some(protocol) = &commit.protocol {
        sqlx::query(
            r#"
            INSERT INTO dl_protocol_updates
                (table_id, version, min_reader_version, min_writer_version)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(table_id)
        .bind(commit.version)
        .bind(protocol.min_reader_version as i32)
        .bind(protocol.min_writer_version as i32)
        .execute(pool)
        .await
        .context("failed to insert protocol")?;
    }

    if let Some(meta) = &commit.metadata {
        let schema_value: Value = serde_json::from_str(&meta.schema_json)
            .context("invalid schema json in commit metadata")?;
        let props = Value::Object(map_to_json_map(&meta.configuration));
        sqlx::query(
            r#"
            INSERT INTO dl_metadata_updates
                (table_id, version, schema_json, partition_columns, table_properties)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(table_id)
        .bind(commit.version)
        .bind(schema_value)
        .bind(meta.partition_columns.clone())
        .bind(props)
        .execute(pool)
        .await
        .context("failed to insert metadata")?;
    }

    for file in &commit.add_files {
        let size_bytes: i64 = file
            .size_bytes
            .try_into()
            .map_err(|_| anyhow!("file size exceeds i64"))?;
        let partitions = Value::Object(map_to_json_map(&file.partition_values));
        sqlx::query(
            r#"
            INSERT INTO dl_add_files
                (table_id, version, path, size_bytes, partition_values, stats, data_change, modification_time)
            VALUES ($1, $2, $3, $4, $5, NULL, TRUE, $6)
            "#,
        )
        .bind(table_id)
        .bind(commit.version)
        .bind(&file.path)
        .bind(size_bytes)
        .bind(partitions)
        .bind(file.modification_time)
        .execute(pool)
        .await
        .context("failed to insert add file")?;
    }

    for file in &commit.remove_files {
        sqlx::query(
            r#"
            INSERT INTO dl_remove_files
                (table_id, version, path, deletion_timestamp, data_change)
            VALUES ($1, $2, $3, $4, TRUE)
            "#,
        )
        .bind(table_id)
        .bind(commit.version)
        .bind(&file.path)
        .bind(file.deletion_timestamp)
        .execute(pool)
        .await
        .context("failed to insert remove file")?;
    }

    Ok(())
}

async fn update_postgres_head(pool: &PgPool, table_id: Uuid, version: Version) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO dl_table_heads(table_id, current_version)
        VALUES ($1, $2)
        ON CONFLICT (table_id)
        DO UPDATE SET current_version = EXCLUDED.current_version, updated_at = now()
        "#,
    )
    .bind(table_id)
    .bind(version)
    .execute(pool)
    .await
    .context("failed to update table head")?;
    Ok(())
}

async fn import_into_sqlite(
    pool: &SqlitePool,
    name: &str,
    location: &str,
    protocol: &Protocol,
    metadata: &TableMetadata,
    commits: &[VersionActions],
) -> Result<ImportSummary> {
    let existing: Option<String> =
        sqlx::query_scalar("SELECT table_id FROM dl_tables WHERE location = ?")
            .bind(location)
            .fetch_optional(pool)
            .await
            .context("failed to query dl_tables")?;
    let table_id = existing
        .as_deref()
        .and_then(|value| Uuid::parse_str(value).ok())
        .unwrap_or_else(Uuid::new_v4);

    let props = serde_json::to_string(&metadata.configuration)
        .context("failed to encode table properties")?;
    sqlx::query(
        r#"
        INSERT INTO dl_tables(table_id, name, location, protocol_min_reader, protocol_min_writer, properties)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(table_id)
        DO UPDATE SET
            name = excluded.name,
            location = excluded.location,
            protocol_min_reader = excluded.protocol_min_reader,
            protocol_min_writer = excluded.protocol_min_writer,
            properties = excluded.properties
        "#,
    )
    .bind(table_id)
    .bind(name)
    .bind(location)
    .bind(protocol.min_reader_version as i64)
    .bind(protocol.min_writer_version as i64)
    .bind(props)
    .execute(pool)
    .await
    .context("failed to upsert dl_tables")?;

    clear_sqlite_tables(pool, table_id).await?;
    ensure_head_row_sqlite(pool, table_id).await?;

    for commit in commits {
        insert_sqlite_commit(pool, table_id, commit).await?;
    }

    if let Some(last) = commits.last() {
        update_sqlite_head(pool, table_id, last.version).await?;
        return Ok(ImportSummary {
            table_id,
            current_version: last.version,
            commits: commits.len(),
        });
    }

    bail!("no commits processed for table {}", name)
}

async fn clear_sqlite_tables(pool: &SqlitePool, table_id: Uuid) -> Result<()> {
    let tables = [
        "dl_add_files",
        "dl_remove_files",
        "dl_metadata_updates",
        "dl_protocol_updates",
        "dl_txn_actions",
        "dl_mirror_status",
        "dl_table_versions",
        "dl_table_heads",
    ];
    for table in tables {
        let query = format!("DELETE FROM {table} WHERE table_id = ?");
        sqlx::query(&query)
            .bind(table_id)
            .execute(pool)
            .await
            .with_context(|| format!("failed clearing {table}"))?;
    }
    Ok(())
}

async fn ensure_head_row_sqlite(pool: &SqlitePool, table_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO dl_table_heads(table_id, current_version, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(table_id) DO NOTHING
        "#,
    )
    .bind(table_id)
    .bind(-1_i64)
    .execute(pool)
    .await
    .context("failed to create table head")?;
    Ok(())
}

async fn insert_sqlite_commit(
    pool: &SqlitePool,
    table_id: Uuid,
    commit: &VersionActions,
) -> Result<()> {
    let params = build_operation_params(
        &commit.operation,
        commit.action_count(),
        &commit.operation_params,
    );
    let params_str = serde_json::to_string(&params).context("failed to encode op params")?;
    let committed_at = millis_to_datetime(commit.timestamp_millis).to_rfc3339();

    sqlx::query(
        r#"
        INSERT INTO dl_table_versions
            (table_id, version, committed_at, committer, operation, operation_params)
        VALUES (?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(table_id)
    .bind(commit.version)
    .bind(committed_at)
    .bind("dl-import")
    .bind(&commit.operation)
    .bind(params_str)
    .execute(pool)
    .await
    .context("failed to insert table version")?;

    if let Some(protocol) = &commit.protocol {
        sqlx::query(
            r#"
            INSERT INTO dl_protocol_updates
                (table_id, version, min_reader_version, min_writer_version)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(table_id)
        .bind(commit.version)
        .bind(protocol.min_reader_version as i64)
        .bind(protocol.min_writer_version as i64)
        .execute(pool)
        .await
        .context("failed to insert protocol")?;
    }

    if let Some(meta) = &commit.metadata {
        let partitions = serde_json::to_string(&meta.partition_columns)
            .context("failed to encode partition columns")?;
        let props = serde_json::to_string(&meta.configuration)
            .context("failed to encode table properties")?;
        sqlx::query(
            r#"
            INSERT INTO dl_metadata_updates
                (table_id, version, schema_json, partition_columns, table_properties)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind(table_id)
        .bind(commit.version)
        .bind(&meta.schema_json)
        .bind(partitions)
        .bind(props)
        .execute(pool)
        .await
        .context("failed to insert metadata")?;
    }

    for file in &commit.add_files {
        let partitions = serde_json::to_string(&file.partition_values)
            .context("failed to encode partition values")?;
        sqlx::query(
            r#"
            INSERT INTO dl_add_files
                (table_id, version, path, size_bytes, partition_values, stats, data_change, modification_time)
            VALUES (?, ?, ?, ?, ?, NULL, 1, ?)
            "#,
        )
        .bind(table_id)
        .bind(commit.version)
        .bind(&file.path)
        .bind(file.size_bytes as i64)
        .bind(partitions)
        .bind(file.modification_time)
        .execute(pool)
        .await
        .context("failed to insert add file")?;
    }

    for file in &commit.remove_files {
        sqlx::query(
            r#"
            INSERT INTO dl_remove_files
                (table_id, version, path, deletion_timestamp, data_change)
            VALUES (?, ?, ?, ?, 1)
            "#,
        )
        .bind(table_id)
        .bind(commit.version)
        .bind(&file.path)
        .bind(file.deletion_timestamp)
        .execute(pool)
        .await
        .context("failed to insert remove file")?;
    }

    Ok(())
}

async fn update_sqlite_head(pool: &SqlitePool, table_id: Uuid, version: Version) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO dl_table_heads(table_id, current_version, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(table_id) DO UPDATE SET current_version = excluded.current_version, updated_at = datetime('now')
        "#,
    )
        .bind(table_id)
    .bind(version)
    .execute(pool)
    .await
    .context("failed to update table head")?;
    Ok(())
}

fn map_to_json_map(map: &HashMap<String, String>) -> JsonMap<String, Value> {
    map.iter()
        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
        .collect()
}

fn millis_to_datetime(ts: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ts)
        .single()
        .unwrap_or_else(|| Utc.timestamp_millis_opt(0).single().expect("unix epoch"))
}

fn latest_state(commits: &[VersionActions]) -> Result<(Protocol, TableMetadata)> {
    let mut protocol = None;
    let mut metadata = None;
    for commit in commits {
        if let Some(proto) = &commit.protocol {
            protocol = Some(proto.clone());
        }
        if let Some(meta) = &commit.metadata {
            metadata = Some(meta.clone());
        }
    }
    let protocol = protocol.ok_or_else(|| anyhow!("no protocol information found in commits"))?;
    let metadata = metadata.ok_or_else(|| anyhow!("no metadata information found in commits"))?;
    Ok((protocol, metadata))
}

fn build_timeline(log_dir: &Path) -> Result<Vec<VersionActions>> {
    let mut commits = Vec::new();
    if let Some(checkpoint) = read_checkpoint(log_dir)? {
        commits.push(checkpoint);
    }

    let start_version = commits.last().map(|c| c.version + 1).unwrap_or(0);
    let mut files = collect_json_commits(log_dir)?;
    files.retain(|entry| entry.version >= start_version);
    files.sort_by_key(|entry| entry.version);

    for entry in files {
        commits.push(parse_commit_file(&entry.path, entry.version)?);
    }

    Ok(commits)
}

fn parse_commit_file(path: &Path, version: Version) -> Result<VersionActions> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read commit file {}", path.display()))?;
    match parse_delta_actions(&contents, version, path) {
        Ok(commit) => Ok(commit),
        Err(err) => parse_internal_commit(&contents, version)
            .with_context(|| format!("failed to parse {} as Delta JSON: {err}", path.display())),
    }
}

fn parse_delta_actions(contents: &str, version: Version, path: &Path) -> Result<VersionActions> {
    let mut commit = VersionActions {
        version,
        timestamp_millis: file_timestamp(path)?.unwrap_or_else(current_timestamp_millis),
        protocol: None,
        metadata: None,
        add_files: Vec::new(),
        remove_files: Vec::new(),
        operation: "UNKNOWN".into(),
        operation_params: None,
    };

    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let action: DeltaAction = serde_json::from_str(line)
            .with_context(|| format!("invalid JSON action in {}", path.display()))?;
        match action {
            DeltaAction::CommitInfo { commit_info } => {
                commit.timestamp_millis = commit_info.timestamp;
                commit.operation = commit_info.operation;
                commit.operation_params = commit_info.operation_parameters;
            }
            DeltaAction::Protocol { protocol } => {
                commit.protocol = Some(Protocol {
                    min_reader_version: protocol.min_reader_version,
                    min_writer_version: protocol.min_writer_version,
                });
            }
            DeltaAction::MetaData { meta_data } => {
                commit.metadata = Some(TableMetadata::new(
                    meta_data.schema_string,
                    meta_data.partition_columns,
                    meta_data.configuration,
                ));
            }
            DeltaAction::Add { add } => {
                let partitions = add
                    .partition_values
                    .into_iter()
                    .map(|(k, v)| (k, json_value_to_string(v)))
                    .collect();
                let file =
                    ActiveFile::new(add.path, add.size as u64, add.modification_time, partitions);
                commit.add_files.push(file);
            }
            DeltaAction::Remove { remove } => {
                let file = RemovedFile::new(remove.path, remove.deletion_timestamp);
                commit.remove_files.push(file);
            }
        }
    }

    Ok(commit)
}

#[derive(Deserialize)]
struct InternalCommitFile {
    timestamp_millis: i64,
    entries: Vec<InternalCommitEntry>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InternalCommitEntry {
    Metadata(TableMetadata),
    Protocol(Protocol),
    SetProperties { properties: HashMap<String, String> },
    Add { file: ActiveFile },
    Remove { file: RemovedFile },
}

fn parse_internal_commit(contents: &str, version: Version) -> Result<VersionActions> {
    let file: InternalCommitFile = serde_json::from_str(contents)?;
    let mut commit = VersionActions {
        version,
        timestamp_millis: file.timestamp_millis,
        protocol: None,
        metadata: None,
        add_files: Vec::new(),
        remove_files: Vec::new(),
        operation: "LEGACY_IMPORT".into(),
        operation_params: None,
    };

    for entry in file.entries {
        match entry {
            InternalCommitEntry::Metadata(meta) => commit.metadata = Some(meta),
            InternalCommitEntry::Protocol(proto) => commit.protocol = Some(proto),
            InternalCommitEntry::Add { file } => commit.add_files.push(file),
            InternalCommitEntry::Remove { file } => commit.remove_files.push(file),
            InternalCommitEntry::SetProperties { properties } => {
                if let Some(meta) = &mut commit.metadata {
                    meta.configuration.extend(properties);
                }
            }
        }
    }

    Ok(commit)
}

fn collect_json_commits(log_dir: &Path) -> Result<Vec<LogEntry>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(log_dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        if let Some(version) = parse_version_from_name(&file_name) {
            entries.push(LogEntry {
                version,
                path: entry.path(),
            });
        }
    }
    Ok(entries)
}

fn parse_version_from_name(name: &std::ffi::OsStr) -> Option<Version> {
    let name = name.to_string_lossy();
    if !name.ends_with(".json") {
        return None;
    }
    let numeric = &name[..name.len().saturating_sub(5)];
    numeric.parse().ok()
}

struct LogEntry {
    version: Version,
    path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct LastCheckpointPointer {
    version: Version,
    parts: Option<i64>,
}

fn read_checkpoint(log_dir: &Path) -> Result<Option<VersionActions>> {
    let pointer_path = log_dir.join("_last_checkpoint");
    if !pointer_path.exists() {
        return Ok(None);
    }

    let data = fs::read_to_string(&pointer_path)
        .with_context(|| format!("failed to read {}", pointer_path.display()))?;
    let pointer: LastCheckpointPointer = serde_json::from_str(&data)
        .with_context(|| format!("invalid checkpoint pointer {}", pointer_path.display()))?;
    if pointer.parts.unwrap_or(1) != 1 {
        bail!("multi-part checkpoints are not supported yet");
    }

    let file_path = log_dir.join(format!("{:020}.checkpoint.parquet", pointer.version));
    if !file_path.exists() {
        return Ok(None);
    }

    let file = fs::File::open(&file_path)
        .with_context(|| format!("failed to open {}", file_path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|err| anyhow!("failed to read checkpoint parquet: {err}"))?;
    let mut reader = builder.build().map_err(|err| anyhow!("{err}"))?;

    let mut protocol = None;
    let mut metadata = None;
    let mut files = Vec::new();

    while let Some(batch) = reader.next() {
        let batch = batch.map_err(|err| anyhow!("failed reading checkpoint batch: {err}"))?;
        let add = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| anyhow!("invalid checkpoint schema"))?;
        let meta = batch
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| anyhow!("invalid checkpoint schema"))?;
        let proto = batch
            .column(3)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| anyhow!("invalid checkpoint schema"))?;

        append_add_actions(add, &mut files)?;
        if let Some(meta_value) = parse_metadata_action(meta)? {
            metadata = Some(meta_value);
        }
        if let Some(proto_value) = parse_protocol_action(proto)? {
            protocol = Some(proto_value);
        }
    }

    let protocol = protocol.ok_or_else(|| anyhow!("checkpoint missing protocol"))?;
    let metadata = metadata.ok_or_else(|| anyhow!("checkpoint missing metadata"))?;

    Ok(Some(VersionActions {
        version: pointer.version,
        timestamp_millis: current_timestamp_millis(),
        protocol: Some(protocol),
        metadata: Some(metadata),
        add_files: files,
        remove_files: Vec::new(),
        operation: "CHECKPOINT_IMPORT".into(),
        operation_params: Some(json!({ "source": "checkpoint" })),
    }))
}

fn append_add_actions(array: &StructArray, files: &mut Vec<ActiveFile>) -> Result<()> {
    if array.is_empty() {
        return Ok(());
    }
    let path_col = array
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("missing add.path column"))?;
    let size_col = array
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("missing add.size column"))?;
    let partitions_col = array
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("missing add.partitionValues column"))?;
    let mod_time_col = array
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("missing add.modificationTime column"))?;

    for row in 0..array.len() {
        if !array.is_valid(row) {
            continue;
        }
        let path = path_col.value(row).to_string();
        let size = size_col.value(row) as u64;
        let partitions_raw = partitions_col.value(row).to_string();
        let partition_map = parse_partition_map(&partitions_raw)?;
        let modification_time = mod_time_col.value(row);
        files.push(ActiveFile::new(
            path,
            size,
            modification_time,
            partition_map,
        ));
    }
    Ok(())
}

fn parse_metadata_action(array: &StructArray) -> Result<Option<TableMetadata>> {
    if array.is_empty() {
        return Ok(None);
    }
    let schema_col = array
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("missing metadata schema"))?;
    let partitions_col = array
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow!("missing metadata partitions"))?;
    let config_col = array
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("missing metadata configuration"))?;

    for row in 0..array.len() {
        if !array.is_valid(row) {
            continue;
        }
        let schema = schema_col.value(row).to_string();
        let partitions = list_to_strings(partitions_col, row);
        let configuration = parse_properties(config_col.value(row))?;
        return Ok(Some(TableMetadata::new(schema, partitions, configuration)));
    }

    Ok(None)
}

fn parse_protocol_action(array: &StructArray) -> Result<Option<Protocol>> {
    if array.is_empty() {
        return Ok(None);
    }
    let reader_col = array
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow!("missing protocol reader version column"))?;
    let writer_col = array
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow!("missing protocol writer version column"))?;

    for row in 0..array.len() {
        if !array.is_valid(row) {
            continue;
        }
        return Ok(Some(Protocol {
            min_reader_version: reader_col.value(row) as u32,
            min_writer_version: writer_col.value(row) as u32,
        }));
    }

    Ok(None)
}

fn list_to_strings(list: &ListArray, row: usize) -> Vec<String> {
    if list.is_null(row) {
        return Vec::new();
    }
    let values = list.value(row);
    let strings = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("list item must be string");
    let mut result = Vec::new();
    for idx in 0..strings.len() {
        if strings.is_valid(idx) {
            result.push(strings.value(idx).to_string());
        }
    }
    result
}

fn parse_partition_map(raw: &str) -> Result<HashMap<String, String>> {
    if raw.is_empty() {
        return Ok(HashMap::new());
    }
    let value: Value =
        serde_json::from_str(raw).with_context(|| format!("invalid partition JSON: {raw}"))?;
    let Value::Object(obj) = value else {
        return Ok(HashMap::new());
    };
    Ok(obj
        .into_iter()
        .map(|(k, v)| (k, json_value_to_string(v)))
        .collect())
}

fn parse_properties(raw: &str) -> Result<HashMap<String, String>> {
    if raw.is_empty() {
        return Ok(HashMap::new());
    }
    let value: Value =
        serde_json::from_str(raw).with_context(|| format!("invalid properties JSON: {raw}"))?;
    let Value::Object(obj) = value else {
        return Ok(HashMap::new());
    };
    Ok(obj
        .into_iter()
        .map(|(k, v)| (k, json_value_to_string(v)))
        .collect())
}

fn file_timestamp(path: &Path) -> Result<Option<i64>> {
    let meta = fs::metadata(path)?;
    if let Ok(modified) = meta.modified() {
        let millis = modified
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        return Ok(Some(millis));
    }
    Ok(None)
}

fn current_timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn build_operation_params(operation: &str, count: usize, extra: &Option<Value>) -> Value {
    let mut map = JsonMap::new();
    map.insert("operation".into(), Value::String(operation.to_string()));
    map.insert("actionCount".into(), Value::Number((count as u64).into()));
    map.insert("source".into(), Value::String("dl-import".into()));
    if let Some(extra) = extra {
        map.insert("operationParameters".into(), extra.clone());
    }
    Value::Object(map)
}
