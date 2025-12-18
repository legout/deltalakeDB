# deltalakedb

SQL-backed metadata plane for Delta Lake.

`deltalakedb` stores Delta table metadata (protocol, schema, add/remove actions, etc.) in a relational database for fast reads and transactional writes, while keeping tables compatible with existing Delta engines by mirroring commits back into `_delta_log/`.

> **Status:** pre-release (`0.0.0`). The Rust core is usable; the Python package (`delkalakedb`) and packaging story are still evolving.

## What you get

- **Fast snapshot reads** via SQL-backed metadata (`TxnLogReader`)
- **Optimistic concurrency control** for writes (CAS on `dl_table_heads`)
- **Multi-table atomic commits** on Postgres
- **Delta compatibility** by mirroring committed actions to `_delta_log/` (JSON + optional checkpoints)
- **CLI tooling** to bootstrap/import an existing `_delta_log` into SQL

## Crates / packages

- `deltalakedb-core`: domain model + `TxnLogReader` / `TxnLogWriter` traits
- `deltalakedb-sql`: Postgres / SQLite / DuckDB adapters + DeltaSQL URI parser
- `deltalakedb-mirror`: mirror worker + checkpoint serializer
- `deltalakedb-cli` (`dl`): utilities (currently: `dl import`)
- `delkalakedb` (Python): high-level API surface (bindings are currently stubs; see `README_PYTHON.md`)

## Quick start (local Postgres + CLI import)

This quick start uses the included Docker Compose stack, imports an existing Delta table into SQL, then reads a snapshot back.

### 1) Start the local stack

```bash
./scripts/test-env.sh start
```

Postgres will be available at:

- `postgres://postgres:postgres@localhost:5432/deltalakedb`

### 2) Create the metadata tables

The catalog schema is currently defined implicitly by the Rust adapters/tests.

For a quick local setup, you can apply the minimal Postgres schema below:

```bash
psql "postgres://postgres:postgres@localhost:5432/deltalakedb" <<'SQL'
CREATE TABLE IF NOT EXISTS dl_tables (
  table_id UUID PRIMARY KEY,
  name TEXT,
  location TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  protocol_min_reader INT NOT NULL,
  protocol_min_writer INT NOT NULL,
  properties JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS dl_table_heads (
  table_id UUID PRIMARY KEY REFERENCES dl_tables(table_id) ON DELETE CASCADE,
  -- current_version starts at -1 (INITIAL_VERSION) before the first commit
  current_version BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dl_table_versions (
  table_id UUID NOT NULL REFERENCES dl_tables(table_id) ON DELETE CASCADE,
  version BIGINT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  committer TEXT,
  operation TEXT,
  operation_params JSONB,
  PRIMARY KEY (table_id, version)
);

CREATE TABLE IF NOT EXISTS dl_add_files (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  path TEXT NOT NULL,
  size_bytes BIGINT,
  partition_values JSONB,
  stats JSONB,
  data_change BOOLEAN DEFAULT TRUE,
  modification_time BIGINT,
  PRIMARY KEY (table_id, version, path)
);

CREATE TABLE IF NOT EXISTS dl_remove_files (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  path TEXT NOT NULL,
  deletion_timestamp BIGINT,
  data_change BOOLEAN DEFAULT TRUE,
  PRIMARY KEY (table_id, version, path)
);

CREATE TABLE IF NOT EXISTS dl_metadata_updates (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  schema_json JSONB NOT NULL,
  partition_columns TEXT[],
  table_properties JSONB,
  PRIMARY KEY (table_id, version)
);

CREATE TABLE IF NOT EXISTS dl_protocol_updates (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  min_reader_version INT NOT NULL,
  min_writer_version INT NOT NULL,
  PRIMARY KEY (table_id, version)
);

CREATE TABLE IF NOT EXISTS dl_txn_actions (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  app_id TEXT NOT NULL,
  last_update BIGINT NOT NULL,
  PRIMARY KEY (table_id, version, app_id)
);

CREATE TABLE IF NOT EXISTS dl_mirror_status (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  status TEXT NOT NULL DEFAULT 'PENDING',
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT,
  digest TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (table_id, version)
);
SQL
```

### 3) Import an existing Delta table into SQL

```bash
# Imports /path/to/table/_delta_log into Postgres.
cargo run -p deltalakedb-cli -- import /path/to/delta-table \
  --dsn "postgres://postgres:postgres@localhost:5432/deltalakedb" \
  --schema public \
  --table my_table
```

The command prints the `table_id` and the current version that was imported.

### 4) Read a snapshot (Rust)

```rust
use deltalakedb_core::txn_log::TxnLogReader;
use deltalakedb_sql::PostgresTxnLogReader;
use uuid::Uuid;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = "postgres://postgres:postgres@localhost:5432/deltalakedb";

    // Use the table_id printed by `dl import`.
    let table_id = Uuid::parse_str("00000000-0000-0000-0000-000000000000")?;

    // table_uri is informational today (used for display/logging).
    let reader = PostgresTxnLogReader::connect("file:///tmp/my_table", table_id, dsn)?;

    let snapshot = reader.snapshot_at_version(None)?;
    println!("version={} files={}", snapshot.version, snapshot.files.len());

    Ok(())
}
```

## Examples

### Parse a DeltaSQL URI (Rust)

`deltalakedb-sql` includes a small URI parser for identifying the catalog backend and table identifier.

```rust
use deltalakedb_sql::DeltasqlUri;

let uri = DeltasqlUri::parse("deltasql://postgres/mydb/public/mytable")?;
println!("{uri:?}");
```

### Write a commit (Postgres)

```rust
use chrono::Utc;
use deltalakedb_core::txn_log::{ActiveFile, CommitRequest, Protocol, TableMetadata, TxnLogWriter, INITIAL_VERSION};
use deltalakedb_sql::PostgresTxnLogWriter;
use std::collections::HashMap;
use uuid::Uuid;

let dsn = "postgres://postgres:postgres@localhost:5432/deltalakedb";
let table_id = Uuid::parse_str("00000000-0000-0000-0000-000000000000")?;

let writer = PostgresTxnLogWriter::connect("file:///tmp/my_table", table_id, dsn)?;

let mut req = CommitRequest::new(INITIAL_VERSION);
req.protocol = Some(Protocol { min_reader_version: 2, min_writer_version: 5 });
req.metadata = Some(TableMetadata::new(
    r#"{"type":"struct","fields":[{"name":"id","type":"long"}]}"#,
    vec!["id".to_string()],
    HashMap::new(),
));
req.add_actions.push(ActiveFile::new(
    "part-000.parquet",
    128,
    Utc::now().timestamp_millis(),
    HashMap::new(),
));

let result = writer.commit(req)?;
println!("committed version={}", result.version);
```

### Mirror pending commits into `_delta_log/`

```rust
use deltalakedb_mirror::{AlertSink, LagAlert, LocalFsObjectStore, MirrorRunner, MirrorService};
use sqlx::postgres::PgPoolOptions;

#[derive(Clone)]
struct StdoutAlerts;

impl AlertSink for StdoutAlerts {
    fn emit(&self, alert: LagAlert) {
        eprintln!(
            "mirror lag: table_id={} version={} lag_seconds={} severity={:?}",
            alert.table_id, alert.version, alert.lag_seconds, alert.severity
        );
    }
}

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let dsn = "postgres://postgres:postgres@localhost:5432/deltalakedb";
let pool = PgPoolOptions::new().max_connections(5).connect(dsn).await?;

let runner = MirrorRunner::new(pool, LocalFsObjectStore::default());
let service = MirrorService::new(runner, StdoutAlerts);

// Runs forever, processing one pending mirror job per tick.
service.run_forever().await?;
# Ok(()) }
```

## Development

This repo is a Rust workspace plus a Python package (managed with `uv`).

```bash
# Python dev deps
uv sync --extra dev

# Rust workspace
cargo build
cargo test
```

## Local testing environment

For integration testing with Postgres and multiple S3-compatible object stores, use the provided Docker Compose setup:

```bash
./scripts/test-env.sh start
./scripts/test-env.sh status
./scripts/test-env.sh stop
```

You can customize endpoints/credentials by copying `.env.example` to `.env` and modifying the values.

## License

MIT OR Apache-2.0
