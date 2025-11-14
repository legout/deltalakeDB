# deltalakedb

SQL-backed metadata plane for Delta Lake.

> **Status:** Early scaffolding. See `SQL_Backed_Delta_Metadata_PRD.md` for the product requirements document.

## Overview

`deltalakedb` provides a SQL-backed metadata plane for Delta Lake while preserving full Delta compatibility by mirroring commits to `_delta_log/` for external engines. It improves metadata read latency and enables multi-table ACID semantics using a relational database as the source of truth.

## Migration CLI

The `dl` CLI (installed with this package) bootstraps SQL metadata from existing Delta tables and validates parity:

```bash
# Bootstrap SQL metadata into a local SQLite catalog
dl import \
  --database /tmp/metadata.sqlite \
  --log-dir /data/delta/my_table/_delta_log \
  --table-location s3://bucket/path/my_table

# Compare SQL-derived state against _delta_log with alert thresholds
dl diff \
  --database /tmp/metadata.sqlite \
  --table-id <uuid-from-import> \
  --log-dir /data/delta/my_table/_delta_log \
  --lag-threshold-seconds 5 \
  --max-drift-files 0 \
  --metrics-format prometheus
```

Use `--mode full-history` during import to replay every JSON commit when historical metadata is required. Diff exits with code `2` when drift is detected and `3` when only the lag SLO is breached, enabling CI/CD gates.

## Development

This project uses:
- Rust for the core (extends `delta-rs`) with a Cargo workspace.
- Python 3.12 for bindings/UX, managed with `uv`.
- See `pyproject.toml` for Python tooling configuration.

### Quickstart

```bash
# Install dependencies and set up the dev environment
uv sync --extra dev

# Run Python tooling
uv run ruff check .
uv run black --check .
uv run pytest

# Build Rust workspace
cargo build
cargo test
```

## Local Testing

For integration testing with Postgres and multiple S3-compatible object stores, use the provided Docker Compose setup:

```bash
# Start the test environment
./scripts/test-env.sh start

# Stop the test environment
./scripts/test-env.sh stop

# Check status
./scripts/test-env.sh status
```

**Endpoints and credentials**

### Database
- Postgres: `localhost:5432` (user: `postgres`, password: `postgres`, database: `deltalakedb`)

### S3-Compatible Object Stores
- **MinIO**: `http://localhost:9000` (access key: `minioadmin`, secret key: `minioadmin`)
  - Console: `http://localhost:9001`
- **SeaweedFS S3**: `http://localhost:8333` (access key: `seaweedfsadmin`, secret key: `seaweedfsadmin`)
  - Master: `http://localhost:9333`
- **RustFS**: `http://localhost:9100` (access key: `rustfsadmin`, secret key: `rustfsadmin`)
  - Console: `http://localhost:9101`
- **Garage**: `http://localhost:3900` (access key: created via Garage CLI, secret key: created via Garage CLI)
  - Admin API: `http://localhost:3903` (admin token: `garageadmin`)

You can customize these by copying `.env.example` to `.env` and modifying the values.

## License

TBD (to be added in a future change).
