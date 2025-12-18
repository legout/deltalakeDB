# Project Context

## Purpose
- Provide a SQL‑backed metadata plane for Delta Lake while preserving full Delta compatibility by mirroring commits to `_delta_log/` for external engines.
- Improve metadata read latency and enable multi‑table ACID semantics using a relational database as the source of truth.
- Extend `delta-rs` with a Rust‑first core and thin Python bindings; DB connections and transaction logic live in Rust, not Python.

Status: early scaffolding; spec and PRD in place, implementation pending (see `SQL_Backed_Delta_Metadata_PRD.md:1`).

## Tech Stack
- Languages: Rust (primary core, extends `delta-rs`), Python 3.12 (bindings/UX).
- Runtime: Tokio for async I/O.
- Build/packaging: Cargo for Rust; Python wheels via `pyo3` + `maturin` (planned). Current `pyproject.toml` uses `uv_build` as a placeholder until Rust core lands.
- Repo layout: today `src/` with Python package `delkalakedb` (`src/delkalakedb/__init__.py:1`); will add Rust crate(s) under `crates/` integrated with `delta-rs`.
- Databases (supported): SQLite, DuckDB, Postgres — accessed via Rust drivers.
- DB layer: `sqlx` for Postgres and SQLite; `duckdb-rs` for DuckDB; optional `tokio-postgres` (feature `pg-copy`) for high‑throughput COPY.
- Object store: Any Delta‑compatible store (e.g., S3, GCS, ABFS) for `_delta_log/` mirroring.
- Ecosystem integration: `delta-rs` crate as core; Python API via `deltalake`/pyo3 bindings.

## Project Conventions

### Code Style
- Rust: `rustfmt` and `clippy` enforced in CI; idiomatic ownership/borrowing and `Result`‑based error handling.
- Python: Black (line length 88) and isort; linting with Ruff (error‑first rules, autofix on save).
- Typing: PEP 484 type hints; gradual typing via `mypy` (strict in core modules) or Pyright.
- Docstrings: Google style; public APIs documented.
- Naming: modules `snake_case`, classes `CapWords`, functions/vars `snake_case`.
- Tooling: enforce via `pre-commit` hooks once configured.

### Architecture Patterns
- Abstractions: Rust traits `TxnLogReader`/`TxnLogWriter`; implementations `SqlTxnLogReader`/`SqlTxnLogWriter`; mirror engine that emits canonical Delta JSON + Parquet checkpoints (see PRD §6).
- Layers/modules (planned):
  - Rust `core` crate: domain models and actions (add/remove/metadata/protocol/txn).
  - Rust `sql` crate: engine‑specific adapters (Postgres/SQLite/DuckDB) behind a consistent interface.
  - Rust `mirror` crate: deterministic serializer for `_delta_log` artifacts.
  - Rust `io` layer: object‑store interactions, reusing `delta-rs` where possible.
  - Python package: thin bindings and CLI surfaces over Rust core.
- Extensibility: engine adapters registered by scheme (e.g., `deltasql://postgres/...`, `deltasql://sqlite/...`, `deltasql://duckdb/...`).
 - DB library choice: prefer `sqlx` (PG/SQLite) with offline compile‑time query checking (`sqlx prepare`) to avoid DB at build time; use `duckdb-rs` for DuckDB; optional `tokio-postgres` for COPY performance under feature `pg-copy`.

### Testing Strategy
- Rust unit/integration: `cargo test` with property tests (`proptest`) for mirror serialization invariants.
- DB integration: embedded engines (SQLite, DuckDB) in‑process; Postgres via `testcontainers` or docker‑compose from Rust tests.
- Python bindings: `pytest` smoke/contract tests over the pyo3 layer.
- Conformance: golden tests comparing SQL‑derived snapshots vs `_delta_log` replay for equivalence.
- Lint/type in CI: clippy/rustfmt for Rust; ruff + mypy/pyright for Python.

### Git Workflow
- Model changes via OpenSpec first: proposals live under `openspec/changes/` and must pass `openspec validate --strict` before implementation.
- Branching: trunk‑based with short‑lived feature branches; PR required before merge.
- Commits: Conventional Commits (`feat:`, `fix:`, `refactor:`, `docs:`...).
- Reviews: at least one approval; green CI mandatory.

## Domain Context
- Delta Lake compatibility is non‑negotiable: `_delta_log` remains the public truth for external engines; our SQL catalog is authoritative for our stack.
- Writers use optimistic concurrency with strictly increasing `version`; readers default to read‑committed with time‑travel support.
- Multi‑table ACID: commit spans multiple table IDs in a single DB transaction; mirroring runs post‑commit per table.

## Important Constraints
- Compatibility: mirror canonical Delta artifacts (JSON + checkpoint Parquet) byte‑for‑byte per spec.
- Sequence: DB commit first; mirror after commit with durable, idempotent retries and lag monitoring.
- Performance: target sub‑second table open on large logs (see PRD NFRs).
- Security/ops: DB auth/TLS, audit logs, metrics/tracing, documented backup/restore.
- Python baseline: 3.12 only (for now).
- Naming note: package is `delkalakedb` (`pyproject.toml:1`), repo is `deltalakeDB`—keep as is until a rename decision is proposed via OpenSpec.

## External Dependencies
- Rust crates (core): `delta-rs`, `sqlx` (Postgres/SQLite), `duckdb`/`duckdb-rs`; optional `tokio-postgres` for COPY under feature `pg-copy`.
- Python layer: `deltalake` for compatibility/shims; bindings via `pyo3` (wheels built with `maturin`).
- Object store: provider SDKs as needed (or reuse `delta-rs`) to write `_delta_log/`.
- Tooling: OpenSpec CLI for spec/change management; cargo + rustfmt/clippy; pytest/ruff/black/mypy in dev/CI.

## Implementation Order
The following sequence minimizes risk and enables incremental value. Items marked (parallel) can start once their dependency exists.

1. add-txnlog-abstractions — Land traits and file-backed impl (PRD §15 M0)
2. add-sql-schema-core — Create authoritative SQL schema (PRD §7)
3. add-sql-read-path-postgres — Postgres reader (PRD §4.1, §15 M1)
4. add-sql-write-path-postgres — Postgres writer with CAS (PRD §4.2, §15 M2)
5. add-mirror-json-after-commit — JSON mirroring after DB commit (PRD §4.2, §17)
6. add-mirror-reconciler-and-alerts — Ensure eventual convergence + SLOs (PRD §8, §10)
7. add-mirror-parquet-checkpoints — Periodic checkpoints (PRD §17)
8. add-multi-table-transaction-postgres — Atomic multi-table commits (PRD §4.3, §15 M3)

Parallel/Supporting tracks:
- add-deltasql-uri-scheme — Can start after step 1; needed for UX (PRD §4.4, §11)
- add-migration-bootstrap-cli — After step 2 for early adoption (PRD §12)
- add-sql-read-path-sqlite — After step 2 (PRD §13 Phase 1)
- add-sql-read-path-duckdb — After step 2 (PRD §13 Phase 1)
- add-observability-baseline — Begin at step 3 and expand through step 8 (PRD §10)
