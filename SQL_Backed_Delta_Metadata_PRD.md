
# PRD: SQL-Backed Metadata for Delta Lake (Goal A — Delta-Compatible, Faster Metadata)

**Owner:** Data Platform Team  
**Author:** (You + contributors)  
**Date:** 2025-10-31  
**Status:** Draft v0.1

---

## 1) Background & Motivation

Delta Lake’s transaction log lives in object storage under `_delta_log/` as JSON commits and Parquet checkpoints. This is portable and simple but creates latency and operational pain:
- Listing thousands of `_delta_log` files on object stores is slow and rate-limited.
- Replay-heavy cold starts degrade UX, especially for Python-first stacks using `delta-rs`.
- Cross-table ACID is infeasible because each table’s log is independent.
- Governance/auditing require stitching across many tiny files.

**Vision:** Keep Delta compatibility for the ecosystem, but introduce a SQL-backed metadata plane that is the **authoritative source of truth** for writers/readers in our stack. On every successful commit, **mirror** canonical Delta JSON/Parquet to `_delta_log/` so external engines (Spark, Databricks, Polars, DuckDB Delta connector, etc.) remain fully compatible.

This yields:
- Sub-second metadata reads via SQL (indexes/materialized views instead of object listings).
- True multi-table ACID via the SQL DB transaction.
- Easier governance/auditing/lineage (SELECT history…), simpler backups, alerting, and RBAC.
- Zero lock-in: `_delta_log` still exists and is correct for all third-party readers.

---

## 2) Product Goals

### 2.1 Primary Goals
1. **Delta compatibility preserved**: Every table continues to present a valid `_delta_log/` that third-party Delta readers accept without changes.
2. **Fast metadata reads**: Table open and planning latency reduced by 3–10× vs. file-based log scanning.
3. **Multi-table ACID**: Enable atomic commits that update multiple Delta tables within a single SQL transaction.
4. **No JVM dependency**: Implemented in `delta-rs` (Rust + Python bindings).
5. **Database support**: Provide read/write support for at least three engines: SQLite, DuckDB, and Postgres.

### 2.2 Non-Goals
- Creating a new table format or changing the Delta public protocol.
- Removing `_delta_log`; it remains the public, portable changelog.
- Performance optimizations on the data plane (Parquet IO) beyond what `delta-rs` already provides.

---

## 3) Personas & Use Cases

- **Data Engineer (Python-first):** Fast table opens, reliable compaction/vacuum, easy time-travel/history queries, cross-table upserts in pipelines.
- **Analytics Engineer:** Lower cold-start latency when querying from DuckDB, DataFusion, Polars through `delta-rs`-powered tools.
- **Platform Admin:** Centralized auditing (who changed what, when), backups of metadata via standard DB tooling, resource monitoring and alerting.
- **ML Engineer:** Multi-table commits for feature store + labels snapshot atomics.

Key Use Cases:
- Open a table with 50k commits in < 500 ms on a warm DB.
- Atomic deploy of a dimension + fact table rewrite.
- Audit: “List all operations on table X between two timestamps.”
- Quick lineage: “Which commits produced version 123?”

---

## 4) Functional Requirements

### 4.1 Read Path (delta-rs)
- Provide a `SqlTxnLogReader` that reconstructs the latest snapshot exclusively from SQL.
- Support time travel by version or timestamp.
- Support listing active files, schema, partition columns, protocol, table properties.
- Respect Delta constraints (e.g., protocol checks) as stored in SQL.

### 4.2 Write Path (delta-rs)
- Provide a `SqlTxnLogWriter` that:
  - Opens a SQL transaction.
  - Validates `next_version = current_version + 1` (optimistic concurrency).
  - Inserts the commit’s actions (add/remove/metadata/protocol/txn) into normalized SQL tables.
  - Commits the transaction.
  - **After commit succeeds**, emits canonical Delta artifacts to object storage:
    - `NNNNNNNNNN.json` (new-line delimited actions)
    - Periodic `_delta_log/<ver>.checkpoint.parquet` per Delta checkpointing rules.
- Enforce writer idempotency and external concurrent writer detection.

### 4.3 Multi-Table Transactions
- Support an API to stage actions across multiple tables and commit atomically.
- On success, mirror each table’s actions to its `_delta_log` in the correct version order.

### 4.4 Table Discovery
- Tables are discoverable via:
  - SQL catalog (`tables` registry) **and**
  - Existing object-store paths (location metadata maintained in SQL).
- Provide a URI scheme, e.g. `deltasql://pg/<db>/<schema>/<table>` or DSN + table identifier.

### 4.5 Administration
- Metadata retention/configurable GC, mirroring parity checks, lag alerts if `_delta_log` mirroring fails.
- Migration tooling (one-time import from existing `_delta_log` to SQL).

---

## 5) Non-Functional Requirements

- **Performance:**  
  - Cold open of a table with 100k active files: p95 < 800 ms (local network DB).  
  - Planning “files for predicate X”: 2–5× faster vs. baseline object-store-only.
- **Scalability:** Handle millions of actions, 100M+ file rows with proper indexing/partitioning.
- **Reliability:** Mirroring to `_delta_log` is exactly-once w.r.t. a committed SQL tx (at-least-once write attempts allowed, but object-store state must converge). Recovery on partial mirror write.
- **Security:** Support DB auth (user/role), TLS, row-level audit logs, optional encryption-at-rest (DB native) and object-store SSE/KMS.
- **Observability:** Metrics (latency, versions/sec), logs (commit id, actor, tables), tracing around SQL transactions and object-store writes.
- **Backups/DR:** RPO/RTO determined by DB backups and object-store durability. Provide a documented restore runbook.

---

## 6) Architecture Overview

### 6.1 High-Level
```
+-------------------------+        mirror          +-----------------------------+
|  delta-rs (reader/writer) +--------------------->|  _delta_log/ (JSON/Parquet) |
+-----------+-------------+                        +---------------+-------------+
            |                                                     ^
  SQL read/write via TxnLog*                                     /
            v                                                    /
+-----------------------------+  authoritative store of actions  /
|  SQL Metadata Catalog (DB)  |---------------------------------+
|  - tables, versions, files  |
|  - metadata, protocol, txn  |
+-----------------------------+
```

**Source of truth:** SQL DB.  
**Compatibility surface:** `_delta_log` remains correct for every commit.

### 6.2 Components
- `TxnLogReader` / `TxnLogWriter` traits (new abstractions).
- `SqlTxnLogReader` / `SqlTxnLogWriter` (new implementations).
- `FileTxnLogReader` / `FileTxnLogWriter` (existing behavior).
- **Mirror Engine:** deterministic serializer producing canonical Delta JSON & Parquet checkpoints from the committed actions; writes to object storage with conflict checks.
- **Migration Tool:** bootstrap SQL from existing `_delta_log` (replay JSON + checkpoints into DB).

### 6.3 Consistency Model
- Writers: optimistic concurrency, version strictly increases by 1 per table. Enforced by a `current_version` row-lock or CAS update in DB.
- Readers: default to **read committed** (latest committed version). Time travel supported.
- Multi-table commits: a single DB transaction across rows for multiple `table_id`s. Mirroring occurs table-by-table after commit; see §8 for failure handling.

---

## 7) Data Model (Proposed SQL Schema)

> Concrete DDL varies by engine (Postgres, SQLite, DuckDB). Example below uses Postgres types (JSONB, arrays). Equivalent schemas will be provided for SQLite (use TEXT/JSON, no arrays; emulate MVs) and DuckDB (JSON, lists; adapt indexes/constraints accordingly).

```sql
CREATE TABLE dl_tables (
  table_id UUID PRIMARY KEY,
  name TEXT,
  location TEXT NOT NULL,          -- parquet root on object store
  created_at TIMESTAMPTZ DEFAULT now(),
  protocol_min_reader INT NOT NULL,
  protocol_min_writer INT NOT NULL,
  properties JSONB DEFAULT '{}'
);

CREATE TABLE dl_table_versions (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  committer TEXT,
  operation TEXT,                  -- e.g., 'WRITE', 'MERGE', 'OPTIMIZE'
  operation_params JSONB,
  PRIMARY KEY (table_id, version)
);

-- Active file state is derived: sum of adds minus removes up to a version
CREATE TABLE dl_add_files (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  path TEXT NOT NULL,
  size_bytes BIGINT,
  partition_values JSONB,
  stats JSONB,                     -- Delta stats JSON
  data_change BOOLEAN DEFAULT true,
  modification_time BIGINT,
  PRIMARY KEY (table_id, version, path)
);

CREATE TABLE dl_remove_files (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  path TEXT NOT NULL,
  deletion_timestamp BIGINT,
  data_change BOOLEAN DEFAULT true,
  PRIMARY KEY (table_id, version, path)
);

CREATE TABLE dl_metadata_updates (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  schema_json JSONB NOT NULL,
  partition_columns TEXT[],
  table_properties JSONB,
  PRIMARY KEY (table_id, version)
);

CREATE TABLE dl_protocol_updates (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  min_reader_version INT NOT NULL,
  min_writer_version INT NOT NULL,
  PRIMARY KEY (table_id, version)
);

-- Optional: stream progress / txn actions
CREATE TABLE dl_txn_actions (
  table_id UUID NOT NULL,
  version BIGINT NOT NULL,
  app_id TEXT NOT NULL,
  last_update BIGINT NOT NULL,
  PRIMARY KEY (table_id, version, app_id)
);

-- Fast path: current active files (MV or maintained table)
-- Option A: Materialized View refreshed on each commit.
-- Option B: Incrementally maintained by triggers.
```

**Indexes & Partitioning (examples):**
- `dl_add_files(table_id, path)` btree + `stats` GIN for predicate pushdown planning.
- `dl_table_versions(table_id DESC, version DESC)` for latest lookup.
- Partition large tables by `table_id` or hash part for hot shards.

---

## 8) Failure Modes & Recovery

- **DB commit succeeded, mirror failed** (network or object-store error):
  - Mark mirror status `FAILED` for `(table_id, version)` in an internal table `dl_mirror_status`.
  - Background reconciler retries until JSON + checkpoint are present. Readers in our stack are unaffected (read from SQL). External engines may be stale until mirror completes.
  - SLO: mirror lag p95 < 5s; alert if > 1min.

- **Mirror wrote JSON but not checkpoint**:
  - Acceptable; checkpoint is an optimization. Continue retries; ensure future readers can still replay JSON.

- **DB rollback**:
  - No mirroring attempted. Safe.

- **Concurrent writers**:
  - Enforced via `current_version` CAS (`UPDATE ... WHERE current_version = ?`). Loser retries with fresh snapshot.

- **Cross-table commit with partial mirror**:
  - Each table mirrors independently but all DB state is committed atomically. External readers may temporarily see versions diverge across tables; document this consistency caveat.

---

## 9) Security & Governance

- **AuthN/Z:** Use DB-native auth; optionally map roles to table namespaces. Object-store credentials managed via existing mechanism.
- **Auditing:** `dl_table_versions` holds operation + committer; optional structured audit table with IP/user-agent.
- **PII/Secrets:** No secrets in table properties; redact in logs.
- **Row-level security:** Optional per-table policies for metadata visibility.

---

## 10) Telemetry

- Metrics:
  - `metadata_open_latency_ms` (reader), `commit_latency_ms` (writer)
  - `mirror_latency_ms`, `mirror_failures_total`
  - `versions_committed_total`, `files_active_gauge`
- Logs:
  - Per-commit structured event: table_id, version, operation, counts of add/remove, bytes moved.
- Tracing:
  - Spans around SQL tx and object-store writes (commit JSON, checkpoint parquet).

---

## 11) API & UX (delta-rs / Python)

**URI schemes**
- SQL-backed (Postgres): `deltasql://postgres/mydb/public/mytable`
- SQL-backed (SQLite): `deltasql://sqlite//var/data/meta.db?table=mytable`
- SQL-backed (DuckDB): `deltasql://duckdb//var/data/catalog.duckdb?table=mytable`
- File-backed (existing): `s3://bucket/path/to/table`

**Python sketch**
```python
from deltalake import DeltaTable, write_deltalake

# Read (SQL-backed)
dt = DeltaTable("deltasql://postgres/mydb/public/mytable")

# Write
write_deltalake("deltasql://postgres/mydb/public/mytable", df, mode="append")

# Multi-table transaction (conceptual)
with deltars.begin() as tx:
    tx.write(table="deltasql://.../dim_customer", df=df1, mode="overwrite")
    tx.write(table="deltasql://.../fact_sales", df=df2, mode="append")
# on success: mirror to each table’s _delta_log
```

**Config**
- `mirror.checkpoint_interval=10`
- `mirror.retry_backoff=...`
- `sql.schema=create_if_missing|error`
- `concurrency.retries=...`

---

## 12) Migration Plan

1. **Bootstrap:** For an existing Delta table, run `dl import` CLI:
   - Read latest checkpoint (if present) + replay JSON to produce rows for `dl_*` tables.
   - Initialize `current_version` to latest.
2. **Cutover:** Point writers to `deltasql://...` URI; keep readers on either path.
3. **Validation:** Periodically diff SQL-derived snapshot vs `_delta_log`-derived snapshot; alert on drift.
4. **Rollback:** Switch writer back to file-backed if needed; `_delta_log` has remained authoritative for externals.

---

## 13) Rollout Strategy

- **Phase 1 (Alpha):** Postgres + SQLite + DuckDB basic support (read/write paths), internal teams only, non‑critical datasets.
- **Phase 2 (Beta):** Hardening and performance tuning for SQLite/DuckDB; enable multi‑table ACID across all three; introduce mirror reconciler.
- **Phase 3 (GA):** SLOs, observability dashboards, DR docs, perf tuning guide across all three engines.

---

## 14) Risks & Mitigations

- **Dual-write complexity (DB + object store):** Strict sequencing; mirror only after DB commit; durable retry queue; comprehensive validation tool.
- **Ecosystem drift:** Keep mirroring byte-for-byte canonical JSON; add continuous conformance tests against Delta spec versions.
- **Hotspot in DB (large tables):** Sharding/partitioning; careful indexing; use COPY/bulk-insert for large `add_files` commits.
- **Operational burden:** Provide Helm charts/Terraform modules and runbooks; support managed DBs.
- **Lock-in perception:** Clear docs that `_delta_log` remains the public interface; disable SQL-only mode by default.

---

## 15) Milestones & Acceptance Criteria

### M0 – Abstractions Landed (2–3 weeks)
- `TxnLogReader`/`Writer` traits merged; file-backed path unchanged.
- **AC:** All existing tests green.

### M1 – SQL Read Path (3–4 weeks)
- `SqlTxnLogReader` implemented for Postgres, SQLite, and DuckDB.
- Basic schema + indexes (or engine equivalents); table open, time travel, file listing.
- **AC:** p95 table open < 800 ms on 100k-file table (local DB) on Postgres; functional parity confirmed on SQLite and DuckDB for read path.

### M2 – SQL Write Path + Single-Table Commit (4–6 weeks)
- `SqlTxnLogWriter` with optimistic concurrency for Postgres, SQLite, and DuckDB.
- Mirror engine writes JSON commits + periodic checkpoints.
- **AC:** End-to-end append/overwrite works on all three engines; conformance tests against `_delta_log` equivalence.

### M3 – Multi-Table ACID + Reconciler (4 weeks)
- Transaction API committing across tables; background reconciler with alerts.
- **AC:** Atomic two-table update demo; induced mirror failures recover automatically; SLO met.

### M4 – Hardening & GA (4–6 weeks)
- Observability, docs, migration tooling, perf tuning, DR runbook.
- **AC:** Production pilot success; 99.9% mirror-lag < 5s; no correctness bugs over 30 days.

---

## 16) Open Questions

1. Which DBs to support at GA? (Postgres first? DuckDB for embedded?)
2. Exact checkpoint cadence and size thresholds for mirroring.
3. Will we support “read from SQL, write from either path” concurrently? (Probably **no**; writers must be SQL to avoid divergence.)
4. Should we expose a read-only mode that consumes `_delta_log` into SQL on the fly (cache), without becoming authoritative?

---

## 17) Appendix — Mirror Serialization Rules

- JSON commit serialization must be a faithful mapping of the actions stored in SQL, preserving ordering and canonicalized formatting as required by Delta spec.
- Checkpoint Parquet schema must match the Delta protocol version; compression and row group sizing follow current best practices.
- Object-store write semantics: atomic rename or multi-part put as supported by the provider; if idempotent put is unavailable, use temporary keys and finalize by rename.

---

**End of PRD**
