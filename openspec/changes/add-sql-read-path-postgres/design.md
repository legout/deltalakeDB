## Context
Implement a Postgres-backed `SqlTxnLogReader` that reconstructs snapshots directly from the SQL catalog (PRD §4.1, §7), targeting sub‑second table opens on large logs.

## Goals / Non-Goals
- Goals
  - Open latest snapshot, list active files, read schema/protocol/properties.
  - Support time travel by version and by timestamp.
  - Use efficient queries with appropriate indexes; paginate large result sets.
- Non-Goals
  - Write path, mirroring, or multi-table transactions.

## Decisions
- Transactionality & isolation
  - Use a single `READ COMMITTED` transaction per open to bind version reads consistently.
  - Resolve head with `SELECT current_version FROM dl_table_heads WHERE table_id = $1`.
- Version resolution by timestamp
  - `SELECT max(version) FROM dl_table_versions WHERE table_id=$1 AND committed_at <= $2`.
- Snapshot reconstruction (version `V`)
  - Metadata/protocol: pick latest ≤ `V`:
    - `SELECT ... FROM dl_metadata_updates WHERE table_id=$1 AND version <= $2 ORDER BY version DESC LIMIT 1`.
    - Same for `dl_protocol_updates`.
  - Active files (latest action per path ≤ `V`):
    ```sql
    WITH actions AS (
      SELECT path, version, true  AS is_add, size_bytes, partition_values, stats, modification_time
      FROM dl_add_files WHERE table_id = $1 AND version <= $2
      UNION ALL
      SELECT path, version, false AS is_add, NULL, NULL, NULL, NULL
      FROM dl_remove_files WHERE table_id = $1 AND version <= $2
    ), ranked AS (
      SELECT *, row_number() OVER (PARTITION BY path ORDER BY version DESC) AS rn FROM actions
    )
    SELECT path, size_bytes, partition_values, stats, modification_time
    FROM ranked WHERE rn = 1 AND is_add = true
    ORDER BY path
    LIMIT $3 OFFSET $4;
    ```
- Pagination
  - Prefer keyset pagination on `(path, version)` for large tables; provide OFFSET for simplicity first.
- Implementation notes
  - Use `sqlx` with offline prepare for compile-time query checks.
  - Map JSON fields to serde_json::Value; decode partition values/stats on demand.
  - Expose streaming iterator for active files to avoid loading all rows.
- Observability
  - Emit `metadata_open_latency_ms`, `rows_scanned` counters; add tracing spans around each query.

## Risks / Trade-offs
- Windowed UNION can be heavy on very large tables; indexes on `(table_id, path)` and `(table_id, version)` mitigate.
- Timestamp resolution assumes monotonic `committed_at`; acceptable per PRD.

## Open Questions
- Do we introduce an optional maintained `dl_active_files` to bypass windowing for hot tables in v1?

