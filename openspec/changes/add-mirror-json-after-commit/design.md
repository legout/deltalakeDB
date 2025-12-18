## Context
Maintain Delta compatibility by emitting canonical `_delta_log/NNNNNNNNNN.json` after a successful SQL commit (PRD §4.2, §8, §17). Focus on JSON; checkpoints will come later.

## Goals / Non-Goals
- Goals
  - Serialize committed actions deterministically to Delta JSON.
  - Write JSON after DB commit; record status and retry on failure until convergence.
  - Ensure idempotency and correct ordering per table.
- Non-Goals
  - Parquet checkpointing (follow-up change).
  - Cross-table consistency for external readers (documented caveat; DB is authoritative internally).

## Decisions
- Serialization
  - Canonical field ordering and newline-delimited actions; stable numeric/text formatting; UTF‑8 only.
  - Input source is the committed action list used for the SQL write, not a re-query to avoid drift.
- Write pipeline (per table/version `V`)
  1) Build payload bytes and content digest (e.g., SHA‑256).
  2) Attempt idempotent create of `_delta_log/V.json`:
     - Prefer provider preconditions (e.g., create-if-not-exists / ETag checks) or temp key + atomic rename.
     - If neither is available, use write-then-VERIFY (read-back & digest) and treat equal content as success.
  3) On success, mark status `SUCCEEDED`; else record error and retry with backoff.
- Mirror status table
  - `dl_mirror_status(table_id, version, status ENUM('PENDING','FAILED','SUCCEEDED'), attempts INT, last_error TEXT, updated_at TIMESTAMPTZ, digest TEXT)`.
  - Insert `PENDING` within writer flow before returning to caller; worker transitions states.
- Ordering & concurrency
  - Per-table single-flight: do not mirror `V+1` before `V` succeeds; parallelize across tables.
  - Safe to retry the same `(table_id, V)` multiple times; treat existing identical object as success.
- Retry policy
  - Exponential backoff with jitter; cap attempts but keep periodic background retries (e.g., hourly) until success.
- Observability
  - Metrics: `mirror_latency_ms`, `mirror_failures_total`, `mirror_backlog_gauge`.
  - Logs: table_id, version, object key, attempt, error summary.

## Risks / Trade-offs
- Object stores differ in conditional write capabilities; must implement provider-specific strategies.
- Temporary keys risk leakage if process crashes mid-run; add TTL/lifecycle rules where supported.
- Ensuring per-table ordering reduces throughput; acceptable for correctness in v1.

## Migration Plan
1) Add `dl_mirror_status` DDL and integrate status inserts in writer flow.
2) Implement background reconciler worker polling `PENDING`/`FAILED` rows.
3) Add configuration: checkpoint interval (ignored here), retry backoff, max concurrency.

## Open Questions
- Should we embed commit metadata (digest, bytes) into `dl_table_versions` to simplify reconciliation?
- Do we require encryption/SSE-by-default for `_delta_log/` writes in managed deployments?

