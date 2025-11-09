## Context
Delta metadata operations must work against multiple backends (file-backed `_delta_log`, SQL-backed catalog) without impacting call sites. Introducing `TxnLogReader`/`TxnLogWriter` traits formalizes the boundary and enables incremental adoption per PRD §6.

## Goals / Non-Goals
- Goals
  - Define minimal, stable traits for reading snapshots and committing actions.
  - Preserve existing file-backed behavior via `FileTxnLog*` impls.
  - Provide typed domain models for actions (add/remove/metadata/protocol/txn).
  - Establish optimistic concurrency API (CAS on expected version).
  - Standardize error types and telemetry hooks.
- Non-Goals
  - Implement SQL adapters, mirroring, or multi-table ACID (separate changes).
  - Define storage schemas; this design is API-level only.

## Decisions
- Trait shape (initial)
  - Use async methods via `async-trait` for ergonomics; revisit when native async-in-traits suffices across MSRV.
  - Require `Send + Sync + 'static` for trait objects used across async executors.
  - Return a single `TxnLogError` enum (thiserror) with categories: Io, Serialization, Protocol, Concurrency, NotFound, Unsupported, Internal.
- Domain types
  - `Version(u64)`, `TimestampMillis(i64)` newtypes for clarity.
  - `Action` enum: `AddFile`, `RemoveFile`, `Metadata`, `Protocol`, `Txn`.
  - `Snapshot` struct includes: version, protocol, metadata (schema + properties), partition columns, active files iterator (paged).
- Concurrency model
  - Writer API accepts `expected_version: Option<Version>`; if provided, enforce `current == expected` else `Err(Concurrency)`.
  - Writer returns `CommitResult { version, counts, operation, committed_at }`.
- Feature flags
  - Gate adapters: `file`, `sql-pg`, `sql-sqlite`, `sql-duckdb`, `mirror`. Core traits always on.
- Telemetry
  - Trait takes optional context: `&Telemetry` (metrics, tracing spans) or uses global subscriber; do not hard‑wire any exporter.

## Risks / Trade-offs
- Async-trait adds minor dyn dispatch overhead; acceptable for I/O-bound ops.
- Freezing trait too early may constrain SQL path; mitigate with sealed traits and additive methods behind feature flags.
- Paged file iteration API must balance ergonomics and memory safety (use streams/iterators not Vec for large tables).

## Migration Plan
1) Introduce traits + error types behind feature flag; adapt file-backed code to implement them.
2) Flip call sites to use traits (no behavior change).
3) Remove feature flag and make traits the default boundary.

## Open Questions
- Should `Snapshot` expose a stable schema representation independent of backend (Arrow JSON vs Delta JSON)?
- Do we need a streaming commit API (append as we go) for very large commits, or is all-in-memory acceptable initially?

