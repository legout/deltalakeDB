## Context
Define an engine-appropriate relational schema for the authoritative metadata store per PRD §7, enabling fast reads and durable history while remaining Delta-compatible via later mirroring.

## Goals / Non-Goals
- Goals
  - Provide DDL for Postgres, SQLite, DuckDB with equivalent semantics.
  - Normalize core actions into tables; store semi-structured fields as JSON where appropriate.
  - Support optimistic concurrency and efficient snapshot reconstruction.
- Non-Goals
  - Background reconciler and mirror status processing (handled by mirror change).
  - Advanced partitioning/sharding rollout; document options, adopt defaults first.

## Decisions
- Core tables (normalized)
  - `dl_tables(table_id PK, name, location, protocol_min_reader, protocol_min_writer, properties JSON/JSONB)`
  - `dl_table_versions(table_id, version PK, committed_at, committer, operation, operation_params JSON)`
  - `dl_add_files(table_id, version, path PK, size_bytes, partition_values JSON, stats JSON, data_change, modification_time)`
  - `dl_remove_files(table_id, version, path PK, deletion_timestamp, data_change)`
  - `dl_metadata_updates(table_id, version PK, schema_json JSON, partition_columns TEXT[]/TEXT JSON, table_properties JSON)`
  - `dl_protocol_updates(table_id, version PK, min_reader_version, min_writer_version)`
- Head/version tracking
  - Add `dl_table_heads(table_id PK, current_version BIGINT NOT NULL)` for CAS updates and fast head lookup.
  - In Postgres, enforce monotonicity via `CHECK (current_version >= 0)` and transactional updates.
- Engine-specific types
  - Postgres: JSONB, arrays; DuckDB: JSON, LIST; SQLite: TEXT for JSON, no arrays (use JSON array text), emulate MVs with tables/triggers.
- Indexing (initial)
  - `dl_table_versions(table_id, version DESC)`
  - `dl_add_files(table_id, path)`; optional GIN on `stats`/`partition_values` (Postgres only).
  - `dl_remove_files(table_id, path)`
- Active files derivation
  - Start with view-based approach:
    - `vw_latest_action_per_path`: window over UNION(ADD, REMOVE) ≤ V selecting latest by `(table_id, path, version DESC)`.
    - `vw_active_files(V)`: filter `is_add = true` from the above for a given version.
  - Upgrade path: incremental maintenance via triggers into a maintained table `dl_active_files` if performance requires.
- Constraints & FKs
  - Avoid cross-table FKs for hot paths to reduce write amplification; rely on composite PKs and application invariants.

## Risks / Trade-offs
- Views over large tables can be expensive; mitigate with indexes and pagination, then move to maintained tables if needed.
- JSON storage eases evolution but limits pushdown in SQLite/DuckDB; accept for v1.
- Divergent engine capabilities require careful test coverage to keep semantics aligned.

## Migration Plan
1) Ship Postgres DDL first; add SQLite/DuckDB variants next.
2) Provide migration scripts (create tables + indexes + `dl_table_heads`).
3) Add optional helper to (re)build maintained `dl_active_files` if adopted later.

## Open Questions
- Thresholds to switch from views → maintained tables (row count, latency budgets)?
- Should we partition Postgres tables by hash(`table_id`) in v1 or defer?

