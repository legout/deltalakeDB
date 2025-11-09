## Context

SQL-backed metadata requires a normalized relational schema to efficiently store and query Delta Lake actions (add/remove files, metadata updates, protocol changes). The schema design directly impacts query performance, storage efficiency, and ease of maintenance.

PostgreSQL is chosen as the first target database for its robustness, JSONB support for flexible fields, and production-readiness. This schema will be the foundation for all read/write operations and must scale to millions of actions.

Key constraints from PRD:
- Support 100k+ active files per table with p95 open < 800ms
- Enable time travel by version and timestamp
- Support predicate pushdown via stats
- Handle millions of historical actions

## Goals / Non-Goals

**Goals:**
- Normalized schema that faithfully represents Delta actions
- Efficient queries for common patterns (latest version, active files, time travel)
- Support for JSONB fields (stats, partition values, properties)
- Proper indexing for sub-second reads
- Clear migration path and versioning

**Non-Goals:**
- Multi-database compatibility in initial schema (Postgres-specific features OK)
- Materialized views or advanced optimizations (can add later)
- Full-text search on properties
- Built-in archival/retention (application concern)

## Decisions

### Normalization Level: One Table Per Action Type

**Decision:** Separate tables for each action type: `dl_add_files`, `dl_remove_files`, `dl_metadata_updates`, `dl_protocol_updates`, `dl_txn_actions`.

**Rationale:**
- Matches Delta action model semantically
- Allows type-specific columns (e.g., size_bytes only for add files)
- Simplifies querying (no discriminator column needed)
- Enables targeted indexing per action type

**Alternatives considered:**
- Single `dl_actions` table with JSONB payload: rejected due to query complexity and poor index selectivity
- EAV (Entity-Attribute-Value): rejected as anti-pattern for known schema

### Active Files Computation: Query vs Materialized

**Decision:** Compute active files on-demand via query: `SELECT FROM dl_add_files WHERE NOT EXISTS (SELECT FROM dl_remove_files)`.

**Rationale:**
- Simpler implementation (no trigger maintenance)
- Always accurate (no stale MV risk)
- Acceptable performance with proper indexes
- Can add materialized view later if needed

**Alternatives considered:**
- Materialized view refreshed on commit: adds complexity and potential for staleness
- Trigger-maintained active_files table: tight coupling, harder to debug
- **Future optimization:** Add MV if benchmarks show need

### JSONB for Flexible Fields

**Decision:** Use JSONB for: `stats`, `partition_values`, `properties`, `operation_params`.

**Rationale:**
- Schema flexibility for evolving Delta features
- Native GIN indexing for predicate pushdown
- Efficient storage and querying in Postgres
- Natural match for Delta's JSON-based format

**Storage estimate:**
- Stats: ~500 bytes per file (min/max/null_count)
- Partition values: ~100 bytes per file
- Properties: ~200 bytes per table

### Index Strategy

**Decision:** Core indexes:
1. `dl_table_versions(table_id, version DESC)` - latest version lookup
2. `dl_add_files(table_id, path)` - file existence checks
3. `dl_add_files(table_id, version)` - files at version
4. `dl_add_files(stats) GIN` - stats filtering
5. Foreign keys from all action tables to `dl_tables(table_id)`

**Rationale:**
- Covers common query patterns from PRD use cases
- GIN index enables JSON predicate pushdown
- DESC on version optimizes "latest" queries
- Foreign keys enforce referential integrity

**Trade-offs:**
- Write overhead: ~15% slower inserts (acceptable for metadata)
- Storage overhead: ~30% more disk space (acceptable)
- Query speedup: 10-100x for indexed queries

### Primary Keys: Composite Natural Keys

**Decision:** Use composite PKs: `(table_id, version, ...)` instead of synthetic IDs.

**Rationale:**
- Natural clustering for version-based queries
- Prevents duplicate actions per version
- Matches Delta's version-based model
- One less index to maintain

**Example:**
- `dl_add_files`: `(table_id, version, path)`
- `dl_table_versions`: `(table_id, version)`

### UUID for table_id

**Decision:** Use UUID v4 for `table_id` in `dl_tables`.

**Rationale:**
- Globally unique across databases (multi-tenant future)
- No coordination needed for ID generation
- 128-bit space prevents collisions
- Standard Postgres UUID type

**Alternatives considered:**
- BIGSERIAL: rejected due to coordination issues in distributed scenarios
- Name-based UUID: rejected as table names aren't globally unique

### Table Versioning Strategy

**Decision:** Track current version in `dl_tables.current_version` column, updated atomically on commit.

**Rationale:**
- Single source of truth for latest version
- Enables optimistic concurrency with `UPDATE ... WHERE current_version = ?`
- Fast lookup without scanning versions table

**Concurrency pattern:**
```sql
UPDATE dl_tables 
SET current_version = current_version + 1 
WHERE table_id = ? AND current_version = ?
RETURNING current_version;
```

### Migration Framework: sqlx

**Decision:** Use sqlx migrations for schema versioning.

**Rationale:**
- Compile-time query checking
- Standard Rust database library
- Built-in migration tracking (`_sqlx_migrations` table)
- Up/down migrations for rollback

**Migration naming:** `YYYYMMDDHHMMSS_description.sql`

## Risks / Trade-offs

**Risk: JSONB query performance on large datasets**
- Mitigation: GIN indexes; can extract hot columns if needed
- Benchmark: Test with 1M rows and stats filtering

**Risk: Index maintenance overhead on write-heavy workloads**
- Mitigation: Batch inserts; use COPY for large commits
- Acceptable: Metadata writes are infrequent compared to reads

**Trade-off: Storage cost vs query flexibility**
- Cost: ~2x storage vs raw JSON files (indexes + normalization)
- Benefit: 10-100x faster queries for metadata operations

**Risk: Schema changes after production deployment**
- Mitigation: Conservative initial schema; extensibility via JSONB; careful migration planning
- Policy: Breaking changes require major version bump

**Risk: Postgres-specific features lock us in**
- Mitigation: JSONB is widely supported; can port to SQLite (TEXT+JSON), DuckDB (JSON type)
- Acceptable: Postgres is primary target; other DBs are stretch goals

## Migration Plan

**Phase 1: Initial Schema**
1. Create migration `001_create_base_schema.sql` with all tables
2. Add indexes and constraints
3. Test on empty database

**Phase 2: Performance Validation**
1. Populate with 100k-1M synthetic actions
2. Benchmark key queries (latest version, active files, time travel)
3. Adjust indexes if needed

**Phase 3: Production Deployment**
1. Deploy to staging environment
2. Import small production table
3. Validate query performance meets SLOs
4. Document tuning parameters (work_mem, random_page_cost, etc.)

**Rollback:** Drop all `dl_*` tables; no impact on existing `_delta_log` files.

## Open Questions

1. **Should we partition dl_add_files by table_id?**
   - Leaning: Not initially; add when > 100 tables with > 1M files each
   - Decision point: After initial benchmarks

2. **Materialized view for active files?**
   - Leaning: No initially; add if query-based approach doesn't meet p95 < 800ms
   - Decision point: After performance testing with 100k files

3. **Compression for JSONB columns?**
   - Leaning: Use Postgres TOAST (default); enable ZSTD compression if available
   - Decision point: After storage benchmarks

4. **Separate table for mirror status?**
   - Decision: Yes, add `dl_mirror_status(table_id, version, status, error, retries)` in mirror engine proposal
   - Not needed for basic read/write functionality

5. **Archive strategy for old versions?**
   - Leaning: Application-level retention policy; consider partitioning by version range for hot/cold separation
   - Decision point: After understanding typical retention requirements
