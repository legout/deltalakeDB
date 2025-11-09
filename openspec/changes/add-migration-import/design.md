## Context

Existing Delta tables store their complete history in `_delta_log` on object storage. To adopt SQL-backed metadata, users need a migration path that:
- Imports all historical versions and actions into SQL
- Preserves version numbers, timestamps, and committer information
- Validates correctness of imported data
- Supports incremental import for ongoing tables

This tool is **critical for adoption**—without it, users can only use SQL-backed metadata for new tables.

Key challenges:
- Parsing diverse Delta artifacts (JSON commits, Parquet checkpoints)
- Handling schema evolution and protocol upgrades during replay
- Validating imported state matches source
- Dealing with corrupt or missing files gracefully

## Goals / Non-Goals

**Goals:**
- One-time import of existing Delta tables into SQL database
- Preserve complete version history and all actions
- Validate imported state equals original table state
- Support dry-run mode for preview without changes
- Provide clear progress reporting for large imports
- Handle incremental import (resume from last version)

**Non-Goals:**
- Continuous sync (this is one-time migration, not replication)
- Bi-directional sync (SQL is authoritative post-migration)
- Performance optimization for real-time import
- Schema migration or transformation (preserve as-is)

## Decisions

### Import Strategy: Checkpoint + Incremental Replay

**Decision:** Import follows Delta's replay model:
1. Find and read latest checkpoint (if exists)
2. Read all JSON commits from checkpoint version to latest
3. Parse and insert each action into SQL tables
4. Preserve original version numbers and timestamps

**Rationale:**
- Matches how Delta readers reconstruct state
- Efficient: Checkpoint avoids replaying millions of actions
- Correct: Standard Delta replay logic is well-tested
- Reusable: Leverage existing delta-rs checkpoint reading

**Fallback:** If no checkpoint exists, replay from version 0.

**Optimization:** For very large tables (> 100k commits), consider processing checkpoints only and marking intermediate versions as "compacted" (future enhancement).

### SQL Population: Batch Insertion per Version

**Decision:** Process each version atomically:
1. Begin SQL transaction
2. Insert all actions for version N (add/remove/metadata/protocol/txn)
3. Insert row into `dl_table_versions`
4. Commit transaction
5. Repeat for version N+1

**Rationale:**
- Transaction per version ensures consistency (partial import is valid state)
- Can resume from last successfully imported version
- Matches natural Delta version boundaries

**Batch size:** Use COPY or multi-row INSERT for files within a version (same as writer).

**Alternatives considered:**
- Single transaction for entire import: rejected due to long lock times and memory usage
- No transactions: rejected due to risk of inconsistent state on failure

### Validation Strategy: Snapshot Comparison

**Decision:** After import completes, validate by comparing:
1. Active file set: SQL-derived vs `_delta_log`-derived
2. File counts and total size
3. Latest schema and partition columns
4. Protocol versions

**Rationale:**
- Snapshot comparison catches any import bugs
- Provides confidence in correctness
- Alerts user to discrepancies before switching to SQL

**Implementation:**
```rust
let sql_snapshot = sql_reader.read_snapshot(None).await?;
let file_snapshot = file_reader.read_snapshot(None).await?;
compare_snapshots(&sql_snapshot, &file_snapshot)?;
```

**Discrepancy handling:** Report detailed diff; user decides whether to accept or re-import.

### CLI Interface: Simple and Clear

**Decision:** Provide `deltasql import` subcommand:
```bash
deltasql import <source_table_uri> <target_db_uri> [options]
```

**Options:**
- `--dry-run`: Preview import without modifying database
- `--up-to-version=N`: Import only up to version N
- `--force`: Overwrite existing table in database
- `--skip-validation`: Skip post-import validation (faster, but risky)
- `--checkpoint-only`: Import checkpoint snapshot only (skip history)

**Examples:**
```bash
# Basic import
deltasql import s3://bucket/table deltasql://postgres/mydb/public/mytable

# Dry run to preview
deltasql import s3://bucket/table deltasql://postgres/mydb/public/mytable --dry-run

# Import with progress
deltasql import file:///data/table deltasql://sqlite/metadata.db?table=mytable

# Resume incremental import
deltasql import s3://bucket/table deltasql://postgres/mydb/public/mytable --resume
```

**Output:**
- Progress bar showing versions processed
- Summary: versions imported, actions inserted, time taken
- Validation results (if enabled)

### Progress Reporting

**Decision:** Show progress using indicatif crate:
- Progress bar: `[=============>    ] 750/1000 versions (75%)`
- Status messages: "Reading checkpoint...", "Importing version 500...", "Validating..."
- ETA calculation based on recent processing rate

**Rationale:**
- Large imports take minutes to hours; user needs feedback
- Allows killing import if it's unexpectedly slow
- Helps debug stuck imports

### Incremental Import: Version Tracking

**Decision:** Track last imported version in `dl_tables.properties.import_metadata`:
```json
{
  "last_imported_version": 999,
  "import_timestamp": "2025-11-09T12:00:00Z",
  "source_uri": "s3://bucket/table"
}
```

**Resume logic:**
- `--resume` flag reads `last_imported_version`
- Continues from `last_imported_version + 1` to latest in `_delta_log`
- Useful for importing new versions into already-migrated table

**Use case:** Import table that's still being written by legacy system, then periodically sync new versions.

### Error Handling: Fail Fast with Context

**Decision:** Import stops on first error with detailed message:
- Missing commit file: "Version 42 not found in _delta_log/"
- Corrupt checkpoint: "Failed to parse checkpoint at version 100: [error]"
- SQL constraint violation: "Failed to insert action: [constraint]"

**Rollback:** Transaction for current version is rolled back; previous versions remain imported.

**Resume:** User can fix issue (e.g., upload missing file) and resume import.

**Rationale:**
- Fail fast prevents importing incorrect data
- Detailed errors help user diagnose and fix
- Partial import is valid intermediate state

**Graceful degradation:**
- Corrupt checkpoint: Fall back to JSON replay with warning
- Missing intermediate version: Report gap and stop (user decides)

### Dry-Run Mode: Read-Only Preview

**Decision:** `--dry-run` performs all reading and validation without database modifications.

**Output:**
- "Would import 1000 versions"
- "Would insert 50000 add actions, 10000 remove actions, ..."
- "Estimated time: 5 minutes"
- "Estimated database size: 500 MB"

**Rationale:**
- Allows user to preview impact before committing
- Validates source table is readable
- Helps estimate resource requirements

**Implementation:** Use read-only transaction or skip SQL operations entirely.

## Risks / Trade-offs

**Risk: Import takes hours for large tables**
- Mitigation: Progress bar, ETA; document expected times
- Optimization: Use COPY for bulk inserts; process in parallel batches (future)
- Acceptable: One-time operation; user can schedule during maintenance window

**Risk: Corrupt checkpoint or missing commit files**
- Mitigation: Graceful fallback to full replay; clear error messages
- Prevention: Validate source table health before import (`delta-rs` table open)

**Risk: Database space requirements unknown upfront**
- Mitigation: Dry-run estimates size; document rule of thumb (2x JSON size)
- Monitoring: Show imported row counts and database size during import

**Trade-off: Version-by-version vs bulk import**
- Cost: Version-by-version is slower (more transactions)
- Benefit: Can resume from any version; simpler error handling
- Acceptable: One-time operation; correctness over speed

**Risk: Schema evolution during import confuses parser**
- Mitigation: Use delta-rs parser which handles evolution correctly
- Testing: Test with tables that have schema changes, protocol upgrades

**Risk: Import disrupts ongoing writes to source table**
- Mitigation: Import reads snapshot at start; doesn't lock source table
- Caveat: Source table may advance during import; use --up-to-version or resume later

## Migration Plan

**Phase 1: Basic Import**
1. Implement checkpoint reading using delta-rs
2. Implement JSON commit replay
3. Insert actions into SQL with batch optimization
4. Add progress reporting

**Phase 2: Validation**
1. Implement snapshot comparison logic
2. Add detailed diff reporting
3. Test with various table sizes and histories

**Phase 3: CLI and UX**
1. Build `deltasql` CLI binary with import subcommand
2. Add dry-run mode
3. Add progress bar and status messages
4. Write user-facing documentation

**Phase 4: Incremental Import**
1. Implement version tracking in `dl_tables.properties`
2. Add resume logic with `--resume` flag
3. Test incremental scenarios

**Phase 5: Hardening**
1. Error handling for corrupt files, missing versions
2. Graceful fallback from checkpoint to full replay
3. Add `--force` and `--checkpoint-only` options
4. Performance testing with large tables (1M+ commits)

**Phase 6: Documentation and Examples**
1. Write migration guide with step-by-step instructions
2. Provide example commands for common scenarios
3. Document troubleshooting (common errors and fixes)
4. Add rollback/recovery procedures

**Rollback:** Delete imported table from `dl_tables` and all action rows; source table in `_delta_log` is unchanged.

## Open Questions

1. **Should we support filtering during import (e.g., skip old versions)?**
   - Leaning: No initially; import complete history for correctness
   - Future: Add `--from-version` option if users need it
   - Decision point: After understanding real-world requirements

2. **How to handle very large checkpoints (> 1 GB)?**
   - Leaning: Stream checkpoint rows rather than loading in memory
   - Use delta-rs streaming reader if available
   - Decision point: During implementation

3. **Should import be multi-threaded?**
   - Leaning: No initially; single-threaded is simpler and still fast enough
   - Future: Parallelize across versions if needed (requires careful ordering)
   - Decision point: After performance benchmarks

4. **What to do if source table is actively being written during import?**
   - Leaning: Document that import captures snapshot at start
   - Use `--up-to-version` to import up to specific version
   - Use `--resume` to import new versions later
   - Decision point: Document as known limitation

5. **Should we support importing into non-empty database table?**
   - Leaning: Require empty table or `--force` flag to overwrite
   - Risk: Merging histories is complex and error-prone
   - Decision point: Keep simple unless clear use case emerges

6. **How to handle tables with non-UTF8 paths or metadata?**
   - Leaning: Import as-is; let SQL database handle encoding
   - Postgres supports UTF8; SQLite is more permissive
   - Decision point: Handle during implementation if errors arise

7. **Should validation be default or opt-in?**
   - Decision: Default to enabled; use `--skip-validation` to disable
   - Rationale: Correctness critical; cost is small (< 10% extra time)
