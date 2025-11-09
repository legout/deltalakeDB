## Context

The mirror engine is the **compatibility bridge** between our SQL-backed metadata and the Delta ecosystem. Every commit must be mirrored to `_delta_log` as canonical JSON and Parquet files so that external tools (Spark, Databricks, DuckDB, Polars) can read tables without modifications.

This is the most critical component for avoiding lock-in and maintaining Delta compatibility. Failure here means external tools can't read our tables, violating a core PRD goal.

Key challenges:
- Produce byte-for-byte canonical Delta artifacts
- Handle transient failures without corrupting state
- Minimize mirror lag while accepting eventual consistency
- Detect and repair stalled mirrors

## Goals / Non-Goals

**Goals:**
- Generate canonical Delta JSON commits (`NNNNNNNNNN.json`)
- Generate valid Parquet checkpoints at configured intervals
- Ensure idempotent mirroring (safe retries)
- Track mirror status per (table_id, version)
- Provide reconciliation for failed mirrors
- Meet SLO: p95 mirror lag < 5s, alert if > 60s

**Non-Goals:**
- Optimizing object storage costs (use lifecycle policies)
- Supporting non-standard checkpoint formats
- Bi-directional sync (SQL is authoritative, mirror is one-way)
- Real-time mirroring guarantees (eventual consistency is acceptable)

## Decisions

### Mirroring Sequence: Post-Commit Only

**Decision:** Mirror artifacts are written **after** SQL transaction commits successfully. Never mirror before commit.

**Rationale:**
- SQL is source of truth; object storage is derived
- Prevents orphaned artifacts if SQL transaction rolls back
- Simplifies reasoning about consistency
- Matches "dual write" best practice (authoritative write first)

**Implementation:**
```rust
let version = sql_writer.finalize_commit(handle).await?; // SQL commit
mirror_engine.mirror_version(table_id, version).await?;  // Object store write
```

**Failure mode:** SQL committed but mirror fails → SQL is ahead, reconciler repairs later.

### Idempotency: Conditional Put or Content Hash Check

**Decision:** Use object store features for idempotent writes:
- **S3/GCS/Azure:** Use conditional put (if-none-match) or check existing content hash
- **Fallback:** Write to temp key, verify content, rename atomically

**Rationale:**
- Retrying mirror must not corrupt existing artifacts
- Content-based idempotency ensures correctness
- Supports all major object stores

**Implementation pattern:**
```rust
let content = serialize_commit(actions);
let key = format!("_delta_log/{:020}.json", version);
match object_store.put_if_absent(&key, content.clone()).await {
    Ok(_) => { /* success */ }
    Err(AlreadyExists) => {
        // Verify existing content matches
        let existing = object_store.get(&key).await?;
        if existing == content {
            // Idempotent retry, treat as success
        } else {
            // Conflict! Different content for same version
            return Err(MirrorConflict);
        }
    }
}
```

**Trade-off:** Extra read on collision, but collisions should be rare (version sequence ensures uniqueness).

### Checkpoint Cadence: Configurable Interval

**Decision:** Generate checkpoint every N commits (default N=10), configurable per table.

**Rationale:**
- Matches Delta default behavior
- Balances checkpoint cost vs replay cost for readers
- Allows tuning per table characteristics (small vs large tables)

**Configuration:**
- `mirror.checkpoint_interval=10` (default)
- Override per table in `dl_tables.properties`

**Checkpoint strategy:**
- Simple checkpoint: all active files at version
- Future: Support incremental checkpoints (Delta protocol v2+)

### JSON Serialization: Deterministic and Canonical

**Decision:** Serialize actions with deterministic field ordering and formatting per Delta spec.

**Requirements:**
- Field order: alphabetical within each action type
- Timestamps: Unix epoch milliseconds (integers, not floats)
- Nulls: omit optional fields rather than `null` values
- Protocol/metadata actions first, then txn, then add, then remove

**Rationale:**
- Ensures byte-for-byte compatibility with Delta spec
- Prevents spurious diffs when validating
- Matches reference implementation behavior

**Implementation:** Use serde with custom serializer or `BTreeMap` to enforce ordering.

### Mirror Status Tracking

**Decision:** Add `dl_mirror_status` table:
```sql
CREATE TABLE dl_mirror_status (
    table_id UUID NOT NULL,
    version BIGINT NOT NULL,
    status TEXT NOT NULL,  -- 'SUCCESS', 'PENDING', 'FAILED'
    json_written BOOLEAN DEFAULT FALSE,
    checkpoint_written BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    PRIMARY KEY (table_id, version)
);
```

**Rationale:**
- Enables reconciler to find gaps
- Tracks partial failures (JSON written but checkpoint failed)
- Provides observability for mirror lag
- Supports alerting on stuck mirrors

### Reconciliation Strategy

**Decision:** Background reconciler runs periodically (default: every 30s):
1. Find versions with status = 'FAILED' or 'PENDING' and `last_attempt_at < now() - threshold`
2. Retry mirror with exponential backoff (1s, 2s, 4s, 8s, 16s, max 5 attempts)
3. After max attempts, mark as 'FAILED' and alert
4. Emit metrics: `mirror_lag_seconds`, `mirror_failures_total`

**Rationale:**
- Handles transient failures automatically
- Backoff prevents thundering herd
- Manual intervention only for persistent failures
- Decouples writer latency from mirror retries

**Configuration:**
- `mirror.reconcile_interval=30s`
- `mirror.max_retry_attempts=5`
- `mirror.retry_backoff_base=1s`

### Failure Handling: Best-Effort with Eventual Consistency

**Decision:** Mirror failures do **not** roll back SQL commit. Reconciler repairs later.

**Rationale:**
- SQL commit is authoritative; rolling back corrupts SQL state
- Mirror is derived data (can be regenerated from SQL)
- Availability over immediate consistency for external readers
- Matches "eventual consistency" model in PRD

**Consistency model:**
- SQL readers: Always see latest committed version (strongly consistent)
- External readers via `_delta_log`: May lag during mirror failures (eventually consistent)

**SLO:** p95 mirror lag < 5s, p99 < 60s, alert if any version lags > 5 minutes.

## Risks / Trade-offs

**Risk: Mirror falls behind during object store outage**
- Mitigation: Reconciler continues retrying; metrics alert operators
- Acceptable: SQL readers unaffected; external readers tolerate lag per PRD

**Risk: Non-canonical serialization breaks external readers**
- Mitigation: Comprehensive conformance tests against Delta spec; validate with Spark/DuckDB
- Testing: Property tests for serialization determinism

**Trade-off: Immediate consistency vs availability**
- Cost: External readers may see stale data during mirror lag
- Benefit: SQL commit never blocks on object store issues
- Acceptable: PRD explicitly allows this trade-off

**Risk: Checkpoint generation cost for large tables**
- Mitigation: Checkpoints are async; don't block writer; can rate-limit
- Future: Implement incremental checkpoints for very large tables

**Risk: Mirror conflict (different content for same version)**
- Indicates bug or external writer bypassing SQL
- Mitigation: Error loudly; require manual intervention
- Prevention: Document that mixing SQL and file-based writers is unsupported

**Risk: Reconciler overhead on database**
- Mitigation: Limit concurrency (e.g., 10 parallel reconcile tasks); add rate limiting
- Monitoring: Track reconciler query load

## Migration Plan

**Phase 1: JSON Mirroring**
1. Implement JSON commit serialization with determinism tests
2. Implement write to object store with idempotency
3. Add `dl_mirror_status` table
4. Hook into writer post-commit

**Phase 2: Checkpoint Support**
1. Implement Parquet checkpoint writer
2. Add checkpoint interval configuration
3. Test checkpoint readability with external tools

**Phase 3: Reconciliation**
1. Implement background reconciler
2. Add exponential backoff and retry logic
3. Add metrics and alerting

**Phase 4: Validation and Hardening**
1. Conformance tests against Delta spec
2. Cross-validation with Spark/DuckDB readers
3. Chaos testing (simulate object store failures)
4. Performance testing for checkpoint generation

**Rollback:** Disable mirror engine; tables revert to SQL-only mode. External readers can't access until mirroring restored.

## Open Questions

1. **Should mirror be synchronous or asynchronous?**
   - Leaning: Synchronous for JSON (fast, < 100ms), async for checkpoints (slow, > 1s)
   - Decision point: After performance testing

2. **How to handle version gaps in reconciliation?**
   - Scenario: Versions 0-99, 101-200 exist; version 100 missing
   - Leaning: Reconciler processes in order; gap blocks progress; alert operator
   - Decision point: During implementation

3. **Should we support "mirror-on-demand" for manual repair?**
   - Leaning: Yes, add CLI command `deltasql mirror repair <table> --version=N`
   - Decision point: After basic reconciler works

4. **How to validate mirror correctness in production?**
   - Leaning: Periodic "mirror validation" job compares SQL vs `_delta_log` derived snapshots
   - Decision point: During production deployment planning

5. **What object store permissions are needed?**
   - Minimum: `PutObject`, `GetObject`, `ListObjects` on `_delta_log/` prefix
   - Optional: `DeleteObject` for cleanup tools
   - Document: Security best practices for credentials
