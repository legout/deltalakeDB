## Context

The SQL writer is responsible for safely committing Delta actions to PostgreSQL while enforcing Delta's optimistic concurrency model (strictly incrementing versions). This is the critical path for data integrity—failures here could corrupt table state or violate ACID guarantees.

Key challenges:
- Concurrent writers must not create version conflicts or gaps
- All actions in a commit must be atomic (all or nothing)
- Version must increment by exactly 1 per commit
- Performance target: support commits with 1000+ files in < 5s

## Goals / Non-Goals

**Goals:**
- Implement `TxnLogWriter` trait for PostgreSQL
- Ensure atomicity via database transactions
- Prevent version conflicts with optimistic concurrency control
- Support efficient batch inserts for large commits
- Provide clear error messages for debugging

**Non-Goals:**
- Cross-table transactions (separate proposal: add-multi-table-txn)
- Mirroring to `_delta_log` (separate proposal: add-mirror-engine)
- Support for non-Postgres databases (future work)
- Performance optimization beyond basic batching

## Decisions

### Optimistic Concurrency: SELECT FOR UPDATE with Version Check

**Decision:** Use `SELECT current_version FROM dl_tables WHERE table_id = ? FOR UPDATE` followed by validation that `current_version + 1 = expected_version`.

**Rationale:**
- Row-level lock prevents concurrent writers from seeing same version
- `FOR UPDATE` blocks other writers until commit/rollback
- Explicit version check provides clear error on conflict
- Works correctly even with READ COMMITTED isolation

**Implementation pattern:**
```rust
let tx = pool.begin().await?;
let row = sqlx::query!("SELECT current_version FROM dl_tables WHERE table_id = $1 FOR UPDATE", table_id)
    .fetch_one(&mut tx).await?;
if row.current_version + 1 != expected_version {
    return Err(VersionConflict { expected, actual: row.current_version });
}
// ... insert actions ...
sqlx::query!("UPDATE dl_tables SET current_version = $1 WHERE table_id = $2", expected_version, table_id)
    .execute(&mut tx).await?;
tx.commit().await?;
```

**Alternatives considered:**
- Compare-and-swap without lock: race condition between SELECT and UPDATE
- Serializable isolation: higher lock contention, more transaction aborts
- Advisory locks: less portable, harder to reason about

### Batch Insertion Strategy

**Decision:** Use multi-row INSERT statements with sqlx for batches up to 1000 rows; switch to COPY for larger batches.

**Rationale:**
- Multi-row INSERT: Simple, supported by sqlx macros, good for typical commits
- COPY: Much faster for large batches (10-100x), but more complex
- Threshold of 1000 rows balances simplicity and performance

**Implementation:**
```rust
if files.len() < 1000 {
    // Use parameterized multi-row INSERT
    sqlx::query!(/*...*/).execute(&mut tx).await?;
} else {
    // Use COPY with tokio-postgres (optional feature)
    copy_to_table(&mut tx, "dl_add_files", files).await?;
}
```

**Trade-offs:**
- COPY requires feature flag and separate driver (tokio-postgres)
- Adds complexity but critical for large compactions/rewrites

### Transaction Lifecycle

**Decision:** Transaction spans entire commit operation:
1. Begin transaction
2. Lock version
3. Validate version
4. Insert all actions (adds, removes, metadata, protocol, txn)
5. Insert commit metadata into `dl_table_versions`
6. Update `current_version`
7. Commit transaction

**Rationale:**
- Single transaction ensures atomicity
- Lock held for entire duration prevents conflicts
- Rollback on any error leaves database unchanged
- Matches Delta's atomic commit semantics

**Performance consideration:** Long transactions can block readers on `dl_tables` row. Mitigation: keep operations fast (< 5s target), use connection pooling to avoid serialization.

### Error Handling and Retry

**Decision:** Writer returns specific error types; caller decides retry strategy.

**Error types:**
- `VersionConflict`: Caller should refresh and retry
- `ValidationError`: Caller should fix input
- `DatabaseError`: Caller may retry with backoff
- `TimeoutError`: Caller should retry or escalate

**Rationale:**
- Separation of concerns: writer handles atomicity, caller handles retries
- Different errors need different responses
- Avoid built-in retry logic that hides issues

### Idempotency Handling

**Decision:** Reject duplicate version writes at database level (version conflict error).

**Rationale:**
- Primary key on `(table_id, version)` prevents duplicates
- Caller sees conflict and knows not to retry
- Simple and correct

**Alternative considered:**
- Application-level idempotency tokens: adds complexity without clear benefit for version-based system

### Action Validation

**Decision:** Validate actions before starting transaction.

**Checks:**
- Required fields present (path for add/remove, schema for metadata)
- Protocol versions are valid integers
- Partition values match table's partition columns (future)
- File paths don't contain illegal characters

**Rationale:**
- Fail fast before acquiring locks
- Clearer error messages
- Reduces wasted database resources

## Risks / Trade-offs

**Risk: Long transaction blocks readers**
- Mitigation: Target < 5s for 10k file commits; use COPY for large batches
- Monitoring: Track transaction duration; alert if > 10s p95

**Risk: Deadlocks with concurrent transactions**
- Mitigation: Acquire locks in consistent order (always lock table before inserting actions)
- Postgres detects deadlocks and aborts one transaction; caller retries

**Trade-off: Lock duration vs throughput**
- Cost: Row lock held during entire commit blocks other writers
- Benefit: Simplicity and correctness; version conflicts are rare in practice
- Acceptable: Write throughput is secondary to correctness for metadata

**Risk: Connection pool exhaustion**
- Mitigation: Configure appropriate pool size; use timeouts
- Monitoring: Track active connections and queue depth

**Risk: SQL injection via dynamic queries**
- Mitigation: Use sqlx compile-time checked queries; never interpolate user input
- Policy: All queries use `query!` or `query_as!` macros

## Migration Plan

**Phase 1: Single-Action Commits**
1. Implement basic writer for add/remove actions only
2. Test version conflict handling with concurrent writers
3. Validate rollback on errors

**Phase 2: Full Action Support**
1. Add metadata and protocol action support
2. Add transaction action support
3. Test with realistic commit payloads

**Phase 3: Batch Optimization**
1. Implement multi-row INSERT for moderate batches
2. Add COPY support behind feature flag
3. Benchmark and tune thresholds

**Phase 4: Production Hardening**
1. Add comprehensive error handling and logging
2. Implement connection pool tuning
3. Add metrics for commit latency and throughput
4. Stress test with concurrent writers

**Rollback:** Writer failure leaves database unchanged (transaction rollback); application can retry or fail gracefully.

## Open Questions

1. **Should we implement writer-side validation of Delta protocol rules?**
   - Leaning: Yes, basic validation (required fields, protocol versions)
   - But skip complex rules (e.g., schema compatibility) initially
   - Decision point: During implementation

2. **Connection pool size and timeout configuration?**
   - Leaning: pool_size = 10, timeout = 30s as defaults; configurable
   - Decision point: After performance testing

3. **Should we support "dry-run" mode?**
   - Leaning: No initially; can add if needed for testing
   - Transaction rollback achieves similar effect

4. **How to handle very large commits (100k+ files)?**
   - Leaning: Document limits; recommend splitting into multiple transactions
   - Future: Support chunked commits if needed
   - Decision point: After understanding real-world usage patterns

5. **Should we prune old version data?**
   - Decision: No, retention is application concern (separate from write path)
   - Can add administrative tools later
