## Context

Delta Lake's file-based transaction log doesn't support atomic commits across multiple tables. This forces applications to implement complex two-phase commit logic or accept inconsistent intermediate states. SQL-backed metadata enables true multi-table ACID transactions, critical for:

- Feature stores (features + labels updated atomically)
- Warehouse consistency (dimension + fact tables)
- Pipeline reliability (checkpoints across multiple outputs)
- ML workflows (model + metrics + metadata)

Key challenge: SQL provides atomicity, but mirroring to `_delta_log` is per-table and may lag, creating temporary inconsistency for external readers.

## Goals / Non-Goals

**Goals:**
- Atomic commits across 2-10 tables in single database transaction
- Pythonic API with context manager (`with deltars.begin()`)
- Clear consistency guarantees and caveats documented
- Proper error reporting indicating which table failed
- Graceful handling of partial mirror failures

**Non-Goals:**
- Distributed transactions across multiple databases
- Two-phase commit protocols (using single DB transaction)
- Guaranteed atomic mirroring (eventual consistency acceptable)
- Support for > 10 tables (can add if needed)

## Decisions

### Transaction API Design: Staged Actions with Single Commit

**Decision:** Multi-table transaction follows this pattern:
1. `tx = begin()` - Start transaction context
2. `tx.stage(table_id, actions)` - Stage actions for each table (multiple calls)
3. `tx.commit()` - Atomically commit all staged tables

**Rationale:**
- Explicit staging separates intent from execution
- Single commit point for atomic operation
- Allows validation before locks acquired
- Matches database transaction semantics

**Python API:**
```python
with deltars.begin() as tx:
    tx.write(table="deltasql://postgres/.../features", df=features_df, mode="append")
    tx.write(table="deltasql://postgres/.../labels", df=labels_df, mode="append")
    # Both committed atomically on exit, or both rolled back on exception
```

**Rust API:**
```rust
let mut tx = MultiTableTransaction::new(pool).await?;
tx.stage_table("features", actions_1)?;
tx.stage_table("labels", actions_2)?;
let result = tx.commit().await?; // Returns map of table_id -> version
```

**Alternatives considered:**
- Builder pattern: more verbose, less Pythonic
- Automatic detection of multi-table: too implicit, error-prone

### Version Validation: Lock All Tables Upfront

**Decision:** Acquire `SELECT ... FOR UPDATE` locks on all tables before inserting any actions.

**Lock order:** Sort table_ids lexicographically to prevent deadlocks.

**Rationale:**
- Consistent lock ordering prevents deadlocks between concurrent multi-table transactions
- All-or-nothing: either all tables update or none
- Detect conflicts early before expensive inserts

**Implementation:**
```rust
let table_ids = staged_tables.keys().sorted(); // Deterministic order
for table_id in table_ids {
    let row = sqlx::query!("SELECT current_version FROM dl_tables WHERE table_id = $1 FOR UPDATE", table_id)
        .fetch_one(&mut tx).await?;
    validate_version(table_id, row.current_version, expected)?;
}
// All locks acquired and validated, now insert actions
```

**Trade-off:** Longer lock hold time, but ensures consistency.

### Single Database Transaction

**Decision:** All table updates occur within one `BEGIN...COMMIT` block.

**Rationale:**
- PostgreSQL guarantees atomicity within transaction
- All version increments happen simultaneously
- Rollback on any failure is automatic
- Simple and correct

**Scope:**
```sql
BEGIN;
  -- Lock and validate all tables
  SELECT current_version FROM dl_tables WHERE table_id = $1 FOR UPDATE;
  -- Insert actions for table 1
  INSERT INTO dl_add_files ...;
  INSERT INTO dl_table_versions ...;
  -- Insert actions for table 2
  INSERT INTO dl_add_files ...;
  INSERT INTO dl_table_versions ...;
  -- Update all versions
  UPDATE dl_tables SET current_version = current_version + 1 WHERE table_id IN (...);
COMMIT;
```

### Mirror Handling: Sequential Per-Table After Commit

**Decision:** After SQL transaction commits, mirror each table sequentially (or in parallel with bounded concurrency).

**Failure model:**
- SQL commit succeeded → All tables at new versions in SQL
- Mirror fails for table A → Mark failed in `dl_mirror_status`, reconciler retries
- Mirror succeeds for table B → External readers see B's new version
- **Temporary inconsistency:** External readers may see different versions across tables

**Rationale:**
- Mirroring is asynchronous by design (separate from SQL commit)
- Per-table mirroring maintains isolation (one table's failure doesn't block others)
- Acceptable per PRD: external readers tolerate eventual consistency

**SLO:** p95 mirror lag < 5s per table; alert if any table lags > 1 minute.

**Documentation caveat:**
> Multi-table transactions provide atomic updates in SQL. External readers accessing `_delta_log` may temporarily observe different versions across tables during mirror lag (typically < 5 seconds).

### Error Reporting Strategy

**Decision:** Return detailed error indicating which table caused failure and at what stage.

**Error types:**
- `VersionConflict { table_id, expected, actual }` - One table had conflict
- `ValidationError { table_id, message }` - One table's actions invalid
- `MirrorFailure { table_id, version, error }` - Mirroring failed (after SQL success)

**Rationale:**
- Caller needs to know which table to investigate
- Different tables may have different retry strategies
- Clear attribution for debugging

**Python error example:**
```python
try:
    with deltars.begin() as tx:
        tx.write(table="table_a", df=df1)
        tx.write(table="table_b", df=df2)
except VersionConflict as e:
    print(f"Table {e.table_id} had conflict: expected v{e.expected}, got v{e.actual}")
```

### Isolation and Concurrency

**Decision:** Support concurrent multi-table transactions with overlapping and disjoint table sets.

**Behavior:**
- Overlapping tables: Row-level locks serialize transactions (first commits, second waits or conflicts)
- Disjoint tables: Transactions proceed concurrently without blocking

**Example:**
- TX1: Updates tables A, B
- TX2: Updates tables C, D
- Result: Both commit successfully in parallel

- TX1: Updates tables A, B
- TX2: Updates tables A, C
- Result: One commits first, other waits or gets version conflict

**Rationale:** Maximizes concurrency while ensuring correctness.

### Transaction Size Limits

**Decision:** Support up to 10 tables and 1000 files per table (10k files total) as initial limits.

**Rationale:**
- Covers 95% of use cases (feature store, dimension+fact, pipeline checkpoints)
- Prevents unbounded transaction size
- Can increase limits based on real-world needs

**Configuration:**
- `transaction.max_tables=10`
- `transaction.max_files_per_table=1000`
- `transaction.timeout=60s`

**Error:** Return clear message if limits exceeded, suggesting to split transaction.

## Risks / Trade-offs

**Risk: Long transaction hold times block other writers**
- Mitigation: Target < 10s for typical multi-table commit; use timeouts
- Monitoring: Track transaction duration; alert if p95 > 30s

**Risk: Deadlocks with complex transaction patterns**
- Mitigation: Deterministic lock ordering (sorted table_ids)
- Recovery: Postgres detects deadlocks; application retries

**Risk: Partial mirror creates inconsistency for external readers**
- Acceptable: Documented trade-off; SQL readers always consistent
- Mitigation: Fast mirroring (< 5s p95); reconciler repairs quickly

**Trade-off: Consistency vs availability**
- Cost: Multi-table transactions reduce concurrency on overlapping tables
- Benefit: Atomic updates critical for correctness-sensitive applications
- Acceptable: Use case warrants the trade-off (feature stores, warehouses)

**Risk: Transaction timeout during large commits**
- Mitigation: Use COPY for large file batches; document limits
- Future: Support chunked commits if needed

**Risk: Python context manager doesn't call commit on normal exit**
- Mitigation: Implement `__exit__` to auto-commit on success, rollback on exception
- Testing: Verify behavior with and without exceptions

## Migration Plan

**Phase 1: Core Transaction API**
1. Implement `MultiTableTransaction` struct in Rust
2. Add staging, validation, and commit logic
3. Test with 2-table scenarios

**Phase 2: Lock Ordering and Concurrency**
1. Implement deterministic lock ordering
2. Test concurrent transactions (overlapping and disjoint)
3. Verify deadlock prevention

**Phase 3: Python Bindings**
1. Expose transaction API via pyo3
2. Implement context manager
3. Add Python examples and tests

**Phase 4: Mirror Integration**
1. Hook multi-table commit into mirror engine
2. Handle partial mirror failures
3. Add metrics for multi-table mirror lag

**Phase 5: Production Hardening**
1. Add transaction size limits and validation
2. Implement timeouts and error handling
3. Document consistency model and caveats
4. Add monitoring and alerting

**Rollback:** Transaction rollback leaves database unchanged; applications fall back to single-table commits.

## Open Questions

1. **Should mirroring be parallel or sequential across tables?**
   - Leaning: Parallel with bounded concurrency (e.g., 5 tables at once)
   - Trade-off: Faster completion vs resource usage
   - Decision point: After performance testing

2. **How to handle transaction timeout?**
   - Leaning: Postgres statement_timeout or application-level timeout
   - On timeout: Rollback, return clear error
   - Decision point: During implementation

3. **Should we support savepoints for partial rollback?**
   - Leaning: No initially; full rollback is simpler
   - Use case: Rarely needed given validation before commit
   - Decision point: If user requests emerge

4. **What happens if reconciler fixes mirrors in different order than commit?**
   - Example: SQL commits A then B; reconciler mirrors B then A
   - Leaning: Acceptable; external readers see eventual consistency anyway
   - Document: Mirroring order is not guaranteed

5. **Should we expose transaction ID for debugging?**
   - Leaning: Yes, return UUID or timestamp-based ID
   - Use: Correlate logs, metrics, and errors
   - Decision point: During implementation

6. **Limits on transaction duration?**
   - Leaning: Default timeout 60s; configurable per transaction
   - Prevents stuck transactions holding locks
   - Decision point: After understanding typical durations
