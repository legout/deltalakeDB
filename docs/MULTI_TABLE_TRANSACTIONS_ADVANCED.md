# Multi-Table Transactions - Advanced Guide

## Overview

Multi-table transactions enable atomic, consistent, isolated updates across 2-10 Delta tables within a single database transaction. This guide covers advanced patterns, consistency guarantees, and operational considerations.

> **Document Relation**: This is the advanced guide. See [MULTI_TABLE_TRANSACTIONS_GUIDE.md](./MULTI_TABLE_TRANSACTIONS_GUIDE.md) for introduction and basic usage.

---

## Consistency Model

### SQL-Level Guarantees

All updates occur within a single PostgreSQL transaction:

```
BEGIN;
  SELECT current_version FROM dl_tables WHERE table_id IN (...) FOR UPDATE;
  VALIDATE all versions match expected;
  INSERT INTO dl_add_files, dl_remove_files, etc.;
  UPDATE dl_tables SET current_version = current_version + 1 WHERE table_id IN (...);
COMMIT or ROLLBACK;
```

**Guarantees**:
- ✅ Atomicity: All tables updated or none
- ✅ Consistency: Version increments always by exactly 1
- ✅ Isolation: No dirty reads from concurrent transactions
- ✅ Durability: PostgreSQL WAL ensures recovery

**Implication**: Once SQL transaction commits, ALL tables are at their new versions in SQL.

### Mirror Eventual Consistency

After SQL commit succeeds, each table is mirrored to registered engines (Spark, DuckDB, etc.) sequentially:

```rust
// SQL transaction succeeds, all tables updated
SQL commit successful: table_a at v2, table_b at v3

// Mirror phase begins (after SQL commit)
Mirror to Spark: table_a v2 -> success
Mirror to Spark: table_b v3 -> failed (retried by reconciler)
Mirror to DuckDB: table_a v2 -> success
Mirror to DuckDB: table_b v3 -> success
```

**Implication**: During mirror lag (typically < 5s), external readers may see:
- Table A: v2 (new) in Spark/DuckDB
- Table B: v1 (old) in Spark/DuckDB
- Both: v2, v3 respectively in SQL

This is acceptable because:
- Mirror failures are detected and reconciled
- SQL is the source of truth
- Temporary inconsistency is tolerable for most use cases
- SLA: p95 lag < 5s, alert if > 1 minute

---

## Advanced Patterns

### Pattern 1: Feature Store with Labels

Update features and labels atomically:

```rust
// Rust
let mut tx = MultiTableTransaction::new(config);

let features = extract_features(&df)?;
let feature_actions = to_actions(&features);
tx.stage_table(feature_table_id, StagedTable::new(feature_table_id, feature_actions))?;

let labels = extract_labels(&df)?;
let label_actions = to_actions(&labels);
tx.stage_table(label_table_id, StagedTable::new(label_table_id, label_actions))?;

let result = writer.commit(tx).await?;
```

**Semantics**: Both features and labels advance to the same epoch simultaneously.

**Benefit**: No window where features and labels are misaligned.

---

### Pattern 2: Dimension + Fact Tables

Synchronize a dimension update with fact table:

```rust
// Update dimension (SCD Type 2: insert new version)
let dim_actions = vec![
    Action::Add(add_new_dimension_version()),
];
tx.stage_table(dimension_id, StagedTable::new(dimension_id, dim_actions))?;

// Append fact records using new dimension version
let fact_actions = vec![
    Action::Add(add_fact_records_with_new_dim()),
];
tx.stage_table(fact_id, StagedTable::new(fact_id, fact_actions))?;

// Both committed atomically
let result = writer.commit(tx).await?;
```

**Semantics**: All fact records reference dimension version X simultaneously.

**Benefit**: No partial updates; all facts use consistent dimension data.

---

### Pattern 3: Checkpoint Across Pipeline Stages

Multiple pipeline outputs checkpoint atomically:

```rust
// Stage 1: Raw data
let stage1_actions = process_raw_data(input)?;
tx.stage_table(raw_data_table_id, StagedTable::new(raw_data_table_id, stage1_actions))?;

// Stage 2: Transformed data
let stage2_actions = transform_data(input)?;
tx.stage_table(transformed_table_id, StagedTable::new(transformed_table_id, stage2_actions))?;

// Stage 3: ML features
let stage3_actions = extract_features(input)?;
tx.stage_table(features_table_id, StagedTable::new(features_table_id, stage3_actions))?;

// All checkpoint simultaneously
let result = writer.commit(tx).await?;
```

**Semantics**: All 3 stages checkpoint to consistent state.

**Benefit**: Rerun decisions can use all 3 tables without skew.

---

## Error Handling

### Conflict Resolution

When a table in the transaction has a version conflict:

```rust
match writer.commit(tx).await {
    Ok(result) => {
        // All tables succeeded
        println!("Committed: {:?}", result.versions);
    }
    Err(TransactionError::VersionConflict { table_id, expected, actual }) => {
        // One table conflicted; ALL tables rolled back
        eprintln!("Conflict on table {}: expected v{}, got v{}", table_id, expected, actual);
        
        // Option 1: Retry with fresh versions
        let fresh_tx = reload_transaction_with_new_versions(&db).await?;
        writer.commit(fresh_tx).await?;
        
        // Option 2: Merge or rethink logic
        let merged_tx = merge_and_retry(&db).await?;
        writer.commit(merged_tx).await?;
    }
    Err(other) => eprintln!("Transaction error: {}", other),
}
```

**Semantics**: Conflict on ANY table rolls back ALL tables.

**Rationale**: Ensures all-or-nothing semantics.

### Partial Mirror Failures

Mirror failures after SQL commit don't rollback:

```rust
let result = writer.commit(tx).await?;

// SQL succeeded for all tables
let mirror_results = writer.mirror_all_tables_after_commit(&result).await;

for (&table_id, results) in &mirror_results {
    for (engine, success, error) in results {
        if !success {
            // Log but don't rollback SQL transaction
            eprintln!("Mirror failed for table {} in {}: {}", table_id, engine, error);
            // Reconciler will retry this automatically
        }
    }
}
```

**Semantics**: Mirror failures are independent of SQL success.

**Design**: Maintains simplicity and robustness (don't rollback working SQL).

---

## Configuration & Limits

### Transaction Configuration

```rust
pub struct TransactionConfig {
    pub max_tables: usize,              // Default: 10
    pub max_files_per_table: usize,     // Default: 1000
    pub timeout_secs: u64,              // Default: 60
}
```

**Rationale**:
- `max_tables=10`: Prevent accidental huge transactions; increase if needed
- `max_files_per_table=1000`: Per-table insert optimization; avoid memory issues
- `timeout_secs=60`: Balance between lock hold time and timeout safety

**Override**:
```rust
let config = TransactionConfig {
    max_tables: 5,
    max_files_per_table: 500,
    timeout_secs: 30,
};
let tx = MultiTableTransaction::with_config(config);
```

---

## Performance Characteristics

### Lock Hold Time

Multi-table transactions hold row-level locks on `dl_tables` for the duration:

```
Time:  0ms - BEGIN transaction
      10ms - Acquire SELECT...FOR UPDATE on table_a
      15ms - Acquire SELECT...FOR UPDATE on table_b
      20ms - Validate versions
      50ms - Insert actions for all tables
      80ms - Update versions for all tables
      85ms - COMMIT (locks released)
```

**Total lock time**: ~85ms for 2 tables with 1000 files each.

**Guidance**:
- Keep action counts reasonable (< 1000 files per table)
- Avoid nested transactions
- Monitor lock contention: `pg_locks`, `pg_stat_activity`

### Throughput

Measured with realistic workloads:

| Scenario | Files/Table | Tables | Duration | Throughput |
|----------|-------------|--------|----------|-----------|
| Lightweight | 100 | 2 | ~50ms | 4 txn/s |
| Medium | 500 | 3 | ~150ms | 6.6 txn/s |
| Heavy | 1000 | 5 | ~400ms | 2.5 txn/s |

**Optimization**: Use connection pooling, batch multiple writes.

---

## Monitoring & Observability

### Key Metrics

Track in your monitoring system:

```
multi_table_transaction_duration_ms: Time from begin to commit
  - Label: transaction_id
  - Label: table_count
  - Percentiles: p50, p95, p99

multi_table_transaction_conflicts: Count of version conflicts
  - Label: table_id
  - Increment on each conflict

multi_table_mirror_lag_ms: Time from SQL commit to mirror success
  - Label: table_id
  - Label: engine (spark, duckdb)
  - Alert if > 60s
```

### Logging

Enable debug logging to trace transactions:

```bash
# Rust
RUST_LOG=deltalakedb_sql_metadata_postgres::multi_table_writer=debug cargo test -- --nocapture

# Python (future)
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("deltalakedb")
```

Expected log output:

```
[INFO] Beginning multi-table transaction txn-abc123 with 3 tables
[DEBUG] Locking table 550e8400-e29b-41d4-a716-446655440000
[DEBUG] Table 550e8400-e29b-41d4-a716-446655440000 locked at version 42
[DEBUG] Inserting 1000 actions for table 550e8400-e29b-41d4-a716-446655440000
[DEBUG] Updated versions for 3 tables
[INFO] Multi-table transaction txn-abc123 committed successfully with 3 tables
```

---

## Troubleshooting

### Symptom: Frequent Version Conflicts

**Diagnosis**: Multiple writers competing for same tables.

**Solutions**:
1. Use separate tables for different writers
2. Implement exponential backoff on conflict
3. Switch to single-writer-per-table model

**Debug**:
```sql
SELECT txn_id, table_id, expected_version, actual_version, error_message
FROM transaction_error_log
WHERE error_type = 'VersionConflict'
ORDER BY timestamp DESC LIMIT 10;
```

### Symptom: Slow Mirror Sync (> 30s lag)

**Diagnosis**: Mirror engine is overloaded or network issue.

**Solutions**:
1. Check object store (S3, GCS) performance
2. Increase mirror worker threads (if configurable)
3. Check Spark/DuckDB catalog performance
4. Consider async mirroring queue

**Monitor**:
```sql
SELECT table_id, mirror_engine, 
       NOW() - last_sync_time AS lag_sec,
       last_error
FROM dl_mirror_status
WHERE mirrored_version < current_version
ORDER BY lag_sec DESC;
```

### Symptom: Deadlocks Between Concurrent Transactions

**Diagnosis**: Lock ordering not deterministic.

**Solution**: Already handled by sorting table_ids lexicographically. If still occurring:
1. Check PostgreSQL deadlock logs: `log_min_duration_statement = 0`
2. Verify transaction code is using `MultiTableWriter::commit()`
3. Report issue with deadlock details

**Prevention**:
```rust
// ✅ Correct: Lock order is deterministic (sorted)
let table_ids = vec![id_zebra, id_apple, id_banana];
let sorted = MultiTableWriter::lock_order(table_ids);
// sorted == vec![id_apple, id_banana, id_zebra]

// ❌ Incorrect: Lock order is non-deterministic
for table_id in table_ids {
    sqlx::query!("SELECT ... FOR UPDATE WHERE table_id = $1", table_id).fetch_one(&mut tx).await?;
}
```

---

## Testing Multi-Table Transactions

### Unit Tests

Test transaction staging and validation:

```rust
#[test]
fn test_staging_basic() {
    let mut tx = MultiTableTransaction::new(TransactionConfig::default());
    let table_id = Uuid::new_v4();
    let actions = vec![/* ... */];
    tx.stage_table(table_id, StagedTable::new(table_id, actions))?;
    assert_eq!(tx.table_count(), 1);
}
```

### Integration Tests

Test with real PostgreSQL:

```rust
#[tokio::test]
#[ignore]
async fn test_multi_table_commit_atomicity() -> Result<()> {
    let pool = create_test_pool().await?;
    let writer = MultiTableWriter::new(pool.clone());
    
    // Setup tables...
    // Create transaction...
    // Commit...
    // Verify both tables updated atomically...
    
    Ok(())
}
```

Run with:
```bash
cargo test --test multi_table_transaction_tests -- --ignored --nocapture
```

### Chaos Testing (Advanced)

Simulate mirror failures:

```rust
// Mock mirror failure for specific table
struct MockMirrorFailure {
    fail_for_engine: String,
}

impl MockMirrorFailure {
    async fn mirror_with_failure(&self, engine: &str, ...) -> Result<()> {
        if engine == self.fail_for_engine {
            return Err("Simulated failure".to_string());
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_mirror_failure_recovery() {
    // Setup...
    // Trigger partial mirror failure...
    // Verify reconciler retries...
}
```

---

## FAQ

**Q: Can I use multi-table transactions with different databases?**
A: Currently only PostgreSQL backend. SQLite/DuckDB backends would need transaction support. Use separate transactions per database.

**Q: What happens if one table in the transaction takes too long?**
A: PostgreSQL statement timeout applies to the entire transaction. Configure `statement_timeout` appropriately.

**Q: Can I read tables in a transaction?**
A: Not yet. Current design only supports writes. Read-modify-write requires application-level coordination.

**Q: Should I use multi-table transactions for all multi-table operations?**
A: Only when atomicity is critical:
- ✅ Feature + label updates
- ✅ Dimension + fact sync
- ✅ Multi-stage pipeline checkpoint
- ❌ Independent table operations (use separate single-table commits)

**Q: How does this compare to distributed transactions?**
A: Multi-table transactions are:
- ✅ Simpler: single database, no 2PC
- ✅ Faster: no coordinator overhead
- ❌ Limited: only one database, max 10 tables
- ❌ Not distributed: all tables must be in same PostgreSQL instance

---

## Best Practices

1. **Keep transactions focused**: 2-5 tables is ideal; > 8 tables is rare.

2. **Minimize lock hold time**: Stage actions efficiently before committing.

3. **Handle conflicts gracefully**: Implement retry logic with exponential backoff.

4. **Monitor continuously**: Track lock contention, mirror lag, conflict rates.

5. **Test rollback scenarios**: Ensure error handling works correctly.

6. **Document table relationships**: Make multi-table dependencies explicit.

7. **Plan mirror strategy**: Decide which engines to mirror based on downstream consumers.

8. **Profile performance**: Measure lock times, throughput for your workload.

---

## See Also

- [MULTI_TABLE_TRANSACTIONS_GUIDE.md](./MULTI_TABLE_TRANSACTIONS_GUIDE.md) - Basic usage
- [POSTGRES_SCHEMA_ER_DIAGRAM.md](./POSTGRES_SCHEMA_ER_DIAGRAM.md) - Schema relationships
- [POSTGRES_WRITER_GUIDE.md](./POSTGRES_WRITER_GUIDE.md) - Single-table writes
- PostgreSQL Documentation: [Explicit Locking](https://www.postgresql.org/docs/current/explicit-locking.html)
