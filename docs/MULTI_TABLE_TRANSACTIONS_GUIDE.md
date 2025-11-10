# Multi-Table Transactions Guide

## Overview

Multi-table transactions enable atomic, consistent updates across multiple Delta tables within a single database transaction. This is critical for:

- **Feature Stores**: Update features and labels atomically
- **Data Warehouses**: Maintain dimension/fact table consistency
- **ML Pipelines**: Checkpoint outputs consistently
- **Cross-table Upserts**: Atomic updates across related tables

## Key Guarantees

### SQL-Level Atomicity ✅
- All tables commit or all rollback
- No partial updates
- Snapshot isolation prevents conflicts

### Consistency ✅
- All version numbers updated atomically
- No orphaned rows or inconsistent state
- Constraints enforced per Delta protocol

### External Reader Consistency ⚠️
- SQL metadata always consistent
- `_delta_log` mirrors eventual (< 5s typical lag)
- External readers may temporarily see different versions across tables

## Architecture

### Transaction Flow

```
┌─────────────────────────┐
│ Begin Transaction       │
├─────────────────────────┤
│ Stage Table A Actions   │
│ Stage Table B Actions   │
│ Stage Table C Actions   │
├─────────────────────────┤
│ Lock All Tables         │ (Sorted by UUID - prevents deadlocks)
│ (SELECT...FOR UPDATE)   │
├─────────────────────────┤
│ Validate Versions       │
├─────────────────────────┤
│ Insert All Actions      │
│ (all tables in one TX)  │
├─────────────────────────┤
│ Update Versions         │
├─────────────────────────┤
│ Commit                  │
├─────────────────────────┤
│ Mirror Each Table       │ (Asynchronous, sequential or parallel)
│ (may fail independently)│
└─────────────────────────┘
```

### Lock Ordering

Tables are locked in deterministic order (sorted by UUID):
- Prevents deadlocks between concurrent multi-table transactions
- Lock order example: `[uuid-0001, uuid-0002, uuid-0003]`
- Same order regardless of staging order

### Version Validation

Each table validates its expected version before insertion:
```rust
// Check expected version with lock
SELECT current_version FROM dl_tables 
WHERE table_id = $1 FOR UPDATE;
```

If version mismatch → `VersionConflict` error → automatic rollback

## Rust API

### Basic Usage

```rust
use deltalakedb_core::{MultiTableTransaction, TransactionConfig, Action};
use deltalakedb_sql_metadata_postgres::MultiTableWriter;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to database
    let pool = sqlx::postgres::PgPool::connect("postgres://...").await?;
    
    // Create transaction
    let mut tx = MultiTableTransaction::new_default();
    
    // Prepare actions for table A
    let table_a_id = Uuid::new_v4();
    let actions_a = vec![/* ... */];
    
    // Prepare actions for table B
    let table_b_id = Uuid::new_v4();
    let actions_b = vec![/* ... */];
    
    // Stage tables
    tx.stage_table(table_a_id, actions_a)?;
    tx.stage_table(table_b_id, actions_b)?;
    
    // Commit atomically
    let writer = MultiTableWriter::new(pool);
    let result = writer.commit(tx).await?;
    
    // Get new versions
    println!("Table A new version: {}", result.get_version(&table_a_id).unwrap());
    println!("Table B new version: {}", result.get_version(&table_b_id).unwrap());
    
    Ok(())
}
```

### Configuration

```rust
use deltalakedb_core::{MultiTableTransaction, TransactionConfig};
use std::time::Duration;

let config = TransactionConfig {
    max_tables: 10,                    // Max 10 tables per transaction
    max_files_per_table: 1000,         // Max 1000 files per table
    timeout_secs: 60,                  // 60 second timeout
};

let tx = MultiTableTransaction::new(config);
```

### Error Handling

```rust
use deltalakedb_core::TransactionError;

match writer.commit(tx).await {
    Ok(result) => {
        println!("Success: {} tables committed", result.table_count());
    }
    Err(TransactionError::VersionConflict { table_id, expected, actual }) => {
        eprintln!("Table {} conflict: expected v{}, got v{}", table_id, expected, actual);
        // Retry logic: fetch current versions and retry
    }
    Err(TransactionError::TooManyTables { count, limit }) => {
        eprintln!("Too many tables: {} (limit: {})", count, limit);
        // Split into multiple transactions
    }
    Err(TransactionError::TransactionTimeout) => {
        eprintln!("Transaction timeout - too much data?");
        // Reduce data or split into multiple transactions
    }
    Err(e) => {
        eprintln!("Transaction error: {}", e);
    }
}
```

## Python API

### Context Manager (Recommended)

```python
import delkalakedb

# Using context manager - commits on success, rolls back on error
with delkalakedb.begin() as tx:
    # Write to features table
    features_df = pd.DataFrame({'id': [1, 2, 3], 'score': [0.5, 0.7, 0.9]})
    tx.write(
        table='deltasql://postgres/mydb/public/features',
        df=features_df,
        mode='append'
    )
    
    # Write to labels table
    labels_df = pd.DataFrame({'id': [1, 2, 3], 'label': [0, 1, 1]})
    tx.write(
        table='deltasql://postgres/mydb/public/labels',
        df=labels_df,
        mode='append'
    )
    
    # Both tables committed atomically here on exit
```

### Manual Transaction Management

```python
import delkalakedb

tx = delkalakedb.begin()
try:
    tx.write(table='table_a_uri', df=df_a, mode='append')
    tx.write(table='table_b_uri', df=df_b, mode='append')
    result = tx.commit()
    print(f"Committed: {result.versions}")
except delkalakedb.VersionConflict as e:
    print(f"Conflict on table {e.table_id}: expected v{e.expected}, got v{e.actual}")
    tx.rollback()
except Exception as e:
    print(f"Error: {e}")
    tx.rollback()
```

### Error Handling

```python
import delkalakedb
import pandas as pd

try:
    with delkalakedb.begin() as tx:
        # Write operations...
        pass
except delkalakedb.TransactionError as e:
    print(f"Transaction failed: {e}")
    # Handle based on error type
    if isinstance(e, delkalakedb.VersionConflict):
        print(f"Conflict on {e.table_id}")
    elif isinstance(e, delkalakedb.TooManyTables):
        print(f"Too many tables: {e.count} > {e.limit}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Use Cases

### Feature Store Update

```rust
// Update features and labels atomically
let mut tx = MultiTableTransaction::new_default();

let feature_actions = vec![
    Action::Add(AddFile { path: "features.parquet", ... }),
];
let label_actions = vec![
    Action::Add(AddFile { path: "labels.parquet", ... }),
];

tx.stage_table(features_table_id, feature_actions)?;
tx.stage_table(labels_table_id, label_actions)?;

let result = writer.commit(tx).await?;
```

### Dimension + Fact Tables

```python
with delkalakedb.begin() as tx:
    # Update dimensions
    tx.write('deltasql://postgres/wh/dim_customer', dim_df, mode='overwrite')
    tx.write('deltasql://postgres/wh/dim_date', date_df, mode='overwrite')
    
    # Update facts using new dimensions
    tx.write('deltasql://postgres/wh/fact_sales', sales_df, mode='append')
    # All three tables committed together
```

### Multi-Output Pipeline

```python
# ML pipeline with consistent checkpoint
with delkalakedb.begin() as tx:
    # Write model predictions
    tx.write('deltasql://postgres/ml/predictions', pred_df)
    
    # Write model metrics
    tx.write('deltasql://postgres/ml/metrics', metrics_df)
    
    # Write run metadata
    tx.write('deltasql://postgres/ml/runs', run_df)
    # All tables at consistent version
```

## Consistency Model

### Atomicity (ACID)
- **Guaranteed**: SQL-level atomicity within database transaction
- **Scope**: All tables in transaction
- **Behavior**: All-or-nothing per version

### Consistency (ACID)
- **Guaranteed**: Version numbers consistent across tables
- **Enforcement**: Constraints per Delta protocol
- **Validation**: Version checks before commit

### Isolation (ACID)
- **Level**: Snapshot isolation (PostgreSQL default)
- **Prevention**: Serializable transactions available if needed
- **Conflicts**: Optimistic concurrency with retry

### Durability (ACID)
- **Guaranteed**: After commit returns, all data persisted
- **Scope**: SQL metadata only (mirrors eventual)
- **Recovery**: Standard database recovery

### External Reader Consistency ⚠️

**Important caveat**: External readers (Spark, DuckDB, etc.) accessing `_delta_log` may temporarily see different versions across tables.

**Example**:
1. SQL transaction commits Tables A@v10, B@v10
2. Mirror writes A's `_delta_log/0000000010.json` immediately
3. External reader reads Spark table A @ v10
4. Mirror writes B's `_delta_log/0000000010.json` after 2 seconds
5. External reader reads Spark table B @ v9 initially
6. After 5 seconds: Both tables @ v10

**Implications**:
- SQL readers: Always consistent
- External readers: Eventual consistency (< 5s typical)
- For consistent external reads: wait 5-10 seconds or implement application-level coordination

## Performance

### Typical Latencies

| Operation | Latency | Notes |
|-----------|---------|-------|
| 2-table commit | 10-50ms | Small batches |
| 5-table commit | 50-200ms | Medium batches |
| 10-table commit | 100-500ms | Large batches |
| Mirror delay | 1-5s | Per table sequential |

### Throughput

- **Tables per second**: ~10-20 tables/sec (limited by database)
- **Files per second**: ~100-500 files/sec
- **Scaling**: Limited by single database instance (100k+ files need partitioning)

### Tuning

```sql
-- Increase PostgreSQL performance
SET maintenance_work_mem = '4GB';
SET work_mem = '256MB';

-- Use COPY for bulk inserts
ALTER TABLE dl_add_files DISABLE TRIGGER ALL;
-- ... multi-table transaction ...
ALTER TABLE dl_add_files ENABLE TRIGGER ALL;
VACUUM ANALYZE;
```

## Error Handling

### Version Conflict

**Cause**: Another writer updated the table between transaction start and commit.

**Recovery**:
```rust
match result {
    Err(TransactionError::VersionConflict { table_id, expected, actual }) => {
        // Fetch updated versions
        let new_versions = reader.read_snapshot(Some(table_id)).await?;
        
        // Retry transaction
        let mut tx = MultiTableTransaction::new_default();
        // ... re-stage with updated versions ...
        writer.commit(tx).await?
    }
}
```

### Timeout

**Cause**: Transaction taking too long (> 60s default).

**Solutions**:
- Reduce data per transaction
- Increase timeout: `TransactionConfig { timeout_secs: 120, ... }`
- Check for database issues (slow inserts, locks)

### Too Many Tables/Files

**Cause**: Exceeds limits (10 tables or 1000 files/table).

**Solution**: Split into multiple transactions

```rust
// Instead of:
tx.stage_table(a, 1500_actions)?; // Error: 1500 > 1000

// Do:
tx.stage_table(a, first_1000)?;
writer.commit(tx).await?;

let mut tx2 = MultiTableTransaction::new_default();
tx2.stage_table(a, next_500)?;
writer.commit(tx2).await?;
```

## Limitations

- **Max tables per transaction**: 10 (configurable)
- **Max files per table**: 1000 (configurable)
- **Max transaction duration**: 60s (configurable)
- **Lock timeout**: PostgreSQL default (usually 30s)

## Monitoring

### Metrics to Track

```
- transaction_duration_seconds
- tables_per_transaction
- version_conflicts_total
- mirror_lag_seconds (per table)
- transaction_rollbacks_total
- timeout_errors_total
```

### Alerts

```
- Mirror lag > 60s for any table
- Version conflicts spike
- Transaction timeout rate increasing
- Lock wait time exceeding threshold
```

## Best Practices

1. **Keep transactions small**: < 10 tables, < 1000 files per table
2. **Use context managers**: Automatic rollback on error (Python)
3. **Handle version conflicts**: Implement retry logic
4. **Monitor performance**: Track transaction duration and lag
5. **Document consistency model**: External readers see eventual consistency
6. **Test rollback**: Verify transaction semantics in your application
7. **Split large updates**: Multiple transactions for safety and performance

## Future Enhancements

- Parallel mirroring across tables
- Savepoints for partial rollback
- Statement timeout configuration
- Transaction prioritization
- Performance monitoring dashboard
- Cost-based transaction planning
