## Why
Delta Lake's file-based transaction log doesn't support atomic commits across multiple tables, forcing applications to implement complex coordination logic. SQL-backed metadata enables true multi-table ACID transactions, allowing atomic updates to feature stores, dimension/fact tables, or cross-table upserts.

## What Changes
- Add transaction API that spans multiple tables within a single database transaction
- Allow staging actions for multiple tables before commit
- Commit all tables atomically with correct version increments
- Mirror each table's artifacts to `_delta_log` after successful commit
- Handle partial mirror failures gracefully with reconciliation

## Impact
- Affected specs: `sql-metadata-core` (modified)
- Affected code: new `crates/sql-metadata-core/src/transaction.rs`
- Breaking: None - this is an additive API
- Dependencies: Requires all SQL writer and mirror capabilities
- Use cases: Feature store atomicity, warehouse dimension/fact consistency, pipeline reliability
