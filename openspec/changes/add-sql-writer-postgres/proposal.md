## Why
Writers need to safely commit Delta actions to the SQL metadata store with optimistic concurrency control to prevent conflicts. This enables the SQL database to become the authoritative source of truth while maintaining Delta's version-based consistency model.

## What Changes
- Implement `SqlTxnLogWriter` for PostgreSQL that satisfies the `TxnLogWriter` trait
- Support atomic commits with version validation (current_version + 1)
- Insert add/remove/metadata/protocol/txn actions into normalized tables within a transaction
- Enforce idempotency and detect external concurrent writers
- Provide clear error messages for version conflicts

## Impact
- Affected specs: `sql-metadata-postgres` (modified)
- Affected code: new `crates/sql-metadata-postgres/src/writer.rs`
- Breaking: None - additive implementation
- Dependencies: Requires `add-txnlog-traits`, `add-sql-schema-postgres`, and `add-sql-reader-postgres`
