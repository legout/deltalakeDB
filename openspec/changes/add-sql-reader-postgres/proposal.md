## Why
Reading Delta metadata from object storage requires listing and parsing thousands of small JSON files, causing high latency. A SQL-backed reader can query the metadata database directly, reducing table open time from seconds to milliseconds.

## What Changes
- Implement `SqlTxnLogReader` for PostgreSQL that satisfies the `TxnLogReader` trait
- Support reading latest version, snapshot reconstruction, and active file listing via SQL queries
- Enable time travel by version number and timestamp
- Use connection pooling with sqlx for efficient database access

## Impact
- Affected specs: `sql-metadata-postgres` (modified)
- Affected code: new `crates/sql-metadata-postgres/src/reader.rs`
- Breaking: None - additive implementation
- Performance: Target p95 < 800ms for tables with 100k active files (local DB)
- Dependencies: Requires `add-txnlog-traits` and `add-sql-schema-postgres`
