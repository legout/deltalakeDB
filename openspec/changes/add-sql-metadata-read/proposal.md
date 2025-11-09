## Why
To enable fast metadata reads from SQL databases, we need to implement the SQL schema defined in the PRD and create a SqlTxnLogReader that can reconstruct table snapshots from the relational data model.

## What Changes
- Create SQL schema for Postgres, SQLite, and DuckDB based on PRD §7
- Implement SqlTxnLogReader using the TxnLogReader trait
- Add database connection management and query builders
- Support time travel queries by version and timestamp
- Add basic table discovery and schema introspection

## Impact
- Affected specs: rust-workspace, sql
- Affected code: New sql crate under `crates/sql/src/lib.rs`
- Enables sub-second metadata reads vs file-based scanning
- Foundation for SQL write path and multi-table transactions
