## Why
The SQL-Backed Delta Metadata system needs database adapters to provide a unified interface for reading and writing Delta metadata across different SQL databases (Postgres, SQLite, DuckDB). These adapters will implement the TxnLogReader/Writer traits and handle database-specific optimizations, schema management, and connection handling.

## What Changes
- Add TxnLogReader and TxnLogWriter trait abstractions
- Implement database-specific adapters for Postgres, SQLite, and DuckDB
- Add schema migration management and connection pooling
- Include database-specific query optimization and indexing strategies

## Impact
- Affected specs: rust-workspace
- Affected code: crates/sql/src/lib.rs (will be expanded significantly)
- Dependencies: sqlx, tokio, uuid, chrono, serde_json, async-trait