## Why
Users need a simple way to specify SQL-backed Delta tables without exposing connection strings and table identifiers separately. URI schemes provide a familiar, portable way to reference tables across different database backends (PostgreSQL, SQLite, DuckDB).

## What Changes
- Define `deltasql://` URI scheme with database-specific variants
- Parse URIs to extract connection parameters and table identifiers
- Support `deltasql://postgres/database/schema/table`, `deltasql://sqlite/path/to/db.db?table=name`, `deltasql://duckdb/path/to/catalog.duckdb?table=name`
- Integrate URI parsing into Python and Rust APIs
- Maintain compatibility with existing file-based URIs (`s3://`, `file://`)

## Impact
- Affected specs: `sql-metadata-core` (modified)
- Affected code: new `crates/sql-metadata-core/src/uri.rs`, Python bindings updates
- Breaking: None - additive to existing URI handling
- Dependencies: None - this is foundational for user experience
