## Why
Standardize discovery/connection to the SQL-backed metadata plane with a clear URI scheme. This simplifies configuration and enables per-engine adapters.

## What Changes
- Define `deltasql://` URI scheme for Postgres, SQLite, and DuckDB.
- Parse URIs and route to the correct engine adapter.
- Validate table identifiers and surface clear errors for invalid URIs.

## Impact
- Affected specs: delta-uri
- Affected code: Rust URI parser/router; Python thin wrapper that accepts the same URIs.

## References
- PRD §4.4 Table Discovery – URI scheme and SQL catalog
- PRD §11 API & UX – Example URI forms and Python usage
