## Why
Define the minimal SQL schema to store Delta actions and table metadata in a relational database as the source of truth.

## What Changes
- Create core tables: `dl_tables`, `dl_table_versions`, `dl_add_files`, `dl_remove_files`, `dl_metadata_updates`, `dl_protocol_updates`.
- Optional: `dl_txn_actions` for app-id progress.
- Add recommended indexes for common lookups.

## Impact
- Affected specs: sql-metadata-catalog
- Affected code: DB migrations for Postgres, SQLite, DuckDB (engine-appropriate DDL)

## References
- PRD §7 Data Model – Proposed SQL schema and indexes
- PRD §6.3 Consistency Model – Versioning and optimistic concurrency
