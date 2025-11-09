## Why
The SQL-backed metadata system needs a normalized relational schema to store Delta Lake actions (add/remove files, metadata updates, protocol changes, transactions). PostgreSQL is chosen as the first target database for its robustness, JSONB support, and production-readiness.

## What Changes
- Define PostgreSQL schema with tables for Delta metadata: `dl_tables`, `dl_table_versions`, `dl_add_files`, `dl_remove_files`, `dl_metadata_updates`, `dl_protocol_updates`, `dl_txn_actions`
- Add indexes for fast lookup by table_id, version, and path
- Include JSONB columns for flexible stats and properties storage
- Provide migration scripts for schema setup and versioning

## Impact
- Affected specs: new capability `sql-metadata-postgres`
- Affected code: `crates/sql-metadata-postgres/migrations/`
- Breaking: None - this is a new capability
- Dependencies: Requires PostgreSQL 12+ with JSONB support
