Minimal DDL for the SQL-backed metadata catalog.

Files:
- postgres.sql — JSONB/arrays, enum for mirror status, GIN indexes.
- sqlite.sql — JSON as TEXT, booleans as INTEGER, simple indexes.
- duckdb.sql — UUID/TIMESTAMP/JSON types, array of VARCHAR for partition columns.

Notes:
- `dl_table_heads` tracks `current_version` for CAS and fast head lookups.
- `dl_mirror_status` acts as the durable mirror queue for reconciler.
- Foreign keys are minimized on hot paths to reduce write amplification; add if needed by your deployment.
- For JSON planning indexes, only Postgres includes GIN by default.

Alignment with PRD:
- Schema derived from PRD §7; CAS/versioning model from §6.3; mirror status from §8.
