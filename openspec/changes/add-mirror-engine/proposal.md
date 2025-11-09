## Why
To maintain full Delta Lake ecosystem compatibility, every commit in the SQL metadata store must be mirrored to the object store as canonical `_delta_log` artifacts (JSON commits and Parquet checkpoints). This allows external engines like Spark, Databricks, DuckDB, and Polars to read tables without changes.

## What Changes
- Implement mirror engine that writes Delta-compliant JSON and Parquet artifacts to object storage
- Serialize committed SQL actions into newline-delimited JSON files (`NNNNNNNNNN.json`)
- Generate Parquet checkpoints at configured intervals
- Ensure byte-for-byte canonical formatting per Delta protocol spec
- Track mirror status and implement retry logic for failures
- Add reconciliation to detect and repair mirror lag

## Impact
- Affected specs: new capability `sql-metadata-mirror`
- Affected code: new `crates/sql-metadata-mirror/src/`
- Breaking: None - this is additive
- Dependencies: Requires `add-sql-writer-postgres` (or any SQL writer implementation)
- Compatibility: Critical for external tool interoperability
