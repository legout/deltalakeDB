## Why
The SQL-Backed Delta Metadata system must maintain Delta compatibility by mirroring SQL metadata to canonical `_delta_log/` files. This ensures external engines (Spark, Databricks, Polars, etc.) can continue reading tables while our system uses SQL for fast metadata operations. The mirroring engine must generate byte-for-byte compatible Delta JSON and Parquet checkpoint files.

## What Changes
- Add mirroring engine that generates canonical Delta JSON and Parquet files
- Implement retry logic and failure recovery for mirroring operations
- Add lag monitoring and alerting for mirroring delays
- Include validation that ensures `_delta_log/` files match Delta specification exactly

## Impact
- Affected specs: rust-workspace
- Affected code: crates/mirror/src/lib.rs (will be expanded significantly)
- Dependencies: object_store, parquet, tokio, serde_json, chrono, uuid