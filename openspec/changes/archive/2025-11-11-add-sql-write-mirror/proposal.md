## Why
To maintain Delta compatibility while using SQL as the source of truth, we need a SqlTxnLogWriter that commits to SQL first, then mirrors canonical Delta artifacts to `_delta_log/` for external engines.

## What Changes
- Implement SqlTxnLogWriter using the TxnLogWriter trait
- Add optimistic concurrency control with version validation
- Create mirror engine for Delta JSON and Parquet checkpoint generation
- Implement retry logic for failed mirroring operations
- Add mirror status tracking and reconciliation

## Impact
- Affected specs: rust-workspace, sql  
- Affected code: Extensions to sql crate and new mirror crate
- Enables SQL-backed writes while preserving Delta ecosystem compatibility
- Critical for multi-table ACID transactions
