## Why
Provide DuckDB support for SQL-backed reads to enable embedded analytics workflows with low-latency metadata access.

## What Changes
- Implement `SqlTxnLogReader` for DuckDB using equivalent schema and queries adapted to DuckDB.

## Impact
- Affected specs: delta-read-path
- Affected code: DuckDB adapter for reader path.

## References
- PRD §4.1 Read Path – SqlTxnLogReader behavior
- PRD §13 Rollout – Phase 1 includes DuckDB support
