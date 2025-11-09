## Why
Enable fast metadata reads by reconstructing Delta snapshots directly from SQL on Postgres. Start with read-only support to keep scope small.

## What Changes
- Implement `SqlTxnLogReader` for Postgres.
- Support table open, time travel by version or timestamp, and listing active files, schema, partition columns, protocol, and table properties.
- Leave writers and mirroring for later changes.

## Impact
- Affected specs: delta-read-path
- Affected code: Rust `sql` adapter (Postgres), reader wiring via `TxnLogReader` trait.

## References
- PRD §4.1 Read Path – SqlTxnLogReader behavior
- PRD §5 Non-Functional – Performance targets for table open
- PRD §15 M1 – SQL read path acceptance
