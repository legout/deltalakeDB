## Why
Provide SQLite support for SQL-backed reads to enable embedded/local workflows with fast table opens.

## What Changes
- Implement `SqlTxnLogReader` for SQLite using the core schema adapted to SQLite types.

## Impact
- Affected specs: delta-read-path
- Affected code: SQLite adapter for reader path.

## References
- PRD §4.1 Read Path – SqlTxnLogReader behavior
- PRD §13 Rollout – Phase 1 includes SQLite support
