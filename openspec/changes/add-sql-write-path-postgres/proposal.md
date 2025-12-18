## Why
Enable writes to the SQL metadata catalog on Postgres with optimistic concurrency, laying the groundwork for mirroring and multi-table transactions.

## What Changes
- Implement `SqlTxnLogWriter` (Postgres) that records actions in `dl_*` tables within a single SQL transaction.
- Enforce CAS on `dl_table_heads.current_version` to prevent lost updates.
- On success, enqueue mirror status for JSON emission (handled by mirror change).

## Impact
- Affected specs: delta-write-path
- Affected code: Rust Postgres adapter; error types; small integration into commit API.

## References
- PRD §4.2 Write Path – SqlTxnLogWriter sequencing and validation
- PRD §6.3 Consistency Model – Optimistic concurrency and versioning
- PRD §8 Failure Modes – Handling rollback/no mirror on DB rollback
- PRD §15 M2 – Single-table commit acceptance
