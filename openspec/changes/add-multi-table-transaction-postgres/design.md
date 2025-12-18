## Context
Enable atomic commits across multiple Delta tables using a single Postgres transaction, aligning with PRD §4.3 and §6.3. SQL is authoritative; `_delta_log` mirrors follow post-commit per table.

## Goals / Non-Goals
- Goals
  - Stage actions for multiple tables and commit atomically.
  - Enforce optimistic concurrency per table within one DB tx.
  - Return per-table new versions and enqueue mirror work.
- Non-Goals
  - Cross-DB transactions or distributed 2PC.
  - External reader atomicity across tables (document caveat).

## Decisions
- API shape
  - `TransactionBuilder::stage(table_id, expected_head, actions)`; `commit()` returns `Vec<(table_id, new_version)>`.
- Concurrency enforcement
  - Validate all expected heads up front:
    ```sql
    SELECT table_id, current_version FROM dl_table_heads WHERE table_id = ANY($ids);
    ```
  - On commit, update heads with conditional updates for each table inside one tx; abort if any CAS fails.
- Version allocation
  - Compute `new_version = expected + 1` per table; insert ledger/actions with that version inside the same tx.
- Failure handling
  - Any error rolls back the entire transaction; no mirror enqueued.
- Mirroring
  - After commit success, insert `PENDING` rows into `dl_mirror_status` for each `(table_id, new_version)`; reconciler ensures eventual `_delta_log` writes.
- Ordering
  - No cross-table ordering requirement for mirrors; per-table order is maintained.

## Risks / Trade-offs
- CAS contention across many tables can increase retries; mitigate with retry loop and backoff at API level.
- Large staged actions across multiple tables may increase tx size; recommend chunking by table or limiting per-commit rows.

## Open Questions
- Should we expose a maximum number of tables per transaction to limit blast radius?

