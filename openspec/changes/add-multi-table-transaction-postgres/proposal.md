## Why
Support atomic commits that update multiple Delta tables in a single Postgres transaction, as outlined in the PRD multi-table ACID goal.

## What Changes
- Add a transaction API to stage per-table actions and commit atomically.
- Enforce per-table CAS on heads within the same DB transaction.
- Post-commit, enqueue mirror work per affected table.

## Impact
- Affected specs: multi-table-acid
- Affected code: Postgres writer transaction builder; dependency on mirror status enqueue.

## References
- PRD §4.3 Multi-Table Transactions – Atomic updates across tables
- PRD §6.3 Consistency Model – Version monotonicity and CAS
- PRD §15 M3 – Multi-table ACID milestone
