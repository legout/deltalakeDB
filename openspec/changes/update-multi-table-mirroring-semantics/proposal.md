## Why
Align multi-table mirroring semantics with the PRD: database commit is atomic across tables, and mirroring to `_delta_log/` proceeds per-table after commit. External readers may temporarily observe divergent table versions until mirroring completes; this behavior must be explicitly specified with monitoring and reconciliation.

## What Changes
- Modify transactions spec to replace "atomic mirroring across tables" with per-table mirroring after an atomic DB commit
- Add requirements for per-table mirror status tracking, lag SLOs, and background reconciliation
- Document temporary external inconsistency and alerting

## Impact
- Affected specs: transactions
- Affected code: mirroring coordination logic and observability around mirror status/lag
- Non-goals: Removing existing DB atomicity; changing Delta compatibility guarantees

