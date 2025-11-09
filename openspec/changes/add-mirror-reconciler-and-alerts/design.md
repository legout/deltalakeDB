## Context
Introduce a background reconciler to ensure `_delta_log` artifacts converge and surface mirror lag per PRD §8 and §10.

## Goals / Non-Goals
- Goals
  - Retry failed or pending mirrors until success with backoff.
  - Enforce per-table ordering of mirrors.
  - Emit metrics and basic alerts for lag and failures.
- Non-Goals
  - Complex workflow orchestration; keep a single worker/process simple loop initially.

## Decisions
- Work queue
  - Use `dl_mirror_status` as the durable queue with states: PENDING, FAILED, SUCCEEDED.
  - Poll newest first but enforce not to advance beyond the smallest missing version for a table.
- Backoff policy
  - Exponential backoff with jitter; configurable min/max; persist attempt count and next eligibility timestamp.
- Concurrency
  - Per-table single-flight; allow parallelism across tables up to a configurable limit.
- Lag computation
  - `mirror_lag_seconds = now() - committed_at` from `dl_table_versions` for versions not yet SUCCEEDED.
- Alerting
  - Thresholds: warn > 60s, critical > 300s (defaults); surface via logs/metrics and adapter hook for external alerting systems.

## Risks / Trade-offs
- Long-running failures can cause backlog; mitigate with dead-letter snapshot/export and operator guidance.
- Strict per-table ordering reduces throughput; acceptable in v1 for correctness.

## Open Questions
- Should we support partitioned workers (by table hash) for horizontal scaling later?

