## Why
Ensure eventual convergence of `_delta_log` artifacts and operational visibility by retrying failed mirrors and alerting on lag.

## What Changes
- Add a background reconciler that processes `dl_mirror_status` rows in `PENDING`/`FAILED` state.
- Emit metrics and alerts when mirror lag exceeds thresholds.

## Impact
- Affected specs: delta-mirror, observability
- Affected code: Mirror worker, metrics integration.

## References
- PRD §8 Failure Modes & Recovery – Mirror retries and lag
- PRD §10 Telemetry – Metrics and tracing expectations
- PRD §15 M3 – Reconciler and SLOs
