## Why
Provide a minimal, consistent set of metrics, logs, and tracing spans across read/write/mirror paths to meet PRD observability goals.

## What Changes
- Add counters/gauges/histograms for latencies and version throughput.
- Add structured logs per commit and reconcile attempt.
- Add tracing spans around SQL transactions and object-store writes.

## Impact
- Affected specs: observability
- Affected code: shared telemetry layer; instrumentation hooks in adapters.

## References
- PRD §10 Telemetry – Metrics, logs, tracing
- PRD §5 Non-Functional – Reliability and observability SLOs
- PRD §15 M4 – Hardening & GA observability
