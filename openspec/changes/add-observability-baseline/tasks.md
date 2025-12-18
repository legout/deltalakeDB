## 1. Implementation
- [x] 1.1 Define metrics: `metadata_open_latency_ms`, `commit_latency_ms`, `mirror_latency_ms`, `versions_committed_total`, `mirror_failures_total`, `mirror_backlog_gauge`
- [x] 1.2 Add structured logs for commits and mirror attempts
- [x] 1.3 Add tracing spans around SQL tx and object-store writes

## 2. Validation
- [x] 2.1 Metrics emit during happy path operations
- [x] 2.2 Failed mirror increments failure counters and logs details

## 3. Dependencies
- None (wires into existing paths incrementally)
