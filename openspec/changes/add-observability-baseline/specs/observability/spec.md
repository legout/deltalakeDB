## ADDED Requirements
### Requirement: Observability Baseline
The system SHALL provide basic metrics, structured logs, and tracing for read, write, and mirror operations.

#### Scenario: Read latency metric
- **WHEN** opening a table or performing time travel
- **THEN** a metric `metadata_open_latency_ms` is recorded

#### Scenario: Commit and mirror metrics
- **WHEN** a commit is executed and mirrored
- **THEN** `commit_latency_ms`, `versions_committed_total`, and `mirror_latency_ms` are recorded; failures increment `mirror_failures_total`

