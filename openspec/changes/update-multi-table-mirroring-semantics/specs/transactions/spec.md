## MODIFIED Requirements

### Requirement: Integration with Delta Log Mirroring
The system SHALL coordinate multi-table transactions with Delta log mirroring for external compatibility.

#### Scenario: Per-table mirroring after commit
- **WHEN** a multi-table transaction is committed in the database
- **THEN** the mirroring system SHALL mirror each affected table independently in version order immediately after the atomic DB commit
- **AND** SHALL record `IN_FLIGHT → COMPLETE` status transitions per `(table_id, version)` while allowing a temporary divergence across tables for external readers until mirroring completes

#### Scenario: Mirror status state machine
- **WHEN** the mirror process starts for a table-version pair
- **THEN** the system SHALL persist a status row with timestamps and outcome codes (`IN_FLIGHT`, `COMPLETE`, `FAILED`) for that `(table_id, version)`
- **AND** SHALL expose this status through both SQL (`dl_mirror_status`) and metrics endpoints for downstream monitoring

#### Scenario: Mirror lag SLO instrumentation
- **WHEN** mirroring is in progress after a multi-table commit
- **THEN** the system SHALL emit `mirror_lag_seconds{table_id,version}` and `mirror_status{table_id,version}` metrics plus parity counters
- **AND** SHALL alert when lag exceeds defined SLOs (e.g., p95 < 5s, absolute < 60s) or when parity checks detect drift

#### Scenario: Background reconciliation
- **WHEN** a mirroring attempt for any table fails after a successful DB commit
- **THEN** a background reconciler SHALL retry until required JSON and checkpoint files are present
- **AND** SHALL ensure the external view converges to the committed state and updates mirror status/metrics accordingly
