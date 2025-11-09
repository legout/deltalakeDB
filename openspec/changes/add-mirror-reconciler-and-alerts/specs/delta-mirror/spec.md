## ADDED Requirements
### Requirement: Mirror Reconciler and Lag Alerts
The system SHALL include a background reconciler that ensures `_delta_log` artifacts are eventually written, and SHALL surface lag metrics/alerts.

#### Scenario: Retry failed mirrors
- **WHEN** a `(table_id, version)` has mirror status `FAILED`
- **THEN** the reconciler retries with backoff until status becomes `SUCCEEDED`

#### Scenario: Mirror lag alert
- **WHEN** the time between SQL commit and successful mirror exceeds a configured threshold
- **THEN** an alert is emitted and a metric `mirror_lag_seconds` reflects the delay

