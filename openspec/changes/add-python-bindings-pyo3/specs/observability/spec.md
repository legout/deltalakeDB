## MODIFIED Requirements

### Requirement: Telemetry and Observability (Python)
The system SHALL expose observability metrics and tracing from Rust core to Python applications.

#### Scenario: Collect metrics in Python context
- **WHEN** a Python application calls `table.open()` or `transaction.commit()`
- **THEN** latency and operation metrics are collected and available for export

#### Scenario: Structured logging (Python)
- **WHEN** a commit succeeds or fails
- **THEN** structured logs include operation type, duration, version, and any errors

#### Scenario: Tracing spans (Python)
- **WHEN** a Python application enables tracing (e.g., via OpenTelemetry)
- **THEN** Rust operations emit spans that integrate with the tracing context

