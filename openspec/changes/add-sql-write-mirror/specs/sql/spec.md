## ADDED Requirements
### Requirement: Delta Log Mirroring
The system SHALL mirror SQL commits to canonical Delta artifacts in `_delta_log/` for external engine compatibility.

#### Scenario: JSON commit mirroring
- **WHEN** a SQL commit succeeds
- **THEN** the actions are serialized to canonical Delta JSON format

#### Scenario: Checkpoint generation
- **WHEN** the checkpoint interval is reached
- **THEN** a Parquet checkpoint is generated following Delta specification

#### Scenario: Mirror failure handling
- **WHEN** mirroring to object storage fails
- **THEN** the failure is tracked and retried until successful

#### Scenario: Mirror reconciliation
- **WHEN** mirror lag is detected
- **THEN** a background process reconciles the SQL state with `_delta_log/`
