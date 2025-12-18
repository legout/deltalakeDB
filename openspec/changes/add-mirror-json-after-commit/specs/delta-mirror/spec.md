## ADDED Requirements
### Requirement: Mirror JSON Commits After DB Commit
The system SHALL mirror committed actions to `_delta_log/NNNNNNNNNN.json` only after the corresponding SQL transaction commits.

#### Scenario: Successful commit mirrors JSON
- **WHEN** a table commit succeeds in SQL (version `V`)
- **THEN** a canonical JSON file `V.json` exists under the table’s `_delta_log/` with the serialized actions

#### Scenario: Mirror failure recorded and retried
- **WHEN** the JSON write fails due to a transient error
- **THEN** a mirror status is recorded for `(table_id, V)` and the system retries until successful

