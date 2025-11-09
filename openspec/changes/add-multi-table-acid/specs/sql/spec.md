## ADDED Requirements
### Requirement: Multi-Table Transaction Management
The system SHALL provide transaction management that spans multiple Delta tables while maintaining Delta compatibility.

#### Scenario: Staged actions across tables
- **WHEN** actions are staged for multiple tables
- **THEN** they are held in a transaction context until commit

#### Scenario: Ordered mirroring
- **WHEN** a multi-table SQL transaction succeeds
- **THEN** each table's actions are mirrored to `_delta_log/` in correct version order

#### Scenario: Partial mirror failure
- **WHEN** mirroring fails for some tables in a multi-table commit
- **THEN** SQL state remains consistent and failed mirrors are retried
