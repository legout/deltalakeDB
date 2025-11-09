## ADDED Requirements
### Requirement: Multi-Table ACID (Postgres)
The system SHALL support committing actions across multiple tables atomically within a single Postgres transaction.

#### Scenario: Atomic two-table commit
- **WHEN** actions for `table A` and `table B` are committed in one transaction
- **THEN** both tables advance versions and persist actions, or neither does if any step fails

#### Scenario: CAS across tables
- **WHEN** expected heads are provided for each table
- **THEN** the commit validates all expected versions before any write; a mismatch aborts the entire transaction

