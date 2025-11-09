## ADDED Requirements

### Requirement: Multi-Table Transaction API
The system SHALL provide an API to commit changes across multiple tables atomically.

#### Scenario: Two-table atomic commit
- **WHEN** transaction stages changes for table A and table B then commits
- **THEN** both tables are updated atomically or neither is changed

#### Scenario: Rollback on failure
- **WHEN** any table validation fails during commit
- **THEN** entire transaction rolls back and no tables are modified

### Requirement: Transaction Staging
The system SHALL allow staging actions for multiple tables before commit.

#### Scenario: Staging multiple tables
- **WHEN** transaction stages actions for tables A, B, and C
- **THEN** all actions are held in memory until commit is called

#### Scenario: Validation during staging
- **WHEN** staging actions for a table
- **THEN** actions are validated before being accepted into transaction

### Requirement: Version Validation
The system SHALL validate versions for all tables before committing.

#### Scenario: Multi-table version check
- **WHEN** committing transaction with multiple tables
- **THEN** system validates current version for each table with row locks

#### Scenario: Version conflict in any table
- **WHEN** any table has version conflict
- **THEN** entire transaction fails with clear indication of which table conflicted

### Requirement: Atomic Database Commit
The system SHALL commit all table changes in a single database transaction.

#### Scenario: Single transaction scope
- **WHEN** committing multiple tables
- **THEN** all insertions occur within one `BEGIN...COMMIT` block

#### Scenario: Version increment atomicity
- **WHEN** transaction commits
- **THEN** all table versions increment by one simultaneously

### Requirement: Post-Commit Mirroring
The system SHALL mirror each table independently after SQL commit succeeds.

#### Scenario: Sequential mirroring
- **WHEN** multi-table commit succeeds
- **THEN** system mirrors each table's version to its `_delta_log` in order

#### Scenario: Mirror failure isolation
- **WHEN** one table's mirror fails
- **THEN** other tables continue mirroring and failure is tracked for retry

### Requirement: Consistency Model
The system SHALL document consistency guarantees for external readers.

#### Scenario: SQL consistency
- **WHEN** reading via SQL after multi-table commit
- **THEN** all tables reflect new versions immediately

#### Scenario: External reader caveat
- **WHEN** reading via `_delta_log` after multi-table commit
- **THEN** tables may temporarily show different versions due to mirror lag

### Requirement: Python Context Manager
The system SHALL provide Pythonic transaction API using context managers.

#### Scenario: Context manager usage
- **WHEN** using `with deltars.begin() as tx`
- **THEN** transaction commits on exit or rolls back on exception

#### Scenario: Explicit rollback
- **WHEN** calling `tx.rollback()`
- **THEN** transaction is aborted and no changes are committed

### Requirement: Error Reporting
The system SHALL report which table caused transaction failure.

#### Scenario: Table-specific error
- **WHEN** table B causes version conflict in multi-table commit
- **THEN** error message identifies table B with expected and actual versions

#### Scenario: Action validation error
- **WHEN** invalid actions are staged for table C
- **THEN** error indicates table C and specific validation failure

### Requirement: Isolation
The system SHALL ensure proper isolation for concurrent multi-table transactions.

#### Scenario: Overlapping tables
- **WHEN** two transactions both include table A
- **THEN** row-level locking ensures only one commits successfully

#### Scenario: Disjoint tables
- **WHEN** transaction 1 modifies tables A, B and transaction 2 modifies tables C, D
- **THEN** both can commit concurrently without conflict

### Requirement: Transaction Limits
The system SHALL support reasonable transaction sizes.

#### Scenario: Multiple tables limit
- **WHEN** staging changes for up to 10 tables
- **THEN** transaction completes successfully within timeout

#### Scenario: Large payload per table
- **WHEN** each table has 1000+ files in transaction
- **THEN** commit completes within reasonable time (< 30s)
