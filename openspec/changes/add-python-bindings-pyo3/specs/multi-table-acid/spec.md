## MODIFIED Requirements

### Requirement: Multi-Table Transaction API (Python)
The system SHALL provide a transaction builder and multi-table commit API accessible from Python.

#### Scenario: Stage actions for multiple tables (Python)
- **WHEN** creating a `TransactionBuilder` and calling `builder.add_table_actions(table_id, actions)`
- **THEN** actions are staged per table without immediate commit

#### Scenario: Inspect staged state (Python)
- **WHEN** calling `builder.staged_tables()` or `builder.actions_for(table_id)`
- **THEN** returns the currently staged actions for inspection/validation

#### Scenario: Atomic commit (Python)
- **WHEN** calling `builder.commit(connection)` with expected heads for each table
- **THEN** all tables are updated atomically in a single DB transaction (all or nothing)

#### Scenario: Concurrency on multi-table (Python)
- **WHEN** calling `builder.commit()` with mismatched version on any table
- **THEN** the entire transaction is aborted and a `ConcurrencyError` is raised indicating which table conflicted

#### Scenario: Post-commit mirroring (Python)
- **WHEN** a multi-table commit succeeds
- **THEN** mirror status is enqueued for each affected table for eventual `_delta_log` emission

