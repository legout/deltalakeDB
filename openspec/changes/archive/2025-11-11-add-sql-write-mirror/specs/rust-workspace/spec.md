## ADDED Requirements
### Requirement: SQL Transaction Log Writer
The system SHALL provide a SqlTxnLogWriter that commits Delta actions to SQL with optimistic concurrency control.

#### Scenario: Single table commit
- **WHEN** actions are committed via SqlTxnLogWriter
- **THEN** they are inserted into SQL tables within a transaction with version validation

#### Scenario: Optimistic concurrency
- **WHEN** concurrent writers attempt to commit
- **THEN** only one succeeds per version, others retry with fresh snapshot

#### Scenario: Writer idempotency
- **WHEN** the same commit is retried
- **THEN** it is detected and handled gracefully without duplication
