## ADDED Requirements
### Requirement: Multi-Table ACID Transactions
The system SHALL support atomic commits that update multiple Delta tables within a single SQL transaction.

#### Scenario: Atomic multi-table commit
- **WHEN** actions are staged for multiple tables and committed
- **THEN** all tables are updated atomically within a single SQL transaction

#### Scenario: Transaction rollback
- **WHEN** a multi-table transaction fails
- **THEN** no changes are persisted to any of the involved tables

#### Scenario: Cross-table consistency
- **WHEN** related tables are updated together
- **THEN** referential integrity is maintained across the transaction
