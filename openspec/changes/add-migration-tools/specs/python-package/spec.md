## ADDED Requirements
### Requirement: Migration CLI Interface
The Python package SHALL provide a command-line interface for migrating existing Delta tables to SQL-backed metadata.

#### Scenario: CLI import command
- **WHEN** `deltalakedb import` is executed with table path and database connection
- **THEN** it migrates the table metadata and reports success/failure

#### Scenario: Migration validation command
- **WHEN** `deltalakedb validate` is executed on a migrated table
- **THEN** it compares SQL and file-based snapshots and reports any drift

#### Scenario: Migration status
- **WHEN** `deltalakedb migration-status` is executed
- **THEN** it shows the migration progress and any issues encountered
