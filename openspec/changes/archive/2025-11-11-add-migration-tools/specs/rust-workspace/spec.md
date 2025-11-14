## ADDED Requirements
### Requirement: Delta Table Import Tool
The system SHALL provide a CLI tool to import existing Delta tables from `_delta_log/` into the SQL metadata catalog.

#### Scenario: Import existing table
- **WHEN** a Delta table path is provided to the import tool
- **THEN** it replays the checkpoint and JSON logs to populate SQL tables

#### Scenario: Large table migration
- **WHEN** importing a table with many versions
- **THEN** it provides progress reporting and can resume if interrupted

#### Scenario: Migration validation
- **WHEN** an import is completed
- **THEN** it validates that SQL-derived snapshot matches file-based snapshot
