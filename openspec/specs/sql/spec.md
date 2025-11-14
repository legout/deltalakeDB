# sql Specification

## Purpose
TBD - created by archiving change add-multi-table-acid. Update Purpose after archive.
## Requirements
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

### Requirement: SQL Metadata Schema
The system SHALL define a relational schema that stores Delta Lake metadata across multiple database engines.

#### Scenario: Core tables structure
- **WHEN** the schema is created
- **THEN** it includes tables for dl_tables, dl_table_versions, dl_add_files, dl_remove_files, dl_metadata_updates, dl_protocol_updates

#### Scenario: Database-specific adaptations
- **WHEN** creating schema for different engines
- **THEN** Postgres uses JSONB/arrays, SQLite uses TEXT/JSON, DuckDB uses JSON/lists

#### Scenario: Performance optimization
- **WHEN** the schema is deployed
- **THEN** it includes appropriate indexes for fast metadata queries and active file calculation

#### Scenario: Table discovery
- **WHEN** listing available Delta tables
- **THEN** they can be discovered via the dl_tables registry with location metadata

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

