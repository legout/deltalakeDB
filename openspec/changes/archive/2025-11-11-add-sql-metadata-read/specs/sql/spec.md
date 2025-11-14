## ADDED Requirements
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
