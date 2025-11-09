## ADDED Requirements
### Requirement: Core SQL Tables
The system SHALL provide relational tables to persist Delta actions and metadata as the authoritative store.

#### Scenario: Table registry
- **WHEN** creating a SQL-backed Delta table
- **THEN** a row exists in `dl_tables(table_id, location, protocol_min_reader, protocol_min_writer, properties)`

#### Scenario: Version ledger
- **WHEN** a commit is recorded
- **THEN** a row exists in `dl_table_versions(table_id, version, committed_at, committer, operation, operation_params)`

#### Scenario: Add/remove files
- **WHEN** a commit adds or removes files
- **THEN** corresponding rows exist in `dl_add_files` and `dl_remove_files` keyed by `(table_id, version, path)`

#### Scenario: Metadata updates
- **WHEN** table schema or properties change
- **THEN** a row exists in `dl_metadata_updates(table_id, version, schema_json, partition_columns, table_properties)`

#### Scenario: Protocol updates
- **WHEN** minimum reader or writer version changes
- **THEN** a row exists in `dl_protocol_updates(table_id, version, min_reader_version, min_writer_version)`

### Requirement: Recommended Indexes
The system SHALL provide indexes to optimize common reads.

#### Scenario: Latest version lookup
- **WHEN** fetching the latest committed version for a table
- **THEN** an index supports `ORDER BY version DESC` on `(table_id, version)`

#### Scenario: File/path lookups
- **WHEN** looking up active state or planning for a path/predicate
- **THEN** an index exists on `(table_id, path)`; engines with JSON index support MAY index `stats`
