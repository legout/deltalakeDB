## ADDED Requirements

### Requirement: Import from Checkpoint
The system SHALL import Delta metadata starting from latest checkpoint.

#### Scenario: Reading checkpoint
- **WHEN** checkpoint file exists
- **THEN** import reads checkpoint as starting point for efficiency

#### Scenario: Full replay without checkpoint
- **WHEN** no checkpoint exists
- **THEN** import reads all JSON commits from version 0

### Requirement: Commit File Replay
The system SHALL replay all JSON commit files in order.

#### Scenario: Sequential replay
- **WHEN** importing versions 0-100
- **THEN** commits are processed in ascending version order

#### Scenario: Action parsing
- **WHEN** reading commit file
- **THEN** each newline-delimited JSON action is parsed correctly

### Requirement: SQL Population
The system SHALL populate SQL tables with all historical actions.

#### Scenario: Inserting add actions
- **WHEN** commit contains add file actions
- **THEN** rows are inserted into `dl_add_files` with correct version

#### Scenario: Preserving timestamps
- **WHEN** importing commit metadata
- **THEN** original commit timestamps are preserved in `dl_table_versions`

#### Scenario: Batch insertion
- **WHEN** importing large table
- **THEN** actions are inserted in batches for performance

### Requirement: Version Preservation
The system SHALL preserve original version numbers during import.

#### Scenario: Version sequence
- **WHEN** importing versions 0-999
- **THEN** SQL table contains same version numbers as original `_delta_log`

#### Scenario: Version gaps
- **WHEN** commit files have gaps (e.g., missing version 5)
- **THEN** import detects and reports the gap

### Requirement: Post-Import Validation
The system SHALL validate imported data matches source.

#### Scenario: Snapshot comparison
- **WHEN** import completes
- **THEN** system compares SQL-derived snapshot with file-based snapshot

#### Scenario: File count validation
- **WHEN** validating import
- **THEN** active file count from SQL matches original table

#### Scenario: Schema validation
- **WHEN** validating import
- **THEN** latest schema from SQL matches original table schema

### Requirement: CLI Interface
The system SHALL provide command-line tool for import operations.

#### Scenario: Basic import
- **WHEN** running `deltasql import s3://bucket/table deltasql://postgres/db/schema/table`
- **THEN** table is imported from source to target database

#### Scenario: Dry-run mode
- **WHEN** running with `--dry-run` flag
- **THEN** import shows preview without making database changes

#### Scenario: Force overwrite
- **WHEN** running with `--force` flag on existing table
- **THEN** existing table is dropped and reimported

### Requirement: Progress Reporting
The system SHALL show import progress for long operations.

#### Scenario: Progress bar
- **WHEN** importing table with 1000+ commits
- **THEN** CLI shows progress bar indicating completion percentage

#### Scenario: Status messages
- **WHEN** import runs
- **THEN** system logs key milestones (checkpoint read, commits processed, validation)

### Requirement: Incremental Import
The system SHALL support importing specific version ranges.

#### Scenario: Import up to version
- **WHEN** running with `--up-to-version 50`
- **THEN** only versions 0-50 are imported

#### Scenario: Resume import
- **WHEN** import was partially completed
- **THEN** system can resume from last successfully imported version

### Requirement: Error Recovery
The system SHALL handle import failures gracefully.

#### Scenario: Transaction rollback
- **WHEN** import fails midway
- **THEN** SQL transaction is rolled back leaving database unchanged

#### Scenario: Corrupt checkpoint
- **WHEN** checkpoint file is corrupt
- **THEN** import falls back to full JSON replay with warning

#### Scenario: Missing commit file
- **WHEN** commit file is missing from sequence
- **THEN** import reports error with specific version number

### Requirement: Table Registration
The system SHALL register imported table in `dl_tables`.

#### Scenario: Creating table entry
- **WHEN** import begins
- **THEN** table is registered in `dl_tables` with location and protocol

#### Scenario: Existing table check
- **WHEN** table already exists in database
- **THEN** import fails unless `--force` flag is used

### Requirement: Validation Reporting
The system SHALL report validation results with details.

#### Scenario: Successful validation
- **WHEN** import validates successfully
- **THEN** CLI displays confirmation with statistics (versions, files, size)

#### Scenario: Validation failure
- **WHEN** validation detects mismatch
- **THEN** system reports specific differences (missing files, count discrepancy, schema mismatch)

### Requirement: Documentation
The system SHALL provide comprehensive usage documentation.

#### Scenario: Help text
- **WHEN** running `deltasql import --help`
- **THEN** system displays usage, options, and examples

#### Scenario: Migration guide
- **WHEN** user reads documentation
- **THEN** guide covers full migration workflow with best practices
