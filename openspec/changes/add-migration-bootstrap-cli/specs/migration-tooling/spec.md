## ADDED Requirements
### Requirement: Import Existing `_delta_log` into SQL
The system SHALL provide a CLI to bootstrap SQL tables from an existing Delta table’s `_delta_log/`.

#### Scenario: Import with checkpoint
- **WHEN** running `dl import` on a table with a checkpoint
- **THEN** the tool loads the checkpoint, replays newer JSON commits, and populates `dl_*` tables up to the latest version

#### Scenario: Import without checkpoint
- **WHEN** running `dl import` on a table without a checkpoint
- **THEN** the tool replays JSON commits from version 0 and populates `dl_*` tables

