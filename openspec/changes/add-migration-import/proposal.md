## Why
Existing Delta tables have their metadata in `_delta_log` on object storage. To adopt SQL-backed metadata, users need a one-time migration tool to import this history into the SQL database while preserving version numbers, timestamps, and all actions.

## What Changes
- Implement CLI tool to import existing `_delta_log` into SQL database
- Read checkpoint (if present) plus all JSON commits to reconstruct history
- Populate SQL tables with all historical actions and versions
- Validate imported state matches original `_delta_log`
- Support incremental import for tables with ongoing writes
- Provide dry-run mode to preview import without changes

## Impact
- Affected specs: new capability `sql-metadata-tools`
- Affected code: new `crates/sql-metadata-tools/src/import.rs`, CLI binary
- Breaking: None - this is a new tool
- Dependencies: Requires SQL reader/writer and file-based reader
- Critical for adoption: Enables migration of existing production tables
