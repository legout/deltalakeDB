## 1. Import Logic
- [ ] 1.1 Read latest checkpoint from `_delta_log` if present
- [ ] 1.2 Read all JSON commit files from checkpoint to latest
- [ ] 1.3 Parse each action (add/remove/metadata/protocol/txn)
- [ ] 1.4 Insert actions into SQL tables preserving version numbers
- [ ] 1.5 Populate `dl_table_versions` with original timestamps

## 2. State Reconstruction
- [ ] 2.1 Reconstruct full table history from Delta artifacts
- [ ] 2.2 Handle missing checkpoints (full replay from v0)
- [ ] 2.3 Respect Delta protocol evolution during replay
- [ ] 2.4 Track active files computation per version

## 3. Validation
- [ ] 3.1 After import, compute snapshot from SQL
- [ ] 3.2 Compare SQL snapshot with file-based snapshot
- [ ] 3.3 Validate file counts, schema, protocol versions match
- [ ] 3.4 Report any discrepancies with detailed diff

## 4. CLI Tool
- [ ] 4.1 Create binary `deltasql` with `import` subcommand
- [ ] 4.2 Accept source table path (s3://, file://, etc.)
- [ ] 4.3 Accept target database URI (`deltasql://postgres/...`)
- [ ] 4.4 Implement `--dry-run` flag for preview
- [ ] 4.5 Add `--force` flag to overwrite existing table
- [ ] 4.6 Show progress bar for long imports

## 5. Incremental Import
- [ ] 5.1 Support importing up to specific version
- [ ] 5.2 Allow resuming import from last imported version
- [ ] 5.3 Handle concurrent writes during import (warn or lock)

## 6. Error Handling
- [ ] 6.1 Handle corrupt checkpoint files gracefully
- [ ] 6.2 Handle missing commit files
- [ ] 6.3 Rollback SQL changes on import failure
- [ ] 6.4 Provide clear error messages with recovery steps

## 7. Testing
- [ ] 7.1 Unit tests for action parsing
- [ ] 7.2 Integration tests with sample Delta tables
- [ ] 7.3 Test import of table with 10k+ commits
- [ ] 7.4 Test validation against known-good Delta tables
- [ ] 7.5 Test dry-run mode produces no changes
- [ ] 7.6 Test incremental import scenarios

## 8. Documentation
- [ ] 8.1 Write import CLI usage guide
- [ ] 8.2 Document migration workflow
- [ ] 8.3 Provide example commands for common scenarios
- [ ] 8.4 Add troubleshooting section for common import issues
- [ ] 8.5 Document rollback strategy if needed
