## 1. Import Logic
- [x] 1.1 Read latest checkpoint from `_delta_log` if present
- [x] 1.2 Read all JSON commit files from checkpoint to latest
- [x] 1.3 Parse each action (add/remove/metadata/protocol/txn)
- [x] 1.4 Insert actions into SQL tables preserving version numbers
- [x] 1.5 Populate `dl_table_versions` with original timestamps

## 2. State Reconstruction
- [x] 2.1 Reconstruct full table history from Delta artifacts
- [x] 2.2 Handle missing checkpoints (full replay from v0)
- [x] 2.3 Respect Delta protocol evolution during replay
- [x] 2.4 Track active files computation per version

## 3. Validation
- [x] 3.1 After import, compute snapshot from SQL
- [x] 3.2 Compare SQL snapshot with file-based snapshot
- [x] 3.3 Validate file counts, schema, protocol versions match
- [x] 3.4 Report any discrepancies with detailed diff

## 4. CLI Tool
- [x] 4.1 Create binary `deltasql` with `import` subcommand
- [x] 4.2 Accept source table path (s3://, file://, etc.)
- [x] 4.3 Accept target database URI (`deltasql://postgres/...`)
- [x] 4.4 Implement `--dry-run` flag for preview
- [x] 4.5 Add `--force` flag to overwrite existing table
- [x] 4.6 Show progress bar for long imports

## 5. Incremental Import
- [x] 5.1 Support importing up to specific version
- [x] 5.2 Allow resuming import from last imported version
- [x] 5.3 Handle concurrent writes during import (warn or lock)

## 6. Error Handling
- [x] 6.1 Handle corrupt checkpoint files gracefully
- [x] 6.2 Handle missing commit files
- [x] 6.3 Rollback SQL changes on import failure
- [x] 6.4 Provide clear error messages with recovery steps

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
