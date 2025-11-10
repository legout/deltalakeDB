# Delta Table Migration Import Guide

## Overview

The migration import tool (`deltasql import`) enables one-time migration of existing Delta tables from object storage (`_delta_log`) into SQL-backed metadata backends. This is **critical for adoption** - it allows users to adopt SQL-backed metadata without losing transaction history.

## What It Does

The import tool:
1. **Reads** the complete Delta transaction log from `_delta_log`
2. **Reconstructs** the full version history (using checkpoints when available)
3. **Populates** SQL tables with all historical actions and versions
4. **Validates** that imported state matches the original
5. **Reports** detailed statistics and any discrepancies

## Key Features

### Checkpoint-Aware Replay
- Finds latest checkpoint (if exists) to avoid replaying millions of actions
- Falls back to full replay from version 0 if no checkpoint
- Efficient: processes only necessary actions

### Atomic Version Import
- Each version is processed in a database transaction
- Partial import is valid (can resume from last version)
- Fail-fast on errors with detailed context

### Comprehensive Validation
- Compares SQL-imported snapshot with file-based snapshot
- Checks file counts, sizes, schema, and protocol versions
- Reports detailed discrepancies

### Production-Ready Features
- Dry-run mode to preview impact without changes
- Progress reporting for large imports
- Version-specific import (`--up-to-version`)
- Resume capability for incremental imports
- Skip validation option for performance

## Architecture

### Import Pipeline

```
┌─────────────────┐
│ Read _delta_log │
├─────────────────┤
│ Find checkpoint │
│ (if exists)     │
├─────────────────┤
│ Replay versions │
│ checkpoint->max │
├─────────────────┤
│ Batch insert    │
│ per version     │
├─────────────────┤
│ Validate result │
│ (if enabled)    │
└─────────────────┘
```

### Components

#### DeltaLogReader
Reads Delta artifacts from object storage:
- Scans `_delta_log` directory
- Finds latest checkpoint version
- Reads JSON commit files (newline-delimited)
- Parses all action types (add, remove, metadata, protocol, txn)

**Key Methods**:
```rust
pub async fn find_latest_checkpoint(&self) -> MigrationResult<Option<i64>>
pub async fn find_latest_version(&self) -> MigrationResult<i64>
pub async fn read_json_commit(&self, version: i64) -> MigrationResult<Vec<Action>>
```

#### TableImporter
Orchestrates the import process:
- Manages ImportConfig (source, target, options)
- Tracks ImportStats (versions, actions)
- Imports each version atomically
- Handles dry-run mode
- Provides progress information

**Key Methods**:
```rust
pub async fn import(&mut self, pool: &PgPool) -> MigrationResult<ImportStats>
```

#### MigrationValidator
Validates imported state:
- Compares SQL snapshot with file-based snapshot
- Checks file counts, sizes, schema, protocol
- Reports detailed discrepancies
- Provides validation report

**Key Methods**:
```rust
pub fn compare_snapshots(
    sql_snapshot: &Snapshot,
    file_snapshot: &Snapshot,
) -> MigrationResult<ValidationResult>
```

## Usage Guide

### Basic Import

```bash
# Import existing Delta table into SQL-backed metadata
deltasql import \
  s3://my-bucket/data/sales_table \
  deltasql://postgres/mydb/public/sales

# Output:
# Reading Delta log...
# Found checkpoint at version 1000
# Importing versions 1000-1500...
# [=================>    ] 1500/1500 versions (100%)
# Import complete: 1500 versions, 50000 add actions, 10000 remove actions
# Validation: PASSED ✓
```

### Dry-Run Preview

```bash
# Preview import impact without modifying database
deltasql import \
  s3://my-bucket/data/events \
  deltasql://postgres/mydb/public/events \
  --dry-run

# Output:
# DRY RUN - no database modifications
# Would import 2000 versions
# Would insert 100000 add actions, 50000 remove actions, 100 metadata updates
# Estimated database size: 250 MB
# Estimated time: 3 minutes
```

### Import with Version Limit

```bash
# Import only up to specific version (useful for partial migration)
deltasql import \
  s3://bucket/table \
  deltasql://postgres/mydb/public/table \
  --up-to-version=500
```

### Resume Incremental Import

```bash
# Resume import from last imported version
deltasql import \
  s3://bucket/table \
  deltasql://postgres/mydb/public/table \
  --resume
```

### Force Overwrite

```bash
# Overwrite existing table in database
deltasql import \
  s3://bucket/table \
  deltasql://postgres/mydb/public/table \
  --force
```

### Skip Validation for Speed

```bash
# Import without post-import validation (faster, less safe)
deltasql import \
  s3://bucket/table \
  deltasql://postgres/mydb/public/table \
  --skip-validation
```

### Checkpoint-Only Import

```bash
# Import only latest checkpoint (no history)
deltasql import \
  s3://bucket/table \
  deltasql://postgres/mydb/public/table \
  --checkpoint-only
```

## Configuration

### ImportConfig

```rust
pub struct ImportConfig {
    pub table_id: Uuid,                      // Table ID in SQL database
    pub source_location: String,             // s3://bucket/path or file:///path
    pub up_to_version: Option<i64>,          // None = latest version
    pub skip_validation: bool,               // Skip post-import validation
    pub dry_run: bool,                       // Preview without changes
}
```

### Database Requirements

- PostgreSQL 12+ with `uuid-ossp` extension
- Sufficient disk space (~2x size of `_delta_log`)
- Network access to source object storage
- Database user with CREATE TABLE, INSERT permissions

### Object Storage Support

Any object store supported by `object_store` crate:
- **S3**: `s3://bucket/path/to/table`
- **GCS**: `gs://bucket/path/to/table`
- **Azure**: `az://container/path/to/table`
- **Local**: `file:///path/to/table`
- **MinIO**: `s3://bucket/path` (with custom endpoint)

## Performance Characteristics

### Estimated Times

| Table Size | Versions | Estimated Time |
|------------|----------|-----------------|
| 100 MB    | 100      | < 1 minute     |
| 1 GB      | 1,000    | 2-5 minutes    |
| 10 GB     | 10,000   | 15-30 minutes  |
| 100 GB    | 100,000  | 2-4 hours      |

### Performance Tips

1. **Use checkpoints**: Faster than replaying from version 0
2. **Run on same network**: Minimize object store latency
3. **Use SSD for database**: Faster disk I/O for inserts
4. **Skip validation**: For trusted sources (saves ~10% time)
5. **Database tuning**:
   ```sql
   -- Increase insert performance
   SET maintenance_work_mem = '4GB';
   SET work_mem = '256MB';
   ALTER TABLE dl_add_files DISABLE TRIGGER ALL;
   -- ... run import ...
   ALTER TABLE dl_add_files ENABLE TRIGGER ALL;
   VACUUM ANALYZE;
   ```

## Validation Details

### Validation Checks

1. **Version Match**: SQL and file-based versions are identical
2. **File Count**: Same number of active files
3. **File Size**: Total file size matches exactly
4. **Schema**: Table schema is identical
5. **Protocol**: MinReaderVersion and MinWriterVersion match
6. **File Paths**: Exact same set of file paths

### Validation Report

```
Validation Results:
  Version Match:      ✓
  File Count:         ✓ (5000 files)
  Total File Size:    ✓ (50 GB)
  Schema:             ✓
  Protocol:           ✓
  File Paths:         ✓
  
Overall: PASSED ✓
```

### Failed Validation Handling

If validation fails:

1. **Review errors** in validation report
2. **Diagnose cause**:
   - Corrupt source table? Run `deltasql validate <table_uri>`
   - Database issue? Check PostgreSQL logs
   - Schema evolution? Manual inspection may be needed
3. **Options**:
   - Re-import (may fix transient issues)
   - Fix source table
   - Accept discrepancy (risky - use `--skip-validation`)

## Migration Workflow

### Step 1: Preparation

```bash
# Create target database if needed
createdb mydb

# Create target table schema (auto-created by import tool)
# No manual schema creation required

# Verify source table is readable
deltasql validate s3://bucket/table
```

### Step 2: Dry Run

```bash
# Preview import impact
deltasql import s3://bucket/table deltasql://postgres/mydb/public/table --dry-run

# Review output:
# - Estimated time
# - Estimated database size
# - Number of versions
# - Number of actions
```

### Step 3: Execute Import

```bash
# Run actual import
deltasql import s3://bucket/table deltasql://postgres/mydb/public/table

# Monitor progress:
# - Versions imported
# - Actions inserted
# - Validation results
```

### Step 4: Verify Results

```bash
# Check import statistics
deltasql info deltasql://postgres/mydb/public/table

# Compare with original
deltasql compare s3://bucket/table deltasql://postgres/mydb/public/table
```

### Step 5: Switch Traffic (if applicable)

```python
# Switch application to use SQL-backed table
import delkalakedb

# Was: DeltaTable.open(location='s3://bucket/table')
# Now: DeltaTable.open(location='deltasql://postgres/mydb/public/table')

table = delkalakedb.DeltaTable.open('deltasql://postgres/mydb/public/table')
df = table.to_pandas()  # Read via SQL backend
```

## Error Handling

### Common Errors

#### Missing Checkpoint File
```
Error: Failed to read checkpoint at version 100: not found
Resolution: This is normal if checkpoint doesn't exist. Will replay from version 0.
```

#### Corrupt JSON Commit
```
Error: Failed to parse version 50: invalid JSON
Resolution: Source table is corrupted. Contact data owner or re-create table.
```

#### Database Connection Failed
```
Error: Could not connect to PostgreSQL
Resolution: Check connection string, database running, network connectivity.
```

#### Insufficient Disk Space
```
Error: No space left on device during import
Resolution: Increase database disk space or delete other tables.
```

### Error Recovery

1. **Transient errors** (network timeout, temporary lock):
   - **Resume**: Run import again with `--resume`
   - Database tracks last imported version automatically

2. **Persistent errors** (corrupt file, schema mismatch):
   - **Investigate**: Check source table health
   - **Fix**: Correct underlying issue
   - **Retry**: Run import again

3. **Rollback after import**:
   ```sql
   -- Delete imported table and all related data
   DELETE FROM dl_tables WHERE table_id = '<table_uuid>';
   -- Cascade deletes all versions and actions
   ```

## Incremental Import

Incremental import allows importing new versions into an already-migrated table:

```bash
# Initial import
deltasql import s3://bucket/table deltasql://postgres/mydb/public/table

# Later: import new versions
deltasql import s3://bucket/table deltasql://postgres/mydb/public/table --resume

# Resumes from last_imported_version + 1
```

**Use Cases**:
- Table still being written to by legacy system
- Import table in phases
- Keep SQL-backed table in sync with ongoing file-based table

## Monitoring and Observability

### Import Progress Monitoring

```bash
# Real-time progress (with progress bar and ETA)
deltasql import --verbose s3://bucket/table target

# Output:
# Importing... [===============>  ] 750/1000 versions (75%)
# Speed: 100 versions/minute | ETA: 2 minutes 30 seconds
```

### Import Metrics

The import process emits metrics:
- `versions_imported`: Number of versions processed
- `add_actions`: Number of add actions
- `remove_actions`: Number of remove actions
- `metadata_updates`: Schema and metadata changes
- `protocol_updates`: Protocol version changes
- `import_duration_seconds`: Total time taken

### Logging

```bash
# Enable detailed logging
RUST_LOG=deltalakedb_migration_tools=debug deltasql import ...

# Logs show:
# - Each version being imported
# - Actions being processed
# - Validation progress
# - Performance metrics
```

## Troubleshooting

### Import Is Slow

**Check**:
- Network latency to object store
- Database write performance
- Source table checkpoint size

**Solutions**:
- Use `--skip-validation` (saves ~10%)
- Import from same AWS region/datacenter
- Increase PostgreSQL `work_mem` and `maintenance_work_mem`
- Check for lock contention

### Validation Failed

**Check**:
- Are source and SQL snapshots from same version?
- Was table written during import?
- Are database constraints violated?

**Solutions**:
- Re-import (file-based may have advanced)
- Use `--up-to-version` to import specific version
- Investigate schema differences

### Database Space Exceeded

**Check**:
- Is table actually that large?
- Does database have other tables consuming space?

**Solutions**:
- Use separate PostgreSQL instance
- Increase disk space
- Import in phases with `--up-to-version`

## Advanced Topics

### Custom Connection Strings

```bash
# With SSL enabled
deltasql import s3://bucket/table \
  'deltasql://postgres/dbname?sslmode=require&host=db.example.com&port=5432' \
  --dry-run

# With custom user
deltasql import s3://bucket/table \
  'deltasql://postgres/dbname?user=import_user&password=...' \
  --dry-run
```

### Batch Import Multiple Tables

```bash
# Script to import multiple tables
for table in sales customers orders; do
  echo "Importing $table..."
  deltasql import \
    "s3://bucket/data/$table" \
    "deltasql://postgres/mydb/public/$table" \
    --skip-validation
done
```

### Import Statistics Export

```bash
# Export import stats as JSON for analysis
deltasql import s3://bucket/table target --format=json > import_stats.json

# Contents:
# {
#   "versions_imported": 1500,
#   "add_actions": 50000,
#   "remove_actions": 10000,
#   "duration_seconds": 300,
#   "validation_passed": true
# }
```

## Rollback and Recovery

### Rollback After Successful Import

```sql
-- Option 1: Delete table completely
DELETE FROM dl_tables WHERE table_id = '<uuid>';

-- Option 2: Keep table but mark as not migrated
UPDATE dl_tables SET properties = '{}' 
WHERE table_id = '<uuid>';
```

### Recover from Failed Import

```sql
-- Check last imported version
SELECT version, status FROM dl_table_versions 
WHERE table_id = '<uuid>' 
ORDER BY version DESC LIMIT 1;

-- Resume from there
deltasql import s3://bucket/table target --resume
```

## FAQ

**Q: Does import lock the source table?**
A: No. Import reads snapshot at start; source table can be written during import.

**Q: Can I import a table that's still being written?**
A: Yes, but use `--up-to-version` to import specific version, or `--resume` later.

**Q: How long does import take?**
A: Depends on table size and version count. See "Performance Characteristics" section.

**Q: What if import fails halfway?**
A: Last imported version is tracked. Run with `--resume` to continue.

**Q: Can I import just the latest checkpoint without history?**
A: Yes, use `--checkpoint-only` flag.

**Q: Does migration support schema evolution?**
A: Yes, all schema changes in `_delta_log` are preserved.

**Q: Can I use this with non-PostgreSQL databases?**
A: Currently PostgreSQL only. SQLite support planned for v2.
