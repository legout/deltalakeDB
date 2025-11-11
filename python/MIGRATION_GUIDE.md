# DeltaLakeDB Migration Guide

This guide provides comprehensive instructions for migrating existing Delta tables to SQL-backed metadata using the DeltaLakeDB migration tools.

## Overview

The DeltaLakeDB migration tools enable you to:
- Import existing Delta tables from `_delta_log/` files into SQL metadata catalog
- Validate migrated tables against original file-based metadata
- Monitor migration progress and resume interrupted migrations
- Compare performance between file-based and SQL-based access
- Verify rollback capabilities

## Prerequisites

### System Requirements
- Python 3.8 or higher
- Sufficient disk space for temporary files and database storage
- Network access to target database (if using remote database)

### Database Support
- **SQLite**: For testing and small-scale migrations
- **PostgreSQL**: Recommended for production use
- **MySQL**: Planned support

### Python Dependencies
```bash
pip install deltalakedb[postgresql]  # For PostgreSQL support
pip install deltalakedb[sqlite]      # For SQLite support
```

## Quick Start

### Single Table Migration

```bash
# Basic migration
deltalakedb import /path/to/delta/table --database-url postgresql://user:pass@host/db

# With custom table ID
deltalakedb import /path/to/delta/table --database-url postgresql://user:pass@host/db --table-id my_table

# Dry run (no changes made)
deltalakedb import /path/to/delta/table --database-url postgresql://user:pass@host/db --dry-run

# Resume interrupted migration
deltalakedb import /path/to/delta/table --database-url postgresql://user:pass@host/db --resume
```

### Validation

```bash
# Validate migrated table
deltalakedb validate /path/to/delta/table --database-url postgresql://user:pass@host/db

# Performance comparison
deltalakedb performance-compare /path/to/delta/table --database-url postgresql://user:pass@host/db
```

### Batch Migration

```bash
# Discover tables in directory
deltalakedb discover /path/to/delta/tables --recursive

# Batch migrate all tables
deltalakedb batch-import /path/to/delta/tables --database-url postgresql://user:pass@host/db

# Exclude specific tables
deltalakedb batch-import /path/to/delta/tables --database-url postgresql://user:pass@host/db --exclude test --exclude temp
```

## Detailed Migration Process

### Phase 1: Preparation

1. **Assess Table Structure**
   ```bash
   deltalakedb discover /path/to/tables --recursive
   ```

2. **Database Setup**
   - Ensure database exists
   - Verify connection permissions
   - Check available disk space

3. **Backup Strategy**
   - Backup original Delta tables
   - Backup target database (if existing data)

### Phase 2: Migration

#### Single Table Migration

```bash
# Step 1: Dry run to validate
deltalakedb import /path/to/table --database-url $DB_URL --dry-run --verbose

# Step 2: Actual migration
deltalakedb import /path/to/table --database-url $DB_URL --batch-size 1000

# Step 3: Validation
deltalakedb validate /path/to/table --database-url $DB_URL
```

#### Batch Migration

```bash
# Step 1: Discover all tables
deltalakedb discover /data/delta-tables --recursive > table_list.txt

# Step 2: Batch migration with monitoring
deltalakedb batch-import /data/delta-tables \
  --database-url $DB_URL \
  --batch-size 500 \
  --parallel 2 \
  --exclude "_tmp" \
  --exclude "_staging"
```

### Phase 3: Validation

#### Basic Validation
```bash
deltalakedb validate /path/to/migrated/table --database-url $DB_URL
```

#### Performance Comparison
```bash
deltalakedb performance-compare /path/to/table --database-url $DB_URL
```

#### Rollback Verification
```bash
deltalakedb verify-rollback table_id --database-url $DB_URL
```

## Configuration Options

### Migration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--batch-size` | 1000 | Number of files processed per database batch |
| `--resume` | False | Resume interrupted migration |
| `--dry-run` | False | Simulate migration without making changes |
| `--verbose` | False | Enable detailed logging |

### Database Configuration

#### SQLite
```bash
--database-url sqlite:///path/to/database.db
```

#### PostgreSQL
```bash
--database-url postgresql://username:password@hostname:port/database
```

### Performance Tuning

#### Batch Size Optimization
- **Small tables (< 100 files)**: 100-500
- **Medium tables (100-1000 files)**: 500-1000  
- **Large tables (> 1000 files)**: 1000-2000

#### Parallel Processing
```bash
# For batch migrations
deltalakedb batch-import /path/to/tables --database-url $DB_URL --parallel 4
```

## Monitoring and Troubleshooting

### Migration Status

```bash
# Check all migrations
deltalakedb migration-status --database-url $DB_URL

# Check specific table
deltalakedb migration-status --database-url $DB_URL --table-id my_table
```

### Common Issues

#### Migration Fails Mid-Process
```bash
# Check status
deltalakedb migration-status --database-url $DB_URL --table-id failed_table

# Resume from last checkpoint
deltalakedb import /path/to/table --database-url $DB_URL --resume --verbose
```

#### Validation Errors
```bash
# Detailed validation with verbose output
deltalakedb validate /path/to/table --database-url $DB_URL --verbose

# Check for specific issues
deltalakedb performance-compare /path/to/table --database-url $DB_URL
```

#### Performance Issues
- Reduce batch size: `--batch-size 500`
- Increase database connection pool
- Check database indexing
- Monitor disk I/O

### Log Analysis

Migration logs are stored in the database `migration_status` table:

```sql
-- Check migration history
SELECT table_id, status, version, files_processed, 
       started_at, completed_at, error_message
FROM migration_status 
ORDER BY started_at DESC;

-- Check failed migrations
SELECT table_id, error_message, started_at
FROM migration_status 
WHERE status = 'failed'
ORDER BY started_at DESC;
```

## Rollback Procedures

### Pre-Rollback Verification

```bash
# Verify rollback is possible
deltalakedb verify-rollback table_id --database-url $DB_URL
```

### Manual Rollback Steps

1. **Stop Application Access**
   ```bash
   # Ensure no applications are writing to the table
   ```

2. **Remove SQL Metadata**
   ```sql
   -- Remove from delta_files table
   DELETE FROM delta_files WHERE table_id = 'your_table_id';
   
   -- Remove from migration_status
   DELETE FROM migration_status WHERE table_id = 'your_table_id';
   ```

3. **Verify Delta Log Integrity**
   ```bash
   # Check that _delta_log directory is intact
   ls -la /path/to/table/_delta_log/
   
   # Validate Delta table can be read normally
   ```

4. **Restart Applications**
   - Applications will now use file-based metadata again

## Performance Benchmarks

### Expected Performance

| Table Size | Files | Migration Time | Validation Time |
|-----------|-------|----------------|-----------------|
| Small | < 100 | < 30 seconds | < 10 seconds |
| Medium | 100-1000 | 1-5 minutes | < 30 seconds |
| Large | 1000-10000 | 5-30 minutes | 1-5 minutes |
| Very Large | > 10000 | 30+ minutes | 5+ minutes |

### Performance Optimization Tips

1. **Database Configuration**
   - Use SSD storage for database
   - Configure appropriate connection pooling
   - Ensure proper indexing

2. **Network Considerations**
   - Place database close to Delta tables
   - Use high-bandwidth network connections
   - Consider database replication for read-heavy workloads

3. **Resource Allocation**
   - Allocate sufficient memory for batch processing
   - Monitor CPU usage during migration
   - Use parallel processing for batch migrations

## Security Considerations

### Database Security
- Use encrypted connections (SSL/TLS)
- Implement proper authentication
- Limit database user permissions
- Regular database backups

### File System Security
- Ensure proper file permissions on Delta tables
- Backup _delta_log directories
- Monitor file system integrity

### Access Control
- Restrict migration tool access to authorized users
- Audit migration activities
- Implement role-based access control

## Best Practices

### Migration Planning
1. **Assessment Phase**
   - Inventory all Delta tables
   - Estimate migration time and resources
   - Identify potential issues

2. **Testing Phase**
   - Test migration on sample tables
   - Validate performance benchmarks
   - Test rollback procedures

3. **Production Phase**
   - Schedule migrations during low-traffic periods
   - Monitor migration progress
   - Validate post-migration performance

### Operational Excellence
1. **Monitoring**
   - Set up alerts for migration failures
   - Monitor database performance
   - Track migration metrics

2. **Documentation**
   - Document migration procedures
   - Maintain rollback runbooks
   - Track configuration changes

3. **Continuous Improvement**
   - Analyze migration performance
   - Optimize batch sizes and parallelism
   - Update procedures based on lessons learned

## Support and Troubleshooting

### Getting Help
- Check migration logs for detailed error messages
- Review database error logs
- Consult the validation output for specific issues

### Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| "No checkpoint or JSON files found" | Empty or missing _delta_log | Verify table path and _delta_log contents |
| "Database connection failed" | Network or authentication issues | Check database URL and credentials |
| "Permission denied" | File system or database permissions | Verify user permissions on files and database |
| "Disk space exhausted" | Insufficient storage | Free up disk space or use alternative storage |

### Escalation Procedures
1. Collect migration logs and database error messages
2. Document the exact error and reproduction steps
3. Provide table size and structure information
4. Include database configuration and version information