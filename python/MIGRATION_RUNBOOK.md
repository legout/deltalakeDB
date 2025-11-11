# DeltaLakeDB Migration Runbook

This runbook provides step-by-step procedures for handling common migration scenarios and emergencies.

## Emergency Contacts
- **Database Administrator**: [Contact Information]
- **System Administrator**: [Contact Information]
- **DeltaLakeDB Support**: [Contact Information]

## Table of Contents
1. [Migration Failure Procedures](#migration-failure-procedures)
2. [Rollback Procedures](#rollback-procedures)
3. [Performance Issues](#performance-issues)
4. [Data Integrity Issues](#data-integrity-issues)
5. [System Outages](#system-outages)
6. [Maintenance Procedures](#maintenance-procedures)

## Migration Failure Procedures

### Scenario 1: Migration Fails Mid-Process

**Symptoms:**
- Migration stops unexpectedly
- Error messages in logs
- Partial data in database

**Immediate Actions:**
1. **Stop the Migration Process**
   ```bash
   # Kill any running migration processes
   pkill -f deltalakedb
   ```

2. **Assess Current State**
   ```bash
   # Check migration status
   deltalakedb migration-status --database-url $DB_URL --table-id failed_table
   
   # Check database connections
   # Verify database is accessible and responsive
   ```

3. **Review Error Logs**
   ```bash
   # Check migration logs
   tail -f /var/log/deltalakedb/migration.log
   
   # Check database logs
   # Location varies by database system
   ```

4. **Resume Migration**
   ```bash
   # Attempt to resume from last checkpoint
   deltalakedb import /path/to/table --database-url $DB_URL --resume --verbose
   ```

**If Resume Fails:**
1. **Rollback Partial Migration**
   ```bash
   # Verify rollback capability
   deltalakedb verify-rollback failed_table --database-url $DB_URL
   
   # Perform manual rollback if needed
   # See Rollback Procedures section
   ```

2. **Start Fresh Migration**
   ```bash
   # Clean up partial migration
   # Remove entries from database tables
   
   # Start new migration
   deltalakedb import /path/to/table --database-url $DB_URL --dry-run
   deltalakedb import /path/to/table --database-url $DB_URL
   ```

### Scenario 2: Database Connection Issues

**Symptoms:**
- Connection timeout errors
- Authentication failures
- Network connectivity issues

**Immediate Actions:**
1. **Verify Database Connectivity**
   ```bash
   # Test basic connectivity
   psql -h hostname -p port -U username -d database
   
   # Check network connectivity
   ping hostname
   telnet hostname port
   ```

2. **Check Database Status**
   ```bash
   # PostgreSQL
   pg_isready -h hostname -p port
   
   # Check database logs for errors
   tail -f /var/log/postgresql/postgresql.log
   ```

3. **Verify Credentials**
   ```bash
   # Test connection with current credentials
   deltalakedb migration-status --database-url $DB_URL
   ```

4. **Update Configuration**
   ```bash
   # Update database URL if needed
   export DB_URL="postgresql://user:pass@host:port/db"
   
   # Test new configuration
   deltalakedb migration-status --database-url $DB_URL
   ```

## Rollback Procedures

### Emergency Rollback

**When to Use:**
- Migration cannot be completed
- Data corruption detected
- Performance degradation severe

**Pre-Rollback Checklist:**
- [ ] Applications stopped
- [ ] Database backed up
- [ ] Rollback capability verified
- [ ] Maintenance window confirmed

**Rollback Steps:**

1. **Verify Rollback Capability**
   ```bash
   deltalakedb verify-rollback table_id --database-url $DB_URL
   ```

2. **Stop Application Access**
   ```bash
   # Stop all applications using the table
   systemctl stop application-service
   
   # Verify no active connections
   # Check application logs
   ```

3. **Database Cleanup**
   ```sql
   -- Connect to database
   \c database_name
   
   -- Remove SQL metadata for specific table
   BEGIN;
   DELETE FROM delta_files WHERE table_id = 'table_id';
   DELETE FROM migration_status WHERE table_id = 'table_id';
   COMMIT;
   
   -- Verify cleanup
   SELECT COUNT(*) FROM delta_files WHERE table_id = 'table_id';
   ```

4. **Verify Delta Log Integrity**
   ```bash
   # Check _delta_log directory
   ls -la /path/to/table/_delta_log/
   
   # Verify latest files are present
   find /path/to/table/_delta_log/ -name "*.json" | sort | tail -5
   
   # Check for checkpoint files
   find /path/to/table/_delta_log/ -name "*.checkpoint.parquet"
   ```

5. **Test Table Access**
   ```bash
   # Test reading table using original Delta log
   # Use your preferred Delta Lake reader
   ```

6. **Restart Applications**
   ```bash
   # Restart applications
   systemctl start application-service
   
   # Monitor application logs
   tail -f /var/log/application/application.log
   ```

7. **Post-Rollback Validation**
   ```bash
   # Verify applications are working correctly
   # Check application metrics
   # Validate data access patterns
   ```

### Partial Rollback

**When to Use:**
- Only specific tables affected
- Other migrations successful

**Steps:**
1. **Identify Affected Tables**
   ```bash
   deltalakedb migration-status --database-url $DB_URL | grep "failed\|error"
   ```

2. **Rollback Individual Tables**
   ```sql
   -- Remove specific table metadata
   DELETE FROM delta_files WHERE table_id = 'affected_table';
   DELETE FROM migration_status WHERE table_id = 'affected_table';
   ```

3. **Continue with Unaffected Tables**
   ```bash
   # Resume migration for remaining tables
   deltalakedb batch-import /path/to/tables --database-url $DB_URL --resume
   ```

## Performance Issues

### Scenario 1: Slow Migration Performance

**Symptoms:**
- Migration taking longer than expected
- High CPU or memory usage
- Database performance degradation

**Diagnostic Steps:**

1. **Check System Resources**
   ```bash
   # CPU and memory usage
   top
   htop
   
   # Disk I/O
   iostat -x 1
   
   # Network usage
   iftop
   ```

2. **Database Performance**
   ```sql
   -- Check active connections
   SELECT count(*) FROM pg_stat_activity;
   
   -- Check slow queries
   SELECT query, mean_time, calls 
   FROM pg_stat_statements 
   ORDER BY mean_time DESC 
   LIMIT 10;
   
   -- Check table sizes
   SELECT schemaname, tablename, 
          pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
   FROM pg_tables 
   WHERE tablename LIKE '%delta%';
   ```

3. **Migration Progress**
   ```bash
   # Check current migration status
   deltalakedb migration-status --database-url $DB_URL --verbose
   
   # Monitor progress
   watch -n 30 'deltalakedb migration-status --database-url $DB_URL'
   ```

**Optimization Actions:**

1. **Adjust Batch Size**
   ```bash
   # Reduce batch size if memory issues
   deltalakedb import /path/to/table --database-url $DB_URL --batch-size 500
   
   # Increase batch size if CPU bound
   deltalakedb import /path/to/table --database-url $DB_URL --batch-size 2000
   ```

2. **Database Tuning**
   ```sql
   -- Increase work_mem for large sorts
   SET work_mem = '256MB';
   
   -- Increase maintenance_work_mem for index creation
   SET maintenance_work_mem = '1GB';
   
   -- Check and adjust checkpoint settings
   SHOW checkpoint_completion_target;
   ```

3. **Resource Allocation**
   ```bash
   # Limit migration CPU usage
   nice -n 10 deltalakedb import /path/to/table --database-url $DB_URL
   
   # Limit I/O priority
   ionice -c2 -n7 deltalakedb import /path/to/table --database-url $DB_URL
   ```

### Scenario 2: Database Performance Degradation

**Symptoms:**
- Overall database slowness
- Other applications affected
- High database load

**Immediate Actions:**

1. **Identify Resource Contention**
   ```sql
   -- Check blocking queries
   SELECT blocked_locks.pid AS blocked_pid,
          blocked_activity.usename AS blocked_user,
          blocking_locks.pid AS blocking_pid,
          blocking_activity.usename AS blocking_user,
          blocked_activity.query AS blocked_statement,
          blocking_activity.query AS current_statement_in_blocking_process
   FROM pg_catalog.pg_locks blocked_locks
   JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
   JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
   JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
   WHERE NOT blocked_locks.granted;
   ```

2. **Pause Migration if Necessary**
   ```bash
   # Stop migration process
   pkill -f deltalakedb
   
   # Check if performance improves
   ```

3. **Database Maintenance**
   ```sql
   -- Update table statistics
   ANALYZE delta_files;
   ANALYZE migration_status;
   
   -- Rebuild indexes if fragmented
   REINDEX TABLE delta_files;
   ```

## Data Integrity Issues

### Scenario 1: Validation Failures

**Symptoms:**
- Validation reports mismatches
- Data inconsistencies detected
- File count differences

**Investigation Steps:**

1. **Detailed Validation**
   ```bash
   # Run validation with verbose output
   deltalakedb validate /path/to/table --database-url $DB_URL --verbose
   
   # Check specific validation issues
   deltalakedb performance-compare /path/to/table --database-url $DB_URL
   ```

2. **Compare Snapshots**
   ```sql
   -- Check file counts
   SELECT 'SQL' as source, COUNT(*) as file_count 
   FROM delta_files WHERE table_id = 'table_id'
   UNION ALL
   SELECT 'File' as source, COUNT(*) as file_count 
   FROM ( -- This would need to be implemented based on your file system
         SELECT COUNT(*) FROM delta_log_files 
         WHERE table_id = 'table_id'
   ) t;
   ```

3. **Identify Specific Issues**
   ```sql
   -- Find files in SQL but not in Delta log
   SELECT path, version, action_type 
   FROM delta_files 
   WHERE table_id = 'table_id'
   AND path NOT IN (
     SELECT path FROM file_based_snapshot WHERE table_id = 'table_id'
   );
   
   -- Find files in Delta log but not in SQL
   SELECT path, version 
   FROM file_based_snapshot 
   WHERE table_id = 'table_id'
   AND path NOT IN (
     SELECT path FROM delta_files WHERE table_id = 'table_id'
   );
   ```

**Resolution Actions:**

1. **Partial Re-migration**
   ```bash
   # Re-migrate specific missing files
   # This would require custom implementation
   
   # Or perform full re-migration
   deltalakedb import /path/to/table --database-url $DB_URL --resume
   ```

2. **Manual Data Correction**
   ```sql
   -- Manually insert missing files
   INSERT INTO delta_files (table_id, path, size, modification_time, action_type, version)
   VALUES ('table_id', 'missing_file.parquet', 1024, 1609459200000, 'add', 10);
   ```

### Scenario 2: Corruption Detection

**Symptoms:**
- Unable to read Delta log files
- Checkpoint file corruption
- JSON parsing errors

**Actions:**

1. **Identify Corrupted Files**
   ```bash
   # Check Delta log file integrity
   find /path/to/table/_delta_log/ -name "*.json" -exec python -c "
   import json, sys
   try:
       with open(sys.argv[1], 'r') as f:
           for line in f:
               json.loads(line.strip())
   except:
       print(f'Corrupted: {sys.argv[1]}')
   " {} \;
   
   # Check checkpoint files
   find /path/to/table/_delta_log/ -name "*.checkpoint.parquet" -exec python -c "
   import pyarrow.parquet as pq, sys
   try:
       pq.read_table(sys.argv[1])
   except:
       print(f'Corrupted checkpoint: {sys.argv[1]}')
   " {} \;
   ```

2. **Recovery Options**
   ```bash
   # If checkpoint corrupted, rely on JSON files
   # Migration should handle this automatically
   
   # If JSON files corrupted, may need to restore from backup
   # Or use latest valid checkpoint
   ```

## System Outages

### Scenario 1: Database Outage

**Symptoms:**
- Database unreachable
- Connection timeouts
- Migration failures

**Response:**

1. **Assess Outage Scope**
   ```bash
   # Check database service status
   systemctl status postgresql
   
   # Check network connectivity
   ping database_host
   
   # Check other applications
   # Verify if outage affects other services
   ```

2. **Implement Contingency**
   ```bash
   # Pause all migrations
   pkill -f deltalakedb
   
   # Switch to read-only mode if applicable
   # Notify stakeholders
   ```

3. **Recovery Procedures**
   ```bash
   # Start database service
   systemctl start postgresql
   
   # Verify database health
   pg_isready
   
   # Resume migrations when stable
   deltalakedb migration-status --database-url $DB_URL
   ```

### Scenario 2: File System Issues

**Symptoms:**
- Unable to access Delta tables
- Disk space full
- Permission errors

**Response:**

1. **Diagnose File System Issues**
   ```bash
   # Check disk space
   df -h /path/to/delta/tables
   
   # Check permissions
   ls -la /path/to/delta/tables
   
   # Check file system integrity
   fsck -n /dev/device
   ```

2. **Immediate Actions**
   ```bash
   # Free up disk space if needed
   find /tmp -name "*.tmp" -delete
   
   # Fix permissions
   chmod -R 755 /path/to/delta/tables
   
   # Stop migrations to prevent further issues
   pkill -f deltalakedb
   ```

## Maintenance Procedures

### Regular Maintenance Tasks

**Daily:**
- Monitor migration progress
- Check error logs
- Verify database performance

**Weekly:**
- Review migration statistics
- Clean up temporary files
- Update documentation

**Monthly:**
- Performance benchmarking
- Security audit
- Backup verification

### Health Checks

**Migration System Health:**
```bash
# Check all migration statuses
deltalakedb migration-status --database-url $DB_URL

# Verify database connectivity
deltalakedb migration-status --database-url $DB_URL --table-id health_check

# Check system resources
df -h
free -h
uptime
```

**Database Health:**
```sql
-- Check database size
SELECT pg_size_pretty(pg_database_size(current_database()));

-- Check table sizes
SELECT schemaname, tablename,
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE tablename LIKE '%delta%';

-- Check index usage
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;
```

### Backup Procedures

**Database Backup:**
```bash
# PostgreSQL backup
pg_dump -h hostname -U username -d database > backup_$(date +%Y%m%d).sql

# Compressed backup
pg_dump -h hostname -U username -d database | gzip > backup_$(date +%Y%m%d).sql.gz
```

**Delta Table Backup:**
```bash
# Backup _delta_log directories
find /path/to/delta/tables -name "_delta_log" -exec rsync -av {} /backup/delta_logs/ \;

# Verify backup integrity
find /backup/delta_logs -name "*.json" | head -10 | xargs -I {} python -c "
import json, sys
try:
    with open(sys.argv[1], 'r') as f:
        json.load(f)
    print(f'Valid: {sys.argv[1]}')
except:
    print(f'Invalid: {sys.argv[1]}')
" {}
```

## Communication Procedures

### Incident Communication

**Severity Levels:**
- **Critical**: System-wide outage, data loss risk
- **High**: Significant functionality impact
- **Medium**: Partial functionality impact  
- **Low**: Minor issues, workarounds available

**Communication Templates:**

**Critical Incident:**
```
SUBJECT: CRITICAL: DeltaLakeDB Migration System Outage

SEVERITY: Critical
START TIME: [Timestamp]
ESTIMATED RESOLUTION: [Timeframe]
IMPACT: [Description of impact]

CURRENT STATUS:
[Status update]

NEXT UPDATE: [Time]

ACTIONS TAKEN:
[Actions completed]

CONTACT: [Incident commander]
```

**Status Update:**
```
SUBJECT: UPDATE: DeltaLakeDB Migration Incident [Ticket #]

SEVERITY: [Current severity]
START TIME: [Original timestamp]
CURRENT TIME: [Update timestamp]

STATUS UPDATE:
[Detailed status]

PROGRESS:
[What has been accomplished]

NEXT STEPS:
[Planned actions]

ESTIMATED RESOLUTION: [Updated timeframe]
```

## Escalation Procedures

### Escalation Triggers
- Migration failure > 2 hours
- Data integrity issues confirmed
- System outage > 30 minutes
- Customer impact reported

### Escalation Levels

**Level 1: On-call Engineer**
- Initial response
- Basic troubleshooting
- Documentation updates

**Level 2: Senior Engineer**
- Complex issues
- Performance optimization
- Code-level debugging

**Level 3: Engineering Manager**
- Resource coordination
- Customer communication
- Incident management

**Level 4: Leadership**
- Major incidents
- Business impact assessment
- Strategic decisions

### Escalation Contact Matrix

| Issue Type | Level 1 | Level 2 | Level 3 | Level 4 |
|------------|---------|---------|---------|---------|
| Migration Failure | On-call DBA | Senior DBA | DBA Manager | Director |
| Performance Issues | On-call Engineer | Performance Team | Engineering Manager | CTO |
| Data Integrity | On-call DBA | Data Engineer | Data Director | CDO |
| System Outage | On-call SRE | SRE Lead | Infrastructure Manager | CIO |

## Post-Incident Procedures

### Incident Review

**Timeline Creation:**
- Document all events with timestamps
- Capture all actions taken
- Include decision points and rationale

**Root Cause Analysis:**
- Identify immediate triggers
- Analyze contributing factors
- Determine systemic issues

**Lessons Learned:**
- What went well
- What could be improved
- Action items for prevention

### Improvement Actions

**Short-term (1-2 weeks):**
- Fix immediate issues
- Update monitoring
- Improve documentation

**Medium-term (1-3 months):**
- Process improvements
- Tool enhancements
- Training updates

**Long-term (3+ months):**
- Architecture changes
- Strategic initiatives
- Organizational improvements

### Documentation Updates

**Update Runbook:**
- Add new procedures
- Revise existing steps
- Include lessons learned

**Knowledge Base:**
- Document new issues
- Create troubleshooting guides
- Share best practices

**Training Materials:**
- Update training content
- Create new scenarios
- Schedule refresher sessions