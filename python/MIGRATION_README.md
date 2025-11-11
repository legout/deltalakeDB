# DeltaLakeDB Migration Tools

This document provides an overview of the migration tools for converting existing Delta tables to SQL-backed metadata.

## Installation

```bash
# Install with SQLite support
pip install delkalakedb[sqlite]

# Install with PostgreSQL support  
pip install delkalakedb[postgres]

# Install with all database support
pip install delkalakedb[all-dbs]

# Install development dependencies
pip install delkalakedb[dev]
```

## CLI Commands

### Discover Delta Tables

Find Delta tables in a directory:

```bash
deltalakedb discover /path/to/delta/tables --recursive
```

### Import Delta Table

Migrate an existing Delta table to SQL-backed metadata:

```bash
# Basic import
deltalakedb import-table /path/to/delta/table --database-url "sqlite:///metadata.db"

# With custom table ID
deltalakedb import-table /path/to/delta/table --database-url "sqlite:///metadata.db" --table-id "my_table"

# Dry run (no changes made)
deltalakedb import-table /path/to/delta/table --database-url "sqlite:///metadata.db" --dry-run

# Resume interrupted migration
deltalakedb import-table /path/to/delta/table --database-url "sqlite:///metadata.db" --resume

# Verbose output
deltalakedb import-table /path/to/delta/table --database-url "sqlite:///metadata.db" --verbose
```

### Validate Migrated Table

Validate a migrated table against the original file-based metadata:

```bash
deltalakedb validate /path/to/delta/table --database-url "sqlite:///metadata.db"
```

### Check Migration Status

View migration status:

```bash
# All tables
deltalakedb migration-status --database-url "sqlite:///metadata.db"

# Specific table
deltalakedb migration-status --database-url "sqlite:///metadata.db" --table-id "my_table"
```

## Database URLs

### SQLite
```
sqlite:///path/to/database.db
```

### PostgreSQL
```
postgresql://username:password@host:port/database
```

## Migration Process

The migration follows a checkpoint-first strategy:

1. **Checkpoint Processing**: Reads the latest checkpoint to get the current table state
2. **JSON Log Replay**: Processes JSON commits after the checkpoint version
3. **Bulk Insert**: Inserts file actions into SQL database in batches
4. **Validation**: Compares SQL and file-based snapshots for consistency

## Features

- **Resumable Migrations**: Can resume from interrupted migrations
- **Progress Tracking**: Real-time progress reporting and status tracking
- **Validation**: Comprehensive validation between SQL and file-based metadata
- **Multiple Databases**: Support for SQLite, PostgreSQL, and MySQL
- **Dry Run Mode**: Test migrations without making changes
- **Error Recovery**: Robust error handling and recovery procedures

## Error Handling

The migration tools include comprehensive error handling:

- Database connection failures
- Corrupted checkpoint files
- Invalid JSON log entries
- Schema mismatches
- Permission issues

## Performance Considerations

- Use appropriate batch sizes for your database (default: 1000)
- Schedule large migrations during maintenance windows
- Monitor database performance during migration
- Consider database-specific optimizations (indexes, etc.)

## Troubleshooting

### Common Issues

1. **Permission Denied**: Ensure write access to database file and Delta log directory
2. **Database Connection Failed**: Verify database URL and credentials
3. **Memory Issues**: Reduce batch size for large tables
4. **Validation Failures**: Check for schema changes during migration

### Debug Mode

Use `--verbose` flag for detailed logging and debugging information.