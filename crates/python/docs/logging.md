# Logging Framework Integration

This document describes the comprehensive logging framework integration in DeltaLake DB Python bindings, providing structured logging, performance monitoring, error tracking, and seamless integration with Python's standard logging framework.

## Overview

The DeltaLake Python package provides extensive logging capabilities that allow you to:

- Integrate seamlessly with Python's standard logging framework
- Use structured logging with extra data and context
- Track database operations, transactions, and performance metrics
- Monitor errors and exceptions with detailed context
- Export logs in multiple formats (JSON, CSV, plain text)
- Configure specialized loggers for different components
- Manage log entries and perform analysis

## Quick Start

```python
import deltalakedb as dl

# Setup basic logging
dl.setup_logging(level="INFO", enable_console=True)

# Create a logger
logger = dl.create_logger(name="my_app")

# Log messages with context
logger.info("Starting application", extra_data={
    "version": "1.0.0",
    "environment": "production"
})

# Log database operations
logger.log_operation(
    operation="CREATE_TABLE",
    table_name="sales_data",
    transaction_id="txn_001",
    duration_ms=1250.5,
    success=True
)
```

## Core Components

### LoggerConfig

Configuration class for logger settings:

```python
from deltalakedb import LoggerConfig

config = LoggerConfig(
    name="deltalakedb.myapp",
    level="INFO",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=["console", "file"],
    propagate=True
)

# Modify configuration
config.add_handler("syslog")
config.set_level("DEBUG")
config.set_format("%(levelname)s: %(message)s")
```

### DeltaLogger

Advanced logger with structured logging capabilities:

```python
from deltalakedb import DeltaLogger

logger = DeltaLogger(
    config=config,
    max_entries=10000,
    enable_file_logging=True,
    enable_console_logging=True,
    log_file_path="logs/app.log"
)

# Log messages with context
logger.info("Processing batch",
           module="batch_processor",
           function="process_batch",
           line=42,
           extra_data={"batch_size": 1000})

# Log database operations
logger.log_operation(
    operation="QUERY",
    table_name="users",
    transaction_id="txn_123",
    duration_ms=245.7,
    success=True
)

# Log performance metrics
logger.log_performance(
    operation="index_scan",
    metrics={
        "execution_time_ms": 150.2,
        "rows_scanned": 50000,
        "index_hits": 45000
    }
)
```

### LogEntry

Structured log entry with rich context:

```python
from deltalakedb import LogEntry

entry = LogEntry(
    level="INFO",
    logger_name="deltalakedb.app",
    message="Table scan completed",
    module="scanner",
    function="scan_table",
    line=125
)

# Add context
entry.add_extra_data("rows_scanned", "1500000")
entry.add_extra_data("duration_ms", "2340")

# Set transaction context
entry.set_transaction_context(
    transaction_id="txn_456",
    table_name="sales_data",
    operation="SCAN"
)

# Set performance context
entry.set_performance_context(2340.5)

# Set error context (if applicable)
entry.set_error_context("SCAN_TIMEOUT")
```

## Specialized Loggers

### Database Logger

For database operations and connection events:

```python
from deltalakedb import LoggingUtils

db_logger = LoggingUtils.create_database_logger(
    log_level="INFO",
    log_file="logs/database.log"
)

db_logger.log_operation(
    operation="CREATE_TABLE",
    table_name="sales_data",
    transaction_id="txn_001",
    duration_ms=890.3,
    success=True
)
```

### Transaction Logger

For transaction monitoring and debugging:

```python
tx_logger = LoggingUtils.create_transaction_logger(
    log_level="DEBUG",
    log_file="logs/transactions.log"
)

tx_logger.info("Transaction started",
              extra_data={
                  "transaction_id": "txn_001",
                  "isolation_level": "READ_COMMITTED",
                  "participants": 3
              })
```

### Performance Logger

For performance monitoring and optimization:

```python
perf_logger = LoggingUtils.create_performance_logger(
    log_level="INFO",
    log_file="logs/performance.log"
)

perf_logger.log_performance(
    operation="query_execution",
    metrics={
        "execution_time_ms": 1250.7,
        "cpu_usage_percent": 45.2,
        "memory_used_mb": 256.8,
        "rows_processed": 100000
    }
)
```

### Error Logger

For error tracking and debugging:

```python
error_logger = LoggingUtils.create_error_logger(
    log_level="ERROR",
    log_file="logs/errors.log"
)

error_logger.log_operation(
    operation="CONNECT_DATABASE",
    table_name="users",
    transaction_id=None,
    duration_ms=5000.0,
    success=False,
    error_message="Connection timeout"
)
```

## Configuration Management

### Basic Setup

```python
import deltalakedb as dl

# Setup basic logging with console output
dl.setup_logging(
    level="INFO",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    enable_console=True
)

# Setup with file logging
dl.setup_logging(
    level="DEBUG",
    log_file="logs/app.log",
    enable_console=True
)
```

### Advanced Configuration

```python
# Custom format with transaction context
custom_format = "%(asctime)s [TX:%(transaction_id)s] %(levelname)s %(name)s: %(message)s"

dl.setup_logging(
    level="DEBUG",
    format=custom_format,
    log_file="logs/detailed.log",
    enable_console=True
)
```

### Dynamic Level Management

```python
from deltalakedb import LoggingUtils

# Get current logging level
current_level = LoggingUtils.get_logging_level("deltalakedb.database")
print(f"Current level: {current_level}")

# Set logging level dynamically
LoggingUtils.set_logging_level("DEBUG", "deltalakedb.database")

# Test logging configuration
LoggingUtils.test_logging("deltalakedb.test")
```

## Log Analysis and Management

### Retrieving Log Entries

```python
# Get recent entries
recent_entries = logger.get_recent_entries(100)

# Get entries by level
error_entries = logger.get_entries_by_level("ERROR")
warning_entries = logger.get_entries_by_level("WARNING")

# Get entries by time range
from datetime import datetime

start_time = "2023-12-01T00:00:00Z"
end_time = "2023-12-01T23:59:59Z"
entries = logger.get_entries_by_time_range(start_time, end_time)
```

### Log Statistics

```python
# Get comprehensive statistics
stats = logger.get_statistics()
print(f"Total entries: {stats['total_entries']}")
print(f"Error count: {stats['error_count']}")
print(f"Warning count: {stats['warning_count']}")
```

### Log Export

```python
# Export to JSON
logger.export_logs("logs/export.json", "json")

# Export to CSV
logger.export_logs("logs/export.csv", "csv")

# Export to plain text
logger.export_logs("logs/export.txt", "text")
```

### Log Entry Management

```python
# Clear all log entries
logger.clear_entries()

# Set maximum number of entries
logger.set_max_entries(50000)

# Enable/disable file logging
logger.enable_file_logging("logs/app.log")
logger.disable_file_logging()

# Enable/disable console logging
logger.enable_console_logging()
logger.disable_console_logging()
```

## Integration Examples

### Application Integration

```python
import deltalakedb as dl
from deltalakedb import LoggingUtils

# Initialize logging for the application
def setup_app_logging():
    # Setup global logging
    dl.setup_logging(
        level="INFO",
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        log_file="logs/app.log",
        enable_console=True
    )

    # Create specialized loggers
    global db_logger, tx_logger, perf_logger

    db_logger = LoggingUtils.create_database_logger()
    tx_logger = LoggingUtils.create_transaction_logger()
    perf_logger = LoggingUtils.create_performance_logger()

# Example function with logging
def process_table_data(table_name):
    tx_id = f"tx_{int(time.time())}"

    try:
        # Log start of operation
        db_logger.info(f"Starting processing for table: {table_name}",
                      extra_data={"transaction_id": tx_id})

        start_time = time.time()

        # Simulate processing
        # ... actual processing logic here ...

        duration = (time.time() - start_time) * 1000

        # Log success
        db_logger.log_operation(
            operation="PROCESS_TABLE",
            table_name=table_name,
            transaction_id=tx_id,
            duration_ms=duration,
            success=True
        )

        # Log performance metrics
        perf_logger.log_performance(
            operation="table_processing",
            metrics={
                "duration_ms": duration,
                "table_name": table_name,
                "transaction_id": tx_id
            }
        )

    except Exception as e:
        # Log error
        db_logger.log_operation(
            operation="PROCESS_TABLE",
            table_name=table_name,
            transaction_id=tx_id,
            duration_ms=duration,
            success=False,
            error_message=str(e)
        )

        # Re-raise exception
        raise
```

### Database Operation Logging

```python
import deltalakedb as dl
from contextlib import contextmanager

@contextmanager
def logged_database_operation(operation, table_name, logger):
    tx_id = f"tx_{int(time.time())}"
    start_time = time.time()

    logger.info(f"Starting {operation} on {table_name}",
                extra_data={"transaction_id": tx_id})

    try:
        yield tx_id

        duration = (time.time() - start_time) * 1000
        logger.log_operation(
            operation=operation,
            table_name=table_name,
            transaction_id=tx_id,
            duration_ms=duration,
            success=True
        )

    except Exception as e:
        duration = (time.time() - start_time) * 1000
        logger.log_operation(
            operation=operation,
            table_name=table_name,
            transaction_id=tx_id,
            duration_ms=duration,
            success=False,
            error_message=str(e)
        )
        raise

# Usage
db_logger = LoggingUtils.create_database_logger()

with logged_database_operation("CREATE_TABLE", "sales_data", db_logger) as tx_id:
    # Create table logic here
    pass
```

## Best Practices

### 1. Structured Logging

```python
# Good: Use structured data
logger.info("User logged in",
           extra_data={
               "user_id": "12345",
               "session_id": "abcde",
               "ip_address": "192.168.1.1"
           })

# Avoid: Unstructured messages
logger.info("User 12345 logged in from IP 192.168.1.1 with session abcde")
```

### 2. Performance Logging

```python
# Log performance metrics for critical operations
def expensive_operation():
    start_time = time.time()

    try:
        # ... operation logic ...
        result = perform_operation()

        duration = (time.time() - start_time) * 1000
        perf_logger.log_performance(
            operation="expensive_operation",
            metrics={
                "duration_ms": duration,
                "success": True,
                "result_size": len(result) if result else 0
            }
        )

        return result

    except Exception as e:
        duration = (time.time() - start_time) * 1000
        perf_logger.log_performance(
            operation="expensive_operation",
            metrics={
                "duration_ms": duration,
                "success": False,
                "error_type": type(e).__name__
            }
        )
        raise
```

### 3. Error Logging

```python
# Log errors with full context
try:
    risky_operation()
except DatabaseError as e:
    error_logger.log_operation(
        operation="RISKY_OPERATION",
        table_name="critical_data",
        transaction_id=None,
        duration_ms=None,
        success=False,
        error_message=str(e)
    )

    # Also create detailed error entry
    error_entry = LogEntry(
        level="ERROR",
        logger_name="deltalakedb.errors",
        message=f"Database error in risky_operation: {e}",
        module="operations",
        function="risky_operation",
        line=123
    )
    error_entry.set_error_context("DATABASE_ERROR")
    error_entry.add_extra_data("error_code", e.error_code)
```

### 4. Transaction Logging

```python
# Log transaction lifecycle
def execute_transaction():
    tx_id = f"tx_{int(time.time())}"

    tx_logger.info("Transaction started",
                  extra_data={
                      "transaction_id": tx_id,
                      "isolation_level": "READ_COMMITTED"
                  })

    try:
        # Transaction operations
        perform_operation_1(tx_id)
        perform_operation_2(tx_id)
        commit_transaction(tx_id)

        tx_logger.info("Transaction committed successfully",
                      extra_data={"transaction_id": tx_id})

    except Exception as e:
        rollback_transaction(tx_id)
        tx_logger.error("Transaction rolled back",
                       extra_data={
                           "transaction_id": tx_id,
                           "error": str(e)
                       })
        raise
```

### 5. Configuration Management

```python
# Environment-specific logging
import os

def setup_environment_logging():
    env = os.getenv("DELTALAKE_ENV", "development")

    if env == "production":
        level = "WARNING"
        log_file = "logs/production.log"
        enable_console = False
    elif env == "staging":
        level = "INFO"
        log_file = "logs/staging.log"
        enable_console = True
    else:  # development
        level = "DEBUG"
        log_file = "logs/development.log"
        enable_console = True

    dl.setup_logging(
        level=level,
        log_file=log_file,
        enable_console=enable_console
    )
```

## Log Formats

### JSON Export Format

```json
[
  {
    "timestamp": "2023-12-01T10:30:45.123456Z",
    "level": "INFO",
    "logger_name": "deltalakedb.database",
    "message": "Table scan completed",
    "module": "scanner",
    "function": "scan_table",
    "line": 125,
    "extra_data": {
      "rows_scanned": "1500000",
      "duration_ms": "2340"
    },
    "transaction_id": "txn_456",
    "table_name": "sales_data",
    "operation": "SCAN",
    "duration_ms": 2340.5,
    "error_code": null
  }
]
```

### CSV Export Format

```csv
timestamp,level,logger_name,message,module,function,line,transaction_id,table_name,operation,duration_ms,error_code
2023-12-01T10:30:45.123456Z,INFO,deltalakedb.database,Table scan completed,scanner,scan_table,125,txn_456,sales_data,SCAN,2340.5,
```

### Plain Text Format

```
2023-12-01T10:30:45.123456Z - deltalakedb.database - INFO - Table scan completed
2023-12-01T10:30:46.234567Z - deltalakedb.transaction - DEBUG - [TX:txn_456] Starting transaction
2023-12-01T10:30:47.345678Z - deltalakedb.performance - INFO - [PERF] Query execution completed in 1250.7ms
```

## Troubleshooting

### Common Issues

1. **Logging Level Not Working**: Ensure the logger hierarchy is properly configured
2. **File Logging Not Working**: Check file permissions and directory existence
3. **Performance Impact**: Use appropriate logging levels and consider async logging for high-throughput applications
4. **Missing Context**: Always include relevant transaction_id, table_name, and operation in log entries

### Debug Logging

```python
# Enable debug logging for troubleshooting
LoggingUtils.set_logging_level("DEBUG", "deltalakedb")

# Test logging configuration
LoggingUtils.test_logging()

# Check current levels
current_level = LoggingUtils.get_logging_level()
print(f"Current level: {current_level}")
```

This logging framework provides comprehensive capabilities for monitoring, debugging, and analyzing DeltaLake operations while maintaining compatibility with Python's standard logging ecosystem.