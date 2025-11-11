# DeltaLake DB Python Bindings - API Reference

This document provides comprehensive API reference documentation for the deltalakedb Python library, which offers SQL-Backed Delta Lake metadata management with enterprise-grade performance optimizations.

## Table of Contents

1. [Core Classes](#core-classes)
2. [Connection and Configuration](#connection-and-configuration)
3. [Multi-Table Transactions](#multi-table-transactions)
4. [Performance Optimization](#performance-optimization)
5. [Testing and Validation](#testing-and-validation)
6. [Migration and Utilities](#migration-and-utilities)
7. [Error Handling](#error-handling)
8. [Configuration](#configuration)

## Core Classes

### Table

The main class for interacting with Delta Lake tables stored in SQL databases.

```python
class Table:
    """Delta Lake table with SQL backend storage."""

    def __init__(self, uri: str, config: Optional[SqlConfig] = None):
        """
        Initialize a Delta Lake table.

        Args:
            uri: DeltaSQL URI (e.g., "deltasql://postgresql://user:pass@host:5432/db.table")
            config: Optional SQL configuration

        Raises:
            DeltaLakeError: If table cannot be accessed
        """

    @property
    def version(self) -> int:
        """Get current table version."""

    @property
    def metadata(self) -> Dict[str, Any]:
        """Get table metadata."""

    def snapshot(self, version: Optional[int] = None) -> 'TableSnapshot':
        """Get table snapshot at specific version."""

    def history(self, limit: int = 100) -> List['Commit']:
        """Get commit history."""

    def vacuum(self, retention_hours: int = 168) -> Dict[str, Any]:
        """Clean up old files."""

    def optimize(self, mode: str = "compact") -> Dict[str, Any]:
        """Optimize table layout."""
```

### Commit

Represents a Delta Lake commit with metadata and file actions.

```python
class Commit:
    """Delta Lake commit information."""

    def __init__(self, version: int, timestamp: datetime,
                 operation: str, parameters: Dict[str, Any]):
        """Initialize commit information."""

    @property
    def version(self) -> int:
        """Commit version number."""

    @property
    def timestamp(self) -> datetime:
        """Commit timestamp."""

    @property
    def operation(self) -> str:
        """Operation type (WRITE, UPDATE, DELETE, etc.)."""

    @property
    def parameters(self) -> Dict[str, Any]:
        """Operation parameters."""

    def get_file_actions(self) -> List['FileAction']:
        """Get file modifications in this commit."""
```

### File

Represents a file in the Delta Lake table.

```python
class File:
    """File in Delta Lake table."""

    def __init__(self, path: str, size: int, modification_time: datetime,
                 partition_values: Dict[str, str], stats: Optional[str] = None):
        """Initialize file information."""

    @property
    def path(self) -> str:
        """File path."""

    @property
    def size(self) -> int:
        """File size in bytes."""

    @property
    def modification_time(self) -> datetime:
        """Last modification time."""

    @property
    def partition_values(self) -> Dict[str, str]:
        """Partition column values."""

    def get_stats(self) -> Optional[Dict[str, Any]]:
        """Parse file statistics."""
```

### Protocol

Delta Lake protocol version information.

```python
class Protocol:
    """Delta Lake protocol version."""

    def __init__(self, min_reader_version: int, min_writer_version: int):
        """Initialize protocol version."""

    @property
    def min_reader_version(self) -> int:
        """Minimum reader protocol version."""

    @property
    def min_writer_version(self) -> int:
        """Minimum writer protocol version."""

    def is_supported(self, feature: str) -> bool:
        """Check if a protocol feature is supported."""
```

### Metadata

Table metadata including schema, configuration, and partitioning.

```python
class Metadata:
    """Delta Lake table metadata."""

    def __init__(self, schema: TableSchema, partition_columns: List[str],
                 configuration: Dict[str, str], created_time: datetime):
        """Initialize metadata."""

    @property
    def schema(self) -> 'TableSchema':
        """Table schema."""

    @property
    def partition_columns(self) -> List[str]:
        """Partition column names."""

    @property
    def configuration(self) -> Dict[str, Str]:
        """Table configuration."""

    def get_description(self) -> Optional[str]:
        """Get table description."""
```

## Connection and Configuration

### DeltaSqlUri

URI parsing and handling for DeltaSQL connections.

```python
class DeltaSqlUri:
    """DeltaSQL URI parser and validator."""

    def __init__(self, uri: str):
        """Parse and validate DeltaSQL URI."""

    @classmethod
    def parse(cls, uri: str) -> 'DeltaSqlUri':
        """Parse a DeltaSQL URI."""

    @classmethod
    def create(cls, database_url: str, table_name: str,
               schema: str = "public") -> 'DeltaSqlUri':
        """Create a DeltaSQL URI from components."""

    @property
    def database_url(self) -> str:
        """Database connection URL."""

    @property
    def table_name(self) -> str:
        """Table name."""

    @property
    def schema(self) -> str:
        """Schema name."""

    def get_database_type(self) -> str:
        """Get database type (postgresql, mysql, sqlite, etc.)."""

    def get_connection_string(self) -> str:
        """Get database connection string."""
```

### SqlConfig

Configuration for SQL database connections.

```python
class SqlConfig:
    """SQL database connection configuration."""

    def __init__(self, connection_string: str,
                 pool_size: int = 10, max_overflow: int = 20,
                 pool_timeout: int = 30, pool_recycle: int = 3600):
        """Initialize SQL configuration."""

    @property
    def connection_string(self) -> str:
        """Database connection string."""

    @property
    def pool_size(self) -> int:
        """Connection pool size."""

    def create_engine(self) -> Any:
        """Create SQLAlchemy engine."""

    def create_pool(self) -> 'ConnectionPool':
        """Create connection pool."""

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> 'SqlConfig':
        """Create configuration from dictionary."""
```

### SqlConnection

Database connection with transaction management.

```python
class SqlConnection:
    """SQL database connection with transaction support."""

    def __init__(self, config: SqlConfig):
        """Initialize connection."""

    def execute(self, query: str, params: Optional[Dict] = None) -> Any:
        """Execute SQL query."""

    def fetchone(self, query: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Execute query and fetch single row."""

    def fetchall(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute query and fetch all rows."""

    def begin_transaction(self) -> 'TransactionContext':
        """Begin database transaction."""

    def close(self) -> None:
        """Close connection."""
```

### ConnectionPool

Thread-safe connection pooling for database connections.

```python
class ConnectionPool:
    """Thread-safe database connection pool."""

    def __init__(self, config: SqlConfig):
        """Initialize connection pool."""

    def get_connection(self) -> 'SqlConnection':
        """Get connection from pool."""

    def return_connection(self, connection: 'SqlConnection') -> None:
        """Return connection to pool."""

    def close_all(self) -> None:
        """Close all connections."""

    def get_pool_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
```

### TransactionContext

Database transaction with rollback and commit support.

```python
class TransactionContext:
    """Database transaction context manager."""

    def __init__(self, connection: SqlConnection):
        """Initialize transaction context."""

    def __enter__(self) -> 'TransactionContext':
        """Enter transaction context."""

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit transaction context."""

    def commit(self) -> None:
        """Commit transaction."""

    def rollback(self) -> None:
        """Rollback transaction."""

    def execute(self, query: str, params: Optional[Dict] = None) -> Any:
        """Execute query within transaction."""
```

## Multi-Table Transactions

### MultiTableTransaction

ACID transactions across multiple Delta Lake tables.

```python
class MultiTableTransaction:
    """Multi-table ACID transaction."""

    def __init__(self, config: 'MultiTableTransactionConfig'):
        """Initialize multi-table transaction."""

    def add_participant(self, table_uri: str, operations: List['CrossTableOperationType']) -> None:
        """Add table participant to transaction."""

    def prepare(self) -> 'RecoveryPoint':
        """Prepare phase of two-phase commit."""

    def commit(self) -> 'RecoveryResult':
        """Commit phase of two-phase commit."""

    def rollback(self) -> 'RecoveryResult':
        """Rollback transaction."""

    def get_status(self) -> 'MultiTableTransactionStatus':
        """Get transaction status."""

    def get_participants(self) -> List['CrossTableParticipant']:
        """Get transaction participants."""
```

### MultiTableTransactionManager

Manager for distributed multi-table transactions.

```python
class MultiTableTransactionManager:
    """Manager for multi-table transactions."""

    def __init__(self, config: 'MultiTableTransactionConfig'):
        """Initialize transaction manager."""

    def create_transaction(self, isolation_level: 'MultiTableIsolationLevel' = MultiTableIsolationLevel.READ_COMMITTED) -> 'MultiTableTransaction':
        """Create new multi-table transaction."""

    def get_transaction(self, transaction_id: str) -> Optional['MultiTableTransaction']:
        """Get existing transaction."""

    def list_transactions(self, status: Optional['MultiTableTransactionStatus'] = None) -> List['MultiTableTransaction']:
        """List transactions."""

    def recover_transactions(self) -> List['RecoveryResult']:
        """Recover in-progress transactions."""
```

### CrossTableParticipant

Participant in multi-table transaction.

```python
class CrossTableParticipant:
    """Participant in multi-table transaction."""

    def __init__(self, table_uri: str, connection: SqlConnection,
                 operations: List['CrossTableOperationType']):
        """Initialize participant."""

    @property
    def table_uri(self) -> str:
        """Table URI."""

    @property
    def status(self) -> 'MultiTableTransactionStatus':
        """Participant status."""

    def prepare(self) -> 'RecoveryPoint':
        """Prepare participant for commit."""

    def commit(self) -> 'RecoveryResult':
        """Commit participant changes."""

    def rollback(self) -> 'RecoveryResult':
        """Rollback participant changes."""
```

## Performance Optimization

### LazyLoadingManager

Lazy loading for large metadata objects.

```python
class LazyLoadingManager:
    """Manager for lazy loading of metadata objects."""

    def __init__(self, config: 'LazyLoadingConfig'):
        """Initialize lazy loading manager."""

    def create_lazy_table_metadata(self, table_uri: str,
                                   loading_strategy: 'LoadingStrategy' = LoadingStrategy.LAZY) -> 'LazyTableMetadata':
        """Create lazy loading table metadata."""

    def create_lazy_commit_metadata(self, table_uri: str, version: int) -> 'LazyCommitMetadata':
        """Create lazy loading commit metadata."""

    def get_loading_stats(self) -> 'LoadingStats':
        """Get loading statistics."""

    def preload_metadata(self, table_uri: str, max_versions: int = 100) -> None:
        """Preload metadata for faster access."""
```

### LazyLoadingConfig

Configuration for lazy loading behavior.

```python
class LazyLoadingConfig:
    """Configuration for lazy loading."""

    def __init__(self, default_strategy: 'LoadingStrategy' = LoadingStrategy.LAZY,
                 chunk_size: int = 1000, cache_size: int = 100,
                 preload_threshold: int = 50):
        """Initialize lazy loading configuration."""

    @property
    def default_strategy(self) -> 'LoadingStrategy':
        """Default loading strategy."""

    @property
    def chunk_size(self) -> int:
        """Chunk size for paginated loading."""

    def with_strategy(self, strategy: 'LoadingStrategy') -> 'LazyLoadingConfig':
        """Create config with different strategy."""

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> 'LazyLoadingConfig':
        """Create configuration from dictionary."""
```

### LoadingStrategy

Strategies for loading data.

```python
class LoadingStrategy(Enum):
    """Loading strategy enumeration."""

    EAGER = "eager"        # Load all data immediately
    LAZY = "lazy"          # Load data on demand
    PAGINATED = "paginated"  # Load data in pages
    STREAMING = "streaming"  # Stream data continuously
```

### DeltaLakeCacheManager

Multi-level caching for frequently accessed metadata.

```python
class DeltaLakeCacheManager:
    """Cache manager for Delta Lake metadata."""

    def __init__(self, config: 'CacheConfig'):
        """Initialize cache manager."""

    def get_cache(self, cache_type: str) -> 'MemoryCache':
        """Get cache by type."""

    def get(self, key: str, cache_type: str = "metadata") -> Optional[Any]:
        """Get item from cache."""

    def set(self, key: str, value: Any, ttl: Optional[int] = None,
            cache_type: str = "metadata") -> None:
        """Set item in cache."""

    def invalidate(self, key: str, cache_type: str = "metadata") -> None:
        """Invalidate cache item."""

    def get_stats(self) -> 'CacheStats':
        """Get cache statistics."""

    def clear_all(self) -> None:
        """Clear all caches."""
```

### MemoryCache

In-memory cache with configurable eviction policies.

```python
class MemoryCache:
    """In-memory cache with eviction policies."""

    def __init__(self, max_size: int = 1000,
                 eviction_policy: 'EvictionPolicy' = EvictionPolicy.LRU):
        """Initialize memory cache."""

    def get(self, key: str) -> Optional[Any]:
        """Get item from cache."""

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set item in cache."""

    def delete(self, key: str) -> bool:
        """Delete item from cache."""

    def clear(self) -> None:
        """Clear cache."""

    def get_stats(self) -> 'CacheStats':
        """Get cache statistics."""

    def keys(self) -> List[str]:
        """Get all cache keys."""
```

### EvictionPolicy

Cache eviction policies.

```python
class EvictionPolicy(Enum):
    """Cache eviction policy enumeration."""

    LRU = "lru"      # Least Recently Used
    LFU = "lfu"      # Least Frequently Used
    FIFO = "fifo"    # First In, First Out
    TTL = "ttl"      # Time To Live
```

### MemoryOptimizedFileList

Memory-efficient file list for large datasets.

```python
class MemoryOptimizedFileList:
    """Memory-optimized file list."""

    def __init__(self, optimization_strategy: 'OptimizationStrategy' = OptimizationStrategy.CHUNKED,
                 chunk_size: int = 1000, compression_type: 'CompressionType' = CompressionType.NONE):
        """Initialize memory-optimized file list."""

    def add_file(self, file_info: 'File') -> None:
        """Add file to list."""

    def get_files(self, start_index: int = 0, limit: Optional[int] = None) -> List['File']:
        """Get files with pagination."""

    def get_file_count(self) -> int:
        """Get total file count."""

    def get_size_stats(self) -> Dict[str, int]:
        """Get size statistics."""

    def optimize_memory(self) -> 'MemoryStats':
        """Optimize memory usage."""

    def compress(self) -> None:
        """Compress file list."""

    def decompress(self) -> None:
        """Decompress file list."""
```

### AsyncIOExecutor

Async I/O executor for concurrent operations.

```python
class AsyncIOExecutor:
    """Async I/O executor for concurrent operations."""

    def __init__(self, config: 'AsyncTaskConfig'):
        """Initialize async executor."""

    async def submit_task(self, operation: 'AsyncOperationType',
                         params: Dict[str, Any]) -> str:
        """Submit async task."""

    async def get_task_result(self, task_id: str) -> 'AsyncTaskResult':
        """Get task result."""

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel task."""

    async def get_task_status(self, task_id: str) -> 'AsyncTaskStatus':
        """Get task status."""

    def get_running_tasks(self) -> List[str]:
        """Get running task IDs."""

    async def shutdown(self) -> None:
        """Shutdown executor."""
```

### AsyncDeltaLakeOperations

Async Delta Lake operations.

```python
class AsyncDeltaLakeOperations:
    """Async Delta Lake operations."""

    def __init__(self, executor: AsyncIOExecutor):
        """Initialize async operations."""

    async def read_table_async(self, table_uri: str, version: Optional[int] = None) -> 'TableSnapshot':
        """Async table read."""

    async def write_table_async(self, table_uri: str, data: Any,
                                mode: 'WriteMode' = WriteMode.APPEND) -> 'WriteResult':
        """Async table write."""

    async def vacuum_async(self, table_uri: str, retention_hours: int = 168) -> Dict[str, Any]:
        """Async vacuum operation."""

    async def optimize_async(self, table_uri: str, mode: str = "compact") -> Dict[str, Any]:
        """Async optimize operation."""
```

## Testing and Validation

### TestSuiteExecutor

Comprehensive testing framework executor.

```python
class TestSuiteExecutor:
    """Test suite executor for comprehensive testing."""

    def __init__(self, config: 'TestSuiteConfig'):
        """Initialize test suite executor."""

    def run_unit_tests(self, test_paths: List[str]) -> 'TestResult':
        """Run unit tests."""

    def run_integration_tests(self, test_paths: List[str]) -> 'TestResult':
        """Run integration tests."""

    def run_performance_tests(self, test_paths: List[str]) -> 'TestResult':
        """Run performance tests."""

    def run_compatibility_tests(self, test_paths: List[str]) -> 'TestResult':
        """Run compatibility tests."""

    def run_comprehensive_tests(self) -> 'TestResult':
        """Run all test suites."""

    def generate_coverage_report(self) -> Dict[str, Any]:
        """Generate test coverage report."""

    def run_performance_benchmarks(self) -> 'TestResult':
        """Run performance benchmarks."""
```

### TestResult

Comprehensive test result information.

```python
class TestResult:
    """Comprehensive test result."""

    def __init__(self, test_type: 'TestType', status: 'TestStatus',
                 total_tests: int, passed_tests: int, failed_tests: int,
                 execution_time: float):
        """Initialize test result."""

    @property
    def test_type(self) -> 'TestType':
        """Type of test run."""

    @property
    def status(self) -> 'TestStatus':
        """Overall test status."""

    @property
    def success_rate(self) -> float:
        """Test success rate percentage."""

    def get_passed_tests(self) -> List[str]:
        """Get list of passed test names."""

    def get_failed_tests(self) -> List[str]:
        """Get list of failed test names."""

    def get_coverage_report(self) -> Optional[Dict[str, Any]]:
        """Get test coverage report."""

    def get_performance_metrics(self) -> Optional[Dict[str, Any]]:
        """Get performance metrics."""

    def to_dict(self) -> Dict[str, Any]:
        """Convert test result to dictionary."""

    def save_to_file(self, filepath: str) -> None:
        """Save test result to file."""
```

### TestAssertions

Utilities for standardized testing.

```python
class TestAssertions:
    """Test assertion utilities."""

    @staticmethod
    def assert_table_metadata_equal(metadata1: Dict, metadata2: Dict) -> None:
        """Assert table metadata equality."""

    @staticmethod
    def assert_file_lists_equal(files1: List['File'], files2: List['File']) -> None:
        """Assert file lists equality."""

    @staticmethod
    def assert_transaction_consistency(transaction: 'MultiTableTransaction') -> None:
        """Assert transaction consistency."""

    @staticmethod
    def assert_cache_performance(cache: 'MemoryCache',
                               expected_hit_ratio: float) -> None:
        """Assert cache performance."""

    @staticmethod
    def assert_memory_optimization(file_list: 'MemoryOptimizedFileList',
                                  expected_reduction: float) -> None:
        """Assert memory optimization results."""

    @staticmethod
    def assert_async_operations_complete(executor: AsyncIOExecutor) -> None:
        """Assert all async operations complete."""
```

### TestDataGenerator

Automated test data generation.

```python
class TestDataGenerator:
    """Generate test data for Delta Lake operations."""

    @staticmethod
    def generate_table_schema(num_columns: int = 10,
                              include_partitions: bool = True) -> 'TableSchema':
        """Generate table schema for testing."""

    @staticmethod
    def generate_file_list(num_files: int = 1000,
                          file_size_range: Tuple[int, int] = (1024, 1024*1024)) -> List['File']:
        """Generate file list for testing."""

    @staticmethod
    def generate_commit_history(num_commits: int = 100,
                               operations: List[str] = None) -> List['Commit']:
        """Generate commit history for testing."""

    @staticmethod
    def generate_test_data(num_rows: int = 10000,
                          num_columns: int = 10) -> Any:
        """Generate test data rows."""

    @staticmethod
    def generate_large_dataset(size_mb: int = 100) -> Any:
        """Generate large test dataset."""

    @staticmethod
    def generate_concurrent_operations(num_operations: int = 100) -> List[Dict[str, Any]]:
        """Generate concurrent operations for stress testing."""
```

## Migration and Utilities

### DeltaTableMigrator

Migration utilities for existing Delta tables.

```python
class DeltaTableMigrator:
    """Migrate existing Delta tables to SQL backend."""

    def __init__(self, source_uri: str, target_config: SqlConfig):
        """Initialize migrator."""

    def analyze_source(self) -> 'MigrationStatus':
        """Analyze source Delta table."""

    def migrate_table(self, target_table_name: str,
                      preserve_partitions: bool = True) -> 'TableMigrationResult':
        """Migrate table to SQL backend."""

    def validate_migration(self, source_uri: str, target_uri: str) -> 'MigrationResult':
        """Validate migration success."""

    def rollback_migration(self, target_table_name: str) -> 'MigrationResult':
        """Rollback failed migration."""
```

### DeltaLakeBridge

Bridge to deltalake Python package for compatibility.

```python
class DeltaLakeBridge:
    """Bridge to deltalake Python package."""

    def __init__(self, enable_patches: bool = True):
        """Initialize bridge."""

    def patch_deltalake(self) -> bool:
        """Patch deltalake package with DeltaSQL support."""

    def create_compatible_table(self, uri: str) -> Any:
        """Create table compatible with deltalake package."""

    def convert_to_deltalake_table(self, table: Table) -> Any:
        """Convert to deltalake.DeltaTable object."""
```

### DeltaLogger

Structured logging integration.

```python
class DeltaLogger:
    """Structured logger for Delta Lake operations."""

    def __init__(self, name: str = "deltalakedb",
                 config: Optional['LoggerConfig'] = None):
        """Initialize logger."""

    def info(self, message: str, **kwargs) -> None:
        """Log info message."""

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""

    def error(self, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        """Log error message."""

    def log_transaction(self, transaction_id: str, operation: str, **kwargs) -> None:
        """Log transaction operation."""

    def log_performance(self, operation: str, duration_ms: float, **kwargs) -> None:
        """Log performance metrics."""
```

## Error Handling

### DeltaLakeError

Main exception class for Delta Lake operations.

```python
class DeltaLakeError(Exception):
    """Delta Lake operation error."""

    def __init__(self, message: str, kind: 'DeltaLakeErrorKind',
                 details: Optional[Dict[str, Any]] = None,
                 cause: Optional[Exception] = None):
        """Initialize error."""

    @property
    def kind(self) -> 'DeltaLakeErrorKind':
        """Error kind/category."""

    @property
    def details(self) -> Optional[Dict[str, Any]]:
        """Error details."""

    @property
    def cause(self) -> Optional[Exception]:
        """Root cause exception."""

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary."""
```

### DeltaLakeErrorKind

Categorization of Delta Lake errors.

```python
class DeltaLakeErrorKind(Enum):
    """Delta Lake error kinds."""

    CONNECTION_ERROR = "connection_error"
    VALIDATION_ERROR = "validation_error"
    TRANSACTION_ERROR = "transaction_error"
    CONCURRENCY_ERROR = "concurrency_error"
    SCHEMA_ERROR = "schema_error"
    PERMISSION_ERROR = "permission_error"
    RESOURCE_ERROR = "resource_error"
    TIMEOUT_ERROR = "timeout_error"
    CONFIGURATION_ERROR = "configuration_error"
```

## Configuration

### ConfigFactory

Factory for creating validated configurations.

```python
class ConfigFactory:
    """Factory for creating validated configurations."""

    @staticmethod
    def create_sql_config(config_dict: Dict[str, Any]) -> SqlConfig:
        """Create validated SQL configuration."""

    @staticmethod
    def create_cache_config(config_dict: Dict[str, Any]) -> 'CacheConfig':
        """Create validated cache configuration."""

    @staticmethod
    def create_lazy_loading_config(config_dict: Dict[str, Any]) -> LazyLoadingConfig:
        """Create validated lazy loading configuration."""

    @staticmethod
    def create_async_config(config_dict: Dict[str, Any]) -> AsyncTaskConfig:
        """Create validated async configuration."""
```

### ConfigLoader

Load configuration from files.

```python
class ConfigLoader:
    """Load configuration from files."""

    @staticmethod
    def load_config_file(filepath: str) -> Dict[str, Any]:
        """Load configuration from file."""

    @staticmethod
    def load_yaml_config(filepath: str) -> Dict[str, Any]:
        """Load YAML configuration."""

    @staticmethod
    def load_toml_config(filepath: str) -> Dict[str, Any]:
        """Load TOML configuration."""

    @staticmethod
    def load_json_config(filepath: str) -> Dict[str, Any]:
        """Load JSON configuration."""

    @staticmethod
    def save_yaml_config(config: Dict[str, Any], filepath: str) -> None:
        """Save configuration as YAML."""

    @staticmethod
    def save_toml_config(config: Dict[str, Any], filepath: str) -> None:
        """Save configuration as TOML."""
```

## CLI Functions

### Core CLI Functions

```python
def quick_connect(uri: str, config_file: Optional[str] = None) -> Table:
    """Quick connect to Delta Lake table."""

def list_tables_cli(database_url: str, schema: str = "public") -> List[str]:
    """List tables using CLI."""

def create_table_cli(uri: str, schema_dict: Dict[str, Any],
                    partition_columns: Optional[List[str]] = None) -> Table:
    """Create table using CLI."""
```

### Utility Functions

```python
def connect_to_table(uri: str, config: Optional[SqlConfig] = None) -> Table:
    """Connect to Delta Lake table."""

def create_table(uri: str, schema: 'TableSchema',
                partition_columns: Optional[List[str]] = None,
                configuration: Optional[Dict[str, str]] = None) -> Table:
    """Create new Delta Lake table."""

def list_tables(database_url: str, schema: str = "public") -> List[str]:
    """List all Delta Lake tables."""
```

## Example Usage

### Basic Usage

```python
import deltalakedb as dl

# Connect to table
table = dl.connect_to_table("deltasql://postgresql://user:pass@host/db.mytable")

# Get current version and metadata
print(f"Current version: {table.version}")
print(f"Table metadata: {table.metadata}")

# Get snapshot at specific version
snapshot = table.snapshot(version=10)
print(f"Files at v10: {len(snapshot.files)}")

# Get commit history
history = table.history(limit=10)
for commit in history:
    print(f"v{commit.version}: {commit.operation}")
```

### Multi-Table Transaction

```python
import deltalakedb as dl

# Create transaction manager
manager = dl.MultiTableTransactionManager()

# Create transaction
tx = manager.create_transaction(
    isolation_level=dl.MultiTableIsolationLevel.SERIALIZABLE
)

# Add participants
tx.add_participant("deltasql://pg://localhost/db.table1", [
    dl.CrossTableOperationType.WRITE,
    dl.CrossTableOperationType.UPDATE
])
tx.add_participant("deltasql://pg://localhost/db.table2", [
    dl.CrossTableOperationType.WRITE
])

# Execute transaction
try:
    tx.prepare()
    result = tx.commit()
    print(f"Transaction committed: {result.success}")
except Exception as e:
    tx.rollback()
    print(f"Transaction rolled back: {e}")
```

### Performance Optimization

```python
import deltalakedb as dl

# Configure lazy loading
lazy_config = dl.LazyLoadingConfig(
    default_strategy=dl.LoadingStrategy.PAGINATED,
    chunk_size=500,
    cache_size=50
)

# Configure caching
cache_manager = dl.DeltaLakeCacheManager(
    dl.CacheConfig(
        max_size=1000,
        eviction_policy=dl.EvictionPolicy.LRU,
        ttl_seconds=300
    )
)

# Configure async operations
async_config = dl.AsyncTaskConfig(
    max_concurrent_tasks=10,
    task_timeout_seconds=300
)

executor = dl.AsyncIOExecutor(async_config)
ops = dl.AsyncDeltaLakeOperations(executor)

# Async operations
import asyncio

async def async_example():
    snapshot = await ops.read_table_async("deltasql://pg://localhost/db.mytable")
    result = await ops.optimize_async("deltasql://pg://localhost/db.mytable")
    return snapshot, result

asyncio.run(async_example())
```

### Testing

```python
import deltalakedb as dl

# Create test suite
config = dl.TestSuiteConfig(
    test_paths=["tests/unit", "tests/integration"],
    parallelism=4,
    coverage=True
)

executor = dl.TestSuiteExecutor(config)

# Run comprehensive tests
result = executor.run_comprehensive_tests()
print(f"Tests passed: {result.passed_tests}/{result.total_tests}")
print(f"Success rate: {result.success_rate:.1f}%")

# Generate coverage report
coverage = executor.generate_coverage_report()
print(f"Code coverage: {coverage['percentage']:.1f}%")

# Run performance benchmarks
benchmarks = executor.run_performance_benchmarks()
for benchmark in benchmarks.performance_metrics:
    print(f"{benchmark['name']}: {benchmark['duration_ms']}ms")
```

This API reference provides comprehensive documentation for all major components of the deltalakedb Python library, enabling users to effectively utilize SQL-Backed Delta Lake metadata management with enterprise-grade performance optimizations.