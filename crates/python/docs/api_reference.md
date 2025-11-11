# DeltaLake DB Python API Reference

## Overview

DeltaLake DB Python provides high-performance Python bindings for SQL-Backed Delta Lake metadata operations. This comprehensive API enables seamless integration with the Python data ecosystem while maintaining compatibility with existing Delta Lake workflows.

## Installation

```bash
pip install deltalakedb-python
```

## Core Modules

### 1. Core Bindings (`deltalakedb.bindings`)

#### Table
```python
class Table:
    """Represents a Delta table with SQL backend storage."""

    def __init__(self, uri: str, config: Optional[SqlConfig] = None)
    def get_version(self, version: Optional[int] = None) -> TableSnapshot
    def get_metadata(self) -> Metadata
    def get_protocol(self) -> Protocol
    def get_files(self) -> List[File]
    def commit(self, actions: List[dict], metadata: dict) -> Commit
    def history(self, limit: Optional[int] = None) -> List[Commit]
```

#### Commit
```python
class Commit:
    """Represents a Delta Lake commit."""

    @property
    def version(self) -> int
    @property
    def timestamp(self) -> datetime
    @property
    def user_id(self) -> str
    @property
    def operation(self) -> str
    @property
    def metadata(self) -> Dict[str, Any]
    @property
    def actions(self) -> List[dict]
```

#### File
```python
class File:
    """Represents a file in a Delta table."""

    @property
    def path(self) -> str
    @property
    def size(self) -> int
    @property
    def modification_time(self) -> datetime
    @property
    def stats(self) -> Optional[str]
    @property
    def partition_values(self) -> Dict[str, str]
```

### 2. Connection Management (`deltalakedb.connection`)

#### SqlConnection
```python
class SqlConnection:
    """Manages database connections for Delta operations."""

    def __init__(self, config: SqlConfig)
    def execute_query(self, query: str, params: Optional[List] = None) -> List[Dict]
    def execute_command(self, command: str, params: Optional[List] = None) -> int
    def begin_transaction(self) -> TransactionContext
    def close(self) -> None
    def is_alive(self) -> bool
```

#### ConnectionPool
```python
class ConnectionPool:
    """Connection pool for efficient resource management."""

    def __init__(self, config: SqlConfig, max_size: int = 10)
    def get_connection(self) -> SqlConnection
    def return_connection(self, connection: SqlConnection) -> None
    def close_all(self) -> None
    def get_pool_stats(self) -> Dict[str, Any]
```

#### TransactionContext
```python
class TransactionContext:
    """Manages database transactions."""

    def commit(self) -> None
    def rollback(self) -> None
    def execute_query(self, query: str, params: Optional[List] = None) -> List[Dict]
    def execute_command(self, command: str, params: Optional[List] = None) -> int
```

### 3. URI Handling (`deltalakedb.uri`)

#### DeltaSqlUri
```python
class DeltaSqlUri:
    """Represents a Delta SQL URI for table connections."""

    def __init__(self, uri: str)
    def __str__(self) -> str
    def __repr__(self) -> str

    @property
    def database_type(self) -> str
    @property
    def connection_string(self) -> str
    @property
    def table_name(self) -> str
    @property
    def options(self) -> Dict[str, str]

    def validate(self) -> bool
    def to_connection_string(self) -> str
```

#### URI Functions
```python
def parse_uri(uri: str) -> DeltaSqlUri
def create_uri(database_type: str, connection_string: str, table_name: str, **options) -> DeltaSqlUri
def validate_uri(uri: str) -> bool
def get_database_type(uri: str) -> str
def get_connection_string(uri: str) -> str
def get_table_name(uri: str) -> str
```

### 4. Configuration (`deltalakedb.config`)

#### SqlConfig
```python
class SqlConfig:
    """Configuration for SQL database connections."""

    def __init__(
        self,
        database_type: str,
        connection_string: str,
        table_prefix: str = "delta_",
        connection_timeout: int = 30,
        max_connections: int = 10,
        batch_size: int = 1000,
        enable_compression: bool = True,
        **database_options
    )

    def validate(self) -> bool
    def get_connection_url(self) -> str
    def to_dict(self) -> Dict[str, Any]
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> SqlConfig
    @classmethod
    def from_yaml(cls, yaml_path: str) -> SqlConfig
    @classmethod
    def from_toml(cls, toml_path: str) -> SqlConfig
```

### 5. Type System (`deltalakedb.types`)

#### DeltaDataType
```python
class DeltaDataType:
    """Represents Delta Lake data types."""

    @classmethod
    def primitive(cls, name: str) -> DeltaDataType
    @classmethod
    def array(cls, element_type: DeltaDataType, contains_null: bool = True) -> DeltaDataType
    @classmethod
    def struct(cls, fields: List[SchemaField]) -> DeltaDataType
    @classmethod
    def map(cls, key_type: DeltaDataType, value_type: DeltaDataType, value_contains_null: bool = True) -> DeltaDataType

    @property
    def type_name(self) -> str
    @property
    def is_primitive(self) -> bool
    @property
    def is_complex(self) -> bool
    def to_dict(self) -> Dict[str, Any]
    def to_string(self) -> str
```

#### SchemaField
```python
class SchemaField:
    """Represents a field in a Delta table schema."""

    def __init__(self, name: str, data_type: DeltaDataType, nullable: bool = True, metadata: Optional[Dict] = None)

    @property
    def name(self) -> str
    @property
    def data_type(self) -> DeltaDataType
    @property
    def nullable(self) -> bool
    @property
    def metadata(self) -> Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]
    def to_string(self) -> str
```

#### TableSchema
```python
class TableSchema:
    """Represents a Delta table schema."""

    def __init__(self, fields: List[SchemaField], partition_columns: Optional[List[str]] = None)

    @property
    def fields(self) -> List[SchemaField]
    @property
    def partition_columns(self) -> List[str]
    @property
    def column_names(self) -> List[str]

    def get_field(self, name: str) -> Optional[SchemaField]
    def get_field_index(self, name: str) -> int
    def add_field(self, field: SchemaField) -> None
    def remove_field(self, name: str) -> None
    def to_dict(self) -> Dict[str, Any]
    def to_string(self) -> str
```

### 6. Write Operations (`deltalakedb.write_operations`)

#### DeltaWriter
```python
class DeltaWriter:
    """High-level interface for writing data to Delta tables."""

    def __init__(self, table: Table, config: Optional[WriteConfig] = None)
    def write(
        self,
        data: Union[pd.DataFrame, Any],
        mode: WriteMode = WriteMode.APPEND,
        partition_by: Optional[List[str]] = None,
        overwrite_schema: bool = False
    ) -> WriteResult
    def write_batch(
        self,
        batches: Iterator[Any],
        mode: WriteMode = WriteMode.APPEND,
        partition_by: Optional[List[str]] = None
    ) -> List[WriteResult]
    def close(self) -> None
```

#### WriteMode
```python
class WriteMode(Enum):
    """Write modes for Delta operations."""
    APPEND = "append"
    OVERWRITE = "overwrite"
    OVERWRITE_DYNAMIC = "overwrite_dynamic"
    IGNORE = "ignore"
    ERRORIfExists = "error"
```

#### WriteConfig
```python
class WriteConfig:
    """Configuration for write operations."""

    def __init__(
        self,
        batch_size: int = 1000,
        compression: Optional[str] = None,
        max_concurrent_files: int = 10,
        checkpoint_interval: int = 10,
        optimize_write: bool = True
    )
```

### 7. Multi-table Transactions (`deltalakedb.multi_table_transactions`)

#### MultiTableTransaction
```python
class MultiTableTransaction:
    """ACID transaction across multiple Delta tables."""

    def __init__(self, config: MultiTableTransactionConfig)
    def add_participant(self, table: Table, operations: List[dict]) -> None
    def prepare(self) -> MultiTableTransactionStatus
    def commit(self) -> MultiTableTransactionResult
    def rollback(self) -> None
    def get_status(self) -> MultiTableTransactionStatus
    def get_timeout(self) -> int
```

#### MultiTableTransactionConfig
```python
class MultiTableTransactionConfig:
    """Configuration for multi-table transactions."""

    def __init__(
        self,
        isolation_level: MultiTableIsolationLevel = MultiTableIsolationLevel.SERIALIZABLE,
        timeout_seconds: int = 300,
        max_retries: int = 3,
        enable_compatibility_check: bool = True
    )
```

#### Transaction Functions
```python
def create_transaction_context(
    tables: List[Table],
    isolation_level: MultiTableIsolationLevel = MultiTableIsolationLevel.SERIALIZABLE,
    timeout_seconds: int = 300
) -> MultiTableTransaction

def execute_in_transaction(
    tables: List[Table],
    operations: Callable[[MultiTableTransaction], Any],
    isolation_level: MultiTableIsolationLevel = MultiTableIsolationLevel.SERIALIZABLE
) -> MultiTableTransactionResult
```

### 8. Performance Optimization

#### Lazy Loading (`deltalakedb.lazy_loading`)
```python
class LazyLoadingConfig:
    """Configuration for lazy loading strategies."""

    def __init__(
        self,
        strategy: LoadingStrategy = LoadingStrategy.LAZY,
        chunk_size: int = 1000,
        cache_size: int = 100,
        prefetch_count: int = 5
    )

class LazyLoadingManager:
    """Manages lazy loading for large metadata objects."""

    def __init__(self, config: LazyLoadingConfig)
    def load_table_metadata(self, table: Table) -> LazyTableMetadata
    def load_commit_metadata(self, table: Table, version: int) -> LazyCommitMetadata
    def preload_data(self, table: Table, prefetch_spec: Dict) -> None
    def get_loading_stats(self) -> Dict[str, Any]
```

#### Caching (`deltalakedb.caching`)
```python
class DeltaLakeCacheManager:
    """Comprehensive caching system for Delta operations."""

    def __init__(self, config: CacheConfig)
    def get(self, key: str) -> Optional[Any]
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None
    def invalidate(self, key: str) -> None
    def clear_all(self) -> None
    def get_stats(self) -> CacheStats

    def cache_table_metadata(self, table: Table, metadata: Any) -> None
    def cache_file_list(self, table: Table, files: List[File]) -> None
    def cache_query_result(self, query: str, result: Any) -> None
```

#### Memory Optimization (`deltalakedb.memory_optimization`)
```python
class MemoryOptimizedFileList:
    """Memory-optimized file list for large tables."""

    def __init__(self, strategy: OptimizationStrategy = OptimizationStrategy.HYBRID)
    def add_files(self, files: List[File]) -> None
    def get_files(self, offset: int = 0, limit: Optional[int] = None) -> List[File]
    def get_file_count(self) -> int
    def get_memory_usage(self) -> MemoryStats
    def optimize(self) -> None
```

#### Async I/O (`deltalakedb.async_io`)
```python
class AsyncIOExecutor:
    """Async I/O executor for Delta operations."""

    def __init__(self, config: AsyncTaskConfig)
    async def execute_query_async(self, query: str, params: Optional[List] = None) -> List[Dict]
    async def execute_command_async(self, command: str, params: Optional[List] = None) -> int
    async def batch_execute_async(self, operations: List[Dict]) -> List[Dict]
    def submit_task(self, operation: str, **kwargs) -> AsyncTaskResult
    def get_task_status(self, task_id: str) -> AsyncTaskStatus
    def cancel_task(self, task_id: str) -> bool
```

### 9. Configuration Management (`deltalakedb.pydantic_models`)

#### Configuration Loading
```python
class ConfigLoader:
    """Load and validate configuration files."""

    @classmethod
    def load_config_file(cls, path: str) -> SqlConfig
    @classmethod
    def load_yaml_config(cls, path: str) -> SqlConfig
    @classmethod
    def load_toml_config(cls, path: str) -> SqlConfig
    @classmethod
    def save_yaml_config(cls, config: SqlConfig, path: str) -> None
    @classmethod
    def save_toml_config(cls, config: SqlConfig, path: str) -> None
    @classmethod
    def generate_sample_configs(cls, output_dir: str) -> None
    @classmethod
    def validate_config_file(cls, path: str) -> List[str]
    @classmethod
    def get_config_schema(cls) -> Dict[str, Any]
```

### 10. Migration (`deltalakedb.migration`)

#### DeltaTableMigrator
```python
class DeltaTableMigrator:
    """Migrate existing Delta tables to SQL backend."""

    def __init__(self, source_uri: str, target_config: SqlConfig)
    def analyze_migration(self) -> MigrationStatus
    def execute_migration(
        self,
        strategy: MigrationStrategy = MigrationStrategy.INCREMENTAL,
        batch_size: int = 1000
    ) -> TableMigrationResult
    def validate_migration(self) -> List[str]
    def rollback_migration(self) -> None
```

### 11. Logging (`deltalakedb.logging`)

#### DeltaLogger
```python
class DeltaLogger:
    """Structured logging for Delta operations."""

    def __init__(self, name: str, config: Optional[LoggerConfig] = None)
    def info(self, message: str, **kwargs) -> None
    def warning(self, message: str, **kwargs) -> None
    def error(self, message: str, error: Optional[Exception] = None, **kwargs) -> None
    def debug(self, message: str, **kwargs) -> None
    def log_operation(self, operation: str, table: str, **metadata) -> None
    def log_performance(self, operation: str, duration: float, **metrics) -> None
```

### 12. Testing (`deltalakedb.testing`)

#### TestSuiteExecutor
```python
class TestSuiteExecutor:
    """Comprehensive testing framework for Delta operations."""

    def __init__(self, config: TestSuiteConfig)
    def run_unit_tests(self, test_paths: List[str]) -> TestResult
    def run_integration_tests(self, test_paths: List[str]) -> TestResult
    def run_performance_tests(self, benchmark_paths: List[str]) -> TestResult
    def run_comprehensive_tests(self) -> TestResult
    def generate_coverage_report(self) -> Dict[str, Any]
    def run_performance_benchmarks(self) -> Dict[str, Any]
```

#### Test Functions
```python
def create_test_suite(
    test_types: List[TestType],
    test_level: TestLevel = TestLevel.ALL,
    parallel_execution: bool = True
) -> TestSuiteExecutor

def run_comprehensive_tests(
    config: Optional[TestSuiteConfig] = None
) -> TestResult

def generate_test_coverage_report(test_results: List[TestResult]) -> Dict[str, Any]

def run_performance_benchmarks(
    benchmark_suite: Optional[str] = None
) -> Dict[str, Any]
```

## Utility Functions

### Connection Functions
```python
def connect_to_table(uri: str, config: Optional[SqlConfig] = None) -> Table
def create_table(uri: str, schema: TableSchema, config: SqlConfig) -> Table
def list_tables(config: SqlConfig) -> List[str]
```

### DeltaTable Integration
```python
def load_table(uri: str) -> deltatable.DeltaTable
def is_delta_table(uri: str) -> bool
def get_latest_version(uri: str) -> int
def get_metadata(uri: str) -> Metadata
```

### CLI Integration
```python
def quick_connect(uri: str) -> Table
def list_tables_cli(config: SqlConfig) -> None
def create_table_cli(name: str, config: SqlConfig) -> None
```

### DeltaLake Integration
```python
def patch_deltalake() -> None
def auto_patch() -> None
def handle_deltasql_uri(uri: str) -> Any
def register_uri_handler() -> None
def check_uri_compatibility(uri: str) -> bool
```

## Error Handling

### DeltaLakeError
```python
class DeltaLakeError(Exception):
    """Base exception for DeltaLake operations."""

    @property
    def kind(self) -> DeltaLakeErrorKind
    @property
    def message(self) -> str
    @property
    def details(self) -> Optional[Dict[str, Any]]
```

### Error Types
```python
class DeltaLakeErrorKind(Enum):
    CONNECTION_ERROR = "connection_error"
    VALIDATION_ERROR = "validation_error"
    TRANSACTION_ERROR = "transaction_error"
    SCHEMA_ERROR = "schema_error"
    IO_ERROR = "io_error"
    CONFIGURATION_ERROR = "configuration_error"
    MIGRATION_ERROR = "migration_error"
    PERFORMANCE_ERROR = "performance_error"
```

## Examples

### Basic Usage
```python
import deltalakedb
from deltalakedb import Table, SqlConfig

# Connect to a table
config = SqlConfig(
    database_type="postgresql",
    connection_string="postgresql://user:pass@localhost/delta_db",
    table_name="my_table"
)
table = Table("delta+sql://postgresql://user:pass@localhost/delta_db/my_table")

# Get table version
snapshot = table.get_version()
print(f"Table version: {snapshot.version}")

# Get files
files = table.get_files()
print(f"Files: {len(files)}")
```

### Multi-table Transaction
```python
from deltalakedb.multi_table_transactions import create_transaction_context, MultiTableIsolationLevel

# Create transaction context
transaction = create_transaction_context(
    tables=[table1, table2],
    isolation_level=MultiTableIsolationLevel.SERIALIZABLE
)

# Execute operations
transaction.add_participant(table1, [{"operation": "write", "data": data1}])
transaction.add_participant(table2, [{"operation": "delete", "condition": "id > 100"}])

# Commit transaction
result = transaction.commit()
```

### Performance Optimization
```python
from deltalakedb.lazy_loading import LazyLoadingConfig, LoadingStrategy
from deltalakedb.caching import DeltaLakeCacheManager
from deltalakedb.async_io import AsyncIOExecutor

# Configure lazy loading
lazy_config = LazyLoadingConfig(
    strategy=LoadingStrategy.PAGINATED,
    chunk_size=1000,
    prefetch_count=5
)

# Setup caching
cache_manager = DeltaLakeCacheManager()
cache_manager.cache_table_metadata(table, metadata)

# Use async operations
async_executor = AsyncIOExecutor()
result = await async_executor.execute_query_async("SELECT * FROM table")
```

### Configuration Management
```python
from deltalakedb.pydantic_models import ConfigLoader

# Load from YAML
config = ConfigLoader.load_yaml_config("config.yaml")

# Validate configuration
errors = ConfigLoader.validate_config_file("config.yaml")
if errors:
    print(f"Configuration errors: {errors}")

# Generate sample configs
ConfigLoader.generate_sample_configs("./configs")
```

This comprehensive API reference provides complete coverage of all DeltaLake DB Python functionality, enabling developers to leverage the full power of SQL-Backed Delta Lake metadata operations in their applications.