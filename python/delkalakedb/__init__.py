"""delkalakedb: SQL-backed metadata plane for Delta Lake."""

from typing import Optional, List, Dict, Any, Union
from datetime import datetime
import uuid

__version__ = "0.0.0"


def is_rust_extension_available() -> bool:
    """Check if the Rust extension is available."""
    try:
        import delkalakedb._delkalakedb  # type: ignore

        return True
    except ImportError:
        return False


def hello() -> str:
    """Placeholder function to ensure the package imports."""
    return "Hello from delkalakedb"


def _require_rust_extension():
    """Helper function to check if Rust extension is available."""
    if not is_rust_extension_available():
        raise NotImplementedError(
            "Rust extension not built. Install with: pip install -e ."
        )


# Lazy import functions to avoid import errors
def get_multi_table_config():
    """Get MultiTableConfig class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyMultiTableConfig


def get_database_config():
    """Get DatabaseConfig class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyDatabaseConfig


def get_multi_table_writer():
    """Get MultiTableWriter class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyMultiTableWriter


def get_multi_table_transaction():
    """Get MultiTableTransaction class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyMultiTableTransaction


def get_table_actions():
    """Get TableActions class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyTableActions


def get_add_file():
    """Get AddFile class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyAddFile


def get_remove_file():
    """Get RemoveFile class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyRemoveFile


def get_metadata():
    """Get Metadata class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyMetadata


def get_format():
    """Get Format class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyFormat


def get_protocol():
    """Get Protocol class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyProtocol


def get_multi_table_commit_result():
    """Get MultiTableCommitResult class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyMultiTableCommitResult


def get_transaction_summary():
    """Get TransactionSummary class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyTransactionSummary


def get_table_commit_result():
    """Get TableCommitResult class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyTableCommitResult


def get_mirroring_result():
    """Get MirroringResult class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyMirroringResult


def get_consistency_violation():
    """Get ConsistencyViolation class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyConsistencyViolation


def get_database_schema():
    """Get DatabaseSchema class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyDatabaseSchema


def get_table_schema():
    """Get TableSchema class."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.PyTableSchema


def get_create_test_schema():
    """Get create_test_schema function."""
    _require_rust_extension()
    import delkalakedb._delkalakedb as _delkalakedb  # type: ignore

    return _delkalakedb.create_test_schema


class MultiTableClient:
    """High-level client for multi-table operations."""

    def __init__(self, database_config, config=None):
        """Initialize multi-table client.

        Args:
            database_config: Database connection configuration
            config: Multi-table configuration (optional)
        """
        _require_rust_extension()

        MultiTableWriter = get_multi_table_writer()
        self.writer = MultiTableWriter(database_config, config)

    def create_transaction(self):
        """Create a new multi-table transaction."""
        return self.writer.begin_transaction()

    async def commit_transaction(self, transaction):
        """Commit a multi-table transaction atomically."""
        return await self.writer.commit_transaction(transaction)

    async def begin_and_stage(self, table_actions):
        """Begin a transaction and stage actions for multiple tables."""
        return await self.writer.begin_and_stage(table_actions)

    async def validate_consistency(self, transaction):
        """Validate cross-table consistency for a transaction."""
        return await self.writer.validate_cross_table_consistency(transaction)


class TableActionsBuilder:
    """Builder for creating table actions."""

    def __init__(self, table_id: str, expected_version: int = -1):
        """Initialize builder.

        Args:
            table_id: ID of table
            expected_version: Expected version of table
        """
        _require_rust_extension()

        TableActions = get_table_actions()
        self.actions = TableActions(table_id, expected_version)

    def add_files(self, files):
        """Add files to table.

        Args:
            files: List of files to add (can be AddFile objects or dicts)

        Returns:
            Self for method chaining
        """
        AddFile = get_add_file()
        add_files = []
        for file in files:
            if isinstance(file, dict):
                add_files.append(AddFile(**file))
            else:
                add_files.append(file)

        self.actions.add_files(add_files)
        return self

    def remove_files(self, files):
        """Remove files from table.

        Args:
            files: List of files to remove (can be RemoveFile objects or dicts)

        Returns:
            Self for method chaining
        """
        RemoveFile = get_remove_file()
        remove_files = []
        for file in files:
            if isinstance(file, dict):
                remove_files.append(RemoveFile(**file))
            else:
                remove_files.append(file)

        self.actions.remove_files(remove_files)
        return self

    def update_metadata(self, metadata):
        """Update table metadata.

        Args:
            metadata: New metadata (can be Metadata object or dict)

        Returns:
            Self for method chaining
        """
        Metadata = get_metadata()
        if isinstance(metadata, dict):
            metadata = Metadata(**metadata)

        self.actions.update_metadata(metadata)
        return self

    def update_protocol(self, protocol):
        """Update table protocol.

        Args:
            protocol: New protocol (can be Protocol object or dict)

        Returns:
            Self for method chaining
        """
        Protocol = get_protocol()
        if isinstance(protocol, dict):
            protocol = Protocol(**protocol)

        self.actions.update_protocol(protocol)
        return self

    def with_operation(self, operation: str):
        """Set operation type.

        Args:
            operation: Operation type (e.g., "WRITE", "UPDATE", "DELETE")

        Returns:
            Self for method chaining
        """
        self.actions.with_operation(operation)
        return self

    def with_operation_params(self, params):
        """Set operation parameters.

        Args:
            params: Operation parameters

        Returns:
            Self for method chaining
        """
        self.actions.with_operation_params(params)
        return self

    def build(self):
        """Build table actions.

        Returns:
            The constructed TableActions object
        """
        return self.actions


# Convenience functions
def create_sqlite_config(database_path: str):
    """Create a SQLite database configuration.

    Args:
        database_path: Path to SQLite database file

    Returns:
        Database configuration for SQLite
    """
    DatabaseConfig = get_database_config()
    return DatabaseConfig("sqlite", database_path)


def create_postgres_config(
    host: str, port: int, database: str, username: str, password: str
):
    """Create a PostgreSQL database configuration.

    Args:
        host: Database host
        port: Database port
        database: Database name
        username: Database username
        password: Database password

    Returns:
        Database configuration for PostgreSQL
    """
    DatabaseConfig = get_database_config()
    connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    return DatabaseConfig("postgres", connection_string)


def create_mysql_config(
    host: str, port: int, database: str, username: str, password: str
):
    """Create a MySQL database configuration.

    Args:
        host: Database host
        port: Database port
        database: Database name
        username: Database username
        password: Database password

    Returns:
        Database configuration for MySQL
    """
    DatabaseConfig = get_database_config()
    connection_string = f"mysql://{username}:{password}@{host}:{port}/{database}"
    return DatabaseConfig("mysql", connection_string)


# Utility functions for working with transactions
async def execute_multi_table_operation(client, operations):
    """Execute a multi-table operation with automatic transaction management.

    Args:
        client: Multi-table client
        operations: Dictionary mapping table IDs to operation specifications

    Returns:
        Result of committed transaction

    Example:
        >>> operations = {
        ...     "users": {
        ...         "add_files": [{"path": "users_1.parquet", "size": 1000}],
        ...         "expected_version": 0
        ...     },
        ...     "orders": {
        ...         "add_files": [{"path": "orders_1.parquet", "size": 2000}],
        ...         "expected_version": 0
        ...     }
        ... }
        >>> result = await execute_multi_table_operation(client, operations)
    """
    table_actions = []

    for table_id, op_spec in operations.items():
        expected_version = op_spec.get("expected_version", -1)
        builder = TableActionsBuilder(table_id, expected_version)

        if "add_files" in op_spec:
            builder.add_files(op_spec["add_files"])

        if "remove_files" in op_spec:
            builder.remove_files(op_spec["remove_files"])

        if "metadata" in op_spec:
            builder.update_metadata(op_spec["metadata"])

        if "protocol" in op_spec:
            builder.update_protocol(op_spec["protocol"])

        if "operation" in op_spec:
            builder.with_operation(op_spec["operation"])

        if "operation_params" in op_spec:
            builder.with_operation_params(op_spec["operation_params"])

        table_actions.append(builder.build())

    # Begin transaction and stage all actions
    transaction = await client.begin_and_stage(table_actions)

    # Commit transaction
    return await client.commit_transaction(transaction)


# Export all public symbols
__all__ = [
    # Functions
    "hello",
    "is_rust_extension_available",
    "create_sqlite_config",
    "create_postgres_config",
    "create_mysql_config",
    "execute_multi_table_operation",
    # Lazy accessors
    "get_multi_table_config",
    "get_database_config",
    "get_multi_table_writer",
    "get_multi_table_transaction",
    "get_table_actions",
    "get_add_file",
    "get_remove_file",
    "get_metadata",
    "get_format",
    "get_protocol",
    "get_multi_table_commit_result",
    "get_transaction_summary",
    "get_table_commit_result",
    "get_mirroring_result",
    "get_consistency_violation",
    "get_database_schema",
    "get_table_schema",
    "get_create_test_schema",
    # High-level classes
    "MultiTableClient",
    "TableActionsBuilder",
]
