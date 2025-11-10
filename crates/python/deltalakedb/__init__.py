"""
DeltaLakeDB Python Bindings

Python bindings for SQL-Backed Delta Lake metadata system.
Provides seamless integration with the Python data ecosystem while maintaining
compatibility with existing deltalake workflows.
"""

__version__ = "0.1.0"
__author__ = "DeltaLakeDB Authors"
__email__ = "authors@deltalakedb.org"

# Import the Rust extension
try:
    from ._deltalakedb import (
        # Core domain models
        Table,
        Commit,
        File,
        Protocol,
        Metadata,

        # Connection and configuration
        DeltaSqlUri,
        SqlConfig,
        SqlConnection,
        ConnectionPool,
        TransactionContext,

        # Error handling
        DeltaLakeError,
        DeltaLakeErrorKind,

        # Compatibility layer
        DeltaLakeCompatibility,
        DeltaLakeBridge,

        # CLI utilities
        CliResult,
        CliUtils,

        # Type system
        DeltaDataType,
        SchemaField,
        TableSchema,
        TypeConverter,

        # Multi-table transactions
        MultiTableTransaction,
        TransactionOptions,
        TransactionResult as _InternalTransactionResult,
        Action,
        TransactionContext as MultiTableTransactionContext,

        # Functions
        connect_to_table,
        create_table,
        list_tables,
        parse_uri,
        create_uri,
        validate_uri,
        get_database_type,
        get_connection_string,
        get_table_name,
        quick_connect,
        list_tables_cli,
        create_table_cli,
        create_transaction_context,
        create_transaction_context_with_options,
        execute_in_transaction,
    )

    # Re-export the version from Rust
    from ._deltalakedb import version as _rust_version

    # Verify Python and Rust versions match
    if __version__ != _rust_version:
        import warnings
        warnings.warn(
            f"Version mismatch: Python package version {__version__} != Rust version {_rust_version}",
            RuntimeWarning
        )

    # Clean up the internal variable
    del _rust_version

except ImportError as e:
    import warnings
    warnings.warn(
        f"Failed to import Rust extension: {e}. "
        "Please ensure the package was built correctly with 'pip install .'",
        ImportWarning
    )

    # Provide fallback implementations for development
    __version__ = "0.1.0-dev"

    def _not_implemented(*args, **kwargs):
        """Placeholder function for when Rust extension is not available."""
        raise NotImplementedError(
            "This functionality requires the Rust extension to be built. "
            "Please install the package with: pip install ."
        )

    # Create placeholder classes
    class _Placeholder:
        def __init__(self, *args, **kwargs):
            _not_implemented()

    # Export placeholders that will raise NotImplementedError when used
    Table = _Placeholder
    Commit = _Placeholder
    File = _Placeholder
    Protocol = _Placeholder
    Metadata = _Placeholder
    DeltaSqlUri = _Placeholder
    SqlConfig = _Placeholder
    SqlConnection = _Placeholder
    ConnectionPool = _Placeholder
    TransactionContext = _Placeholder
    DeltaLakeError = _Placeholder
    DeltaLakeErrorKind = _Placeholder
    DeltaLakeCompatibility = _Placeholder
    DeltaLakeBridge = _Placeholder
    CliResult = _Placeholder
    CliUtils = _Placeholder
    DeltaDataType = _Placeholder
    SchemaField = _Placeholder
    TableSchema = _Placeholder
    TypeConverter = _Placeholder

    # Multi-table transaction placeholders
    MultiTableTransaction = _Placeholder
    TransactionOptions = _Placeholder
    Action = _Placeholder
    MultiTableTransactionContext = _Placeholder

    # Export placeholder functions
    connect_to_table = _not_implemented
    create_table = _not_implemented
    list_tables = _not_implemented
    parse_uri = _not_implemented
    create_uri = _not_implemented
    validate_uri = _not_implemented
    get_database_type = _not_implemented
    get_connection_string = _not_implemented
    get_table_name = _not_implemented
    quick_connect = _not_implemented
    list_tables_cli = _not_implemented
    create_table_cli = _not_implemented
    create_transaction_context = _not_implemented
    create_transaction_context_with_options = _not_implemented
    execute_in_transaction = _not_implemented

# Convenience functions for common operations
def connect(uri: str, config: SqlConfig = None) -> tuple[Table, SqlConnection]:
    """
    Connect to a Delta table using a DeltaSQL URI.

    Args:
        uri: DeltaSQL URI (e.g., 'deltasql://postgres://localhost:5432/db#table')
        config: Optional SQL configuration

    Returns:
        Tuple of (Table, SqlConnection)
    """
    return connect_to_table(uri, config)

def create(uri: str, name: str, description: str = None,
          partition_columns: list[str] = None,
          config: SqlConfig = None) -> Table:
    """
    Create a new Delta table.

    Args:
        uri: DeltaSQL URI for the database
        name: Table name
        description: Optional table description
        partition_columns: Optional list of partition column names
        config: Optional SQL configuration

    Returns:
        Created Table object
    """
    return create_table(uri, name, description, partition_columns, config)

def list(uri: str, config: SqlConfig = None) -> list[Table]:
    """
    List all Delta tables in a database.

    Args:
        uri: DeltaSQL URI for the database
        config: Optional SQL configuration

    Returns:
        List of Table objects
    """
    return list_tables(uri, config)

# Multi-table transaction convenience functions
def transaction(connection: SqlConnection, options: TransactionOptions = None) -> MultiTableTransaction:
    """
    Create a new multi-table transaction.

    Args:
        connection: SQL connection for the transaction
        options: Optional transaction options

    Returns:
        MultiTableTransaction instance
    """
    if options is None:
        return MultiTableTransaction(connection)
    else:
        return MultiTableTransaction.with_options(connection, options)

def transaction_context(connection: SqlConnection, auto_commit: bool = True) -> MultiTableTransactionContext:
    """
    Create a transaction context manager.

    Args:
        connection: SQL connection for the transaction
        auto_commit: Whether to auto-commit on context exit

    Returns:
        TransactionContext instance
    """
    return create_transaction_context_with_options(connection, auto_commit)

# Public API
__all__ = [
    # Version info
    "__version__",
    "__author__",
    "__email__",

    # Core domain models
    "Table",
    "Commit",
    "File",
    "Protocol",
    "Metadata",

    # Connection and configuration
    "DeltaSqlUri",
    "SqlConfig",
    "SqlConnection",
    "ConnectionPool",
    "TransactionContext",

    # Error handling
    "DeltaLakeError",
    "DeltaLakeErrorKind",

    # Compatibility layer
    "DeltaLakeCompatibility",
    "DeltaLakeBridge",

    # CLI utilities
    "CliResult",
    "CliUtils",

    # Type system
    "DeltaDataType",
    "SchemaField",
    "TableSchema",
    "TypeConverter",

    # Multi-table transactions
    "MultiTableTransaction",
    "TransactionOptions",
    "Action",
    "MultiTableTransactionContext",

    # Functions
    "connect_to_table",
    "create_table",
    "list_tables",
    "parse_uri",
    "create_uri",
    "validate_uri",
    "get_database_type",
    "get_connection_string",
    "get_table_name",
    "quick_connect",
    "list_tables_cli",
    "create_table_cli",
    "create_transaction_context",
    "create_transaction_context_with_options",
    "execute_in_transaction",

    # Convenience functions
    "connect",
    "create",
    "list",
    "transaction",
    "transaction_context",
]