"""delkalakedb: SQL-backed metadata plane for Delta Lake.

This package provides a high-level Python API for accessing Delta tables
via a SQL metadata backend. It combines the performance and safety of Rust
with the ergonomics of Python.

Example:
    >>> from delkalakedb import DeltaSQL
    >>> conn = DeltaSQL("deltasql://postgres://user:pass@localhost/mydb")
    >>> table = conn.open_table("my_table")
    >>> snapshot = table.snapshot()
    >>> print(f"Version: {snapshot.version}, Files: {len(snapshot.files)}")
"""

__version__ = "0.0.0"

# Import pyo3 bindings
# These are built from crates/pyo3_bindings and compiled into the package
try:
    from delkalakedb import (  # type: ignore[import-not-found]
        DeltaSQL,
        Table,
        Transaction,
        TransactionBuilder,
        Snapshot,
        ActiveFile,
        RemovedFile,
        Protocol,
        TableMetadata,
        ConcurrencyError,
        ConnectionError,
        ValidationError,
        NotFoundError,
    )
except ImportError as e:
    # Provide helpful error message if pyo3 bindings aren't built
    raise ImportError(
        "delkalakedb Python bindings not found. "
        "Install with: pip install -e . (requires Rust/Cargo)"
    ) from e

__all__ = [
    "DeltaSQL",
    "Table",
    "Transaction",
    "TransactionBuilder",
    "Snapshot",
    "ActiveFile",
    "RemovedFile",
    "Protocol",
    "TableMetadata",
    "ConcurrencyError",
    "ConnectionError",
    "ValidationError",
    "NotFoundError",
]
