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
# NOTE: During development (before maturin integration), these bindings are stubs
# and will be fully implemented in Phase 5-7 of the add-python-bindings-pyo3 proposal
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
except ImportError:
    # During development phase, these are stub implementations in crates/pyo3_bindings
    # Full pyo3 integration will be available after Phase 5
    import warnings
    warnings.warn(
        "delkalakedb is in development: Python bindings are stubs. "
        "Full functionality will be available after maturin integration (Phase 5+). "
        "See IMPLEMENTATION_PLAN.md for details.",
        FutureWarning,
        stacklevel=2,
    )
    # Re-export stub types for type checking purposes
    from delkalakedb.stubs import (  # type: ignore[import-not-found]
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
