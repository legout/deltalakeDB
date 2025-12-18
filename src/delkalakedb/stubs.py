"""Stub implementations of delkalakedb types during development.

These are placeholder classes that will be replaced by pyo3 bindings
in Phase 5 of the add-python-bindings-pyo3 proposal.

For type hints and IDE support, use the __init__.pyi file.
"""

from typing import Dict, List, Optional


class ActiveFile:
    """Stub for ActiveFile type."""
    
    def __init__(
        self,
        path: str,
        size: int,
        modification_time: int,
        partition_values: Optional[str] = None,
        data_change: bool = False,
    ) -> None:
        raise NotImplementedError(
            "ActiveFile is not yet implemented. "
            "See IMPLEMENTATION_PLAN.md Phase 5 for details."
        )


class RemovedFile:
    """Stub for RemovedFile type."""
    
    def __init__(
        self,
        path: str,
        deletion_time: int,
        partition_values: Optional[str] = None,
        data_change: bool = False,
    ) -> None:
        raise NotImplementedError(
            "RemovedFile is not yet implemented. "
            "See IMPLEMENTATION_PLAN.md Phase 5 for details."
        )


class Protocol:
    """Stub for Protocol type."""
    
    def __init__(
        self,
        min_reader_version: int,
        min_writer_version: int,
    ) -> None:
        raise NotImplementedError(
            "Protocol is not yet implemented. "
            "See IMPLEMENTATION_PLAN.md Phase 5 for details."
        )


class TableMetadata:
    """Stub for TableMetadata type."""
    
    def __init__(
        self,
        schema_json: str,
        partition_columns: List[str],
        configuration: str,
    ) -> None:
        raise NotImplementedError(
            "TableMetadata is not yet implemented. "
            "See IMPLEMENTATION_PLAN.md Phase 5 for details."
        )


class Snapshot:
    """Stub for Snapshot type."""
    
    def __init__(
        self,
        version: int,
        timestamp_millis: int,
        protocol: Protocol,
        metadata: TableMetadata,
        files: List[ActiveFile],
    ) -> None:
        raise NotImplementedError(
            "Snapshot is not yet implemented. "
            "See IMPLEMENTATION_PLAN.md Phase 5 for details."
        )


class DeltaSQL:
    """Stub for DeltaSQL connection class."""
    
    def __init__(self, uri: str) -> None:
        raise NotImplementedError(
            "DeltaSQL is not yet implemented. "
            "See IMPLEMENTATION_PLAN.md Phase 5 for details."
        )
    
    def open_table(self, name: str) -> "Table":
        raise NotImplementedError()
    
    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        raise NotImplementedError()
    
    def begin_transaction(self, table_name: str) -> "Transaction":
        raise NotImplementedError()
    
    def transaction_builder(self) -> "TransactionBuilder":
        raise NotImplementedError()


class Table:
    """Stub for Table read class."""
    
    def snapshot(self) -> Snapshot:
        raise NotImplementedError(
            "Table.snapshot() is not yet implemented. "
            "See IMPLEMENTATION_PLAN.md Phase 5 for details."
        )
    
    def version(self, v: int) -> "Table":
        raise NotImplementedError()
    
    def at_timestamp(self, ts: str) -> "Table":
        raise NotImplementedError()
    
    @property
    def current_version(self) -> int:
        raise NotImplementedError()
    
    @property
    def location(self) -> str:
        raise NotImplementedError()


class Transaction:
    """Stub for single-table Transaction class."""
    
    def add_file(
        self,
        path: str,
        size: int,
        modification_time: int,
    ) -> None:
        raise NotImplementedError(
            "Transaction.add_file() is not yet implemented. "
            "See IMPLEMENTATION_PLAN.md Phase 5 for details."
        )
    
    def remove_file(self, path: str) -> None:
        raise NotImplementedError()

    def set_metadata(self, metadata: TableMetadata) -> None:
        raise NotImplementedError(
            "Transaction.set_metadata() is not yet implemented. "
            "See IMPLEMENTATION_PLAN.md Phase 5 for details."
        )

    def commit(self) -> int:
        raise NotImplementedError()


class TransactionBuilder:
    """Stub for multi-table TransactionBuilder class."""
    
    def add_table_actions(
        self, table_id: str, actions: List[str]
    ) -> None:
        raise NotImplementedError(
            "TransactionBuilder.add_table_actions() is not yet implemented. "
            "See IMPLEMENTATION_PLAN.md Phase 5 for details."
        )
    
    def staged_tables(self) -> List[str]:
        raise NotImplementedError()
    
    def commit(self) -> Dict[str, int]:
        raise NotImplementedError()


class ConcurrencyError(Exception):
    """Stub for concurrency error."""
    pass


class ConnectionError(Exception):
    """Stub for connection error."""
    pass


class ValidationError(Exception):
    """Stub for validation error."""
    pass


class NotFoundError(Exception):
    """Stub for not-found error."""
    pass
