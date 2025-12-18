"""Type stubs for delkalakedb Python bindings."""

from typing import List, Dict, Optional, Iterator

class ActiveFile:
    """Represents an active file in a Delta table."""
    path: str
    size: int
    modification_time: int
    partition_values: Optional[str]
    data_change: bool

    def __new__(
        cls,
        path: str,
        size: int,
        modification_time: int,
        partition_values: Optional[str] = None,
        data_change: bool = False,
    ) -> ActiveFile: ...

class RemovedFile:
    """Represents a removed file in a Delta table."""
    path: str
    deletion_time: int
    partition_values: Optional[str]
    data_change: bool

    def __new__(
        cls,
        path: str,
        deletion_time: int,
        partition_values: Optional[str] = None,
        data_change: bool = False,
    ) -> RemovedFile: ...

class Protocol:
    """Represents table protocol version constraints."""
    min_reader_version: int
    min_writer_version: int

    def __new__(
        cls,
        min_reader_version: int,
        min_writer_version: int,
    ) -> Protocol: ...

class TableMetadata:
    """Represents table metadata (schema, properties)."""
    schema_json: str
    partition_columns: List[str]
    configuration: str

    def __new__(
        cls,
        schema_json: str,
        partition_columns: List[str],
        configuration: str,
    ) -> TableMetadata: ...

class Snapshot:
    """Represents a table snapshot at a particular version."""
    version: int
    timestamp_millis: int
    protocol: Protocol
    metadata: TableMetadata
    files: List[ActiveFile]

    def __new__(
        cls,
        version: int,
        timestamp_millis: int,
        protocol: Protocol,
        metadata: TableMetadata,
        files: List[ActiveFile],
    ) -> Snapshot: ...

    def files(self) -> Iterator[ActiveFile]:
        """Iterate over active files in the snapshot."""
        ...

class DeltaSQL:
    """Main entry point for DeltaSQL connections and table operations."""

    def __new__(cls, uri: str) -> DeltaSQL: ...

    def open_table(self, name: str) -> Table:
        """Open a table for reading."""
        ...

    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List tables in the catalog."""
        ...

    def begin_transaction(self, table_name: str) -> Transaction:
        """Begin a single-table transaction."""
        ...

    def transaction_builder(self) -> TransactionBuilder:
        """Create a transaction builder for multi-table commits."""
        ...

class Table:
    """Represents a Delta table for read operations."""

    def snapshot(self) -> Snapshot:
        """Get current snapshot of the table."""
        ...

    def version(self, v: int) -> Table:
        """Time travel to a specific version."""
        ...

    def at_timestamp(self, ts: str) -> Table:
        """Time travel to a specific timestamp."""
        ...

    @property
    def current_version(self) -> int:
        """Get current version number."""
        ...

    @property
    def location(self) -> str:
        """Get table location."""
        ...

class Transaction:
    """Represents a single-table transaction for writing."""

    def add_file(
        self,
        path: str,
        size: int,
        modification_time: int,
    ) -> None:
        """Add a file to the transaction."""
        ...

    def remove_file(self, path: str) -> None:
        """Remove a file from the transaction."""
        ...

    def commit(self) -> int:
        """Commit the transaction and return the new version number."""
        ...

class TransactionBuilder:
    """Transaction builder for multi-table atomic commits."""

    def add_table_actions(
        self, table_id: str, actions: List[str]
    ) -> None:
        """Stage actions for a table."""
        ...

    def staged_tables(self) -> List[str]:
        """List staged tables."""
        ...

    def commit(self) -> Dict[str, int]:
        """Commit all staged tables atomically."""
        ...

class ConcurrencyError(Exception):
    """Raised when a transaction encounters a concurrency conflict (version mismatch)."""
    ...

class ConnectionError(Exception):
    """Raised when a database connection fails."""
    ...

class ValidationError(Exception):
    """Raised when input validation fails (e.g., invalid URI)."""
    ...

class NotFoundError(Exception):
    """Raised when a resource (table, version) is not found."""
    ...
