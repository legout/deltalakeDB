"""High-level Delta table API for opening and manipulating tables via URIs.

This module provides a simplified interface for opening and working with
Delta tables from SQL-backed sources using deltasql:// URIs.

Example:
    Open and read a Delta table from PostgreSQL::

        from delkalakedb.table import DeltaTable

        async with await DeltaTable.open(
            "deltasql://postgres/mydb/public/users"
        ) as table:
            version = await table.get_latest_version()
            print(f"Latest version: {version}")

            snapshot = await table.read_snapshot(None)
            print(f"Files: {len(snapshot.files)}")
"""

from typing import Optional, Any
from dataclasses import dataclass


@dataclass
class TableInfo:
    """Information about an opened Delta table.

    Attributes:
        uri: The URI used to open the table.
        name: The table name.
        is_sql_backed: Whether the table is SQL-backed or file-based.
    """

    uri: str
    name: str
    is_sql_backed: bool


class DeltaTable:
    """Represents an opened Delta table.

    Provides a unified interface for accessing Delta tables from various
    backends (SQL databases, cloud object stores, local files).

    The actual reader/writer implementation is determined by the URI scheme
    at table open time.

    Example:
        >>> uri = "deltasql://postgres/mydb/public/users"
        >>> table = await DeltaTable.open(uri)
        >>> version = await table.get_latest_version()
    """

    def __init__(self, uri: str, table_name: str, is_sql_backed: bool):
        """Initialize a DeltaTable (internal use).

        Args:
            uri: The URI used to open the table.
            table_name: The name of the table.
            is_sql_backed: Whether this is a SQL-backed table.
        """
        self.uri = uri
        self.table_name = table_name
        self.is_sql_backed = is_sql_backed
        self._redacted_uri = self._compute_redacted_uri(uri)

    @staticmethod
    def _compute_redacted_uri(uri: str) -> str:
        """Compute a redacted version of the URI for logging.

        Hides credentials in the URI to prevent exposure in logs.

        Args:
            uri: The original URI string.

        Returns:
            A redacted URI with credentials hidden.
        """
        if "deltasql://" not in uri:
            return uri

        # Replace credentials with placeholder
        if "@" in uri and "://" in uri:
            scheme_end = uri.index("://") + 3
            at_index = uri.index("@")
            return uri[:scheme_end] + "[credentials]@" + uri[at_index + 1 :]
        return uri

    @staticmethod
    async def open(uri: str) -> "DeltaTable":
        """Open a Delta table from a URI.

        Supports:
            - deltasql://postgres/database/schema/table - PostgreSQL
            - deltasql://sqlite/path?table=name - SQLite
            - deltasql://duckdb/path?table=name - DuckDB
            - s3://bucket/path - S3 or S3-compatible stores
            - file:///path/to/table - Local file system

        Args:
            uri: The URI string to open.

        Returns:
            A DeltaTable instance for the opened table.

        Raises:
            ValueError: If the URI format is invalid.
            RuntimeError: If the reader implementation is not available.

        Example:
            >>> table = await DeltaTable.open(
            ...     "deltasql://postgres/mydb/public/users"
            ... )
            >>> latest = await table.get_latest_version()
        """
        # Extract table name from URI
        if "?table=" in uri:
            table_name = uri.split("?table=")[1].split("&")[0]
        elif uri.count("/") >= 3:
            table_name = uri.rstrip("/").split("/")[-1]
        else:
            table_name = "unknown"

        is_sql_backed = uri.startswith("deltasql://")

        return DeltaTable(uri, table_name, is_sql_backed)

    async def get_latest_version(self) -> int:
        """Get the latest version of the table.

        Returns:
            The latest version number.

        Raises:
            RuntimeError: If the reader is not available or the operation fails.

        Example:
            >>> version = await table.get_latest_version()
            >>> print(f"Latest: {version}")
        """
        raise NotImplementedError(
            "Reader implementation not available. "
            "Pending: add-sql-reader-postgres, add-sql-reader-sqlite, add-sql-reader-duckdb"
        )

    async def read_snapshot(self, version: Optional[int] = None) -> Any:
        """Read a snapshot of the table at a specific version.

        Args:
            version: Optional version number. If None, returns the latest snapshot.

        Returns:
            A Snapshot object containing the table state at the version.

        Raises:
            RuntimeError: If the reader is not available or the read fails.

        Example:
            >>> snapshot = await table.read_snapshot(None)
            >>> print(f"Files: {len(snapshot.files)}")

            >>> snapshot_v5 = await table.read_snapshot(5)
        """
        raise NotImplementedError(
            "Reader implementation not available. "
            "Pending: add-sql-reader-postgres, add-sql-reader-sqlite, add-sql-reader-duckdb"
        )

    def info(self) -> TableInfo:
        """Get information about this table.

        Returns:
            A TableInfo object with table metadata.

        Example:
            >>> info = table.info()
            >>> print(f"Table: {info.name}, SQL-backed: {info.is_sql_backed}")
        """
        return TableInfo(
            uri=self.uri,
            name=self.table_name,
            is_sql_backed=self.is_sql_backed,
        )

    async def __aenter__(self) -> "DeltaTable":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        pass


# Helper function for opening tables with traditional URIs
async def open_table(uri: str) -> DeltaTable:
    """Open a Delta table from a URI.

    Convenience function equivalent to DeltaTable.open().

    Args:
        uri: The URI string to open.

    Returns:
        A DeltaTable instance.

    Example:
        >>> from delkalakedb.table import open_table
        >>> table = await open_table("deltasql://postgres/mydb/public/users")
    """
    return await DeltaTable.open(uri)
