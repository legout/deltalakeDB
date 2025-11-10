"""URI parsing and construction utilities for SQL-backed Delta tables.

This module provides utilities for working with deltasql:// URIs to reference
Delta tables stored in SQL databases.

Example:
    Open a Delta table from PostgreSQL::

        from delkalakedb.uri import DeltaSqlUri

        # Parse a URI
        uri = DeltaSqlUri.parse("deltasql://postgres/mydb/public/users")
        print(uri.table)  # "users"
        print(uri.database)  # "mydb"

        # Construct a URI programmatically
        uri = DeltaSqlUri.postgres(
            database="mydb",
            schema="public",
            table="users",
            host="localhost",
            port=5432
        )
        uri_string = str(uri)
"""

from typing import Optional, Dict, Any
from enum import Enum


class DatabaseType(str, Enum):
    """Supported SQL database backends."""

    POSTGRES = "postgres"
    """PostgreSQL database."""
    SQLITE = "sqlite"
    """SQLite database."""
    DUCKDB = "duckdb"
    """DuckDB database."""


class DeltaSqlUri:
    """Represents a parsed deltasql:// URI.

    This class handles parsing and construction of deltasql:// URIs for
    different database backends. URIs can reference both file-based and
    SQL-backed Delta tables.

    Attributes:
        database_type: The type of database backend (postgres, sqlite, duckdb).
        table: The table name to access.
    """

    def __init__(
        self,
        database_type: DatabaseType,
        table: str,
        uri_string: Optional[str] = None,
    ):
        """Initialize a DeltaSqlUri.

        Args:
            database_type: The database backend type.
            table: The table name.
            uri_string: Optional original URI string (used internally).
        """
        self.database_type = database_type
        self.table = table
        self._uri_string = uri_string

    @staticmethod
    def parse(uri: str) -> "DeltaSqlUri":
        """Parse a deltasql:// URI string.

        Supports the following URI formats:

        PostgreSQL:
            - Basic: deltasql://postgres/database/schema/table
            - With credentials: deltasql://postgres/user:pass@host:5432/database/schema/table
            - With options: deltasql://postgres/database/schema/table?sslmode=require

        SQLite:
            - deltasql://sqlite/path/to/database.db?table=name
            - deltasql://sqlite//absolute/path/to/database.db?table=name

        DuckDB:
            - deltasql://duckdb/path/to/catalog.duckdb?table=name
            - deltasql://duckdb/:memory:?table=name (in-memory database)

        Args:
            uri: The URI string to parse.

        Returns:
            A DeltaSqlUri instance representing the parsed URI.

        Raises:
            ValueError: If the URI format is invalid or unsupported.

        Example:
            >>> uri = DeltaSqlUri.parse("deltasql://postgres/mydb/public/users")
            >>> print(uri.table)
            users
        """
        if not isinstance(uri, str):
            raise ValueError("URI must be a string")

        if not uri.startswith("deltasql://"):
            raise ValueError("URI must start with 'deltasql://'")

        # Extract database type and path
        remainder = uri[len("deltasql://") :]  # Remove scheme
        if "/" not in remainder:
            raise ValueError("Invalid URI format: missing database type")

        db_type_str = remainder.split("/")[0]

        try:
            db_type = DatabaseType(db_type_str)
        except ValueError:
            raise ValueError(
                f"Unknown database type: {db_type_str}. "
                f"Supported: {', '.join(dt.value for dt in DatabaseType)}"
            )

        # Parse based on database type
        if db_type == DatabaseType.POSTGRES:
            return DeltaSqlUri._parse_postgres(uri)
        elif db_type == DatabaseType.SQLITE:
            return DeltaSqlUri._parse_sqlite(uri)
        elif db_type == DatabaseType.DUCKDB:
            return DeltaSqlUri._parse_duckdb(uri)

        raise ValueError(f"Unsupported database type: {db_type}")

    @staticmethod
    def _parse_postgres(uri: str) -> "DeltaSqlUri":
        """Parse a PostgreSQL URI."""
        # Format: deltasql://postgres/[user[:pass]@][host[:port]/]database/schema/table
        parts = uri[len("deltasql://postgres/") :].split("/")
        if len(parts) < 3:
            raise ValueError(
                "PostgreSQL URI requires: /database/schema/table"
            )

        database = parts[0]
        schema = parts[1]
        table = parts[2].split("?")[0]  # Remove query params

        return DeltaSqlUri(DatabaseType.POSTGRES, table, uri_string=uri)

    @staticmethod
    def _parse_sqlite(uri: str) -> "DeltaSqlUri":
        """Parse a SQLite URI."""
        # Format: deltasql://sqlite/path/to/db.db?table=name
        remainder = uri[len("deltasql://sqlite") :]

        if "?table=" not in remainder:
            raise ValueError(
                "SQLite URI requires table parameter: ?table=name"
            )

        table = remainder.split("?table=")[1].split("&")[0]

        return DeltaSqlUri(DatabaseType.SQLITE, table, uri_string=uri)

    @staticmethod
    def _parse_duckdb(uri: str) -> "DeltaSqlUri":
        """Parse a DuckDB URI."""
        # Format: deltasql://duckdb/path/to/catalog.duckdb?table=name
        remainder = uri[len("deltasql://duckdb") :]

        if "?table=" not in remainder:
            raise ValueError(
                "DuckDB URI requires table parameter: ?table=name"
            )

        table = remainder.split("?table=")[1].split("&")[0]

        return DeltaSqlUri(DatabaseType.DUCKDB, table, uri_string=uri)

    @staticmethod
    def postgres(
        database: str,
        schema: str,
        table: str,
        host: str = "localhost",
        port: int = 5432,
        user: Optional[str] = None,
        password: Optional[str] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> "DeltaSqlUri":
        """Construct a PostgreSQL URI.

        Args:
            database: PostgreSQL database name.
            schema: Schema name (e.g., "public").
            table: Table name.
            host: Hostname or IP address (default: localhost).
            port: Port number (default: 5432).
            user: Username for authentication (optional).
            password: Password for authentication (optional).
            options: Additional connection parameters like sslmode, connect_timeout, etc.

        Returns:
            A DeltaSqlUri configured for PostgreSQL.

        Example:
            >>> uri = DeltaSqlUri.postgres(
            ...     database="mydb",
            ...     schema="public",
            ...     table="users",
            ...     user="dbuser",
            ...     password="secret"
            ... )
            >>> print(str(uri))
            deltasql://postgres/dbuser:secret@localhost:5432/mydb/public/users
        """
        uri = "deltasql://postgres/"

        if user:
            if password:
                uri += f"{user}:{password}@"
            else:
                uri += f"{user}@"

        uri += f"{host}:{port}/{database}/{schema}/{table}"

        if options:
            params = "&".join(f"{k}={v}" for k, v in options.items())
            uri += f"?{params}"

        return DeltaSqlUri(DatabaseType.POSTGRES, table, uri_string=uri)

    @staticmethod
    def sqlite(path: str, table: str, options: Optional[Dict[str, str]] = None) -> "DeltaSqlUri":
        """Construct a SQLite URI.

        Args:
            path: Path to SQLite database file (relative or absolute).
            table: Table name.
            options: Additional connection parameters.

        Returns:
            A DeltaSqlUri configured for SQLite.

        Example:
            >>> uri = DeltaSqlUri.sqlite(
            ...     path="metadata.db",
            ...     table="users"
            ... )
            >>> print(str(uri))
            deltasql://sqlite/metadata.db?table=users
        """
        uri = f"deltasql://sqlite/{path}?table={table}"

        if options:
            params = "&".join(f"{k}={v}" for k, v in options.items())
            uri += f"&{params}"

        return DeltaSqlUri(DatabaseType.SQLITE, table, uri_string=uri)

    @staticmethod
    def duckdb(
        path: str,
        table: str,
        options: Optional[Dict[str, str]] = None,
    ) -> "DeltaSqlUri":
        """Construct a DuckDB URI.

        Args:
            path: Path to DuckDB catalog file or ":memory:" for in-memory database.
            table: Table name.
            options: Additional connection parameters.

        Returns:
            A DeltaSqlUri configured for DuckDB.

        Example:
            >>> # File-based catalog
            >>> uri = DeltaSqlUri.duckdb(
            ...     path="catalog.duckdb",
            ...     table="users"
            ... )
            >>> print(str(uri))
            deltasql://duckdb/catalog.duckdb?table=users

            >>> # In-memory database
            >>> uri = DeltaSqlUri.duckdb(
            ...     path=":memory:",
            ...     table="users"
            ... )
            >>> print(str(uri))
            deltasql://duckdb/:memory:?table=users
        """
        uri = f"deltasql://duckdb/{path}?table={table}"

        if options:
            params = "&".join(f"{k}={v}" for k, v in options.items())
            uri += f"&{params}"

        return DeltaSqlUri(DatabaseType.DUCKDB, table, uri_string=uri)

    def __str__(self) -> str:
        """Return the URI string representation.

        Returns:
            The URI as a string.
        """
        if self._uri_string:
            return self._uri_string
        raise NotImplementedError("URI string not available")

    def __repr__(self) -> str:
        """Return a detailed string representation.

        Returns:
            A developer-friendly representation.
        """
        return (
            f"DeltaSqlUri("
            f"type={self.database_type.value}, "
            f"table={self.table})"
        )


def create_uri(
    database_type: DatabaseType,
    table: str,
    **kwargs: Any,
) -> DeltaSqlUri:
    """Factory function to create URIs for different database backends.

    This is an alternative to the static methods on DeltaSqlUri for
    more dynamic URI construction.

    Args:
        database_type: The database backend type.
        table: The table name.
        **kwargs: Backend-specific parameters.

    Returns:
        A DeltaSqlUri instance.

    Raises:
        ValueError: If required parameters are missing for the database type.

    Example:
        >>> uri = create_uri(
        ...     DatabaseType.POSTGRES,
        ...     "users",
        ...     database="mydb",
        ...     schema="public",
        ...     host="localhost"
        ... )
    """
    if database_type == DatabaseType.POSTGRES:
        required = {"database", "schema"}
        provided = set(kwargs.keys())
        if not required.issubset(provided):
            raise ValueError(
                f"PostgreSQL requires: {', '.join(required - provided)}"
            )
        return DeltaSqlUri.postgres(table=table, **kwargs)
    elif database_type == DatabaseType.SQLITE:
        if "path" not in kwargs:
            raise ValueError("SQLite requires 'path' parameter")
        return DeltaSqlUri.sqlite(table=table, **kwargs)
    elif database_type == DatabaseType.DUCKDB:
        if "path" not in kwargs:
            raise ValueError("DuckDB requires 'path' parameter")
        return DeltaSqlUri.duckdb(table=table, **kwargs)
    else:
        raise ValueError(f"Unknown database type: {database_type}")
