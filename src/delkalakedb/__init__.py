"""Delta Lake metadata plane with SQL backing and Python bindings.

This package provides SQL-backed Delta Lake metadata access with support for
PostgreSQL, SQLite, and DuckDB backends through deltasql:// URIs.

Example:
    Open a Delta table from PostgreSQL::

        from delkalakedb.table import DeltaTable
        from delkalakedb.uri import DeltaSqlUri

        # Parse URI
        uri = DeltaSqlUri.parse("deltasql://postgres/mydb/public/users")

        # Or construct programmatically
        uri = DeltaSqlUri.postgres(
            database="mydb",
            schema="public",
            table="users"
        )

        # Open table
        table = await DeltaTable.open(str(uri))
"""

__version__ = "0.0.0"

# Public API
from delkalakedb.table import DeltaTable, TableInfo, open_table
from delkalakedb.uri import DeltaSqlUri, DatabaseType, create_uri

__all__ = [
    "DeltaTable",
    "TableInfo",
    "DeltaSqlUri",
    "DatabaseType",
    "create_uri",
    "open_table",
]
