# Migration Guide: From Connection Strings to deltasql:// URIs

This guide helps you migrate from traditional connection strings to the new `deltasql://` URI format for SQL-backed Delta tables.

## Overview

The `deltasql://` URI scheme provides a unified, portable way to reference SQL-backed Delta tables across different database backends. It replaces the need to handle database-specific connection strings and table specifications separately.

### Benefits

- **Unified format**: Same URI structure across PostgreSQL, SQLite, and DuckDB
- **Portable**: Move tables between environments without code changes
- **Familiar**: Similar to other URI schemes (`s3://`, `file://`, etc.)
- **Flexible**: Supports both embedded credentials and environment variables
- **Secure**: Built-in credential redaction for logging

## PostgreSQL Migration

### Before: Traditional Connection Strings

```python
import psycopg2

# Separate connection components
db_host = "localhost"
db_port = 5432
db_name = "delta_metadata"
db_user = "deltalake"
db_password = "password123"
db_schema = "public"
db_table = "users"

# Construct connection string
conn_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
conn = psycopg2.connect(conn_string)

# Query table separately
table_ref = f"{db_schema}.{db_table}"
cursor = conn.cursor()
cursor.execute(f"SELECT * FROM {table_ref}")
```

### After: Using deltasql:// URIs

**Option 1: Direct URI (for development)**

```python
from delkalakedb.table import DeltaTable

# Single URI encodes all information
uri = "deltasql://postgres/deltalake:password123@localhost:5432/delta_metadata/public/users"
table = await DeltaTable.open(uri)
```

**Option 2: Programmatic Construction (recommended)**

```python
from delkalakedb.uri import DeltaSqlUri
from delkalakedb.table import DeltaTable

# Build URI with clear parameters
uri = DeltaSqlUri.postgres(
    database="delta_metadata",
    schema="public",
    table="users",
    host="localhost",
    port=5432,
    user="deltalake",
    password="password123"
)

table = await DeltaTable.open(str(uri))
```

**Option 3: Environment Variables (production)**

```python
import os
from delkalakedb.uri import DeltaSqlUri
from delkalakedb.table import DeltaTable

# Read from environment
uri = DeltaSqlUri.postgres(
    database=os.environ["DB_NAME"],
    schema=os.environ.get("DB_SCHEMA", "public"),
    table=os.environ["TABLE_NAME"],
    host=os.environ.get("DB_HOST", "localhost"),
    port=int(os.environ.get("DB_PORT", "5432")),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    options={
        "sslmode": os.environ.get("DB_SSLMODE", "require")
    }
)

table = await DeltaTable.open(str(uri))
```

## SQLite Migration

### Before: Traditional File-Based Access

```python
import sqlite3

# Manual path and table handling
db_path = "/path/to/metadata.db"
table_name = "delta_tables"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute(f"SELECT * FROM {table_name}")
```

### After: Using deltasql:// URIs

**Option 1: Relative Path**

```python
from delkalakedb.uri import DeltaSqlUri
from delkalakedb.table import DeltaTable

uri = DeltaSqlUri.sqlite(
    path="metadata.db",
    table="delta_tables"
)

table = await DeltaTable.open(str(uri))
# URI: deltasql://sqlite/metadata.db?table=delta_tables
```

**Option 2: Absolute Path**

```python
uri = DeltaSqlUri.sqlite(
    path="/var/lib/deltalake/metadata.db",
    table="delta_tables"
)

table = await DeltaTable.open(str(uri))
# URI: deltasql://sqlite//var/lib/deltalake/metadata.db?table=delta_tables
```

**Option 3: Direct URI**

```python
uri = "deltasql://sqlite/metadata.db?table=delta_tables"
table = await DeltaTable.open(uri)
```

## DuckDB Migration

### Before: Traditional Connection

```python
import duckdb

# Manual database and table setup
db_path = "catalog.duckdb"
table_name = "delta_metadata"

conn = duckdb.connect(db_path)
result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
```

### After: Using deltasql:// URIs

**Option 1: File-Based Catalog**

```python
from delkalakedb.uri import DeltaSqlUri
from delkalakedb.table import DeltaTable

uri = DeltaSqlUri.duckdb(
    path="catalog.duckdb",
    table="delta_metadata"
)

table = await DeltaTable.open(str(uri))
# URI: deltasql://duckdb/catalog.duckdb?table=delta_metadata
```

**Option 2: In-Memory Database**

```python
uri = DeltaSqlUri.duckdb(
    path=":memory:",
    table="delta_metadata"
)

table = await DeltaTable.open(str(uri))
# URI: deltasql://duckdb/:memory:?table=delta_metadata
```

## Multi-Database Application Migration

### Before: Database-Specific Code

```python
import psycopg2
import sqlite3
import duckdb

def get_table_connection(db_type, config):
    if db_type == "postgres":
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )
        return conn, f"{config['schema']}.{config['table']}"
    
    elif db_type == "sqlite":
        conn = sqlite3.connect(config["path"])
        return conn, config["table"]
    
    elif db_type == "duckdb":
        conn = duckdb.connect(config["path"])
        return conn, config["table"]
    
    else:
        raise ValueError(f"Unknown database type: {db_type}")
```

### After: Unified URI Approach

```python
from delkalakedb.uri import DeltaSqlUri, DatabaseType, create_uri

def build_table_uri(config):
    """Build appropriate URI for configured database."""
    return create_uri(
        database_type=DatabaseType(config["type"]),
        table=config["table"],
        **config.get("parameters", {})
    )

# Configuration
configs = {
    "postgres_prod": {
        "type": "postgres",
        "parameters": {
            "database": "delta_prod",
            "schema": "public",
            "host": "prod-db.example.com",
            "user": "${DB_USER}",
            "password": "${DB_PASSWORD}"
        },
        "table": "transactions"
    },
    "sqlite_local": {
        "type": "sqlite",
        "parameters": {
            "path": "./metadata.db"
        },
        "table": "transactions"
    }
}

# Use unified interface
for env, config in configs.items():
    uri = build_table_uri(config)
    print(f"{env}: {uri}")
```

## Configuration File Migration

### Before: Database-Specific Config

```yaml
# config.yaml
database:
  type: postgresql
  host: localhost
  port: 5432
  user: deltalake
  password: ${DB_PASSWORD}
  database: delta_metadata
  schema: public
  
tables:
  users:
    schema: public
    name: users
  transactions:
    schema: transactions
    name: txns
```

### After: URI-Based Config

```yaml
# config.yaml
tables:
  users:
    uri: deltasql://postgres/${DB_HOST}/delta_metadata/public/users
    options:
      sslmode: require
  
  transactions:
    uri: deltasql://postgres/${DB_HOST}/delta_metadata/transactions/txns
    options:
      sslmode: require
      pool_size: 20
```

**Python usage:**

```python
import os
import yaml
from delkalakedb.table import DeltaTable

def load_tables(config_path):
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    tables = {}
    for name, table_config in config["tables"].items():
        uri = table_config["uri"]
        # Expand environment variables
        for var in ["DB_HOST", "DB_USER", "DB_PASSWORD"]:
            if f"${{{var}}}" in uri:
                uri = uri.replace(f"${{{var}}}", os.environ[var])
        
        tables[name] = await DeltaTable.open(uri)
    
    return tables

# Load all tables
tables = await load_tables("config.yaml")
users_table = tables["users"]
```

## Testing Migration

### Before: Test-Specific Setup

```python
import pytest
import psycopg2

@pytest.fixture
def db_connection():
    """Create test database connection."""
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="test_delta",
        user="test_user",
        password="test_pass"
    )
    yield conn
    conn.close()

def test_table_query(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM public.users LIMIT 1")
    result = cursor.fetchone()
    assert result is not None
```

### After: URI-Based Testing

```python
import pytest
from delkalakedb.table import DeltaTable

@pytest.fixture
async def delta_table():
    """Open Delta table for testing."""
    uri = "deltasql://postgres/test_delta/public/users"
    table = await DeltaTable.open(uri)
    yield table

@pytest.mark.asyncio
async def test_table_query(delta_table):
    version = await delta_table.get_latest_version()
    assert version >= 0
```

## Environment Variable Migration

### Before: Individual Variables

```bash
# .env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=delta_metadata
POSTGRES_USER=deltalake
POSTGRES_PASSWORD=password123
POSTGRES_SCHEMA=public
```

### After: Unified URI

```bash
# .env - option 1: Use URI directly
DATABASE_URI=deltasql://postgres/deltalake:password123@localhost:5432/delta_metadata/public/users

# .env - option 2: Use components for flexibility
DB_HOST=localhost
DB_PORT=5432
DB_NAME=delta_metadata
DB_USER=deltalake
DB_PASSWORD=password123
DB_SCHEMA=public
TABLE_NAME=users
```

## Gradual Migration Strategy

If you have a large codebase, migrate gradually:

1. **Create adapter layer** - Wrap URI logic in helper functions
2. **Migrate new code** - Use URIs for new features
3. **Replace old connections** - Gradually update existing code
4. **Verify behavior** - Run comprehensive tests
5. **Remove legacy code** - Clean up old connection handling

### Adapter Example

```python
# adapter.py
from delkalakedb.table import DeltaTable

class LegacyTableAdapter:
    """Adapter to support both old and new code paths."""
    
    def __init__(self, uri=None, **legacy_params):
        if uri:
            self.uri = uri
        else:
            # Convert legacy parameters to URI
            self.uri = self._build_uri_from_legacy(**legacy_params)
    
    def _build_uri_from_legacy(self, db_type, **params):
        from delkalakedb.uri import create_uri, DatabaseType
        return create_uri(
            DatabaseType(db_type),
            **params
        )
    
    async def open(self):
        return await DeltaTable.open(self.uri)

# Usage: Both work
old_style = LegacyTableAdapter(
    db_type="postgres",
    database="mydb",
    schema="public",
    table="users",
    host="localhost"
)

new_style = LegacyTableAdapter(
    uri="deltasql://postgres/localhost/mydb/public/users"
)

# Both return the same DeltaTable
table1 = await old_style.open()
table2 = await new_style.open()
```

## Validation Checklist

After migration, verify:

- ✅ All connection strings converted to URIs
- ✅ Environment variables configured correctly
- ✅ Tests pass with new URI format
- ✅ Credentials properly protected
- ✅ SSL/TLS enabled in production
- ✅ Connection pooling configured
- ✅ Audit logging enabled
- ✅ Error messages don't expose credentials
- ✅ Documentation updated
- ✅ Team trained on new format

## Troubleshooting

### Issue: URI Parse Error

**Error:** `Invalid URI format: missing database type`

**Solution:** Ensure URI starts with `deltasql://` followed by database type:
```python
# Wrong
uri = "postgres://localhost/db"

# Right
uri = "deltasql://postgres/localhost/db/schema/table"
```

### Issue: Credentials Not Working

**Error:** `Authentication failed`

**Solution:** Verify credentials are properly URL-encoded:
```python
# If password contains special characters
import urllib.parse
password = urllib.parse.quote("p@ss$word", safe="")
uri = f"deltasql://postgres/user:{password}@host/db/public/table"
```

### Issue: Table Not Found

**Error:** `Table not found: users`

**Solution:** Verify table name and schema are correct:
```python
# Check schema name
uri = DeltaSqlUri.postgres(
    database="mydb",
    schema="public",  # Not "public_schema"
    table="users"
)
```

## FAQ

**Q: Can I use the old connection strings?**
A: Yes, the URI format is additive. Old connection strings continue to work alongside URIs.

**Q: How do I migrate environment variables?**
A: Update environment variable names to match URI component names, or use URI templates with environment expansion.

**Q: Is the URI format standardized?**
A: Yes, the format follows RFC 3986 (URI specification) with deltasql:// scheme.

**Q: Can I use URIs in config files?**
A: Yes, store URIs in YAML, JSON, or other config formats.

**Q: How do I secure credentials in URIs?**
A: Use environment variables and never commit URIs with embedded credentials to version control.
