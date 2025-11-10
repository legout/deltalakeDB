# Security Best Practices for SQL-Backed Delta Tables

## Overview

This guide provides security best practices for working with `deltasql://` URIs when accessing Delta Lake tables stored in SQL databases. These URIs may contain sensitive information such as database credentials and connection parameters.

## Table of Contents

- [Credential Management](#credential-management)
- [URI Handling](#uri-handling)
- [Environment Variables](#environment-variables)
- [Connection Security](#connection-security)
- [Logging and Debugging](#logging-and-debugging)
- [Deployment Recommendations](#deployment-recommendations)

## Credential Management

### 1. Never Embed Credentials in URIs

**❌ UNSAFE:**
```python
uri = "deltasql://postgres/user:password@localhost/mydb/public/users"
table = await DeltaTable.open(uri)
```

**✅ SAFE:**
```python
import os
username = os.environ.get("DB_USER")
password = os.environ.get("DB_PASSWORD")

uri = DeltaSqlUri.postgres(
    database="mydb",
    schema="public",
    table="users",
    user=username,
    password=password
)
```

### 2. Use Environment Variables

Store credentials in environment variables and reference them in URIs:

**Via Environment Expansion (Rust):**
```rust
// Rust automatically expands ${VAR} and $VAR syntax
let uri_str = "deltasql://postgres/${DB_USER}:${DB_PASSWORD}@${DB_HOST}/mydb/public/users";
let uri = DeltaSqlUri::parse(uri_str)?;
```

**Via Python:**
```python
import os
from delkalakedb.uri import DeltaSqlUri

# Construct URI with environment variables
uri = DeltaSqlUri.postgres(
    database=os.environ["DB_NAME"],
    schema=os.environ["DB_SCHEMA"],
    table=os.environ.get("DB_TABLE", "users"),
    host=os.environ.get("DB_HOST", "localhost"),
    port=int(os.environ.get("DB_PORT", "5432")),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    options={
        "sslmode": os.environ.get("DB_SSLMODE", "require")
    }
)
```

### 3. Use Secrets Management Systems

For production deployments, use dedicated secrets management:

**AWS Secrets Manager:**
```python
import boto3
import json

secrets_client = boto3.client("secretsmanager")
secret = secrets_client.get_secret_value(SecretId="deltalake/db")
creds = json.loads(secret["SecretString"])

uri = DeltaSqlUri.postgres(
    database=creds["database"],
    schema=creds["schema"],
    table="users",
    host=creds["host"],
    user=creds["username"],
    password=creds["password"]
)
```

**HashiCorp Vault:**
```python
import hvac

client = hvac.Client(url="https://vault.example.com:8200")
secret = client.secrets.kv.read_secret_version(path="deltalake/db")
creds = secret["data"]["data"]

uri = DeltaSqlUri.postgres(
    database=creds["database"],
    schema=creds["schema"],
    table="users",
    host=creds["host"],
    user=creds["username"],
    password=creds["password"]
)
```

## URI Handling

### 1. Use Redacted URIs for Logging

Always use the `redacted_uri()` method when logging URIs to prevent credential exposure:

**Rust:**
```rust
let uri = DeltaSqlUri::parse("deltasql://postgres/user:secret@host/db/public/users")?;
println!("Opening table: {}", uri.redacted());
// Output: Opening table: deltasql://postgres/[credentials]@host/db/public/users
```

**Python:**
```python
from delkalakedb.table import DeltaTable

table = await DeltaTable.open(uri_string)
# Always use redacted_uri for logging
print(f"Table info: {table._redacted_uri}")
```

### 2. Don't Store URIs in Version Control

**❌ UNSAFE - in `.env` or committed code:**
```bash
# .env (DO NOT COMMIT)
DATABASE_URI=deltasql://postgres/user:password@prod-db/mydb/public/users
```

**✅ SAFE - use .gitignore:**
```bash
# .gitignore
.env
.env.local
*.secrets
```

### 3. Validate URI Syntax Before Use

```python
from delkalakedb.uri import DeltaSqlUri

def get_table_uri(user_input: str) -> str:
    try:
        # Validate the URI format
        uri = DeltaSqlUri.parse(user_input)
        return str(uri)
    except ValueError as e:
        raise ValueError(f"Invalid URI format: {e}")
```

## Environment Variables

### 1. Standard Environment Variable Names

Use consistent naming conventions across your organization:

```bash
# Database connection
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_USER="deltalake_user"
export DB_PASSWORD="secure_password_here"
export DB_NAME="delta_metadata"
export DB_SCHEMA="public"
export DB_TABLE="my_table"

# SSL/TLS
export DB_SSLMODE="require"
export DB_SSL_CERT="/path/to/cert.pem"
export DB_SSL_KEY="/path/to/key.pem"
```

### 2. Protect Environment Variables

**In Docker:**
```dockerfile
# Use secrets, not environment variables
# See: https://docs.docker.com/engine/swarm/secrets/
```

**In Kubernetes:**
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: deltalake-db
type: Opaque
stringData:
  username: deltalake_user
  password: secure_password_here
  host: db.example.com
---
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
      - name: app
        env:
        - name: DB_USER
          valueFrom:
            secretKeyRef:
              name: deltalake-db
              key: username
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: deltalake-db
              key: password
```

## Connection Security

### 1. Enable TLS/SSL

Always use encrypted connections in production:

**PostgreSQL:**
```python
uri = DeltaSqlUri.postgres(
    database="mydb",
    schema="public",
    table="users",
    host="prod-db.example.com",
    options={
        "sslmode": "require",      # Require SSL
        "sslcert": "/path/to/cert.pem",
        "sslkey": "/path/to/key.pem",
        "sslrootcert": "/path/to/ca.pem"
    }
)
```

### 2. Use Connection Pooling

Configure connection pooling with appropriate limits:

```python
uri = DeltaSqlUri.postgres(
    database="mydb",
    schema="public",
    table="users",
    options={
        "pool_size": "10",
        "max_overflow": "20",
        "pool_timeout": "30",
        "pool_recycle": "3600"  # Recycle connections every hour
    }
)
```

### 3. Set Connection Timeouts

Prevent hanging connections and resource exhaustion:

```python
uri = DeltaSqlUri.postgres(
    database="mydb",
    schema="public",
    table="users",
    options={
        "connect_timeout": "10",    # 10 seconds to connect
        "statement_timeout": "300"  # 5 minutes per query
    }
)
```

### 4. Restrict Database User Permissions

Create dedicated database users with minimal privileges:

```sql
-- PostgreSQL example
CREATE USER deltalake_reader WITH PASSWORD 'secure_password';

-- Grant only necessary permissions
GRANT CONNECT ON DATABASE delta_metadata TO deltalake_reader;
GRANT USAGE ON SCHEMA public TO deltalake_reader;
GRANT SELECT ON TABLE delta_metadata.tables TO deltalake_reader;
GRANT SELECT ON TABLE delta_metadata.add_files TO deltalake_reader;

-- Revoke dangerous permissions
REVOKE CREATE ON SCHEMA public FROM deltalake_reader;
REVOKE DELETE, UPDATE, INSERT ON ALL TABLES IN SCHEMA public FROM deltalake_reader;
```

## Logging and Debugging

### 1. Never Log Full URIs

**❌ UNSAFE:**
```python
logger.debug(f"Opening table: {uri}")  # Exposes credentials
```

**✅ SAFE:**
```python
logger.debug(f"Opening table: {table.redacted_uri}")  # Credentials hidden
```

### 2. Audit Sensitive Operations

Log table access for compliance and security:

```python
import logging

audit_logger = logging.getLogger("audit")

async def open_table_with_audit(uri: str, user: str):
    audit_logger.info(
        "Table opened",
        extra={
            "uri": redact_uri(uri),
            "user": user,
            "timestamp": datetime.utcnow().isoformat()
        }
    )
    return await DeltaTable.open(uri)
```

### 3. Sanitize Error Messages

Error messages should not expose credentials:

```python
try:
    table = await DeltaTable.open(uri)
except ConnectionError as e:
    # Log without the original URI
    logger.error(f"Failed to connect to table: connection refused")
    raise RuntimeError("Database connection failed") from None
```

## Deployment Recommendations

### 1. Development Environment

```bash
# .env (local development only)
DB_HOST=localhost
DB_PORT=5432
DB_USER=devuser
DB_PASSWORD=devpass
DB_NAME=delta_local
DB_SCHEMA=public
```

### 2. Staging Environment

- Use secrets management
- Enable all SSL verification
- Use read-only credentials
- Enable audit logging

### 3. Production Environment

- ✅ Use dedicated secrets manager (Vault, AWS Secrets Manager, etc.)
- ✅ Enable SSL/TLS with certificate verification
- ✅ Use dedicated database user with minimal permissions
- ✅ Set connection timeouts
- ✅ Enable comprehensive audit logging
- ✅ Rotate credentials regularly
- ✅ Monitor for unauthorized access attempts
- ✅ Use VPC/private networking when possible

### 4. CI/CD Pipelines

```yaml
# GitHub Actions example
env:
  DB_HOST: ${{ secrets.DB_HOST }}
  DB_PORT: ${{ secrets.DB_PORT }}
  DB_USER: ${{ secrets.DB_USER }}
  DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
  DB_NAME: ${{ secrets.DB_NAME }}
  DB_SCHEMA: ${{ secrets.DB_SCHEMA }}
```

## Compliance Considerations

### GDPR

- Ensure database credentials are properly encrypted at rest
- Audit and log all access to customer data
- Implement data retention policies

### SOC 2

- Enable TLS/SSL for all database connections
- Maintain comprehensive audit logs
- Implement role-based access control (RBAC)
- Perform regular security reviews

### HIPAA

- Use encrypted connections (SSL/TLS)
- Implement strict access controls
- Maintain detailed audit logs
- Use dedicated credentials with minimal permissions

## References

- [PostgreSQL Security](https://www.postgresql.org/docs/current/sql-syntax.html)
- [SQLite Security](https://www.sqlite.org/security.html)
- [DuckDB Security](https://duckdb.org/docs/index.html)
- [OWASP Credential Storage Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Credential_Storage_Cheat_Sheet.html)
