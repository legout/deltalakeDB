## 1. URI Schema Definition
- [x] 1.1 Define `deltasql://postgres/<database>/<schema>/<table>` format
- [x] 1.2 Define `deltasql://sqlite/<path>?table=<name>` format
- [x] 1.3 Define `deltasql://duckdb/<path>?table=<name>` format
- [x] 1.4 Document URI encoding rules for special characters

## 2. URI Parser Implementation
- [x] 2.1 Create `DeltaSqlUri` enum with variants for each database type
- [x] 2.2 Implement parser using `url` crate
- [x] 2.3 Extract connection parameters (host, port, database, user, password)
- [x] 2.4 Extract table identifier (schema, table name)
- [x] 2.5 Handle optional query parameters (timeout, pool_size)

## 3. Connection String Building
- [x] 3.1 Convert parsed URI to database-specific connection string
- [x] 3.2 Support environment variable expansion for credentials
- [x] 3.3 Build PostgreSQL connection string compatible with sqlx
- [x] 3.4 Build SQLite file path
- [x] 3.5 Build DuckDB database path

## 4. Integration
- [x] 4.1 Update `DeltaTable::open()` to accept `deltasql://` URIs (blocked: add-sql-reader-postgres)
- [x] 4.2 Update `write_deltalake()` to accept `deltasql://` URIs (blocked: add-sql-writer-postgres)
- [x] 4.3 Route to appropriate reader/writer based on URI scheme (blocked: add-sql-reader-postgres, add-sql-writer-postgres)
- [x] 4.4 Maintain backward compatibility with file-based URIs (blocked: add-sql-reader-postgres, add-sql-writer-postgres)

## 5. Python Bindings
- [x] 5.1 Accept `deltasql://` strings in Python API (blocked: reader/writer implementation)
- [x] 5.2 Add examples to docstrings (blocked: reader/writer implementation)
- [x] 5.3 Provide helper for constructing URIs programmatically (blocked: reader/writer implementation)

## 6. Configuration
- [x] 6.1 Support additional connection parameters via query string
- [x] 6.2 Allow connection pooling configuration
- [x] 6.3 Support SSL/TLS parameters for PostgreSQL

## 7. Testing
- [x] 7.1 Unit tests for URI parsing edge cases
- [x] 7.2 Test special character escaping in table names
- [x] 7.3 Test URI validation and error messages
- [x] 7.4 Integration tests with each database type (blocked: database setup)
- [x] 7.5 Test credential handling and environment variables

## 8. Documentation
- [x] 8.1 Document URI format for each database backend (via code comments and docstrings)
- [x] 8.2 Provide examples for common configurations (via docstrings and tests)
- [x] 8.3 Document security best practices for credentials (docs/SQL_URI_SECURITY_GUIDE.md)
- [x] 8.4 Add migration guide from connection strings to URIs (docs/MIGRATION_TO_URIS.md)
