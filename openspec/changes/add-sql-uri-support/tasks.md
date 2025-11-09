## 1. URI Schema Definition
- [ ] 1.1 Define `deltasql://postgres/<database>/<schema>/<table>` format
- [ ] 1.2 Define `deltasql://sqlite/<path>?table=<name>` format
- [ ] 1.3 Define `deltasql://duckdb/<path>?table=<name>` format
- [ ] 1.4 Document URI encoding rules for special characters

## 2. URI Parser Implementation
- [ ] 2.1 Create `DeltaSqlUri` enum with variants for each database type
- [ ] 2.2 Implement parser using `url` crate
- [ ] 2.3 Extract connection parameters (host, port, database, user, password)
- [ ] 2.4 Extract table identifier (schema, table name)
- [ ] 2.5 Handle optional query parameters (timeout, pool_size)

## 3. Connection String Building
- [ ] 3.1 Convert parsed URI to database-specific connection string
- [ ] 3.2 Support environment variable expansion for credentials
- [ ] 3.3 Build PostgreSQL connection string compatible with sqlx
- [ ] 3.4 Build SQLite file path
- [ ] 3.5 Build DuckDB database path

## 4. Integration
- [ ] 4.1 Update `DeltaTable::open()` to accept `deltasql://` URIs
- [ ] 4.2 Update `write_deltalake()` to accept `deltasql://` URIs
- [ ] 4.3 Route to appropriate reader/writer based on URI scheme
- [ ] 4.4 Maintain backward compatibility with file-based URIs

## 5. Python Bindings
- [ ] 5.1 Accept `deltasql://` strings in Python API
- [ ] 5.2 Add examples to docstrings
- [ ] 5.3 Provide helper for constructing URIs programmatically

## 6. Configuration
- [ ] 6.1 Support additional connection parameters via query string
- [ ] 6.2 Allow connection pooling configuration
- [ ] 6.3 Support SSL/TLS parameters for PostgreSQL

## 7. Testing
- [ ] 7.1 Unit tests for URI parsing edge cases
- [ ] 7.2 Test special character escaping in table names
- [ ] 7.3 Test URI validation and error messages
- [ ] 7.4 Integration tests with each database type
- [ ] 7.5 Test credential handling and environment variables

## 8. Documentation
- [ ] 8.1 Document URI format for each database backend
- [ ] 8.2 Provide examples for common configurations
- [ ] 8.3 Document security best practices for credentials
- [ ] 8.4 Add migration guide from connection strings to URIs
