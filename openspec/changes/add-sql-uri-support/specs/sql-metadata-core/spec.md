## ADDED Requirements

### Requirement: PostgreSQL URI Format
The system SHALL support `deltasql://postgres/` URIs for PostgreSQL connections.

#### Scenario: Basic PostgreSQL URI
- **WHEN** URI is `deltasql://postgres/mydb/public/mytable`
- **THEN** system connects to database `mydb`, schema `public`, table `mytable` on localhost

#### Scenario: Full PostgreSQL URI
- **WHEN** URI is `deltasql://postgres/user:pass@host:5432/mydb/public/mytable`
- **THEN** system extracts host, port, credentials, database, schema, and table

#### Scenario: PostgreSQL with query parameters
- **WHEN** URI is `deltasql://postgres/mydb/public/mytable?sslmode=require`
- **THEN** connection uses SSL mode as specified

### Requirement: SQLite URI Format
The system SHALL support `deltasql://sqlite/` URIs for SQLite connections.

#### Scenario: Absolute path SQLite
- **WHEN** URI is `deltasql://sqlite//var/data/meta.db?table=mytable`
- **THEN** system opens SQLite database at absolute path with table name

#### Scenario: Relative path SQLite
- **WHEN** URI is `deltasql://sqlite/metadata.db?table=mytable`
- **THEN** system opens SQLite database at relative path

### Requirement: DuckDB URI Format
The system SHALL support `deltasql://duckdb/` URIs for DuckDB connections.

#### Scenario: DuckDB database file
- **WHEN** URI is `deltasql://duckdb//var/data/catalog.duckdb?table=mytable`
- **THEN** system opens DuckDB database file with table name

#### Scenario: In-memory DuckDB
- **WHEN** URI is `deltasql://duckdb/:memory:?table=mytable`
- **THEN** system creates in-memory DuckDB database

### Requirement: URI Parsing
The system SHALL parse URIs and extract connection parameters.

#### Scenario: Scheme validation
- **WHEN** parsing URI with scheme other than `deltasql`
- **THEN** system rejects with scheme error

#### Scenario: Database type extraction
- **WHEN** parsing `deltasql://postgres/...`
- **THEN** system identifies PostgreSQL as target database

#### Scenario: Malformed URI
- **WHEN** parsing invalid URI
- **THEN** system returns descriptive error indicating problem

### Requirement: Special Character Handling
The system SHALL handle URL-encoded characters in URIs.

#### Scenario: Encoded table name
- **WHEN** table name contains spaces as `my%20table`
- **THEN** system decodes to `my table`

#### Scenario: Encoded password
- **WHEN** password contains special characters
- **THEN** system correctly decodes URL-encoded password

### Requirement: Environment Variable Expansion
The system SHALL support environment variables for credentials.

#### Scenario: Environment variable in URI
- **WHEN** URI contains `deltasql://postgres/${DB_USER}:${DB_PASS}@host/db/schema/table`
- **THEN** system expands environment variables before connecting

#### Scenario: Missing environment variable
- **WHEN** referenced environment variable is not set
- **THEN** system returns clear error about missing variable

### Requirement: API Integration
The system SHALL accept URIs in Delta table open and write operations.

#### Scenario: Opening table with URI
- **WHEN** calling `DeltaTable::open("deltasql://postgres/...")`
- **THEN** system routes to SQL reader implementation

#### Scenario: Writing with URI
- **WHEN** calling `write_deltalake("deltasql://postgres/...", df)`
- **THEN** system routes to SQL writer implementation

#### Scenario: File-based URI compatibility
- **WHEN** using traditional `s3://` or `file://` URIs
- **THEN** system continues to route to file-based implementation

### Requirement: Connection Configuration
The system SHALL support connection parameters via query string.

#### Scenario: Connection pooling
- **WHEN** URI includes `?pool_size=10`
- **THEN** connection pool is configured with specified size

#### Scenario: Timeout configuration
- **WHEN** URI includes `?connect_timeout=30`
- **THEN** connection timeout is set to 30 seconds

### Requirement: Python API
The system SHALL expose URI-based table access in Python bindings.

#### Scenario: Python table open
- **WHEN** Python code calls `DeltaTable("deltasql://postgres/...")`
- **THEN** table opens using SQL backend

#### Scenario: Python write
- **WHEN** Python code calls `write_deltalake("deltasql://...", df)`
- **THEN** write uses SQL backend

### Requirement: Error Messages
The system SHALL provide clear errors for URI problems.

#### Scenario: Invalid scheme
- **WHEN** URI has unsupported scheme
- **THEN** error message lists supported schemes

#### Scenario: Missing table identifier
- **WHEN** URI lacks table name
- **THEN** error indicates required table parameter

#### Scenario: Connection failure
- **WHEN** database connection fails
- **THEN** error includes URI (with credentials redacted) and connection error details
