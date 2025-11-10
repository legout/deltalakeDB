//! URI parsing and connection configuration for SQL-backed Delta tables.
//!
//! This module provides support for parsing `deltasql://` URIs to extract
//! database connection parameters and table identifiers across different
//! database backends (PostgreSQL, SQLite, DuckDB).
//!
//! # URI Formats
//!
//! ## PostgreSQL
//! - Basic: `deltasql://postgres/database/schema/table`
//! - Full: `deltasql://postgres/user:pass@host:5432/database/schema/table`
//! - With options: `deltasql://postgres/db/public/table?sslmode=require`
//!
//! ## SQLite
//! - Relative: `deltasql://sqlite/metadata.db?table=mytable`
//! - Absolute: `deltasql://sqlite//var/data/meta.db?table=mytable`
//!
//! ## DuckDB
//! - File: `deltasql://duckdb//var/data/catalog.duckdb?table=mytable`
//! - In-memory: `deltasql://duckdb/:memory:?table=mytable`

use std::collections::HashMap;
use std::env;
use thiserror::Error;
use url::Url;

/// Errors that can occur during URI parsing and handling.
///
/// URI errors occur when parsing or validating `deltasql://` URIs that specify
/// connections to SQL-backed Delta table stores. This is a low-level error type
/// that distinguishes URI-specific issues from metadata or transaction errors.
///
/// # URI Format
///
/// Expected format: `deltasql://[database]://[host]/[db]/[schema]/[table]`
/// - `database`: postgres, sqlite, duckdb
/// - `host`: server hostname or socket path
/// - `db`: database name
/// - `schema`: schema name (or namespace)
/// - `table`: table name
///
/// # Examples
///
/// ```ignore
/// use deltalakedb::uri::DeltaSqlUri;
///
/// match DeltaSqlUri::parse("deltasql://postgres://localhost/mydb/public/users") {
///     Ok(uri) => println!("Parsed: {:?}", uri),
///     Err(e) => eprintln!("Invalid URI: {}", e),
/// }
/// ```
#[derive(Error, Debug)]
pub enum UriError {
    /// URI format is syntactically invalid
    #[error("Invalid URI: {0}")]
    InvalidUri(String),

    /// URI scheme is not supported or does not match expected format
    #[error("Unsupported URI scheme: {0}. Must use 'deltasql://'")]
    UnsupportedScheme(String),

    /// Database type specified in URI is not supported
    #[error("Unknown database type: {0}. Supported: postgres, sqlite, duckdb")]
    UnknownDatabaseType(String),

    /// Required component is missing from URI (e.g., host, database, schema)
    #[error("Missing required {component} in URI")]
    MissingComponent { component: String },

    /// Environment variable referenced in URI is not set
    #[error("Environment variable not set: {0}")]
    MissingEnvVar(String),

    /// URL parsing failed (invalid characters, malformed structure)
    #[error("URL parsing error: {0}")]
    UrlParseError(String),

    /// A URI parameter has an invalid value or format
    #[error("Invalid {param}: {reason}")]
    InvalidParameter { param: String, reason: String },
}

/// PostgreSQL connection parameters extracted from URI.
#[derive(Debug, Clone)]
pub struct PostgresUri {
    /// Username for authentication
    pub user: Option<String>,
    /// Password for authentication
    pub password: Option<String>,
    /// Hostname (defaults to localhost)
    pub host: String,
    /// Port (defaults to 5432)
    pub port: u16,
    /// Database name
    pub database: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Additional connection parameters (e.g., sslmode)
    pub params: HashMap<String, String>,
}

impl PostgresUri {
    /// Build a sqlx-compatible PostgreSQL connection string.
    pub fn connection_string(&self) -> Result<String, UriError> {
        let mut conn_str = String::from("postgres://");

        if let Some(user) = &self.user {
            conn_str.push_str(&urlencoding::encode(user));
            if let Some(pass) = &self.password {
                conn_str.push(':');
                conn_str.push_str(&urlencoding::encode(pass));
            }
            conn_str.push('@');
        }

        conn_str.push_str(&self.host);
        conn_str.push(':');
        conn_str.push_str(&self.port.to_string());
        conn_str.push('/');
        conn_str.push_str(&urlencoding::encode(&self.database));

        // Add custom parameters
        let mut first = true;
        for (key, val) in &self.params {
            if first {
                conn_str.push('?');
                first = false;
            } else {
                conn_str.push('&');
            }
            conn_str.push_str(key);
            conn_str.push('=');
            conn_str.push_str(&urlencoding::encode(val));
        }

        Ok(conn_str)
    }
}

/// SQLite connection parameters extracted from URI.
#[derive(Debug, Clone)]
pub struct SqliteUri {
    /// File path to SQLite database
    pub path: String,
    /// Table name
    pub table: String,
    /// Additional connection parameters
    pub params: HashMap<String, String>,
}

impl SqliteUri {
    /// Get the database file path, expanding environment variables.
    pub fn database_path(&self) -> Result<String, UriError> {
        expand_env_vars(&self.path)
    }
}

/// DuckDB connection parameters extracted from URI.
#[derive(Debug, Clone)]
pub struct DuckDbUri {
    /// Path to DuckDB database file or `:memory:` for in-memory
    pub path: String,
    /// Table name
    pub table: String,
    /// Additional connection parameters
    pub params: HashMap<String, String>,
}

impl DuckDbUri {
    /// Get the database path, expanding environment variables.
    pub fn database_path(&self) -> Result<String, UriError> {
        if self.path == ":memory:" {
            Ok(self.path.clone())
        } else {
            expand_env_vars(&self.path)
        }
    }
}

/// Represents a parsed `deltasql://` URI with connection and table information.
#[derive(Debug, Clone)]
pub enum DeltaSqlUri {
    /// PostgreSQL URI
    Postgres(PostgresUri),
    /// SQLite URI
    Sqlite(SqliteUri),
    /// DuckDB URI
    DuckDb(DuckDbUri),
}

impl DeltaSqlUri {
    /// Get the table name from the URI.
    pub fn table(&self) -> &str {
        match self {
            DeltaSqlUri::Postgres(pg) => &pg.table,
            DeltaSqlUri::Sqlite(sqlite) => &sqlite.table,
            DeltaSqlUri::DuckDb(duckdb) => &duckdb.table,
        }
    }

    /// Parse a `deltasql://` URI string.
    ///
    /// # Arguments
    ///
    /// * `uri_str` - The URI string to parse
    ///
    /// # Errors
    ///
    /// Returns `UriError` if:
    /// - The URI scheme is not `deltasql`
    /// - The database type is unknown
    /// - Required components are missing
    /// - Environment variables referenced in the URI are not set
    ///
    /// # Example
    ///
    /// ```ignore
    /// let uri = DeltaSqlUri::parse("deltasql://postgres/mydb/public/users")?;
    /// ```
    pub fn parse(uri_str: &str) -> Result<Self, UriError> {
        let url = Url::parse(uri_str).map_err(|e| {
            UriError::UrlParseError(format!("Failed to parse URI: {}", e))
        })?;

        if url.scheme() != "deltasql" {
            return Err(UriError::UnsupportedScheme(url.scheme().to_string()));
        }

        let db_type = url.host_str().ok_or_else(|| {
            UriError::MissingComponent {
                component: "database type (host)".to_string(),
            }
        })?;

        match db_type {
            "postgres" => parse_postgres_uri(url),
            "sqlite" => parse_sqlite_uri(url),
            "duckdb" => parse_duckdb_uri(url),
            _ => Err(UriError::UnknownDatabaseType(db_type.to_string())),
        }
    }

    /// Return a redacted version of the URI suitable for logging (hides credentials).
    pub fn redacted(&self) -> String {
        match self {
            DeltaSqlUri::Postgres(pg) => {
                let has_creds = pg.user.is_some();
                let creds_part = if has_creds {
                    "[credentials]@".to_string()
                } else {
                    String::new()
                };
                format!(
                    "deltasql://postgres/{}{}:{}/{}/{}/{}",
                    creds_part, pg.host, pg.port, pg.database, pg.schema, pg.table
                )
            }
            DeltaSqlUri::Sqlite(sqlite) => {
                format!("deltasql://sqlite/{}?table={}", sqlite.path, sqlite.table)
            }
            DeltaSqlUri::DuckDb(duckdb) => {
                format!("deltasql://duckdb/{}?table={}", duckdb.path, duckdb.table)
            }
        }
    }
}

/// Parse PostgreSQL URI from parsed URL.
fn parse_postgres_uri(url: Url) -> Result<DeltaSqlUri, UriError> {
    let path_str = url.path();
    let path_parts: Vec<&str> = url.path_segments().ok_or_else(|| {
        UriError::InvalidUri("PostgreSQL URI requires database/schema/table path".to_string())
    })?.collect();

    // Handle two formats:
    // 1. Basic: deltasql://postgres/database/schema/table (host="postgres")
    // 2. Full: deltasql://postgres/user:pass@host:port/database/schema/table (credentials in path)
    
    let mut user = None;
    let mut password = None;
    let mut host = "localhost".to_string();
    let mut port = 5432u16;
    let mut db_parts = path_parts.as_slice();
    
    // Check if first path segment contains credentials and host
    if !path_parts.is_empty() && path_parts[0].contains('@') {
        let cred_host = path_parts[0];
        if let Some((creds, host_part)) = cred_host.split_once('@') {
            // Parse credentials
            if let Some((user_part, pass)) = creds.split_once(':') {
                user = Some(decode_component(user_part)?);
                password = Some(decode_component(pass)?);
            } else {
                user = Some(decode_component(creds)?);
            }
            
            // Parse host and port
            if let Some((h, p)) = host_part.split_once(':') {
                host = h.to_string();
                port = p.parse().map_err(|_| UriError::InvalidParameter {
                    param: "port".to_string(),
                    reason: format!("port {} is not a valid number", p),
                })?;
            } else {
                host = host_part.to_string();
            }
        }
        db_parts = &path_parts[1..];
    }
    
    // Default host if url host is "postgres" (database type indicator)
    if host == "localhost" && url.host_str() == Some("postgres") {
        host = "localhost".to_string();
    }

    if db_parts.len() < 3 {
        return Err(UriError::InvalidUri(
            "PostgreSQL URI requires at least 3 path components: /database/schema/table"
                .to_string(),
        ));
    }

    let database = expand_env_vars(db_parts[0])?;
    let schema = expand_env_vars(db_parts[1])?;
    let table = expand_env_vars(db_parts[2])?;

    let mut params = HashMap::new();
    if let Some(query) = url.query() {
        for pair in query.split('&') {
            if let Some((key, val)) = pair.split_once('=') {
                params.insert(
                    key.to_string(),
                    decode_component(val)
                        .unwrap_or_else(|_| val.to_string()),
                );
            }
        }
    }

    Ok(DeltaSqlUri::Postgres(PostgresUri {
        user,
        password,
        host,
        port,
        database,
        schema,
        table,
        params,
    }))
}

/// Parse SQLite URI from parsed URL.
fn parse_sqlite_uri(url: Url) -> Result<DeltaSqlUri, UriError> {
    // SQLite path is everything after deltasql://sqlite/
    let path = url.path();
    if path.is_empty() || path == "/" {
        return Err(UriError::InvalidUri(
            "SQLite URI requires a database file path".to_string(),
        ));
    }

    let db_path = expand_env_vars(path)?;

    // Extract table name from query parameters
    let table = url
        .query_pairs()
        .find(|(k, _)| k == "table")
        .map(|(_, v)| v.to_string())
        .ok_or_else(|| UriError::MissingComponent {
            component: "table (query parameter)".to_string(),
        })?;

    let table = expand_env_vars(&decode_component(&table)?)?;

    let mut params = HashMap::new();
    for (key, val) in url.query_pairs() {
        if key != "table" {
            params.insert(key.to_string(), val.to_string());
        }
    }

    Ok(DeltaSqlUri::Sqlite(SqliteUri {
        path: db_path,
        table,
        params,
    }))
}

/// Parse DuckDB URI from parsed URL.
fn parse_duckdb_uri(url: Url) -> Result<DeltaSqlUri, UriError> {
    // DuckDB path is everything after deltasql://duckdb/
    let path = url.path();
    if path.is_empty() || path == "/" {
        return Err(UriError::InvalidUri(
            "DuckDB URI requires a database file path or :memory:".to_string(),
        ));
    }

    let db_path = expand_env_vars(path)?;

    // Extract table name from query parameters
    let table = url
        .query_pairs()
        .find(|(k, _)| k == "table")
        .map(|(_, v)| v.to_string())
        .ok_or_else(|| UriError::MissingComponent {
            component: "table (query parameter)".to_string(),
        })?;

    let table = expand_env_vars(&decode_component(&table)?)?;

    let mut params = HashMap::new();
    for (key, val) in url.query_pairs() {
        if key != "table" {
            params.insert(key.to_string(), val.to_string());
        }
    }

    Ok(DeltaSqlUri::DuckDb(DuckDbUri {
        path: db_path,
        table,
        params,
    }))
}

/// Decode URL-encoded component.
fn decode_component(component: &str) -> Result<String, UriError> {
    urlencoding::decode(component).map(|s| s.to_string()).map_err(|e| {
        UriError::InvalidUri(format!("Failed to decode component: {}", e))
    })
}

/// Expand environment variables in a string.
/// Supports `${VAR_NAME}` and `$VAR_NAME` syntax.
fn expand_env_vars(s: &str) -> Result<String, UriError> {
    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' {
            if chars.peek() == Some(&'{') {
                chars.next(); // consume '{'
                let mut var_name = String::new();
                while let Some(c) = chars.next() {
                    if c == '}' {
                        break;
                    }
                    var_name.push(c);
                }
                let value = env::var(&var_name).map_err(|_| {
                    UriError::MissingEnvVar(var_name.clone())
                })?;
                result.push_str(&value);
            } else {
                // Simple $VAR_NAME syntax
                let mut var_name = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' {
                        var_name.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                if var_name.is_empty() {
                    result.push('$');
                } else {
                    let value = env::var(&var_name).map_err(|_| {
                        UriError::MissingEnvVar(var_name.clone())
                    })?;
                    result.push_str(&value);
                }
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_basic_uri() {
        let uri_str = "deltasql://postgres/mydb/public/users";
        let uri = DeltaSqlUri::parse(uri_str).unwrap();
        match uri {
            DeltaSqlUri::Postgres(pg) => {
                assert_eq!(pg.database, "mydb");
                assert_eq!(pg.schema, "public");
                assert_eq!(pg.table, "users");
                assert_eq!(pg.host, "localhost");
                assert_eq!(pg.port, 5432);
                assert_eq!(pg.user, None);
            }
            _ => panic!("Expected Postgres URI"),
        }
    }

    #[test]
    fn test_postgres_full_uri() {
        let uri_str = "deltasql://postgres/user:pass@host:5555/mydb/myschema/mytable";
        let uri = DeltaSqlUri::parse(uri_str).unwrap();
        match uri {
            DeltaSqlUri::Postgres(pg) => {
                assert_eq!(pg.user, Some("user".to_string()));
                assert_eq!(pg.password, Some("pass".to_string()));
                assert_eq!(pg.host, "host");
                assert_eq!(pg.port, 5555);
                assert_eq!(pg.database, "mydb");
                assert_eq!(pg.schema, "myschema");
                assert_eq!(pg.table, "mytable");
            }
            _ => panic!("Expected Postgres URI"),
        }
    }

    #[test]
    fn test_postgres_encoded_password() {
        let uri_str = "deltasql://postgres/user:p%40ssword@localhost/db/public/table";
        let uri = DeltaSqlUri::parse(uri_str).unwrap();
        match uri {
            DeltaSqlUri::Postgres(pg) => {
                assert_eq!(pg.password, Some("p@ssword".to_string()));
            }
            _ => panic!("Expected Postgres URI"),
        }
    }

    #[test]
    fn test_postgres_with_params() {
        let uri_str = "deltasql://postgres/mydb/public/users?sslmode=require&connect_timeout=30";
        let uri = DeltaSqlUri::parse(uri_str).unwrap();
        match uri {
            DeltaSqlUri::Postgres(pg) => {
                assert_eq!(pg.params.get("sslmode"), Some(&"require".to_string()));
                assert_eq!(
                    pg.params.get("connect_timeout"),
                    Some(&"30".to_string())
                );
            }
            _ => panic!("Expected Postgres URI"),
        }
    }

    #[test]
    fn test_sqlite_relative_path() {
        let uri_str = "deltasql://sqlite/metadata.db?table=mytable";
        let uri = DeltaSqlUri::parse(uri_str).unwrap();
        match uri {
            DeltaSqlUri::Sqlite(sqlite) => {
                assert_eq!(sqlite.path, "/metadata.db");
                assert_eq!(sqlite.table, "mytable");
            }
            _ => panic!("Expected SQLite URI"),
        }
    }

    #[test]
    fn test_sqlite_absolute_path() {
        let uri_str = "deltasql://sqlite//var/data/meta.db?table=mytable";
        let uri = DeltaSqlUri::parse(uri_str).unwrap();
        match uri {
            DeltaSqlUri::Sqlite(sqlite) => {
                assert_eq!(sqlite.path, "/var/data/meta.db");
                assert_eq!(sqlite.table, "mytable");
            }
            _ => panic!("Expected SQLite URI"),
        }
    }

    #[test]
    fn test_duckdb_file() {
        let uri_str = "deltasql://duckdb//var/data/catalog.duckdb?table=mytable";
        let uri = DeltaSqlUri::parse(uri_str).unwrap();
        match uri {
            DeltaSqlUri::DuckDb(duckdb) => {
                assert_eq!(duckdb.path, "/var/data/catalog.duckdb");
                assert_eq!(duckdb.table, "mytable");
            }
            _ => panic!("Expected DuckDB URI"),
        }
    }

    #[test]
    fn test_duckdb_memory() {
        let uri_str = "deltasql://duckdb/:memory:?table=mytable";
        let uri = DeltaSqlUri::parse(uri_str).unwrap();
        match uri {
            DeltaSqlUri::DuckDb(duckdb) => {
                assert_eq!(duckdb.path, "/:memory:");
                assert_eq!(duckdb.table, "mytable");
            }
            _ => panic!("Expected DuckDB URI"),
        }
    }

    #[test]
    fn test_unsupported_scheme() {
        let result = DeltaSqlUri::parse("s3://bucket/path");
        match result {
            Err(UriError::UnsupportedScheme(scheme)) => {
                assert_eq!(scheme, "s3");
            }
            _ => panic!("Expected UnsupportedScheme error"),
        }
    }

    #[test]
    fn test_missing_table_sqlite() {
        let result = DeltaSqlUri::parse("deltasql://sqlite/meta.db");
        match result {
            Err(UriError::MissingComponent { .. }) => {}
            _ => panic!("Expected MissingComponent error"),
        }
    }

    #[test]
    fn test_postgres_redacted() {
        let uri_str = "deltasql://postgres/user:secret@host:5432/mydb/public/users";
        let uri = DeltaSqlUri::parse(uri_str).unwrap();
        let redacted = uri.redacted();
        assert!(!redacted.contains("secret"));
        assert!(redacted.contains("[credentials]"));
    }

    #[test]
    fn test_special_chars_in_table() {
        let uri_str = "deltasql://postgres/mydb/public/my%20table";
        let uri = DeltaSqlUri::parse(uri_str).unwrap();
        assert_eq!(uri.table(), "my table");
    }

    #[test]
    fn test_invalid_uri() {
        let result = DeltaSqlUri::parse("not a valid uri");
        assert!(result.is_err());
    }
}

// PostgreSQL Integration Tests
#[cfg(test)]
mod postgres_integration_tests {
    use super::*;

    #[test]
    #[ignore] // Requires Docker: run with `cargo test -- --ignored --nocapture`
    fn test_postgres_uri_to_connection_string() {
        let uri_str = "deltasql://postgres/postgres:postgres@localhost:5432/deltalakedb/public/test_table";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        let conn_str = uri.connection_string().expect("Failed to build connection string");

        // Verify connection string format
        assert!(conn_str.contains("postgres://"));
        assert!(conn_str.contains("localhost"));
        assert!(conn_str.contains("5432"));
        assert!(conn_str.contains("deltalakedb"));
    }

    #[test]
    #[ignore]
    fn test_postgres_table_identification() {
        let uri_str = "deltasql://postgres/localhost:5432/deltalakedb/public/users";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        match uri {
            DeltaSqlUri::Postgres(pg) => {
                assert_eq!(pg.schema, "public");
                assert_eq!(pg.table, "users");
                assert_eq!(pg.host, "localhost");
                assert_eq!(pg.port, 5432);
                assert_eq!(pg.database, "deltalakedb");
            }
            _ => panic!("Expected PostgreSQL URI"),
        }
    }

    #[test]
    #[ignore]
    fn test_postgres_with_ssl_parameters() {
        let uri_str = "deltasql://postgres/localhost/deltalakedb/public/secure_table?sslmode=require";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        let conn_str = uri.connection_string().expect("Failed to build connection string");
        assert!(conn_str.contains("sslmode=require"));
    }

    #[test]
    #[ignore]
    fn test_postgres_with_pool_parameters() {
        let uri_str = "deltasql://postgres/localhost/deltalakedb/public/table?pool_size=10&connection_timeout=30";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        let conn_str = uri.connection_string().expect("Failed to build connection string");
        assert!(conn_str.contains("postgres://"));
    }
}

// SQLite Integration Tests
#[cfg(test)]
mod sqlite_integration_tests {
    use super::*;

    #[test]
    #[ignore] // Requires SQLite to be available
    fn test_sqlite_memory_uri() {
        let uri_str = "deltasql://sqlite/:memory:?table=test_table";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        match uri {
            DeltaSqlUri::Sqlite(sqlite) => {
                assert_eq!(sqlite.path, ":memory:");
                assert_eq!(sqlite.table, "test_table");
            }
            _ => panic!("Expected SQLite URI"),
        }

        let conn_str = uri.connection_string().expect("Failed to build connection string");
        assert!(conn_str.contains("sqlite://"));
    }

    #[test]
    #[ignore]
    fn test_sqlite_file_uri() {
        let uri_str = "deltasql://sqlite//tmp/test.db?table=users";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        match uri {
            DeltaSqlUri::Sqlite(sqlite) => {
                assert_eq!(sqlite.path, "/tmp/test.db");
                assert_eq!(sqlite.table, "users");
            }
            _ => panic!("Expected SQLite URI"),
        }

        let conn_str = uri.connection_string().expect("Failed to build connection string");
        assert!(conn_str.contains("sqlite://"));
        assert!(conn_str.contains("/tmp/test.db"));
    }

    #[test]
    #[ignore]
    fn test_sqlite_relative_path_uri() {
        let uri_str = "deltasql://sqlite/./metadata.db?table=delta_log";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        match uri {
            DeltaSqlUri::Sqlite(sqlite) => {
                assert_eq!(sqlite.path, "./metadata.db");
            }
            _ => panic!("Expected SQLite URI"),
        }
    }
}

// DuckDB Integration Tests
#[cfg(test)]
mod duckdb_integration_tests {
    use super::*;

    #[test]
    #[ignore] // Requires DuckDB to be available
    fn test_duckdb_file_uri() {
        let uri_str = "deltasql://duckdb//var/lib/duckdb/data.duckdb?table=events";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        match uri {
            DeltaSqlUri::DuckDb(duckdb) => {
                assert_eq!(duckdb.path, "/var/lib/duckdb/data.duckdb");
                assert_eq!(duckdb.table, "events");
            }
            _ => panic!("Expected DuckDB URI"),
        }

        let conn_str = uri.connection_string().expect("Failed to build connection string");
        assert!(conn_str.contains("duckdb://"));
    }

    #[test]
    #[ignore]
    fn test_duckdb_memory_uri() {
        let uri_str = "deltasql://duckdb/:memory:?table=temp_table";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        match uri {
            DeltaSqlUri::DuckDb(duckdb) => {
                assert_eq!(duckdb.path, ":memory:");
                assert_eq!(duckdb.table, "temp_table");
            }
            _ => panic!("Expected DuckDB URI"),
        }

        let conn_str = uri.connection_string().expect("Failed to build connection string");
        assert!(conn_str.contains("duckdb://"));
    }

    #[test]
    #[ignore]
    fn test_duckdb_relative_path_uri() {
        let uri_str = "deltasql://duckdb/./local.duckdb?table=metrics";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        match uri {
            DeltaSqlUri::DuckDb(duckdb) => {
                assert_eq!(duckdb.path, "./local.duckdb");
                assert_eq!(duckdb.table, "metrics");
            }
            _ => panic!("Expected DuckDB URI"),
        }
    }
}

// Cross-Database Routing Tests
#[cfg(test)]
mod cross_database_routing_tests {
    use super::*;

    #[test]
    fn test_uri_routing_to_postgres() {
        let uri_str = "deltasql://postgres/host/db/schema/table";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse");

        match uri {
            DeltaSqlUri::Postgres(_) => { /* Correct */ }
            _ => panic!("Should route to PostgreSQL"),
        }
    }

    #[test]
    fn test_uri_routing_to_sqlite() {
        let uri_str = "deltasql://sqlite/path/to/db.db?table=t";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse");

        match uri {
            DeltaSqlUri::Sqlite(_) => { /* Correct */ }
            _ => panic!("Should route to SQLite"),
        }
    }

    #[test]
    fn test_uri_routing_to_duckdb() {
        let uri_str = "deltasql://duckdb/path/to/db.duckdb?table=t";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse");

        match uri {
            DeltaSqlUri::DuckDb(_) => { /* Correct */ }
            _ => panic!("Should route to DuckDB"),
        }
    }

    #[test]
    fn test_connection_string_postgres() {
        let uri_str = "deltasql://postgres/localhost/mydb/public/users";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse");

        let conn_str = uri.connection_string().expect("Failed to build connection string");
        assert!(conn_str.starts_with("postgresql://"));
    }

    #[test]
    fn test_connection_string_sqlite() {
        let uri_str = "deltasql://sqlite/:memory:?table=t";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse");

        let conn_str = uri.connection_string().expect("Failed to build connection string");
        assert!(conn_str.starts_with("sqlite://"));
    }

    #[test]
    fn test_connection_string_duckdb() {
        let uri_str = "deltasql://duckdb/:memory:?table=t";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse");

        let conn_str = uri.connection_string().expect("Failed to build connection string");
        assert!(conn_str.starts_with("duckdb://"));
    }
}

// Environment Variable Expansion Tests
#[cfg(test)]
mod environment_variable_tests {
    use super::*;

    #[test]
    fn test_environment_variable_expansion_postgres_password() {
        // Set test environment variable
        std::env::set_var("DB_PASSWORD", "test_password_123");

        let uri_str = "deltasql://postgres/user:$DB_PASSWORD@localhost/mydb/public/table";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        match uri {
            DeltaSqlUri::Postgres(pg) => {
                // Password should be expanded
                assert!(pg.password.is_some());
            }
            _ => panic!("Expected PostgreSQL URI"),
        }

        // Clean up
        std::env::remove_var("DB_PASSWORD");
    }

    #[test]
    fn test_environment_variable_expansion_postgres_host() {
        std::env::set_var("DB_HOST", "prod-db.example.com");

        let uri_str = "deltasql://postgres/$DB_HOST/mydb/public/table";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        match uri {
            DeltaSqlUri::Postgres(pg) => {
                assert_eq!(pg.host, "prod-db.example.com");
            }
            _ => panic!("Expected PostgreSQL URI"),
        }

        std::env::remove_var("DB_HOST");
    }

    #[test]
    fn test_environment_variable_expansion_sqlite_path() {
        std::env::set_var("DATA_DIR", "/data/volumes");

        let uri_str = "deltasql://sqlite/$DATA_DIR/metadata.db?table=t";
        let uri = DeltaSqlUri::parse(uri_str).expect("Failed to parse URI");

        match uri {
            DeltaSqlUri::Sqlite(sqlite) => {
                assert_eq!(sqlite.path, "/data/volumes/metadata.db");
            }
            _ => panic!("Expected SQLite URI"),
        }

        std::env::remove_var("DATA_DIR");
    }
}
