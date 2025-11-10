//! High-level API for opening and manipulating Delta tables via URIs.
//!
//! This module provides a simplified interface for users to open Delta tables
//! from SQL-backed sources using `deltasql://` URIs or traditional file paths.
//! The implementation automatically dispatches to the appropriate reader/writer
//! based on the URI scheme.

use crate::error::TxnLogError;
use crate::traits::{TxnLogReader, TxnLogWriter};
use crate::types::Snapshot;
use crate::uri::{DeltaSqlUri, UriError};
use std::sync::Arc;

/// Error type for table operations.
///
/// High-level errors that can occur when opening, reading, or writing Delta tables.
/// This error type aggregates errors from URI parsing, transaction log operations,
/// and reader/writer availability.
///
/// # Error Handling Strategy
///
/// TableError provides composite error handling:
/// - URI errors (invalid syntax, unsupported schemes)
/// - Metadata errors (TxnLogError for transaction log operations)
/// - Reader/Writer unavailability (missing implementation for URI scheme)
///
/// # Examples
///
/// ```ignore
/// use deltalakedb::table::{DeltaTable, TableError};
///
/// match DeltaTable::open("invalid://uri").await {
///     Err(TableError::UriError(e)) => eprintln!("Invalid URI: {}", e),
///     Err(TableError::UnsupportedScheme(scheme)) => eprintln!("Scheme {} not supported", scheme),
///     Err(TableError::ReaderNotAvailable(uri)) => eprintln!("No reader for: {}", uri),
///     _ => {}
/// }
/// ```
#[derive(Debug, thiserror::Error)]
pub enum TableError {
    /// URI parsing error (syntax, validation)
    #[error("URI error: {0}")]
    UriError(#[from] UriError),

    /// Transaction log metadata operation failed
    #[error("Transaction log error: {0}")]
    TxnLogError(#[from] TxnLogError),

    /// No reader implementation available for this URI scheme
    #[error("No reader implementation available for URI: {0}")]
    ReaderNotAvailable(String),

    /// No writer implementation available for this URI scheme
    #[error("No writer implementation available for URI: {0}")]
    WriterNotAvailable(String),

    /// URI scheme is not supported by this implementation
    #[error("Unsupported URI scheme: {0}. Use 'deltasql://', 's3://', 'file://', or other supported schemes")]
    UnsupportedScheme(String),
}

/// Represents an opened Delta table accessible via URI.
///
/// This struct provides a unified interface for accessing Delta tables from
/// various backends (SQL databases, cloud object stores, local files).
/// The actual reader/writer implementation is determined by the URI scheme
/// at table open time.
///
/// # Example
///
/// ```ignore
/// // Open table from PostgreSQL via URI
/// let table = DeltaTable::open("deltasql://postgres/mydb/public/users").await?;
///
/// // Get the latest snapshot
/// let snapshot = table.read_snapshot(None).await?;
/// println!("Table has {} files", snapshot.files.len());
///
/// // For file-based tables, continue to use traditional URIs
/// let table = DeltaTable::open("s3://my-bucket/path/to/table").await?;
/// ```
pub struct DeltaTable {
    /// The URI used to open this table
    uri: String,
    /// Parsed URI for routing decisions
    parsed_uri: UriType,
    /// Optional reader implementation (filled for SQL-backed tables)
    reader: Option<Arc<dyn TxnLogReader>>,
}

/// Represents the type and routing information for a URI.
enum UriType {
    /// SQL-backed Delta table
    DeltaSql(DeltaSqlUri),
    /// File-based Delta table (s3://, file://, etc.)
    FileBased(String),
}

impl DeltaTable {
    /// Open a Delta table from a URI.
    ///
    /// # Arguments
    ///
    /// * `uri` - The URI string to open. Supports:
    ///   - `deltasql://postgres/database/schema/table` - PostgreSQL
    ///   - `deltasql://sqlite/path?table=name` - SQLite
    ///   - `deltasql://duckdb/path?table=name` - DuckDB
    ///   - `s3://bucket/path` - S3 or S3-compatible stores
    ///   - `file:///path/to/table` - Local file system
    ///
    /// # Errors
    ///
    /// Returns `TableError` if:
    /// - The URI cannot be parsed
    /// - The reader implementation is not available for the URI scheme
    /// - The underlying reader fails to open the table
    ///
    /// # Example
    ///
    /// ```ignore
    /// let table = DeltaTable::open("deltasql://postgres/mydb/public/users").await?;
    /// ```
    pub async fn open(uri: &str) -> Result<Self, TableError> {
        // Try to parse as SQL URI first
        if let Ok(delta_sql_uri) = DeltaSqlUri::parse(uri) {
            return Self::open_sql_backed(uri.to_string(), delta_sql_uri).await;
        }

        // Check if it's a file-based URI (s3://, file://, etc.)
        if uri.contains("://") {
            return Self::open_file_based(uri.to_string()).await;
        }

        // Invalid URI format
        Err(TableError::UnsupportedScheme(uri.to_string()))
    }

    /// Open a Delta table with an injected reader.
    ///
    /// This method allows application code to provide a pre-configured reader
    /// for a URI, avoiding the need for `create_reader_for_uri` to create connections.
    /// This is the recommended pattern for avoiding circular dependencies between
    /// the core crate and backend implementations.
    ///
    /// # Arguments
    ///
    /// * `uri` - The URI string
    /// * `reader` - Pre-configured reader implementation
    ///
    /// # Example
    ///
    /// ```ignore
    /// // In sql-metadata-postgres crate or application layer
    /// use sqlx::postgres::PgPool;
    /// use deltalakedb_sql_metadata_postgres::PostgresReader;
    /// use deltalakedb_core::DeltaTable;
    ///
    /// let pool = PgPool::connect("postgresql://...").await?;
    /// let reader = PostgresReader::new(pool, table_id);
    /// let table = DeltaTable::open_with_reader(
    ///     "deltasql://postgres/mydb/public/users",
    ///     Arc::new(reader),
    /// )?;
    /// ```
    pub fn open_with_reader(
        uri: &str,
        reader: Arc<dyn TxnLogReader>,
    ) -> Result<Self, TableError> {
        let delta_sql_uri = DeltaSqlUri::parse(uri)?;

        Ok(DeltaTable {
            uri: uri.to_string(),
            parsed_uri: UriType::DeltaSql(delta_sql_uri),
            reader: Some(reader),
        })
    }

    /// Open a SQL-backed Delta table.
    async fn open_sql_backed(
        uri: String,
        delta_sql_uri: DeltaSqlUri,
    ) -> Result<Self, TableError> {
        // Route to appropriate reader based on database type
        let reader = create_reader_for_uri(&delta_sql_uri).await?;

        Ok(DeltaTable {
            uri,
            parsed_uri: UriType::DeltaSql(delta_sql_uri),
            reader: Some(reader),
        })
    }

    /// Open a file-based Delta table.
    async fn open_file_based(uri: String) -> Result<Self, TableError> {
        // File-based tables are opened without a reader for now
        // (pending implementation of file-based reader)
        Ok(DeltaTable {
            uri,
            parsed_uri: UriType::FileBased(uri.clone()),
            reader: None,
        })
    }

    /// Get the URI used to open this table.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Read a snapshot of the table at a specific version.
    ///
    /// # Arguments
    ///
    /// * `version` - Optional version number. If `None`, returns the latest snapshot.
    ///
    /// # Errors
    ///
    /// Returns `TableError` if the reader is not available or the read fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snapshot = table.read_snapshot(None).await?;
    /// let snapshot_v5 = table.read_snapshot(Some(5)).await?;
    /// ```
    pub async fn read_snapshot(&self, version: Option<i64>) -> Result<Snapshot, TableError> {
        let reader = self
            .reader
            .as_ref()
            .ok_or_else(|| TableError::ReaderNotAvailable(self.uri.clone()))?;

        Ok(reader.read_snapshot(version).await?)
    }

    /// Get the latest version of the table.
    ///
    /// # Errors
    ///
    /// Returns `TableError` if the reader is not available or the operation fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let latest_version = table.get_latest_version().await?;
    /// println!("Latest version: {}", latest_version);
    /// ```
    pub async fn get_latest_version(&self) -> Result<i64, TableError> {
        let reader = self
            .reader
            .as_ref()
            .ok_or_else(|| TableError::ReaderNotAvailable(self.uri.clone()))?;

        Ok(reader.get_latest_version().await?)
    }

    /// Get the table name from the URI.
    pub fn table_name(&self) -> String {
        match &self.parsed_uri {
            UriType::DeltaSql(uri) => uri.table().to_string(),
            UriType::FileBased(path) => path
                .split('/')
                .last()
                .unwrap_or("table")
                .to_string(),
        }
    }

    /// Check if this is a SQL-backed table.
    pub fn is_sql_backed(&self) -> bool {
        matches!(&self.parsed_uri, UriType::DeltaSql(_))
    }

    /// Get a redacted version of the URI suitable for logging.
    /// Credentials are hidden to prevent exposure in logs.
    pub fn redacted_uri(&self) -> String {
        match &self.parsed_uri {
            UriType::DeltaSql(uri) => uri.redacted(),
            UriType::FileBased(path) => path.clone(),
        }
    }
}

/// Create appropriate reader for the given SQL URI.
/// This is a dispatch function that will route to the correct reader implementation
/// based on the database type.
/// 
/// # Note: Dependency Injection Pattern
///
/// This core crate does not directly depend on backend implementations (postgres, sqlite, duckdb)
/// to avoid circular dependencies. Backend crates should implement this routing themselves by:
/// 1. Creating their own reader adapters implementing TxnLogReader
/// 2. Providing factory methods to create and inject readers
/// 3. Or using conditional compilation features to enable specific backends
///
/// For now, this returns errors indicating that the backend application should
/// handle URI-to-reader instantiation.
async fn create_reader_for_uri(uri: &DeltaSqlUri) -> Result<Arc<dyn TxnLogReader>, TableError> {
    match uri {
        crate::uri::DeltaSqlUri::Postgres(_pg) => {
            // PostgreSQL reader should be provided by application layer
            // Example in sql-metadata-postgres crate:
            //   use sqlx::postgres::PgPoolOptions;
            //   let pool = PgPoolOptions::new().connect(connection_string).await?;
            //   let reader = PostgresReader::new(pool, table_id);
            Err(TableError::ReaderNotAvailable(
                "PostgreSQL reader must be created by application layer using \
                 deltalakedb_sql_metadata_postgres::PostgresReader. \
                 Use DeltaTable::open_with_reader() to inject reader."
                    .to_string(),
            ))
        }
        crate::uri::DeltaSqlUri::Sqlite(_sqlite) => {
            Err(TableError::ReaderNotAvailable(
                "SQLite reader not yet implemented".to_string(),
            ))
        }
        crate::uri::DeltaSqlUri::DuckDb(_duckdb) => {
            Err(TableError::ReaderNotAvailable(
                "DuckDB reader not yet implemented".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_postgres_uri() {
        let result = DeltaTable::open("deltasql://postgres/mydb/public/users").await;
        match result {
            Err(TableError::ReaderNotAvailable(msg)) => {
                assert!(msg.contains("PostgreSQL reader"));
            }
            _ => panic!("Expected ReaderNotAvailable error"),
        }
    }

    #[tokio::test]
    async fn test_parse_sqlite_uri() {
        let result = DeltaTable::open("deltasql://sqlite/metadata.db?table=users").await;
        match result {
            Err(TableError::ReaderNotAvailable(msg)) => {
                assert!(msg.contains("SQLite reader"));
            }
            _ => panic!("Expected ReaderNotAvailable error"),
        }
    }

    #[tokio::test]
    async fn test_parse_duckdb_uri() {
        let result = DeltaTable::open("deltasql://duckdb/catalog.duckdb?table=users").await;
        match result {
            Err(TableError::ReaderNotAvailable(msg)) => {
                assert!(msg.contains("DuckDB reader"));
            }
            _ => panic!("Expected ReaderNotAvailable error"),
        }
    }

    #[tokio::test]
    async fn test_unsupported_uri_scheme() {
        let result = DeltaTable::open("hdfs://cluster/path/to/table").await;
        match result {
            Err(TableError::UnsupportedScheme(_)) => {}
            _ => panic!("Expected UnsupportedScheme error"),
        }
    }

    #[test]
    fn test_redacted_uri_postgres() {
        let uri = "deltasql://postgres/user:secret@localhost/mydb/public/users";
        let table = DeltaTable {
            uri: uri.to_string(),
            parsed_uri: UriType::DeltaSql(
                DeltaSqlUri::parse(uri).expect("Valid URI"),
            ),
            reader: None,
        };
        let redacted = table.redacted_uri();
        assert!(!redacted.contains("secret"));
        assert!(redacted.contains("[credentials]"));
    }

    #[test]
    fn test_table_name_extraction() {
        let uri = "deltasql://postgres/mydb/public/my_users";
        let table = DeltaTable {
            uri: uri.to_string(),
            parsed_uri: UriType::DeltaSql(
                DeltaSqlUri::parse(uri).expect("Valid URI"),
            ),
            reader: None,
        };
        assert_eq!(table.table_name(), "my_users");
    }
}
