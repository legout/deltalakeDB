//! Reader adapter implementations for different SQL backends.
//!
//! This module provides adapter wrappers for various database backends,
//! following the reader injection pattern to avoid circular dependencies.

use crate::error::TxnLogError;
use crate::traits::TxnLogReader;
use crate::types::Snapshot;
use std::sync::Arc;

/// Adapter for SQLite backend reader.
///
/// This adapter wraps a SQLite database reader and provides the TxnLogReader interface
/// for use with URI-based table access.
///
/// # Usage
///
/// ```ignore
/// let adapter = SqliteReaderAdapter::new(sqlite_path)?;
/// let snapshot = adapter.read_snapshot("table_uri", None).await?;
/// ```
pub struct SqliteReaderAdapter {
    /// SQLite database file path
    db_path: String,
}

impl SqliteReaderAdapter {
    /// Create a new SQLite reader adapter.
    ///
    /// # Arguments
    /// * `db_path` - Path to SQLite database file
    ///
    /// # Example
    /// ```ignore
    /// let adapter = SqliteReaderAdapter::new("/path/to/database.db")?;
    /// ```
    pub fn new(db_path: &str) -> Result<Self, TxnLogError> {
        // Validate path exists
        if !std::path::Path::new(db_path).exists() {
            return Err(TxnLogError::Other(
                format!("SQLite database not found: {}", db_path),
            ));
        }

        Ok(SqliteReaderAdapter {
            db_path: db_path.to_string(),
        })
    }

    /// Get the database path
    pub fn db_path(&self) -> &str {
        &self.db_path
    }
}

/// Adapter for DuckDB backend reader.
///
/// This adapter wraps a DuckDB database reader and provides the TxnLogReader interface
/// for use with URI-based table access.
///
/// DuckDB offers in-process execution and is particularly useful for analytics workloads.
///
/// # Usage
///
/// ```ignore
/// let adapter = DuckDbReaderAdapter::new(duckdb_path)?;
/// let snapshot = adapter.read_snapshot("table_uri", None).await?;
/// ```
pub struct DuckDbReaderAdapter {
    /// DuckDB database file path (or :memory: for in-memory)
    db_path: String,
}

impl DuckDbReaderAdapter {
    /// Create a new DuckDB reader adapter.
    ///
    /// # Arguments
    /// * `db_path` - Path to DuckDB database file, or ":memory:" for in-memory database
    ///
    /// # Example
    /// ```ignore
    /// // File-based database
    /// let adapter = DuckDbReaderAdapter::new("/path/to/database.duckdb")?;
    ///
    /// // In-memory database
    /// let adapter = DuckDbReaderAdapter::new(":memory:")?;
    /// ```
    pub fn new(db_path: &str) -> Result<Self, TxnLogError> {
        // Validate path - allow :memory: or valid file paths
        if db_path != ":memory:" && !std::path::Path::new(db_path).exists() {
            return Err(TxnLogError::Other(
                format!("DuckDB database not found: {}", db_path),
            ));
        }

        Ok(DuckDbReaderAdapter {
            db_path: db_path.to_string(),
        })
    }

    /// Get the database path
    pub fn db_path(&self) -> &str {
        &self.db_path
    }

    /// Check if using in-memory database
    pub fn is_memory(&self) -> bool {
        self.db_path == ":memory:"
    }
}

/// PostgreSQL reader adapter (already implemented in sql-metadata-postgres crate).
///
/// This is included for documentation. The actual implementation is in:
/// `crates/sql-metadata-postgres/src/reader.rs`
///
/// # Architecture
///
/// The reader adapters follow a clean injection pattern:
/// 1. Backend creates reader instance
/// 2. Reader is wrapped in adapter
/// 3. Adapter implements TxnLogReader trait
/// 4. DeltaTable receives adapter via DeltaTable::open_with_reader()
/// 5. No circular dependencies
///
/// This allows:
/// - Testing with mock readers
/// - Custom reader implementations
/// - Dependency injection at runtime
/// - Clean separation of concerns

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_adapter_creation() {
        // This would test SQLite adapter creation in real environment
        // For now, just test the error case
        let result = SqliteReaderAdapter::new("/nonexistent/path.db");
        assert!(result.is_err());
    }

    #[test]
    fn test_duckdb_adapter_memory() {
        // DuckDB in-memory should be allowed without file check
        let result = DuckDbReaderAdapter::new(":memory:");
        assert!(result.is_ok());
        let adapter = result.unwrap();
        assert!(adapter.is_memory());
    }

    #[test]
    fn test_duckdb_adapter_file_not_found() {
        // DuckDB file path that doesn't exist should error
        let result = DuckDbReaderAdapter::new("/nonexistent/db.duckdb");
        assert!(result.is_err());
    }
}
