//! Error types for SQL operations and database-specific errors

use deltalakedb_core::DeltaError;
use thiserror::Error;

/// Result type for SQL operations
pub type SqlResult<T> = Result<T, SqlError>;

/// Errors that can occur during SQL operations
#[derive(Error, Debug)]
pub enum SqlError {
    /// Connection error
    #[error("Database connection error: {0}")]
    ConnectionError(String),

    /// Query execution error
    #[error("Query execution error: {0}")]
    QueryError(String),

    /// Transaction error
    #[error("Transaction error: {0}")]
    TransactionError(String),

    /// Schema error
    #[error("Schema error: {0}")]
    SchemaError(String),

    /// Migration error
    #[error("Migration error: {0}")]
    MigrationError(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// Database configuration error
    #[error("Database configuration error: {0}")]
    ConfigError(String),

    /// Unsupported database type
    #[error("Unsupported database type: {0}")]
    UnsupportedDatabase(String),

    /// Timeout error
    #[error("Operation timed out after {0} seconds")]
    TimeoutError(u64),

    /// Pool error
    #[error("Connection pool error: {0}")]
    PoolError(String),

    /// Validation error
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// Table not found
    #[error("Table '{0}' not found")]
    TableNotFound(String),

    /// Commit not found
    #[error("Commit '{0}' not found")]
    CommitNotFound(String),

    /// Version conflict
    #[error("Version conflict: expected {expected}, found {found}")]
    VersionConflict {
        /// Expected version
        expected: i64,
        /// Found version
        found: i64,
    },

    /// Constraint violation
    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    /// Database-specific error
    #[error("Database-specific error ({database}): {message}")]
    DatabaseSpecific {
        /// Database type
        database: String,
        /// Error message
        message: String,
    },

    /// Core Delta error wrapped
    #[error("Delta error: {0}")]
    DeltaError(#[from] DeltaError),

    /// SQLx error wrapped
    #[error("SQLx error: {0}")]
    SqlxError(#[from] sqlx::Error),

    /// DuckDB error wrapped
    #[error("DuckDB error: {0}")]
    DuckDBError(String),

    /// UUID error wrapped
    #[error("UUID error: {0}")]
    UuidError(#[from] uuid::Error),

    /// Chrono error wrapped
    #[error("Chrono error: {0}")]
    ChronoError(#[from] chrono::ParseError),

    /// Generic error
    #[error("Error: {0}")]
    Generic(String),
}

impl SqlError {
    /// Create a connection error
    pub fn connection_error(message: impl Into<String>) -> Self {
        Self::ConnectionError(message.into())
    }

    /// Create a query error
    pub fn query_error(message: impl Into<String>) -> Self {
        Self::QueryError(message.into())
    }

    /// Create a transaction error
    pub fn transaction_error(message: impl Into<String>) -> Self {
        Self::TransactionError(message.into())
    }

    /// Create a schema error
    pub fn schema_error(message: impl Into<String>) -> Self {
        Self::SchemaError(message.into())
    }

    /// Create a migration error
    pub fn migration_error(message: impl Into<String>) -> Self {
        Self::MigrationError(message.into())
    }

    /// Create a config error
    pub fn config_error(message: impl Into<String>) -> Self {
        Self::ConfigError(message.into())
    }

    /// Create a timeout error
    pub fn timeout_error(seconds: u64) -> Self {
        Self::TimeoutError(seconds)
    }

    /// Create a pool error
    pub fn pool_error(message: impl Into<String>) -> Self {
        Self::PoolError(message.into())
    }

    /// Create a validation error
    pub fn validation_error(message: impl Into<String>) -> Self {
        Self::ValidationError(message.into())
    }

    /// Create a table not found error
    pub fn table_not_found(table_name: impl Into<String>) -> Self {
        Self::TableNotFound(table_name.into())
    }

    /// Create a commit not found error
    pub fn commit_not_found(commit_id: impl Into<String>) -> Self {
        Self::CommitNotFound(commit_id.into())
    }

    /// Create a version conflict error
    pub fn version_conflict(expected: i64, found: i64) -> Self {
        Self::VersionConflict { expected, found }
    }

    /// Create a constraint violation error
    pub fn constraint_violation(message: impl Into<String>) -> Self {
        Self::ConstraintViolation(message.into())
    }

    /// Create a database-specific error
    pub fn database_specific(database: impl Into<String>, message: impl Into<String>) -> Self {
        Self::DatabaseSpecific {
            database: database.into(),
            message: message.into(),
        }
    }

    /// Create a generic error
    pub fn generic(message: impl Into<String>) -> Self {
        Self::Generic(message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = SqlError::connection_error("Connection failed");
        assert!(matches!(err, SqlError::ConnectionError(_)));
        assert_eq!(err.to_string(), "Database connection error: Connection failed");
    }

    #[test]
    fn test_version_conflict_error() {
        let err = SqlError::version_conflict(5, 3);
        match err {
            SqlError::VersionConflict { expected, found } => {
                assert_eq!(expected, 5);
                assert_eq!(found, 3);
            }
            _ => panic!("Expected VersionConflict error"),
        }
    }

    #[test]
    fn test_table_not_found_error() {
        let err = SqlError::table_not_found("test_table");
        match err {
            SqlError::TableNotFound(table) => {
                assert_eq!(table, "test_table");
            }
            _ => panic!("Expected TableNotFound error"),
        }
    }

    #[test]
    fn test_database_specific_error() {
        let err = SqlError::database_specific("PostgreSQL", "Connection refused");
        match err {
            SqlError::DatabaseSpecific { database, message } => {
                assert_eq!(database, "PostgreSQL");
                assert_eq!(message, "Connection refused");
            }
            _ => panic!("Expected DatabaseSpecific error"),
        }
    }
}