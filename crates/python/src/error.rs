//! Error handling for Python bindings
//!
//! This module provides Python exception hierarchy and error translation
//! for the SQL-Backed Delta Lake metadata system.

use pyo3::prelude::*;
use pyo3::exceptions::PyException;
use std::fmt;

/// Delta Lake error kinds
#[derive(Debug, Clone, PartialEq)]
pub enum DeltaLakeErrorKind {
    /// URI parsing or validation error
    InvalidUri,
    /// Connection error
    ConnectionError,
    /// Table operation error
    TableError,
    /// Metadata error
    MetadataError,
    /// Query error
    QueryError,
    /// Transaction error
    TransactionError,
    /// Configuration error
    ConfigurationError,
    /// Protocol error
    ProtocolError,
    /// Schema error
    SchemaError,
    /// Validation error
    ValidationError,
    /// Timeout error
    TimeoutError,
    /// I/O error
    IoError,
    /// Database error
    DatabaseError,
    /// Unknown error
    Unknown,
}

impl fmt::Display for DeltaLakeErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeltaLakeErrorKind::InvalidUri => write!(f, "Invalid URI"),
            DeltaLakeErrorKind::ConnectionError => write!(f, "Connection Error"),
            DeltaLakeErrorKind::TableError => write!(f, "Table Error"),
            DeltaLakeErrorKind::MetadataError => write!(f, "Metadata Error"),
            DeltaLakeErrorKind::QueryError => write!(f, "Query Error"),
            DeltaLakeErrorKind::TransactionError => write!(f, "Transaction Error"),
            DeltaLakeErrorKind::ConfigurationError => write!(f, "Configuration Error"),
            DeltaLakeErrorKind::ProtocolError => write!(f, "Protocol Error"),
            DeltaLakeErrorKind::SchemaError => write!(f, "Schema Error"),
            DeltaLakeErrorKind::ValidationError => write!(f, "Validation Error"),
            DeltaLakeErrorKind::TimeoutError => write!(f, "Timeout Error"),
            DeltaLakeErrorKind::IoError => write!(f, "I/O Error"),
            DeltaLakeErrorKind::DatabaseError => write!(f, "Database Error"),
            DeltaLakeErrorKind::Unknown => write!(f, "Unknown Error"),
        }
    }
}

/// Main Delta Lake Python exception
#[pyclass(extends=PyException)]
#[derive(Debug)]
pub struct DeltaLakeError {
    #[pyo3(get)]
    pub kind: DeltaLakeErrorKind,
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub context: Option<String>,
    #[pyo3(get)]
    pub suggestions: Vec<String>,
}

#[pymethods]
impl DeltaLakeError {
    /// Create a new DeltaLakeError
    #[new]
    fn new(kind: DeltaLakeErrorKind, message: String) -> Self {
        Self {
            kind,
            message,
            context: None,
            suggestions: Vec::new(),
        }
    }

    /// Create a DeltaLakeError with context
    #[staticmethod]
    fn with_context(
        kind: DeltaLakeErrorKind,
        message: String,
        context: String,
    ) -> Self {
        let mut error = Self::new(kind, message);
        error.context = Some(context);
        error
    }

    /// Create a DeltaLakeError with suggestions
    #[staticmethod]
    fn with_suggestions(
        kind: DeltaLakeErrorKind,
        message: String,
        suggestions: Vec<String>,
    ) -> Self {
        let mut error = Self::new(kind, message);
        error.suggestions = suggestions;
        error
    }

    /// Create a DeltaLakeError with context and suggestions
    #[staticmethod]
    fn with_context_and_suggestions(
        kind: DeltaLakeErrorKind,
        message: String,
        context: String,
        suggestions: Vec<String>,
    ) -> Self {
        let mut error = Self::new(kind, message);
        error.context = Some(context);
        error.suggestions = suggestions;
        error
    }

    /// Get error representation as string
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "DeltaLakeError(kind='{}', message='{}')",
            self.kind, self.message
        ))
    }

    /// Get error representation as string
    fn __str__(&self) -> PyResult<String> {
        if let Some(context) = &self.context {
            Ok(format!("{}: {} ({})", self.kind, self.message, context))
        } else {
            Ok(format!("{}: {}", self.kind, self.message))
        }
    }

    /// Get suggestions for resolving this error
    fn get_suggestions(&self) -> Vec<String> {
        self.suggestions.clone()
    }

    /// Get detailed error information
    fn to_dict(&self) -> PyResult<std::collections::HashMap<String, String>> {
        let mut dict = std::collections::HashMap::new();
        dict.insert("kind".to_string(), format!("{}", self.kind));
        dict.insert("message".to_string(), self.message.clone());

        if let Some(context) = &self.context {
            dict.insert("context".to_string(), context.clone());
        }

        if !self.suggestions.is_empty() {
            dict.insert("suggestions".to_string(),
                format!("[{}]", self.suggestions.join(", ")));
        }

        Ok(dict)
    }
}

/// Create Python exception classes
pub fn create_exceptions(py: Python, module: &PyModule) -> PyResult<()> {
    let base_exception = py.get_type_bound::<PyException>();

    let invalid_uri = pyo3::create_exception!(py, "InvalidUriError", module, base_exception)?;
    module.add("InvalidUriError", invalid_uri)?;

    let connection_error = pyo3::create_exception!(py, "ConnectionError", module, base_exception)?;
    module.add("ConnectionError", connection_error)?;

    let table_error = pyo3::create_exception!(py, "TableError", module, base_exception)?;
    module.add("TableError", table_error)?;

    let metadata_error = pyo3::create_exception!(py, "MetadataError", module, base_exception)?;
    module.add("MetadataError", metadata_error)?;

    let query_error = pyo3::create_exception!(py, "QueryError", module, base_exception)?;
    module.add("QueryError", query_error)?;

    let transaction_error = pyo3::create_exception!(py, "TransactionError", module, base_exception)?;
    module.add("TransactionError", transaction_error)?;

    let configuration_error = pyo3::create_exception!(py, "ConfigurationError", module, base_exception)?;
    module.add("ConfigurationError", configuration_error)?;

    let protocol_error = pyo3::create_exception!(py, "ProtocolError", module, base_exception)?;
    module.add("ProtocolError", protocol_error)?;

    let schema_error = pyo3::create_exception!(py, "SchemaError", module, base_exception)?;
    module.add("SchemaError", schema_error)?;

    let validation_error = pyo3::create_exception!(py, "ValidationError", module, base_exception)?;
    module.add("ValidationError", validation_error)?;

    let timeout_error = pyo3::create_exception!(py, "TimeoutError", module, base_exception)?;
    module.add("TimeoutError", timeout_error)?;

    let io_error = pyo3::create_exception!(py, "IoError", module, base_exception)?;
    module.add("IoError", io_error)?;

    let database_error = pyo3::create_exception!(py, "DatabaseError", module, base_exception)?;
    module.add("DatabaseError", database_error)?;

    Ok(())
}

/// Convert internal error to Python exception
pub fn to_py_error(py: Python, error: crate::error::DeltaLakeError) -> PyErr {
    let kind = match error.kind {
        crate::error::DeltaLakeErrorKind::InvalidUri => DeltaLakeErrorKind::InvalidUri,
        crate::error::DeltaLakeErrorKind::ConnectionError => DeltaLakeErrorKind::ConnectionError,
        crate::error::DeltaLakeErrorKind::TableError => DeltaLakeErrorKind::TableError,
        crate::error::DeltaLakeErrorKind::MetadataError => DeltaLakeErrorKind::MetadataError,
        crate::error::DeltaLakeErrorKind::QueryError => DeltaLakeErrorKind::QueryError,
        crate::error::DeltaLakeErrorKind::TransactionError => DeltaLakeErrorKind::TransactionError,
        crate::error::DeltaLakeErrorKind::ConfigurationError => DeltaLakeErrorKind::ConfigurationError,
        crate::error::DeltaLakeErrorKind::ProtocolError => DeltaLakeErrorKind::ProtocolError,
        crate::error::DeltaLakeErrorKind::SchemaError => DeltaLakeErrorKind::SchemaError,
        crate::error::DeltaLakeErrorKind::ValidationError => DeltaLakeErrorKind::ValidationError,
        crate::error::DeltaLakeErrorKind::TimeoutError => DeltaLakeErrorKind::TimeoutError,
        crate::error::DeltaLakeErrorKind::IoError => DeltaLakeErrorKind::IoError,
        crate::error::DeltaLakeErrorKind::DatabaseError => DeltaLakeErrorKind::DatabaseError,
        crate::error::DeltaLakeErrorKind::Unknown => DeltaLakeErrorKind::Unknown,
    };

    // Create suggestions based on error kind
    let suggestions = match kind {
        DeltaLakeErrorKind::InvalidUri => vec![
            "Check URI format: 'deltasql://[database_type]://[connection_string]/[table]'".to_string(),
            "Ensure proper database type: postgres, sqlite, duckdb".to_string(),
            "Verify connection string is correctly encoded".to_string(),
        ],
        DeltaLakeErrorKind::ConnectionError => vec![
            "Check database is running and accessible".to_string(),
            "Verify connection credentials and permissions".to_string(),
            "Test connection with database client tools".to_string(),
        ],
        DeltaLakeErrorKind::TableError => vec![
            "Check table exists and is accessible".to_string(),
            "Verify table permissions are sufficient".to_string(),
            "Ensure table is in valid Delta Lake format".to_string(),
        ],
        DeltaLakeErrorKind::MetadataError => vec![
            "Check Delta log directory is accessible".to_string(),
            "Verify log files are not corrupted".to_string(),
            "Ensure proper table version compatibility".to_string(),
        ],
        DeltaLakeErrorKind::QueryError => vec![
            "Check query syntax and table references".to_string(),
            "Verify query parameters are valid".to_string(),
            "Ensure sufficient memory for query results".to_string(),
        ],
        DeltaLakeErrorKind::TransactionError => vec![
            "Check transaction is not already committed or rolled back".to_string(),
            "Verify sufficient resources for transaction".to_string(),
            "Ensure proper transaction isolation level".to_string(),
        ],
        DeltaLakeErrorKind::ConfigurationError => vec![
            "Check configuration file format and syntax".to_string(),
            "Verify required configuration parameters are set".to_string(),
            "Ensure configuration values are within valid ranges".to_string(),
        ],
        DeltaLakeErrorKind::ProtocolError => vec![
            "Check Delta table protocol compatibility".to_string(),
            "Ensure reader/writer versions are supported".to_string(),
            "Verify protocol features are available".to_string(),
        ],
        DeltaLakeErrorKind::SchemaError => vec![
            "Check schema changes are backwards compatible".to_string(),
            "Verify partition column specifications".to_string(),
            "Ensure schema evolution follows Delta Lake rules".to_string(),
        ],
        DeltaLakeErrorKind::ValidationError => vec![
            "Check data types match schema specifications".to_string(),
            "Verify partition values are valid".to_string(),
            "Ensure required fields are present and valid".to_string(),
        ],
        DeltaLakeErrorKind::TimeoutError => vec![
            "Increase timeout duration if operation is expected to take longer".to_string(),
            "Check database performance and resource utilization".to_string(),
            "Reduce query complexity or data volume".to_string(),
        ],
        DeltaLakeErrorKind::IoError => vec![
            "Check file permissions and accessibility".to_string(),
            "Verify sufficient disk space is available".to_string(),
            "Ensure network connectivity is stable".to_string(),
        ],
        DeltaLakeErrorKind::DatabaseError => vec![
            "Check database connection and configuration".to_string(),
            "Verify database server is running properly".to_string(),
            "Check database logs for detailed error information".to_string(),
        ],
        DeltaLakeErrorKind::Unknown => vec![
            "Check error logs for more details".to_string(),
            "Contact support with error details and context".to_string(),
            "Review recent changes that might have caused the error".to_string(),
        ],
    };

    let context = Some(format!("Error occurred at: {}", error.source));

    let py_error = DeltaLakeError::with_context_and_suggestions(
        kind,
        error.message,
        context,
        suggestions,
    );

    PyErr::new::<DeltaLakeError>(py_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_lake_error_creation() {
        let error = DeltaLakeError::new(
            DeltaLakeErrorKind::ConnectionError,
            "Failed to connect".to_string(),
        );

        assert_eq!(error.kind, DeltaLakeErrorKind::ConnectionError);
        assert_eq!(error.message, "Failed to connect");
        assert!(error.context.is_none());
        assert!(error.suggestions.is_empty());
    }

    #[test]
    fn test_delta_lake_error_with_context() {
        let error = DeltaLakeError::with_context(
            DeltaLakeErrorKind::TableError,
            "Table not found".to_string(),
            "table_id: abc123".to_string(),
        );

        assert_eq!(error.kind, DeltaLakeErrorKind::TableError);
        assert_eq!(error.message, "Table not found");
        assert_eq!(error.context, Some("table_id: abc123".to_string()));
    }

    #[test]
    fn test_delta_lake_error_with_suggestions() {
        let suggestions = vec![
            "Check table exists".to_string(),
            "Verify permissions".to_string(),
        ];

        let error = DeltaLakeError::with_suggestions(
            DeltaLakeErrorKind::TableError,
            "Table not found".to_string(),
            suggestions,
        );

        assert_eq!(error.suggestions.len(), 2);
        assert_eq!(error.suggestions[0], "Check table exists");
        assert_eq!(error.suggestions[1], "Verify permissions");
    }

    #[test]
    fn test_delta_lake_error_string_representation() {
        let error = DeltaLakeError::new(
            DeltaLakeErrorKind::MetadataError,
            "Invalid metadata".to_string(),
        );

        let repr = error.__repr__().unwrap();
        assert!(repr.contains("DeltaLakeError"));
        assert!(repr.contains("InvalidUri"));
        assert!(repr.contains("Invalid metadata"));

        let str = error.__str__().unwrap();
        assert_eq!(str, "InvalidUri: Invalid metadata");
    }

    #[test]
    fn test_delta_lake_error_to_dict() {
        let error = DeltaLakeError::with_context_and_suggestions(
            DeltaLakeErrorKind::ConnectionError,
            "Connection failed".to_string(),
            "uri: deltasql://postgres://localhost/test".to_string(),
            vec!["Check database".to_string()],
        );

        let dict = error.to_dict().unwrap();
        assert_eq!(dict.get("kind"), Some(&"ConnectionError".to_string()));
        assert_eq!(dict.get("message"), Some(&"Connection failed".to_string()));
        assert_eq!(dict.get("context"), Some(&"uri: deltasql://postgres://localhost/test".to_string()));
        assert!(dict.contains_key("suggestions"));
    }

    #[test]
    fn test_error_kind_display() {
        assert_eq!(format!("{}", DeltaLakeErrorKind::InvalidUri), "Invalid URI");
        assert_eq!(format!("{}", DeltaLakeErrorKind::ConnectionError), "Connection Error");
        assert_eq!(format!("{}", DeltaLakeErrorKind::TableError), "Table Error");
        assert_eq!(format!("{}", DeltaLakeErrorKind::Unknown), "Unknown Error");
    }
}