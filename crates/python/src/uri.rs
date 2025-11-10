//! URI parsing and validation for DeltaSQL scheme
//!
//! This module provides Python bindings for URI parsing and validation,
//! supporting the custom `deltasql://` scheme for database connections.

use pyo3::prelude::*;
use std::collections::HashMap;
use std::str::FromStr;
use url::{Url, ParseError};

/// DeltaSQL URI components
#[pyclass]
#[derive(Debug, Clone)]
pub struct DeltaSqlUri {
    #[pyo3(get)]
    pub scheme: String,
    #[pyo3(get)]
    pub database_type: String,
    #[pyo3(get)]
    pub connection_string: String,
    #[pyo3(get)]
    pub table_name: Option<String>,
    #[pyo3(get)]
    pub parameters: HashMap<String, String>,
    #[pyo3(get)]
    pub is_valid: bool,
    #[pyo3(get)]
    pub validation_errors: Vec<String>,
}

#[pymethods]
impl DeltaSqlUri {
    /// Create a new DeltaSqlUri
    #[new]
    fn new(
        scheme: String,
        database_type: String,
        connection_string: String,
        table_name: Option<String>,
        parameters: Option<HashMap<String, String>>,
    ) -> Self {
        let params = parameters.unwrap_or_default();
        let (is_valid, validation_errors) = Self::validate_components(&scheme, &database_type, &connection_string, &table_name, &params);

        Self {
            scheme,
            database_type,
            connection_string,
            table_name,
            parameters: params,
            is_valid,
            validation_errors,
        }
    }

    /// Parse a DeltaSQL URI from string
    #[staticmethod]
    fn parse(uri: &str) -> PyResult<Self> {
        // Parse the URL
        let url = Url::parse(uri)
            .map_err(|e| PyErr::new::<crate::error::DeltaLakeError, _>(
                crate::error::DeltaLakeError::new(
                    crate::error::DeltaLakeErrorKind::InvalidUri,
                    format!("Failed to parse URI: {}", e)
                )
            ))?;

        // Validate scheme
        if url.scheme() != "deltasql" {
            return Err(PyErr::new::<crate::error::DeltaLakeError, _>(
                crate::error::DeltaLakeError::new(
                    crate::error::DeltaLakeErrorKind::InvalidUri,
                    format!("Invalid scheme: {}. Expected 'deltasql'", url.scheme())
                )
            ));
        }

        // Extract database type from host
        let database_type = url.host_str()
            .ok_or_else(|| PyErr::new::<crate::error::DeltaLakeError, _>(
                crate::error::DeltaLakeError::new(
                    crate::error::DeltaLakeErrorKind::InvalidUri,
                    "Missing database type in URI".to_string()
                )
            ))?
            .to_string();

        // Extract connection string from path
        let connection_string = url.path()
            .trim_start_matches('/')
            .to_string();

        // Extract table name from fragment
        let table_name = url.fragment().map(|s| s.to_string());

        // Extract parameters from query
        let mut parameters = HashMap::new();
        for (key, value) in url.query_pairs() {
            parameters.insert(key.to_string(), value.to_string());
        }

        Ok(Self::new(
            "deltasql".to_string(),
            database_type,
            connection_string,
            table_name,
            Some(parameters),
        ))
    }

    /// Build URI string from components
    fn build_uri(&self) -> PyResult<String> {
        if !self.is_valid {
            return Err(PyErr::new::<crate::error::DeltaLakeError, _>(
                crate::error::DeltaLakeError::new(
                    crate::error::DeltaLakeErrorKind::InvalidUri,
                    "Cannot build URI from invalid components".to_string()
                )
            ));
        }

        let mut url = Url::parse(&format!("{}://{}", self.scheme, self.database_type))
            .map_err(|e| PyErr::new::<crate::error::DeltaLakeError, _>(
                crate::error::DeltaLakeError::new(
                    crate::error::DeltaLakeErrorKind::InvalidUri,
                    format!("Failed to build URI: {}", e)
                )
            ))?;

        url.set_path(&format!("/{}", self.connection_string));

        if let Some(table_name) = &self.table_name {
            url.set_fragment(Some(table_name));
        }

        // Add parameters
        for (key, value) in &self.parameters {
            url.query_pairs_mut().append_pair(key, value);
        }

        Ok(url.to_string())
    }

    /// Validate URI components
    fn validate(&mut self) -> PyResult<bool> {
        let (is_valid, errors) = Self::validate_components(
            &self.scheme,
            &self.database_type,
            &self.connection_string,
            &self.table_name,
            &self.parameters,
        );

        self.is_valid = is_valid;
        self.validation_errors = errors;

        Ok(is_valid)
    }

    /// Get supported database types
    #[staticmethod]
    fn get_supported_database_types() -> Vec<String> {
        vec![
            "postgres".to_string(),
            "postgresql".to_string(),
            "mysql".to_string(),
            "sqlite".to_string(),
            "duckdb".to_string(),
            "mssql".to_string(),
            "oracle".to_string(),
            "snowflake".to_string(),
            "bigquery".to_string(),
            "redshift".to_string(),
        ]
    }

    /// Check if database type is supported
    #[staticmethod]
    fn is_supported_database_type(database_type: &str) -> bool {
        Self::get_supported_database_types().contains(&database_type.to_lowercase())
    }

    /// Get URI as dictionary
    fn to_dict(&self) -> PyResult<HashMap<String, String>> {
        let mut dict = HashMap::new();
        dict.insert("scheme".to_string(), self.scheme.clone());
        dict.insert("database_type".to_string(), self.database_type.clone());
        dict.insert("connection_string".to_string(), self.connection_string.clone());
        dict.insert("is_valid".to_string(), self.is_valid.to_string());

        if let Some(table_name) = &self.table_name {
            dict.insert("table_name".to_string(), table_name.clone());
        }

        // Convert parameters to string representation
        let params_str = self.parameters
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");
        dict.insert("parameters".to_string(), params_str);

        let errors_str = self.validation_errors.join(";");
        dict.insert("validation_errors".to_string(), errors_str);

        Ok(dict)
    }

    /// Create URI from individual components
    #[staticmethod]
    fn from_components(
        database_type: String,
        connection_string: String,
        table_name: Option<String>,
        parameters: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let uri = Self::new(
            "deltasql".to_string(),
            database_type,
            connection_string,
            table_name,
            parameters,
        );

        Ok(uri)
    }

    /// Normalize connection string for different database types
    fn normalize_connection_string(&self) -> PyResult<String> {
        let normalized = match self.database_type.to_lowercase().as_str() {
            "postgres" | "postgresql" => {
                // Ensure postgres connection string starts with postgresql://
                if !self.connection_string.starts_with("postgresql://") && !self.connection_string.starts_with("postgres://") {
                    format!("postgresql://{}", self.connection_string)
                } else {
                    self.connection_string.clone()
                }
            },
            "mysql" => {
                // Ensure mysql connection string starts with mysql://
                if !self.connection_string.starts_with("mysql://") {
                    format!("mysql://{}", self.connection_string)
                } else {
                    self.connection_string.clone()
                }
            },
            "sqlite" => {
                // SQLite connection strings are typically file paths
                self.connection_string.clone()
            },
            "duckdb" => {
                // DuckDB connection strings are typically file paths
                self.connection_string.clone()
            },
            _ => self.connection_string.clone(),
        };

        Ok(normalized)
    }

    /// Get connection URI without table and parameters
    fn get_base_uri(&self) -> PyResult<String> {
        let mut base_uri = format!("{}://{}", self.scheme, self.database_type);
        if !self.connection_string.is_empty() {
            base_uri = format!("{}/{}", base_uri, self.connection_string);
        }
        Ok(base_uri)
    }

    /// Check if URI contains table specification
    fn has_table(&self) -> bool {
        self.table_name.is_some()
    }

    /// Get table name with validation
    fn get_table_name(&self) -> Option<String> {
        self.table_name.clone()
    }

    /// Set table name
    fn set_table_name(&mut self, table_name: Option<String>) {
        self.table_name = table_name;
        // Re-validate after modification
        let _ = self.validate();
    }

    /// Add parameter
    fn add_parameter(&mut self, key: String, value: String) {
        self.parameters.insert(key, value);
        // Re-validate after modification
        let _ = self.validate();
    }

    /// Remove parameter
    fn remove_parameter(&mut self, key: &str) -> Option<String> {
        let removed = self.parameters.remove(key);
        // Re-validate after modification
        let _ = self.validate();
        removed
    }

    /// Get parameter value
    fn get_parameter(&self, key: &str) -> Option<String> {
        self.parameters.get(key).cloned()
    }

    /// Clear all parameters
    fn clear_parameters(&mut self) {
        self.parameters.clear();
        // Re-validate after modification
        let _ = self.validate();
    }

    /// Clone URI
    fn clone_uri(&self) -> Self {
        self.clone()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "DeltaSqlUri(database_type='{}', connection_string='{}', table={})",
            self.database_type,
            self.connection_string,
            self.table_name.as_ref().unwrap_or(&"None".to_string())
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        if self.is_valid {
            self.build_uri()
        } else {
            self.__repr__()
        }
    }
}

impl DeltaSqlUri {
    /// Validate URI components and return (is_valid, errors)
    fn validate_components(
        scheme: &str,
        database_type: &str,
        connection_string: &str,
        table_name: &Option<String>,
        parameters: &HashMap<String, String>,
    ) -> (bool, Vec<String>) {
        let mut errors = Vec::new();

        // Validate scheme
        if scheme != "deltasql" {
            errors.push(format!("Invalid scheme: {}. Expected 'deltasql'", scheme));
        }

        // Validate database type
        if database_type.is_empty() {
            errors.push("Database type cannot be empty".to_string());
        } else if !Self::is_supported_database_type(database_type) {
            errors.push(format!("Unsupported database type: {}", database_type));
        }

        // Validate connection string
        if connection_string.is_empty() {
            errors.push("Connection string cannot be empty".to_string());
        } else {
            // Database-specific validation
            match database_type.to_lowercase().as_str() {
                "postgres" | "postgresql" => {
                    if !connection_string.starts_with("postgresql://") && !connection_string.starts_with("postgres://") {
                        errors.push("PostgreSQL connection string should start with postgresql:// or postgres://".to_string());
                    }
                },
                "mysql" => {
                    if !connection_string.starts_with("mysql://") {
                        errors.push("MySQL connection string should start with mysql://".to_string());
                    }
                },
                _ => {
                    // No specific validation for other database types
                }
            }
        }

        // Validate table name
        if let Some(table) = table_name {
            if table.is_empty() {
                errors.push("Table name cannot be empty".to_string());
            } else if !table.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
                errors.push("Table name can only contain alphanumeric characters, underscores, and hyphens".to_string());
            }
        }

        // Validate parameters
        for (key, value) in parameters {
            if key.is_empty() {
                errors.push("Parameter key cannot be empty".to_string());
            }
            if value.is_empty() {
                errors.push(format!("Parameter value for '{}' cannot be empty", key));
            }
        }

        (errors.is_empty(), errors)
    }
}

/// Parse a DeltaSQL URI (convenience function)
#[pyfunction]
pub fn parse_uri(uri: &str) -> PyResult<DeltaSqlUri> {
    DeltaSqlUri::parse(uri)
}

/// Create a DeltaSQL URI from components
#[pyfunction]
pub fn create_uri(
    database_type: String,
    connection_string: String,
    table_name: Option<String>,
    parameters: Option<HashMap<String, String>>,
) -> PyResult<DeltaSqlUri> {
    DeltaSqlUri::from_components(database_type, connection_string, table_name, parameters)
}

/// Validate a DeltaSQL URI string
#[pyfunction]
pub fn validate_uri(uri: &str) -> PyResult<bool> {
    match DeltaSqlUri::parse(uri) {
        Ok(parsed_uri) => Ok(parsed_uri.is_valid),
        Err(_) => Ok(false),
    }
}

/// Get database type from URI
#[pyfunction]
pub fn get_database_type(uri: &str) -> PyResult<String> {
    let parsed_uri = DeltaSqlUri::parse(uri)?;
    Ok(parsed_uri.database_type)
}

/// Get connection string from URI
#[pyfunction]
pub fn get_connection_string(uri: &str) -> PyResult<String> {
    let parsed_uri = DeltaSqlUri::parse(uri)?;
    Ok(parsed_uri.connection_string)
}

/// Get table name from URI
#[pyfunction]
pub fn get_table_name(uri: &str) -> PyResult<Option<String>> {
    let parsed_uri = DeltaSqlUri::parse(uri)?;
    Ok(parsed_uri.table_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_postgres_uri() {
        let uri = "deltasql://postgres://localhost:5432/testdb#mytable";
        let parsed = DeltaSqlUri::parse(uri).unwrap();

        assert_eq!(parsed.scheme, "deltasql");
        assert_eq!(parsed.database_type, "postgres");
        assert_eq!(parsed.connection_string, "localhost:5432/testdb");
        assert_eq!(parsed.table_name, Some("mytable".to_string()));
        assert!(parsed.is_valid);
    }

    #[test]
    fn test_parse_uri_with_parameters() {
        let uri = "deltasql://mysql://localhost:3306/testdb?ssl=true&timeout=30";
        let parsed = DeltaSqlUri::parse(uri).unwrap();

        assert_eq!(parsed.database_type, "mysql");
        assert_eq!(parsed.connection_string, "localhost:3306/testdb");
        assert_eq!(parsed.parameters.get("ssl"), Some(&"true".to_string()));
        assert_eq!(parsed.parameters.get("timeout"), Some(&"30".to_string()));
        assert!(parsed.is_valid);
    }

    #[test]
    fn test_parse_invalid_scheme() {
        let uri = "http://postgres://localhost/test";
        let result = DeltaSqlUri::parse(uri);
        assert!(result.is_err());
    }

    #[test]
    fn test_unsupported_database_type() {
        let uri = "deltasql://unsupported://localhost/test";
        let parsed = DeltaSqlUri::parse(uri).unwrap();
        assert!(!parsed.is_valid);
        assert!(parsed.validation_errors.iter().any(|e| e.contains("Unsupported database type")));
    }

    #[test]
    fn test_build_uri() {
        let uri = DeltaSqlUri::new(
            "deltasql".to_string(),
            "postgres".to_string(),
            "localhost:5432/testdb".to_string(),
            Some("mytable".to_string()),
            None,
        );

        let built = uri.build_uri().unwrap();
        assert_eq!(built, "deltasql://postgres/localhost:5432/testdb#mytable");
    }

    #[test]
    fn test_normalize_connection_string() {
        let mut uri = DeltaSqlUri::new(
            "deltasql".to_string(),
            "postgres".to_string(),
            "localhost:5432/testdb".to_string(),
            None,
            None,
        );

        let normalized = uri.normalize_connection_string().unwrap();
        assert_eq!(normalized, "postgresql://localhost:5432/testdb");
    }

    #[test]
    fn test_database_type_support() {
        assert!(DeltaSqlUri::is_supported_database_type("postgres"));
        assert!(DeltaSqlUri::is_supported_database_type("mysql"));
        assert!(DeltaSqlUri::is_supported_database_type("sqlite"));
        assert!(!DeltaSqlUri::is_supported_database_type("unsupported"));
    }
}