//! Command-line interface utilities for Python bindings
//!
//! This module provides CLI utilities and helpers for common operations
//! with the SQL-Backed Delta Lake metadata system.

use pyo3::prelude::*;
use std::collections::HashMap;

/// CLI command result
#[pyclass]
#[derive(Debug, Clone)]
pub struct CliResult {
    #[pyo3(get)]
    pub success: bool,
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub data: Option<HashMap<String, String>>,
    #[pyo3(get)]
    pub exit_code: i32,
}

#[pymethods]
impl CliResult {
    /// Create a new CLI result
    #[new]
    fn new(success: bool, message: String, data: Option<HashMap<String, String>>, exit_code: i32) -> Self {
        Self {
            success,
            message,
            data,
            exit_code,
        }
    }

    /// Create a success result
    #[staticmethod]
    fn success(message: String) -> Self {
        Self::new(true, message, None, 0)
    }

    /// Create an error result
    #[staticmethod]
    fn error(message: String) -> Self {
        Self::new(false, message, None, 1)
    }

    /// Create a success result with data
    #[staticmethod]
    fn success_with_data(message: String, data: HashMap<String, String>) -> Self {
        Self::new(true, message, Some(data), 0)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "CliResult(success={}, exit_code={})",
            self.success, self.exit_code
        ))
    }
}

/// CLI utility functions
#[pyclass]
#[derive(Debug)]
pub struct CliUtils {
    #[pyo3(get)]
    pub version: String,
}

#[pymethods]
impl CliUtils {
    /// Create new CLI utilities instance
    #[new]
    fn new() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Parse connection URI
    fn parse_uri(&self, uri: &str) -> PyResult<CliResult> {
        match crate::uri::parse_uri(uri) {
            Ok(parsed_uri) => {
                let mut data = HashMap::new();
                data.insert("database_type".to_string(), parsed_uri.database_type);
                data.insert("connection_string".to_string(), parsed_uri.connection_string);
                if let Some(table_name) = parsed_uri.table_name {
                    data.insert("table_name".to_string(), table_name);
                }
                Ok(CliResult::success_with_data(
                    "URI parsed successfully".to_string(),
                    data
                ))
            },
            Err(e) => Ok(CliResult::error(format!("Failed to parse URI: {}", e))),
        }
    }

    /// Validate connection URI
    fn validate_uri(&self, uri: &str) -> PyResult<CliResult> {
        match crate::uri::validate_uri(uri) {
            Ok(is_valid) => {
                let message = if is_valid {
                    "URI is valid".to_string()
                } else {
                    "URI is invalid".to_string()
                };
                Ok(CliResult::success(message))
            },
            Err(e) => Ok(CliResult::error(format!("Failed to validate URI: {}", e))),
        }
    }

    /// List supported database types
    fn list_database_types(&self) -> PyResult<CliResult> {
        let types = crate::uri::DeltaSqlUri::get_supported_database_types();
        let mut data = HashMap::new();
        data.insert("database_types".to_string(),
            format!("[{}]", types.join(", ")));
        Ok(CliResult::success_with_data(
            "Supported database types".to_string(),
            data
        ))
    }

    /// Get version information
    fn get_version(&self) -> PyResult<CliResult> {
        let mut data = HashMap::new();
        data.insert("version".to_string(), self.version.clone());
        data.insert("rust_version".to_string(), "1.70+".to_string());
        data.insert("python_bindings".to_string(), "true".to_string());
        Ok(CliResult::success_with_data(
            "Version information".to_string(),
            data
        ))
    }

    /// Check compatibility
    fn check_compatibility(&self) -> PyResult<CliResult> {
        let compat = crate::compatibility::DeltaLakeCompatibility::new();
        let info = compat.get_compatibility_info()?;
        Ok(CliResult::success_with_data(
            "Compatibility check completed".to_string(),
            info
        ))
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("CliUtils(version='{}')", self.version))
    }
}

/// Quick connect utility
#[pyfunction]
pub fn quick_connect(uri: &str, config: Option<crate::config::SqlConfig>) -> PyResult<CliResult> {
    match crate::connection::SqlConnection::new(uri.to_string(), config) {
        Ok(connection) => {
            let mut data = HashMap::new();
            data.insert("database_type".to_string(), connection.database_type);
            data.insert("is_connected".to_string(), connection.is_connected.to_string());
            data.insert("created_at".to_string(), connection.created_at.to_rfc3339());
            Ok(CliResult::success_with_data(
                "Connected successfully".to_string(),
                data
            ))
        },
        Err(e) => Ok(CliResult::error(format!("Failed to connect: {}", e))),
    }
}

/// List tables utility
#[pyfunction]
pub fn list_tables_cli(uri: &str, config: Option<crate::config::SqlConfig>) -> PyResult<CliResult> {
    let mut connection = match crate::connection::SqlConnection::new(uri.to_string(), config) {
        Ok(conn) => conn,
        Err(e) => return Ok(CliResult::error(format!("Failed to connect: {}", e))),
    };

    match connection.list_tables() {
        Ok(tables) => {
            let mut data = HashMap::new();
            data.insert("table_count".to_string(), tables.len().to_string());

            let table_names: Vec<String> = tables.iter()
                .map(|t| t.name.clone())
                .collect();
            data.insert("tables".to_string(),
                format!("[{}]", table_names.join(", ")));

            Ok(CliResult::success_with_data(
                "Tables listed successfully".to_string(),
                data
            ))
        },
        Err(e) => Ok(CliResult::error(format!("Failed to list tables: {}", e))),
    }
}

/// Create table utility
#[pyfunction]
pub fn create_table_cli(
    uri: &str,
    name: String,
    description: Option<String>,
    partition_columns: Option<Vec<String>>,
    config: Option<crate::config::SqlConfig>,
) -> PyResult<CliResult> {
    let mut connection = match crate::connection::SqlConnection::new(uri.to_string(), config) {
        Ok(conn) => conn,
        Err(e) => return Ok(CliResult::error(format!("Failed to connect: {}", e))),
    };

    match connection.create_table(name, description, partition_columns) {
        Ok(table) => {
            let mut data = HashMap::new();
            data.insert("table_id".to_string(), table.table_id);
            data.insert("table_name".to_string(), table.name);
            data.insert("version".to_string(), table.version.to_string());
            Ok(CliResult::success_with_data(
                "Table created successfully".to_string(),
                data
            ))
        },
        Err(e) => Ok(CliResult::error(format!("Failed to create table: {}", e))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_result_creation() {
        let result = CliResult::success("Test success".to_string());
        assert!(result.success);
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.message, "Test success");
    }

    #[test]
    fn test_cli_result_error() {
        let result = CliResult::error("Test error".to_string());
        assert!(!result.success);
        assert_eq!(result.exit_code, 1);
        assert_eq!(result.message, "Test error");
    }

    #[test]
    fn test_cli_utils_creation() {
        let utils = CliUtils::new();
        assert!(!utils.version.is_empty());
    }

    #[test]
    fn test_parse_uri_cli() {
        let utils = CliUtils::new();
        let result = utils.parse_uri("deltasql://postgres://localhost/test");
        assert!(result.success);
        assert!(result.data.is_some());
    }
}