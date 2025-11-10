//! Compatibility layer with existing deltalake package
//!
//! This module provides compatibility utilities to ensure seamless integration
//! with existing deltalake workflows while using the SQL-backed metadata system.

use pyo3::prelude::*;
use std::collections::HashMap;

/// Compatibility layer for Delta Lake operations
#[pyclass]
#[derive(Debug, Clone)]
pub struct DeltaLakeCompatibility {
    #[pyo3(get)]
    pub version: String,
    #[pyo3(get)]
    pub supported_features: Vec<String>,
    #[pyo3(get)]
    pub compatibility_mode: String,
}

#[pymethods]
impl DeltaLakeCompatibility {
    /// Create a new compatibility layer instance
    #[new]
    fn new() -> Self {
        Self {
            version: "1.0.0".to_string(),
            supported_features: vec![
                "read_table".to_string(),
                "write_table".to_string(),
                "create_table".to_string(),
                "delete_table".to_string(),
                "get_version".to_string(),
                "vacuum".to_string(),
                "optimize".to_string(),
                "merge".to_string(),
                "update".to_string(),
                "delete".to_string(),
            ],
            compatibility_mode: "sql_backend".to_string(),
        }
    }

    /// Check if a feature is supported
    fn is_feature_supported(&self, feature: &str) -> bool {
        self.supported_features.contains(&feature.to_string())
    }

    /// Get compatibility information as dictionary
    fn get_compatibility_info(&self) -> PyResult<HashMap<String, String>> {
        let mut info = HashMap::new();
        info.insert("version".to_string(), self.version.clone());
        info.insert("compatibility_mode".to_string(), self.compatibility_mode.clone());
        info.insert("supported_features".to_string(),
            format!("[{}]", self.supported_features.join(", ")));
        Ok(info)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "DeltaLakeCompatibility(version='{}', mode='{}')",
            self.version, self.compatibility_mode
        ))
    }
}

/// Bridge between SQL backend and deltalake API
#[pyclass]
#[derive(Debug)]
pub struct DeltaLakeBridge {
    #[pyo3(get)]
    pub connection: Option<crate::connection::SqlConnection>,
    #[pyo3(get)]
    pub compatibility: DeltaLakeCompatibility,
}

#[pymethods]
impl DeltaLakeBridge {
    /// Create a new bridge instance
    #[new]
    fn new() -> Self {
        Self {
            connection: None,
            compatibility: DeltaLakeCompatibility::new(),
        }
    }

    /// Initialize bridge with connection
    fn initialize(&mut self, connection: crate::connection::SqlConnection) -> PyResult<()> {
        self.connection = Some(connection);
        Ok(())
    }

    /// Check if bridge is initialized
    fn is_initialized(&self) -> bool {
        self.connection.is_some()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "DeltaLakeBridge(initialized={})",
            self.is_initialized()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compatibility_creation() {
        let compat = DeltaLakeCompatibility::new();
        assert_eq!(compat.version, "1.0.0");
        assert_eq!(compat.compatibility_mode, "sql_backend");
        assert!(!compat.supported_features.is_empty());
    }

    #[test]
    fn test_feature_support() {
        let compat = DeltaLakeCompatibility::new();
        assert!(compat.is_feature_supported("read_table"));
        assert!(!compat.is_feature_supported("unsupported_feature"));
    }

    #[test]
    fn test_bridge_creation() {
        let bridge = DeltaLakeBridge::new();
        assert!(!bridge.is_initialized());
        assert_eq!(bridge.compatibility.version, "1.0.0");
    }
}