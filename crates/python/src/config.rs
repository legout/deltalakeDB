//! Configuration management for SQL-Backed Delta Lake metadata
//!
//! This module provides Python bindings for configuration management,
//! including connection parameters, database-specific settings, and
//! environment variable support.

use pyo3::prelude::*;
use std::collections::HashMap;
use std::time::Duration;

/// SQL connection configuration
#[pyclass]
#[derive(Debug, Clone)]
pub struct SqlConfig {
    #[pyo3(get)]
    pub database_type: String,
    #[pyo3(get)]
    pub connection_string: String,
    #[pyo3(get)]
    pub pool_size: u32,
    #[pyo3(get)]
    pub timeout: Duration,
    #[pyo3(get)]
    pub ssl_enabled: bool,
    #[pyo3(get)]
    pub max_connections: u32,
    #[pyo3(get)]
    pub min_connections: u32,
    #[pyo3(get)]
    pub connection_timeout: Duration,
    #[pyo3(get)]
    pub idle_timeout: Duration,
    #[pyo3(get)]
    pub max_lifetime: Duration,
    #[pyo3(get)]
    pub health_check_interval: Duration,
    #[pyo3(get)]
    pub test_query: Option<String>,
    #[pyo3(get)]
    pub application_name: Option<String>,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self {
            database_type: "sqlite".to_string(),
            connection_string: ":memory:".to_string(),
            pool_size: 10,
            timeout: Duration::from_secs(30),
            ssl_enabled: false,
            max_connections: 20,
            min_connections: 1,
            connection_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(300), // 5 minutes
            max_lifetime: Duration::from_secs(3600), // 1 hour
            health_check_interval: Duration::from_secs(30),
            test_query: None,
            application_name: None,
        }
    }
}

#[pymethods]
impl SqlConfig {
    /// Create a new SQL configuration
    #[new]
    fn new(
        database_type: Option<String>,
        connection_string: Option<String>,
        pool_size: Option<u32>,
        timeout_secs: Option<u64>,
        ssl_enabled: Option<bool>,
    ) -> Self {
        Self {
            database_type: database_type.unwrap_or_else(|| "sqlite".to_string()),
            connection_string: connection_string.unwrap_or_else(|| ":memory:".to_string()),
            pool_size: pool_size.unwrap_or(10),
            timeout: Duration::from_secs(timeout_secs.unwrap_or(30)),
            ssl_enabled: ssl_enabled.unwrap_or(false),
            max_connections: 20,
            min_connections: 1,
            connection_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_secs(3600),
            health_check_interval: Duration::from_secs(30),
            test_query: None,
            application_name: None,
        }
    }

    /// Create configuration from environment variables
    #[staticmethod]
    fn from_env() -> PyResult<Self> {
        let mut config = SqlConfig::default();

        // Read from environment variables
        if let Ok(database_type) = std::env::var("DELTASQL_DATABASE_TYPE") {
            config.database_type = database_type;
        }

        if let Ok(connection_string) = std::env::var("DELTASQL_CONNECTION_STRING") {
            config.connection_string = connection_string;
        }

        if let Ok(pool_size) = std::env::var("DELTASQL_POOL_SIZE") {
            if let Ok(size) = pool_size.parse::<u32>() {
                config.pool_size = size;
            }
        }

        if let Ok(timeout) = std::env::var("DELTASQL_TIMEOUT") {
            if let Ok(secs) = timeout.parse::<u64>() {
                config.timeout = Duration::from_secs(secs);
            }
        }

        if let Ok(ssl_enabled) = std::env::var("DELTASQL_SSL_ENABLED") {
            config.ssl_enabled = ssl_enabled.parse().unwrap_or(false);
        }

        if let Ok(max_connections) = std::env::var("DELTASQL_MAX_CONNECTIONS") {
            if let Ok(max) = max_connections.parse::<u32>() {
                config.max_connections = max;
            }
        }

        if let Ok(min_connections) = std::env::var("DELTASQL_MIN_CONNECTIONS") {
            if let Ok(min) = min_connections.parse::<u32>() {
                config.min_connections = min;
            }
        }

        if let Ok(app_name) = std::env::var("DELTASQL_APPLICATION_NAME") {
            config.application_name = Some(app_name);
        }

        Ok(config)
    }

    /// Create configuration from a dictionary
    #[staticmethod]
    fn from_dict(config_dict: HashMap<String, String>) -> PyResult<Self> {
        let mut config = SqlConfig::default();

        if let Some(database_type) = config_dict.get("database_type") {
            config.database_type = database_type.clone();
        }

        if let Some(connection_string) = config_dict.get("connection_string") {
            config.connection_string = connection_string.clone();
        }

        if let Some(pool_size) = config_dict.get("pool_size") {
            if let Ok(size) = pool_size.parse::<u32>() {
                config.pool_size = size;
            }
        }

        if let Some(timeout_secs) = config_dict.get("timeout_secs") {
            if let Ok(secs) = timeout_secs.parse::<u64>() {
                config.timeout = Duration::from_secs(secs);
            }
        }

        if let Some(ssl_enabled) = config_dict.get("ssl_enabled") {
            config.ssl_enabled = ssl_enabled.parse().unwrap_or(false);
        }

        if let Some(max_connections) = config_dict.get("max_connections") {
            if let Ok(max) = max_connections.parse::<u32>() {
                config.max_connections = max;
            }
        }

        if let Some(min_connections) = config_dict.get("min_connections") {
            if let Ok(min) = min_connections.parse::<u32>() {
                config.min_connections = min;
            }
        }

        if let Some(app_name) = config_dict.get("application_name") {
            config.application_name = Some(app_name.clone());
        }

        Ok(config)
    }

    /// Get configuration as dictionary
    fn to_dict(&self) -> PyResult<HashMap<String, String>> {
        let mut dict = HashMap::new();
        dict.insert("database_type".to_string(), self.database_type.clone());
        dict.insert("connection_string".to_string(), self.connection_string.clone());
        dict.insert("pool_size".to_string(), self.pool_size.to_string());
        dict.insert("timeout".to_string(), self.timeout.as_secs().to_string());
        dict.insert("ssl_enabled".to_string(), self.ssl_enabled.to_string());
        dict.insert("max_connections".to_string(), self.max_connections.to_string());
        dict.insert("min_connections".to_string(), self.min_connections.to_string());
        dict.insert("connection_timeout".to_string(), self.connection_timeout.as_secs().to_string());
        dict.insert("idle_timeout".to_string(), self.idle_timeout.as_secs().to_string());
        dict.insert("max_lifetime".to_string(), self.max_lifetime.as_secs().to_string());
        dict.insert("health_check_interval".to_string(), self.health_check_interval.as_secs().to_string());

        if let Some(app_name) = &self.application_name {
            dict.insert("application_name".to_string(), app_name.clone());
        }

        if let Some(test_query) = &self.test_query {
            dict.insert("test_query".to_string(), test_query.clone());
        }

        Ok(dict)
    }

    /// Get database-specific default configuration
    #[staticmethod]
    fn get_database_defaults(database_type: &str) -> PyResult<Self> {
        match database_type.to_lowercase().as_str() {
            "postgres" | "postgresql" => {
                Ok(Self {
                    database_type: "postgres".to_string(),
                    connection_string: "postgresql://localhost:5432/delta".to_string(),
                    pool_size: 20,
                    timeout: Duration::from_secs(30),
                    ssl_enabled: true,
                    max_connections: 100,
                    min_connections: 5,
                    connection_timeout: Duration::from_secs(10),
                    idle_timeout: Duration::from_secs(600), // 10 minutes
                    max_lifetime: Duration::from_secs(7200), // 2 hours
                    health_check_interval: Duration::from_secs(30),
                    test_query: Some("SELECT 1".to_string()),
                    application_name: Some("deltalakedb".to_string()),
                })
            }
            "sqlite" => {
                Ok(Self {
                    database_type: "sqlite".to_string(),
                    connection_string: ":memory:".to_string(),
                    pool_size: 1, // SQLite typically uses single connections
                    timeout: Duration::from_secs(30),
                    ssl_enabled: false,
                    max_connections: 1,
                    min_connections: 1,
                    connection_timeout: Duration::from_secs(5),
                    idle_timeout: Duration::from_secs(300), // 5 minutes
                    max_lifetime: Duration::from_secs(3600), // 1 hour
                    health_check_interval: Duration::from_secs(60),
                    test_query: Some("SELECT 1".to_string()),
                    application_name: Some("deltalakedb".to_string()),
                })
            }
            "duckdb" => {
                Ok(Self {
                    database_type: "duckdb".to_string(),
                    connection_string: "duckdb://memory".to_string(),
                    pool_size: 10,
                    timeout: Duration::from_secs(30),
                    ssl_enabled: false,
                    max_connections: 20,
                    min_connections: 2,
                    connection_timeout: Duration::from_secs(10),
                    idle_timeout: Duration::from_secs(300),
                    max_lifetime: Duration::from_secs(3600),
                    health_check_interval: Duration::from_secs(30),
                    test_query: Some("SELECT 1".to_string()),
                    application_name: Some("deltalakedb".to_string()),
                })
            }
            _ => {
                Err(PyErr::new::<pyo3::exceptions::PyValueError>(
                    format!("Unsupported database type: {}", database_type)
                ))
            }
        }
    }

    /// Validate configuration
    fn validate(&self) -> PyResult<Vec<String>> {
        let mut errors = Vec::new();

        // Validate database type
        match self.database_type.as_str() {
            "postgres" | "postgresql" | "sqlite" | "duckdb" => {
                // Supported types
            }
            _ => {
                errors.push(format!("Unsupported database type: {}", self.database_type));
            }
        }

        // Validate pool size
        if self.pool_size == 0 {
            errors.push("Pool size must be greater than 0".to_string());
        }

        if self.pool_size > 1000 {
            errors.push("Pool size should not exceed 1000".to_string());
        }

        // Validate timeout
        if self.timeout.is_zero() {
            errors.push("Timeout must be greater than 0".to_string());
        }

        if self.timeout > Duration::from_secs(3600) {
            errors.push("Timeout should not exceed 3600 seconds (1 hour)".to_string());
        }

        // Validate connection pool constraints
        if self.min_connections > self.max_connections {
            errors.push("Min connections cannot be greater than max connections".to_string());
        }

        if self.min_connections > self.pool_size {
            errors.push("Min connections cannot be greater than pool size".to_string());
        }

        // Validate connection string
        if self.connection_string.is_empty() {
            errors.push("Connection string cannot be empty".to_string());
        }

        // Validate timeouts
        if self.connection_timeout > self.timeout {
            errors.push("Connection timeout should not be greater than query timeout".to_string());
        }

        if self.idle_timeout > self.max_lifetime {
            errors.push("Idle timeout should not be greater than max lifetime".to_string());
        }

        Ok(errors)
    }

    /// Apply database-specific optimizations
    fn apply_database_optimizations(&mut self) -> PyResult<()> {
        match self.database_type.as_str() {
            "postgres" | "postgresql" => {
                // PostgreSQL optimizations
                if self.pool_size < 5 {
                    self.pool_size = 5;
                }
                self.test_query = Some("SELECT 1".to_string());
                self.application_name = Some("deltalakedb".to_string());
            }
            "sqlite" => {
                // SQLite optimizations
                self.pool_size = 1;
                self.max_connections = 1;
                self.min_connections = 1;
                self.test_query = Some("SELECT 1".to_string());
                self.application_name = Some("deltalakedb".to_string());
            }
            "duckdb" => {
                // DuckDB optimizations
                if self.pool_size < 2 {
                    self.pool_size = 2;
                }
                self.test_query = Some("SELECT 1".to_string());
                self.application_name = Some("deltalakedb".to_string());
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError>(
                    format!("Cannot apply optimizations for database type: {}", self.database_type)
                ));
            }
        }

        Ok(())
    }

    /// Create a copy of the configuration
    fn copy(&self) -> Self {
        self.clone()
    }

    /// Set database type
    fn set_database_type(&mut self, database_type: String) {
        self.database_type = database_type;
    }

    /// Set connection string
    fn set_connection_string(&mut self, connection_string: String) {
        self.connection_string = connection_string;
    }

    /// Set pool size
    fn set_pool_size(&mut self, pool_size: u32) {
        self.pool_size = pool_size;
    }

    /// Set timeout in seconds
    fn set_timeout(&mut self, timeout_secs: u64) {
        self.timeout = Duration::from_secs(timeout_secs);
    }

    /// Enable/disable SSL
    fn set_ssl_enabled(&mut self, ssl_enabled: bool) {
        self.ssl_enabled = ssl_enabled;
    }

    /// Set max connections
    fn set_max_connections(&mut self, max_connections: u32) {
        self.max_connections = max_connections;
    }

    /// Set min connections
    fn set_min_connections(&mut self, min_connections: u32) {
        self.min_connections = min_connections;
    }

    /// Set test query
    fn set_test_query(&mut self, test_query: Option<String>) {
        self.test_query = test_query;
    }

    /// Set application name
    fn set_application_name(&mut self, application_name: Option<String>) {
        self.application_name = application_name;
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "SqlConfig(database_type='{}', pool_size={}, timeout={}s)",
            self.database_type, self.pool_size, self.timeout.as_secs()
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

/// Database-specific configuration presets
#[pyclass]
#[derive(Debug)]
pub struct DatabasePresets;

#[pymethods]
impl DatabasePresets {
    /// Get PostgreSQL configuration preset
    #[staticmethod]
    fn postgresql() -> PyResult<SqlConfig> {
        SqlConfig::get_database_defaults("postgres")
    }

    /// Get SQLite configuration preset
    #[staticmethod]
    fn sqlite() -> PyResult<SqlConfig> {
        SqlConfig::get_database_defaults("sqlite")
    }

    /// Get DuckDB configuration preset
    #[staticmethod]
    fn duckdb() -> PyResult<SqlConfig> {
        SqlConfig::get_database_defaults("duckdb")
    }

    /// Get configuration preset for high-performance workloads
    #[staticmethod]
    fn high_performance() -> PyResult<SqlConfig> {
        let mut config = SqlConfig::default();
        config.pool_size = 50;
        config.max_connections = 100;
        config.min_connections = 10;
        config.timeout = Duration::from_secs(60);
        config.connection_timeout = Duration::from_secs(5);
        config.idle_timeout = Duration::from_secs(1800); // 30 minutes
        config.max_lifetime = Duration::from_secs(7200); // 2 hours
        config.health_check_interval = Duration::from_secs(15);
        Ok(config)
    }

    /// Get configuration preset for low-resource environments
    #[staticmethod]
    fn low_resource() -> PyResult<SqlConfig> {
        let mut config = SqlConfig::default();
        config.pool_size = 5;
        config.max_connections = 10;
        config.min_connections = 1;
        config.timeout = Duration::from_secs(60);
        config.connection_timeout = Duration::from_secs(30);
        config.idle_timeout = Duration::from_secs(600); // 10 minutes
        config.max_lifetime = Duration::from_secs(3600); // 1 hour
        config.health_check_interval = Duration::from_secs(120); // 2 minutes
        Ok(config)
    }

    /// Get configuration preset for development
    #[staticmethod]
    fn development() -> PyResult<SqlConfig> {
        let mut config = SqlConfig::default();
        config.pool_size = 2;
        config.max_connections = 5;
        config.min_connections = 1;
        config.timeout = Duration::from_secs(30);
        config.connection_timeout = Duration::from_secs(10);
        config.idle_timeout = Duration::from_secs(300); // 5 minutes
        config.max_lifetime = Duration::secs(1800); // 30 minutes
        config.health_check_interval = Duration::from_secs(60);
        config.test_query = Some("SELECT 1".to_string());
        config.application_name = Some("deltalakedb-dev".to_string());
        Ok(config)
    }

    /// Get configuration preset for production
    #[staticmethod]
    fn production() -> PyResult<SqlConfig> {
        let mut config = SqlConfig::default();
        config.pool_size = 20;
        config.max_connections = 50;
        config.min_connections = 5;
        config.timeout = Duration::from_secs(60);
        config.connection_timeout = Duration::from_secs(15);
        config.idle_timeout = Duration::from_secs(1800); // 30 minutes
        config.max_lifetime = Duration::from_secs(7200); // 2 hours
        config.health_check_interval = Duration::from_secs(30);
        config.test_query = Some("SELECT 1".to_string());
        config.application_name = Some("deltalakedb".to_string());
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_config_default() {
        let config = SqlConfig::default();
        assert_eq!(config.database_type, "sqlite");
        assert_eq!(config.connection_string, ":memory:");
        assert_eq!(config.pool_size, 10);
        assert_eq!(config.timeout.as_secs(), 30);
        assert!(!config.ssl_enabled);
    }

    #[test]
    fn test_sql_config_new() {
        let config = SqlConfig::new(
            Some("postgres".to_string()),
            Some("postgresql://localhost:5432/test".to_string()),
            Some(20),
            Some(60),
            Some(true),
        );

        assert_eq!(config.database_type, "postgres");
        assert_eq!(config.connection_string, "postgresql://localhost:5432/test");
        assert_eq!(config.pool_size, 20);
        assert_eq!(config.timeout.as_secs(), 60);
        assert!(config.ssl_enabled);
    }

    #[test]
    fn test_sql_config_from_env() {
        // Set environment variables for testing
        std::env::set_var("DELTASQL_DATABASE_TYPE", "postgres");
        std::env::set_var("DELTASQL_POOL_SIZE", "15");
        std::env::set_var("DELTASQL_TIMEOUT", "45");
        std::env::set_var("DELTASQL_SSL_ENABLED", "true");

        let config = SqlConfig::from_env().unwrap();
        assert_eq!(config.database_type, "postgres");
        assert_eq!(config.pool_size, 15);
        assert_eq!(config.timeout.as_secs(), 45);
        assert!(config.ssl_enabled);

        // Clean up
        std::env::remove_var("DELTASQL_DATABASE_TYPE");
        std::env::remove_var("DELTASQL_POOL_SIZE");
        std::env::remove_var("DELTASQL_TIMEOUT");
        std::set_var("DELTASQL_SSL_ENABLED");
    }

    #[test]
    fn test_sql_config_to_dict() {
        let config = SqlConfig::new(
            Some("postgres".to_string()),
            Some("postgresql://localhost/test".to_string()),
            Some(10),
            Some(30),
            Some(false),
        );

        let dict = config.to_dict().unwrap();
        assert_eq!(dict.get("database_type"), Some(&"postgres".to_string()));
        assert_eq!(dict.get("pool_size"), Some(&"10".to_string()));
        assert_eq!(dict.get("timeout"), Some(&"30".to_string()));
        assert_eq!(dict.get("ssl_enabled"), Some(&"false".to_string()));
    }

    #[test]
    fn test_sql_config_validation() {
        let mut config = SqlConfig::default();
        config.pool_size = 0; // Invalid

        let errors = config.validate().unwrap();
        assert!(!errors.is_empty());
        assert!(errors.iter().any(|e| e.contains("Pool size must be greater than 0")));
    }

    #[test]
    fn test_database_presets() {
        let postgres_config = DatabasePresets::postgresql().unwrap();
        assert_eq!(postgres_config.database_type, "postgres");
        assert_eq!(postgres_config.pool_size, 20);

        let sqlite_config = DatabasePresets::sqlite().unwrap();
        assert_eq!(sqlite_config.database_type, "sqlite");
        assert_eq!(sqlite_config.pool_size, 1);

        let duckdb_config = DatabasePresets::duckdb().unwrap();
        assert_eq!(duckdb_config.database_type, "duckdb");
        assert_eq!(duckdb_config.pool_size, 10);
    }

    #[test]
    fn test_optimization_presets() {
        let high_perf = DatabasePresets::high_performance().unwrap();
        assert_eq!(high_perf.pool_size, 50);
        assert_eq!(high_perf.max_connections, 100);

        let low_resource = DatabasePresets::low_resource().unwrap();
        assert_eq!(low_resource.pool_size, 5);
        assert_eq!(low_resource.max_connections, 10);

        let dev_config = DatabasePresets::development().unwrap();
        assert_eq!(dev_config.pool_size, 2);
        assert_eq!(dev_config.max_connections, 5);

        let prod_config = DatabasePresets::production().unwrap();
        assert_eq!(prod_config.pool_size, 20);
        assert_eq!(prod_config.max_connections, 50);
    }

    #[test]
    fn test_config_modifications() {
        let mut config = SqlConfig::default();
        config.set_database_type("postgres".to_string());
        config.set_pool_size(25);
        config.set_timeout(45);
        config.set_ssl_enabled(true);

        assert_eq!(config.database_type, "postgres");
        assert_eq!(config.pool_size, 25);
        assert_eq!(config.timeout.as_secs(), 45);
        assert!(config.ssl_enabled);
    }
}