//! Database connection management for different SQL backends.

use std::sync::Arc;
use std::time::Duration;
use std::str::FromStr;
use crate::schema::{DatabaseEngine, SchemaConfig, SchemaManager};

/// Database connection pool wrapper for different SQL backends.
#[derive(Debug)]
pub enum DatabaseConnection {
    /// PostgreSQL connection pool
    Postgres(sqlx::Pool<sqlx::Postgres>),
    /// SQLite connection pool
    Sqlite(sqlx::Pool<sqlx::Sqlite>),
    /// DuckDB connection pool
    DuckDb(sqlx::Pool<sqlx::Any>),
}

impl DatabaseConnection {
    /// Get the database engine type.
    pub fn engine(&self) -> DatabaseEngine {
        match self {
            DatabaseConnection::Postgres(_) => DatabaseEngine::Postgres,
            DatabaseConnection::Sqlite(_) => DatabaseEngine::Sqlite,
            DatabaseConnection::DuckDb(_) => DatabaseEngine::DuckDB,
        }
    }
    
    /// Get the underlying connection pool as an Any pool for generic operations.
    pub fn as_any_pool(&self) -> Result<sqlx::Pool<sqlx::Any>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            DatabaseConnection::Postgres(pool) => {
                // Convert Postgres pool to Any pool
                let any_pool = sqlx::any::AnyPool::connect_with(
                    sqlx::any::AnyConnectOptions::from_str(&format!("postgres://{}", pool.connect_options().get_database().unwrap_or_default()))?
                ).await?;
                Ok(any_pool)
            }
            DatabaseConnection::Sqlite(pool) => {
                // Convert SQLite pool to Any pool
                let any_pool = sqlx::any::AnyPool::connect_with(
                    sqlx::any::AnyConnectOptions::from_str(&format!("sqlite://{}", pool.connect_options().get_database().unwrap_or_default()))?
                ).await?;
                Ok(any_pool)
            }
            DatabaseConnection::DuckDb(pool) => Ok(pool.clone()),
        }
    }
    
    /// Execute a health check on the connection.
    pub async fn health_check(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            DatabaseConnection::Postgres(pool) => {
                sqlx::query("SELECT 1")
                    .fetch_one(pool)
                    .await?;
            }
            DatabaseConnection::Sqlite(pool) => {
                sqlx::query("SELECT 1")
                    .fetch_one(pool)
                    .await?;
            }
            DatabaseConnection::DuckDb(pool) => {
                sqlx::query("SELECT 1")
                    .fetch_one(pool)
                    .await?;
            }
        }
        Ok(())
    }
    
    /// Get connection pool statistics.
    pub fn pool_stats(&self) -> ConnectionPoolStats {
        match self {
            DatabaseConnection::Postgres(pool) => ConnectionPoolStats {
                size: pool.size(),
                idle: pool.num_idle(),
                active: pool.size() - pool.num_idle(),
            },
            DatabaseConnection::Sqlite(pool) => ConnectionPoolStats {
                size: pool.size(),
                idle: pool.num_idle(),
                active: pool.size() - pool.num_idle(),
            },
            DatabaseConnection::DuckDb(pool) => ConnectionPoolStats {
                size: pool.size(),
                idle: pool.num_idle(),
                active: pool.size() - pool.num_idle(),
            },
        }
    }
    
    /// Close the connection pool.
    pub async fn close(&self) {
        match self {
            DatabaseConnection::Postgres(pool) => pool.close().await,
            DatabaseConnection::Sqlite(pool) => pool.close().await,
            DatabaseConnection::DuckDb(pool) => pool.close().await,
        }
    }
}

/// Configuration for database connections.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Database connection URL
    pub url: String,
    /// Maximum number of connections in the pool
    pub max_connections: u32,
    /// Minimum number of connections in the pool
    pub min_connections: u32,
    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,
    /// Idle timeout in seconds
    pub idle_timeout_secs: u64,
    /// Maximum lifetime of a connection in seconds
    pub max_lifetime_secs: Option<u64>,
    /// Whether to enable SSL for PostgreSQL connections
    pub ssl_mode: Option<SslMode>,
    /// Additional connection options
    pub options: std::collections::HashMap<String, String>,
}

/// SSL mode for PostgreSQL connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SslMode {
    /// Disable SSL
    Disable,
    /// Prefer SSL but allow non-SSL
    Prefer,
    /// Require SSL
    Require,
    /// Verify CA certificate
    VerifyCa,
    /// Verify full certificate chain
    VerifyFull,
}

impl SslMode {
    /// Get the SSL mode as a connection string parameter.
    pub fn as_str(&self) -> &'static str {
        match self {
            SslMode::Disable => "disable",
            SslMode::Prefer => "prefer",
            SslMode::Require => "require",
            SslMode::VerifyCa => "verify-ca",
            SslMode::VerifyFull => "verify-full",
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "sqlite::memory:".to_string(),
            max_connections: 10,
            min_connections: 1,
            connect_timeout_secs: 30,
            idle_timeout_secs: 600,
            max_lifetime_secs: Some(1800), // 30 minutes
            ssl_mode: None,
            options: std::collections::HashMap::new(),
        }
    }
}

impl DatabaseConfig {
    /// Create a new database configuration.
    pub fn new(url: String) -> Self {
        Self {
            url,
            ..Default::default()
        }
    }

    /// Create a configuration for in-memory SQLite.
    pub fn sqlite_memory() -> Self {
        Self::new("sqlite::memory:".to_string())
    }

    /// Create a configuration for file-based SQLite.
    pub fn sqlite_file(path: &str) -> Self {
        Self::new(format!("sqlite:{}", path))
    }

    /// Create a configuration for PostgreSQL.
    pub fn postgres(host: &str, port: u16, database: &str, user: &str, password: &str) -> Self {
        let mut config = Self::new(format!(
            "postgres://{}:{}@{}:{}/{}",
            user, password, host, port, database
        ));
        config.ssl_mode = Some(SslMode::Prefer);
        config
    }

    /// Create a configuration for DuckDB.
    pub fn duckdb(path: &str) -> Self {
        Self::new(format!("duckdb:{}", path))
    }

    /// Create a configuration for in-memory DuckDB.
    pub fn duckdb_memory() -> Self {
        Self::new("duckdb://:memory:".to_string())
    }

    /// Set SSL mode for PostgreSQL connections.
    pub fn ssl_mode(mut self, mode: SslMode) -> Self {
        self.ssl_mode = Some(mode);
        self
    }

    /// Set maximum connections in the pool.
    pub fn max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    /// Set connection timeout.
    pub fn connect_timeout(mut self, timeout_secs: u64) -> Self {
        self.connect_timeout_secs = timeout_secs;
        self
    }

    /// Add a custom connection option.
    pub fn option(mut self, key: String, value: String) -> Self {
        self.options.insert(key, value);
        self
    }

    /// Connect to the database and create a connection pool.
    pub async fn connect(&self) -> Result<Arc<DatabaseConnection>, Box<dyn std::error::Error + Send + Sync>> {
        let connection = if self.url.starts_with("postgres://") {
            let mut opts = sqlx::postgres::PgConnectOptions::from_str(&self.url)?;
            
            // Apply SSL mode if specified
            if let Some(ssl_mode) = &self.ssl_mode {
                opts = opts.ssl_mode(match ssl_mode {
                    SslMode::Disable => sqlx::postgres::PgSslMode::Disable,
                    SslMode::Prefer => sqlx::postgres::PgSslMode::Prefer,
                    SslMode::Require => sqlx::postgres::PgSslMode::Require,
                    SslMode::VerifyCa => sqlx::postgres::PgSslMode::VerifyCa,
                    SslMode::VerifyFull => sqlx::postgres::PgSslMode::VerifyFull,
                });
            }

            // Apply custom options
            for (key, value) in &self.options {
                opts = opts.options(&[(key.as_str(), value.as_str())]);
            }

            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(self.max_connections)
                .min_connections(self.min_connections)
                .acquire_timeout(Duration::from_secs(self.connect_timeout_secs))
                .idle_timeout(Duration::from_secs(self.idle_timeout_secs))
                .max_lifetime(self.max_lifetime_secs.map(Duration::from_secs))
                .connect_with(opts)
                .await?;

            Arc::new(DatabaseConnection::Postgres(pool))

        } else if self.url.starts_with("sqlite:") {
            let mut opts = sqlx::sqlite::SqliteConnectOptions::from_str(&self.url)?;
            
            // Apply custom options
            for (key, value) in &self.options {
                opts = opts.options(&[(key.as_str(), value.as_str())]);
            }

            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(self.max_connections)
                .min_connections(self.min_connections)
                .acquire_timeout(Duration::from_secs(self.connect_timeout_secs))
                .idle_timeout(Duration::from_secs(self.idle_timeout_secs))
                .max_lifetime(self.max_lifetime_secs.map(Duration::from_secs))
                .connect_with(opts)
                .await?;

            Arc::new(DatabaseConnection::Sqlite(pool))

        } else if self.url.starts_with("duckdb:") {
            let path = self.url.strip_prefix("duckdb:").unwrap_or(&self.url);
            let pool = sqlx::any::AnyPoolOptions::new()
                .max_connections(self.max_connections)
                .min_connections(self.min_connections)
                .acquire_timeout(Duration::from_secs(self.connect_timeout_secs))
                .idle_timeout(Duration::from_secs(self.idle_timeout_secs))
                .max_lifetime(self.max_lifetime_secs.map(Duration::from_secs))
                .connect(&format!("sqlite:{}", path)) // DuckDB uses SQLite driver in sqlx
                .await?;

            Arc::new(DatabaseConnection::DuckDb(pool))

        } else {
            return Err(format!("Unsupported database URL: {}", self.url).into());
        };

        Ok(connection)
    }

    /// Connect and initialize the Delta Lake schema.
    pub async fn connect_and_initialize_schema(&self) -> Result<DatabaseConnectionManager, Box<dyn std::error::Error + Send + Sync>> {
        let connection = self.connect().await?;
        let engine = connection.engine();
        let schema_config = SchemaConfig::default();
        let schema_manager = SchemaManager::new(engine, schema_config);
        
        // Initialize schema if needed
        let pool = connection.as_any_pool()?;
        let current_version = schema_manager.get_current_version(&pool).await?.unwrap_or(0);
        
        if current_version == 0 {
            schema_manager.initialize_schema(&pool).await?;
        } else {
            // Apply any pending migrations
            schema_manager.migrate_to_version(&pool, 3).await?; // Target latest version
        }
        
        Ok(DatabaseConnectionManager {
            connection,
            schema_manager,
        })
    }
}

/// Connection pool statistics.
#[derive(Debug, Clone)]
pub struct ConnectionPoolStats {
    /// Total number of connections in the pool
    pub size: u32,
    /// Number of idle connections
    pub idle: u32,
    /// Number of active connections
    pub active: u32,
}

/// Database connection manager with schema management.
#[derive(Debug)]
pub struct DatabaseConnectionManager {
    connection: Arc<DatabaseConnection>,
    schema_manager: SchemaManager,
}

impl DatabaseConnectionManager {
    /// Get the underlying connection.
    pub fn connection(&self) -> &Arc<DatabaseConnection> {
        &self.connection
    }
    
    /// Get the schema manager.
    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }
    
    /// Get the database engine.
    pub fn engine(&self) -> DatabaseEngine {
        self.connection.engine()
    }
    
    /// Perform a health check.
    pub async fn health_check(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.connection.health_check().await?;
        
        // Validate schema integrity
        let pool = self.connection.as_any_pool()?;
        let is_valid = self.schema_manager.validate_schema(&pool).await?;
        
        if !is_valid {
            return Err("Schema validation failed".into());
        }
        
        Ok(())
    }
    
    /// Get connection pool statistics.
    pub fn pool_stats(&self) -> ConnectionPoolStats {
        self.connection.pool_stats()
    }
    
    /// Refresh materialized views if applicable.
    pub async fn refresh_views(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.connection.as_any_pool()?;
        self.schema_manager.refresh_materialized_views(&pool).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sqlite_memory_config() {
        let config = DatabaseConfig::sqlite_memory();
        assert_eq!(config.url, "sqlite::memory:");
        assert_eq!(config.max_connections, 10);
        
        // Test connection
        let connection = config.connect().await.unwrap();
        assert!(matches!(&*connection, DatabaseConnection::Sqlite(_)));
        
        // Test health check
        connection.health_check().await.unwrap();
    }

    #[test]
    fn test_postgres_config() {
        let config = DatabaseConfig::postgres("localhost", 5432, "testdb", "user", "pass");
        assert_eq!(config.url, "postgres://user:pass@localhost:5432/testdb");
        assert_eq!(config.ssl_mode, Some(SslMode::Prefer));
    }

    #[test]
    fn test_duckdb_config() {
        let config = DatabaseConfig::duckdb_memory();
        assert_eq!(config.url, "duckdb://:memory:");
        
        let file_config = DatabaseConfig::duckdb("/tmp/test.duckdb");
        assert_eq!(file_config.url, "duckdb:/tmp/test.duckdb");
    }

    #[test]
    fn test_ssl_mode() {
        assert_eq!(SslMode::Disable.as_str(), "disable");
        assert_eq!(SslMode::Prefer.as_str(), "prefer");
        assert_eq!(SslMode::Require.as_str(), "require");
        assert_eq!(SslMode::VerifyCa.as_str(), "verify-ca");
        assert_eq!(SslMode::VerifyFull.as_str(), "verify-full");
    }

    #[test]
    fn test_config_builder() {
        let config = DatabaseConfig::postgres("localhost", 5432, "testdb", "user", "pass")
            .ssl_mode(SslMode::Require)
            .max_connections(20)
            .connect_timeout(60)
            .option("application_name".to_string(), "deltalakedb".to_string());
            
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.connect_timeout_secs, 60);
        assert_eq!(config.ssl_mode, Some(SslMode::Require));
        assert_eq!(config.options.get("application_name"), Some(&"deltalakedb".to_string()));
    }

    #[tokio::test]
    async fn test_connection_pool_stats() {
        let config = DatabaseConfig::sqlite_memory().max_connections(5);
        let connection = config.connect().await.unwrap();
        let stats = connection.pool_stats();
        
        assert_eq!(stats.size, 5); // Pool size is set to max
        assert!(stats.idle >= 0);
        assert!(stats.active >= 0);
    }
}