//! Database connection management for different SQL backends.

use std::sync::Arc;

/// Database connection types supported by the system.
#[derive(Debug, Clone)]
pub enum DatabaseConnection {
    /// PostgreSQL connection pool (placeholder)
    Postgres(()),
    /// SQLite connection pool (placeholder)
    Sqlite(()),
    /// DuckDB connection (placeholder)
    DuckDb(()),
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
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "sqlite::memory:".to_string(),
            max_connections: 10,
            min_connections: 1,
            connect_timeout_secs: 30,
            idle_timeout_secs: 600,
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

    /// Create a configuration for PostgreSQL.
    pub fn postgres(host: &str, port: u16, database: &str, user: &str, password: &str) -> Self {
        let url = format!(
            "postgres://{}:{}@{}:{}/{}",
            user, password, host, port, database
        );
        Self::new(url)
    }

    /// Connect to the database.
    pub async fn connect(&self) -> Result<Arc<DatabaseConnection>, Box<dyn std::error::Error + Send + Sync>> {
        let connection = if self.url.starts_with("postgres://") {
            Arc::new(DatabaseConnection::Postgres(()))
        } else if self.url.starts_with("sqlite:") {
            Arc::new(DatabaseConnection::Sqlite(()))
        } else if self.url.starts_with("duckdb:") {
            Arc::new(DatabaseConnection::DuckDb(()))
        } else {
            return Err("Unsupported database URL".into());
        };

        Ok(connection)
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
    }

    #[test]
    fn test_postgres_config() {
        let config = DatabaseConfig::postgres("localhost", 5432, "testdb", "user", "pass");
        assert_eq!(config.url, "postgres://user:pass@localhost:5432/testdb");
    }
}