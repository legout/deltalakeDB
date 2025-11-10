//! Database connection and URI handling for migration.

use crate::error::{MigrationError, MigrationResult};
use sqlx::postgres::PgPool;
use uuid::Uuid;

/// Parsed target database URI.
#[derive(Debug, Clone)]
pub struct TargetUri {
    /// Database type (currently only "postgres")
    pub db_type: String,
    /// Database host
    pub host: String,
    /// Database port
    pub port: u16,
    /// Database name
    pub database: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
}

impl TargetUri {
    /// Parse a `deltasql://` URI.
    pub fn parse(uri: &str) -> MigrationResult<Self> {
        let parts: Vec<&str> = uri.split("://").collect();

        if parts.len() != 2 || parts[0] != "deltasql" {
            return Err(MigrationError::ConfigError(
                "URI must start with deltasql://".to_string(),
            ));
        }

        let db_part = parts[1];
        let segments: Vec<&str> = db_part.split('/').collect();

        if segments.len() < 4 {
            return Err(MigrationError::ConfigError(
                "URI format: deltasql://postgres/host/database/schema/table".to_string(),
            ));
        }

        let db_type = segments[0].to_string();
        let host = segments[1].to_string();
        let database = segments[2].to_string();
        let schema = segments[3].to_string();
        let table = segments[4].to_string();

        if db_type != "postgres" {
            return Err(MigrationError::ConfigError(format!(
                "Unsupported database type: {} (only 'postgres' supported)",
                db_type
            )));
        }

        Ok(TargetUri {
            db_type,
            host,
            port: 5432, // default PostgreSQL port
            database,
            schema,
            table,
        })
    }

    /// Build a PostgreSQL connection string.
    pub fn to_connection_string(&self) -> String {
        format!(
            "postgresql://{}:{}/{}",
            self.host, self.port, self.database
        )
    }

    /// Get table ID (UUID based on table name for consistency).
    pub fn table_id(&self) -> Uuid {
        // Generate stable UUID from schema and table name
        let combined = format!("{}.{}", self.schema, self.table);
        Uuid::new_v5(&Uuid::NAMESPACE_DNS, combined.as_bytes())
    }
}

/// Database connection handler.
pub struct DbHandler {
    pool: Option<PgPool>,
    uri: TargetUri,
}

impl DbHandler {
    /// Create a new database handler from target URI.
    pub fn new(target_uri: &str) -> MigrationResult<Self> {
        let uri = TargetUri::parse(target_uri)?;

        Ok(DbHandler { pool: None, uri })
    }

    /// Connect to the database.
    pub async fn connect(&mut self) -> MigrationResult<()> {
        let conn_str = self.uri.to_connection_string();

        match PgPool::connect(&conn_str).await {
            Ok(pool) => {
                self.pool = Some(pool);
                Ok(())
            }
            Err(e) => Err(MigrationError::DatabaseError(format!(
                "Failed to connect to database: {}",
                e
            ))),
        }
    }

    /// Get the connection pool.
    pub fn pool(&self) -> MigrationResult<&PgPool> {
        self.pool.as_ref().ok_or_else(|| {
            MigrationError::DatabaseError("Database not connected".to_string())
        })
    }

    /// Get the target URI.
    pub fn uri(&self) -> &TargetUri {
        &self.uri
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uri_parsing_valid() {
        let uri = TargetUri::parse("deltasql://postgres/localhost/mydb/public/mytable");
        assert!(uri.is_ok());

        let parsed = uri.unwrap();
        assert_eq!(parsed.db_type, "postgres");
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.database, "mydb");
        assert_eq!(parsed.schema, "public");
        assert_eq!(parsed.table, "mytable");
    }

    #[test]
    fn test_uri_parsing_invalid_format() {
        let uri = TargetUri::parse("s3://bucket/table");
        assert!(uri.is_err());
    }

    #[test]
    fn test_uri_parsing_missing_segments() {
        let uri = TargetUri::parse("deltasql://postgres/mydb");
        assert!(uri.is_err());
    }

    #[test]
    fn test_connection_string_generation() {
        let uri = TargetUri::parse("deltasql://postgres/localhost/mydb/public/mytable")
            .unwrap();
        let conn_str = uri.to_connection_string();
        assert!(conn_str.contains("localhost:5432"));
        assert!(conn_str.contains("mydb"));
    }

    #[test]
    fn test_table_id_stable() {
        let uri = TargetUri::parse("deltasql://postgres/localhost/mydb/public/mytable")
            .unwrap();
        let id1 = uri.table_id();
        let id2 = uri.table_id();
        assert_eq!(id1, id2);
    }
}
