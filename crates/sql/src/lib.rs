//! deltalakedb-sql
//!
//! SQL engine adapters (Postgres, SQLite, DuckDB) behind a consistent interface.
//!
//! ## Features
//!
//! - **Multi-table transactions**: Atomic operations across multiple Delta tables
//! - **ACID compliance**: Full transaction support with configurable isolation levels
//! - **Cross-table consistency**: Validation and enforcement of consistency rules
//! - **Ordered mirroring**: Optional data replication to secondary storage systems
//! - **Deadlock detection**: Automatic detection and resolution of transaction deadlocks
//! - **Performance optimization**: Configurable batching and caching strategies
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use deltalakedb_sql::{multi_table::MultiTableWriter, connection::DatabaseConfig};
//! use deltalakedb_core::writer::TxnLogWriterExt;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Setup database connection
//!     let config = DatabaseConfig::new("sqlite:///path/to/database.db")?;
//!     let connection = config.connect().await?;
//!     
//!     // Create multi-table writer
//!     let writer = MultiTableWriter::new(connection, None, Default::default());
//!     
//!     // Begin transaction
//!     let mut tx = writer.begin_transaction();
//!     
//!     // Add files to multiple tables
//!     tx.add_files("table1".to_string(), 0, vec![])?;
//!     tx.add_files("table2".to_string(), 0, vec![])?;
//!     
//!     // Commit atomically
//!     let result = writer.commit_transaction(tx).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Modules
//!
//! - [`connection`]: Database connection management and configuration
//! - [`multi_table`]: Multi-table transaction support and coordination
//! - [`mirror`]: Data mirroring and replication functionality
//! - [`schema`]: Schema management and validation
//! - [`writer`]: Single-table transaction writing operations
//!
//! ## Examples
//!
//! See the `examples/` directory for comprehensive usage examples:
//!
//! ```bash
//! cargo run --example multi_table_transactions
//! ```

#![warn(missing_docs)]

pub mod connection;
pub mod mirror;
pub mod multi_table;
pub mod schema;
pub mod writer;

#[cfg(test)]
mod isolation_test;

#[cfg(test)]
mod concurrent_acid_tests;

/// Placeholder module to ensure the crate compiles.
pub mod placeholder {
    /// Placeholder function.
    pub fn hello() -> &'static str {
        "Hello from deltalakedb-sql"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_hello() {
        assert_eq!(placeholder::hello(), "Hello from deltalakedb-sql");
    }
}
