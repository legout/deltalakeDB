//! deltalakedb-sql
//!
//! SQL engine adapters (Postgres, SQLite, DuckDB) behind a consistent interface.

#![warn(missing_docs)]

/// Postgres-backed transaction log reader implementation.
pub mod postgres;

pub use postgres::{PostgresConnectionOptions, PostgresTxnLogReader};
