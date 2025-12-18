//! deltalakedb-sql
//!
//! SQL engine adapters (Postgres, SQLite, DuckDB) behind a consistent interface.

#![warn(missing_docs)]

/// DuckDB-backed transaction log reader.
pub mod duckdb;
/// Postgres-backed transaction log reader/writer implementations.
pub mod postgres;
/// SQLite-backed transaction log reader.
pub mod sqlite;
/// Shared DeltaSQL URI parser.
pub mod uri;

pub use duckdb::DuckdbTxnLogReader;
pub use postgres::{
    MultiTableTransactionBuilder, PostgresConnectionOptions, PostgresTxnLogReader,
    PostgresTxnLogWriter,
};
pub use sqlite::SqliteTxnLogReader;
pub use uri::{DeltasqlEngine, DeltasqlUri, DeltasqlUriError};
