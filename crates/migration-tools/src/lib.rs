//! Migration tools for importing Delta tables to SQL-backed metadata.
//!
//! This crate provides tools to import existing Delta Lake tables from object storage
//! into SQL-backed metadata backends, enabling adoption of SQL-backed Delta metadata
//! without losing transaction history.

#![warn(missing_docs)]

pub mod cli;
pub mod db_handler;
pub mod error;
pub mod error_handler;
pub mod importer;
pub mod incremental;
pub mod progress;
pub mod reader;
pub mod validator;

pub use db_handler::DbHandler;
pub use error::{MigrationError, MigrationResult};
pub use error_handler::{ErrorContext, ErrorRecovery, RecoveryStrategy};
pub use importer::{ImportConfig, ImportStats, TableImporter};
pub use incremental::IncrementalImporter;
pub use progress::ImportProgress;
pub use validator::MigrationValidator;
