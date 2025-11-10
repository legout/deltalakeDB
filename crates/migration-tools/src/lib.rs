//! Migration tools for importing Delta tables to SQL-backed metadata.
//!
//! This crate provides tools to import existing Delta Lake tables from object storage
//! into SQL-backed metadata backends, enabling adoption of SQL-backed Delta metadata
//! without losing transaction history.

#![warn(missing_docs)]

pub mod error;
pub mod importer;
pub mod reader;
pub mod validator;

pub use error::{MigrationError, MigrationResult};
pub use importer::TableImporter;
pub use validator::MigrationValidator;
