//! deltalakedb-python: Python bindings for SQL-Backed Delta Lake metadata
//!
//! This crate provides Python bindings for the deltalakedb Rust library, enabling
//! seamless integration with the Python data ecosystem while maintaining compatibility
//! with existing deltalake workflows.

#![warn(missing_docs)]

use pyo3::prelude::*;

pub mod bindings;
pub mod error;
pub mod uri;
pub mod compatibility;
pub mod cli;
pub mod config;
pub mod connection;
pub mod types;
pub mod multi_table_transaction;
pub mod deltatable;
pub mod deltalake_integration;
pub mod write_operations;
pub mod dataclasses;
pub mod pydantic_models;

use error::{DeltaLakeError, DeltaLakeErrorKind};

#[pymodule]
fn deltalakedb(_py: Python, m: &PyModule) -> PyResult<()> {
    // Core bindings
    m.add_class::<bindings::Table>()?;
    m.add_class::<bindings::Commit>()?;
    m.add_class::<bindings::File>()?;
    m.add_class::<bindings::Protocol>()?;
    m.add_class::<bindings::Metadata>()?;

    // Connection and configuration
    m.add_class::<uri::DeltaSqlUri>()?;
    m.add_class::<config::SqlConfig>()?;
    m.add_class::<connection::SqlConnection>()?;
    m.add_class::<connection::ConnectionPool>()?;
    m.add_class::<connection::TransactionContext>()?;

    // Error handling
    m.add_class::<error::DeltaLakeError>()?;
    m.add_class::<error::DeltaLakeErrorKind>()?;

    // Compatibility and CLI
    m.add_class::<compatibility::DeltaLakeCompatibility>()?;
    m.add_class::<compatibility::DeltaLakeBridge>()?;
    m.add_class::<cli::CliResult>()?;
    m.add_class::<cli::CliUtils>()?;

    // Type system
    m.add_class::<types::DeltaDataType>()?;
    m.add_class::<types::SchemaField>()?;
    m.add_class::<types::TableSchema>()?;
    m.add_class::<types::TypeConverter>()?;

    // Multi-table transactions
    m.add_class::<multi_table_transaction::PyMultiTableTransaction>()?;
    m.add_class::<multi_table_transaction::PyTransactionOptions>()?;
    m.add_class::<multi_table_transaction::PyTransactionResult>()?;
    m.add_class::<multi_table_transaction::PyAction>()?;
    m.add_class::<multi_table_transaction::PyTransactionContext>()?;

    // DeltaTable compatibility
    m.add_class::<deltatable::DeltaTable>()?;
    m.add_class::<deltatable::PyTableSnapshot>()?;

    // DeltaLake integration
    m.add_class::<deltalake_integration::DeltaLakeSchemeRegistrar>()?;
    m.add_class::<deltalake_integration::DeltaLakeBridge>()?;

    // Write operations
    m.add_class::<write_operations::WriteMode>()?;
    m.add_class::<write_operations::WriteConfig>()?;
    m.add_class::<write_operations::FileAction>()?;
    m.add_class::<write_operations::WriteResult>()?;
    m.add_class::<write_operations::DeltaWriter>()?;
    m.add_class::<write_operations::WriteTransaction>()?;
    m.add_class::<write_operations::WriteErrorHandler>()?;

    // Dataclasses
    m.add_class::<dataclasses::TableData>()?;
    m.add_class::<dataclasses::CommitData>()?;
    m.add_class::<dataclasses::FileData>()?;
    m.add_class::<dataclasses::ProtocolData>()?;
    m.add_class::<dataclasses::MetadataData>()?;
    m.add_class::<dataclasses::DataClassFactory>()?;

    // Pydantic models and validation
    m.add_class::<pydantic_models::PydanticModels>()?;
    m.add_class::<pydantic_models::ConfigFactory>()?;
    m.add_class::<pydantic_models::ValidationUtils>()?;
    m.add_class::<pydantic_models::ConfigLoader>()?;

    // Functions
    m.add_function(wrap_pyfunction!(bindings::connect_to_table))?;
    m.add_function(wrap_pyfunction!(bindings::create_table))?;
    m.add_function(wrap_pyfunction!(bindings::list_tables))?;

    m.add_function(wrap_pyfunction!(uri::parse_uri))?;
    m.add_function(wrap_pyfunction!(uri::create_uri))?;
    m.add_function(wrap_pyfunction!(uri::validate_uri))?;
    m.add_function(wrap_pyfunction!(uri::get_database_type))?;
    m.add_function(wrap_pyfunction!(uri::get_connection_string))?;
    m.add_function(wrap_pyfunction!(uri::get_table_name))?;

    m.add_function(wrap_pyfunction!(cli::quick_connect))?;
    m.add_function(wrap_pyfunction!(cli::list_tables_cli))?;
    m.add_function(wrap_pyfunction!(cli::create_table_cli))?;

  m.add_function(wrap_pyfunction!(multi_table_transaction::create_transaction_context))?;
    m.add_function(wrap_pyfunction!(multi_table_transaction::create_transaction_context_with_options))?;
    m.add_function(wrap_pyfunction!(multi_table_transaction::execute_in_transaction))?;

  m.add_function(wrap_pyfunction!(deltatable::load_table))?;
    m.add_function(wrap_pyfunction!(deltatable::is_delta_table))?;
    m.add_function(wrap_pyfunction!(deltatable::get_latest_version))?;
    m.add_function(wrap_pyfunction!(deltatable::get_metadata))?;

  m.add_function(wrap_pyfunction!(deltalake_integration::patch_deltalake))?;
    m.add_function(wrap_pyfunction!(deltalake_integration::auto_patch))?;
    m.add_function(wrap_pyfunction!(deltalake_integration::handle_deltasql_uri))?;
    m.add_function(wrap_pyfunction!(deltalake_integration::register_uri_handler))?;
    m.add_function(wrap_pyfunction!(deltalake_integration::check_uri_compatibility))?;

    // Write operations functions
    m.add_function(wrap_pyfunction!(write_operations::write_deltalake))?;

    // Dataclass validation functions
    m.add_function(wrap_pyfunction!(dataclasses::validate_table_data))?;
    m.add_function(wrap_pyfunction!(dataclasses::validate_commit_data))?;
    m.add_function(wrap_pyfunction!(dataclasses::validate_file_data))?;

    // Constants
    m.add("version", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}