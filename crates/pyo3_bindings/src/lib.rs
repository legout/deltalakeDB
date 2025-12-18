//! delkalakedb Python bindings via pyo3
//!
//! This crate provides Python bindings for the Rust core libraries:
//! - deltalakedb-core: Domain models and traits
//! - deltalakedb-sql: SQL adapters (Postgres, SQLite, DuckDB)
//! - deltalakedb-mirror: Mirror engine for Delta compatibility
//! - deltalakedb-observability: Telemetry

#![warn(missing_docs)]

use pyo3::prelude::*;

/// Python module name
#[pymodule]
fn delkalakedb(py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__version__", "0.0.0")?;

    // Submodules
    m.add_submodule(errors::create_module(py)?)?;
    m.add_submodule(types::create_module(py)?)?;
    m.add_submodule(api::create_module(py)?)?;

    Ok(())
}

pub mod errors;
pub mod types;
pub mod api;
