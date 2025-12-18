//! Python bindings for URI types

use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Create the uri submodule
pub fn create_module(py: Python) -> PyResult<PyModule> {
    let m = PyModule::new(py, "uri")?;

    m.add_class::<DeltasqlEngine>()?;
    m.add_class::<DeltasqlUri>()?;

    Ok(m)
}

/// Enumeration of supported database engines
#[pyclass]
#[derive(Clone)]
pub enum DeltasqlEngine {
    /// Postgres backend
    Postgres,
    /// SQLite backend
    Sqlite,
    /// DuckDB backend
    Duckdb,
}

#[pymethods]
impl DeltasqlEngine {
    /// String representation
    pub fn __repr__(&self) -> String {
        match self {
            DeltasqlEngine::Postgres => "DeltasqlEngine.Postgres".to_string(),
            DeltasqlEngine::Sqlite => "DeltasqlEngine.Sqlite".to_string(),
            DeltasqlEngine::Duckdb => "DeltasqlEngine.Duckdb".to_string(),
        }
    }

    /// Get engine name
    pub fn name(&self) -> String {
        match self {
            DeltasqlEngine::Postgres => "postgres".to_string(),
            DeltasqlEngine::Sqlite => "sqlite".to_string(),
            DeltasqlEngine::Duckdb => "duckdb".to_string(),
        }
    }
}

/// Parsed DeltaSQL URI with connection details
#[pyclass]
pub struct DeltasqlUri {
    /// Engine type (postgres, sqlite, duckdb)
    pub engine: DeltasqlEngine,
    /// Connection string or path
    pub connection_string: String,
    /// Table name or schema.table
    pub table: String,
}

#[pymethods]
impl DeltasqlUri {
    /// Parse a DeltaSQL URI
    ///
    /// Examples:
    /// - `deltasql://postgres://user:pass@localhost/db?table=my_table`
    /// - `deltasql://sqlite:///path/to/db.sqlite?table=my_table`
    /// - `deltasql://duckdb:///path/to/db?table=my_table`
    #[new]
    pub fn parse(uri: String) -> PyResult<Self> {
        // Basic URI parsing - this will be replaced with actual parsing from deltalakedb-sql
        if !uri.starts_with("deltasql://") {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "URI must start with 'deltasql://'",
            ));
        }

        let remainder = uri.strip_prefix("deltasql://").unwrap();

        // Determine engine from the URI
        let (engine, conn_rest) = if remainder.starts_with("postgres://") {
            (
                DeltasqlEngine::Postgres,
                remainder.strip_prefix("postgres://").unwrap(),
            )
        } else if remainder.starts_with("sqlite://") {
            (
                DeltasqlEngine::Sqlite,
                remainder.strip_prefix("sqlite://").unwrap(),
            )
        } else if remainder.starts_with("duckdb://") {
            (
                DeltasqlEngine::Duckdb,
                remainder.strip_prefix("duckdb://").unwrap(),
            )
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Unknown engine. Use postgres://, sqlite://, or duckdb://",
            ));
        };

        // Split on ? to get query parameters
        let (connection_string, table) = if let Some(pos) = conn_rest.find('?') {
            let (conn, query) = conn_rest.split_at(pos);
            let table = query
                .strip_prefix("?table=")
                .unwrap_or("unknown")
                .to_string();
            (conn.to_string(), table)
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "URI must include ?table=<table_name> query parameter",
            ));
        };

        Ok(DeltasqlUri {
            engine,
            connection_string,
            table,
        })
    }

    /// String representation
    pub fn __repr__(&self) -> String {
        format!(
            "DeltasqlUri(engine={}, connection='{}...', table='{}')",
            self.engine.name(),
            &self.connection_string[0..std::cmp::min(20, self.connection_string.len())],
            self.table
        )
    }

    /// Get engine
    #[getter]
    pub fn get_engine(&self) -> DeltasqlEngine {
        self.engine.clone()
    }

    /// Get connection string
    #[getter]
    pub fn get_connection_string(&self) -> String {
        self.connection_string.clone()
    }

    /// Get table name
    #[getter]
    pub fn get_table(&self) -> String {
        self.table.clone()
    }
}
