//! Python exception types mapping from Rust error domain

use pyo3::prelude::*;
use pyo3::exceptions;

/// Create the errors submodule
pub fn create_module(py: Python) -> PyResult<PyModule> {
    let m = PyModule::new(py, "errors")?;

    // Base exceptions
    m.add_class::<ConcurrencyError>()?;
    m.add_class::<ConnectionError>()?;
    m.add_class::<ValidationError>()?;
    m.add_class::<NotFoundError>()?;

    Ok(m)
}

/// Raised when a transaction encounters a concurrency conflict (version mismatch).
#[pyclass(extends=exceptions::PyException)]
pub struct ConcurrencyError;

#[pymethods]
impl ConcurrencyError {
    /// Create a concurrency error with a message.
    #[new]
    pub fn new(message: String) -> (Self, PyErr) {
        let err = PyErr::new::<exceptions::PyException, _>(message);
        (ConcurrencyError, err)
    }
}

/// Raised when a database connection fails.
#[pyclass(extends=exceptions::PyException)]
pub struct ConnectionError;

#[pymethods]
impl ConnectionError {
    /// Create a connection error with a message.
    #[new]
    pub fn new(message: String) -> (Self, PyErr) {
        let err = PyErr::new::<exceptions::PyException, _>(message);
        (ConnectionError, err)
    }
}

/// Raised when input validation fails (e.g., invalid URI).
#[pyclass(extends=exceptions::PyException)]
pub struct ValidationError;

#[pymethods]
impl ValidationError {
    /// Create a validation error with a message.
    #[new]
    pub fn new(message: String) -> (Self, PyErr) {
        let err = PyErr::new::<exceptions::PyException, _>(message);
        (ValidationError, err)
    }
}

/// Raised when a resource (table, version) is not found.
#[pyclass(extends=exceptions::PyException)]
pub struct NotFoundError;

#[pymethods]
impl NotFoundError {
    /// Create a not-found error with a message.
    #[new]
    pub fn new(message: String) -> (Self, PyErr) {
        let err = PyErr::new::<exceptions::PyException, _>(message);
        (NotFoundError, err)
    }
}
