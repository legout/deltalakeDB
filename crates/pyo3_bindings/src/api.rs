//! High-level Python API for DeltaSQL

use pyo3::prelude::*;
use uuid::Uuid;

/// Create the api submodule
pub fn create_module(py: Python) -> PyResult<PyModule> {
    let m = PyModule::new(py, "api")?;

    m.add_class::<DeltaSQL>()?;
    m.add_class::<Table>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<TransactionBuilder>()?;

    Ok(m)
}

/// Main entry point for DeltaSQL connections and table operations.
#[pyclass]
pub struct DeltaSQL {
    uri: String,
    // Connection pool managed by Rust backend (not exposed to Python)
}

#[pymethods]
impl DeltaSQL {
    /// Create a DeltaSQL connection from a URI.
    ///
    /// Example:
    /// ```python
    /// conn = DeltaSQL("deltasql://postgres://user:pass@localhost/mydb")
    /// ```
    #[new]
    pub fn new(uri: String) -> PyResult<Self> {
        // Validate URI format
        if !uri.starts_with("deltasql://") {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Invalid URI scheme; expected 'deltasql://'",
            ));
        }

        Ok(DeltaSQL { uri })
    }

    /// Open a table for reading.
    ///
    /// Returns a Table object that can be used to read snapshots and time-travel.
    pub fn open_table(&self, name: &str) -> PyResult<Table> {
        if name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Table name cannot be empty",
            ));
        }

        Ok(Table {
            name: name.to_string(),
            connection_uri: self.uri.clone(),
            version_filter: None,
            timestamp_filter: None,
        })
    }

    /// List tables in the catalog.
    ///
    /// Optional schema parameter to filter by schema.
    pub fn list_tables(&self, schema: Option<String>) -> PyResult<Vec<String>> {
        // TODO: Implement once Rust adapters expose table discovery
        Ok(vec![])
    }

    /// Begin a single-table transaction.
    pub fn begin_transaction(&self, table_name: &str) -> PyResult<Transaction> {
        Ok(Transaction {
            table_name: table_name.to_string(),
            connection_uri: self.uri.clone(),
            actions: vec![],
        })
    }

    /// Create a transaction builder for multi-table commits.
    pub fn transaction_builder(&self) -> PyResult<TransactionBuilder> {
        Ok(TransactionBuilder {
            connection_uri: self.uri.clone(),
            staged_actions: std::collections::HashMap::new(),
        })
    }

    /// Get connection URI (for internal use)
    pub fn __repr__(&self) -> String {
        format!(
            "DeltaSQL(uri='{}...', ...)",
            &self.uri[0..std::cmp::min(50, self.uri.len())]
        )
    }
}

/// Represents a Delta table for read operations.
#[pyclass]
pub struct Table {
    name: String,
    connection_uri: String,
    version_filter: Option<i32>,
    timestamp_filter: Option<String>,
}

#[pymethods]
impl Table {
    /// Get current snapshot of the table.
    pub fn snapshot(&self) -> PyResult<crate::types::Snapshot> {
        // TODO: Implement once Rust adapters are integrated
        // For now, return a stub
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "Snapshot reading not yet implemented",
        ))
    }

    /// Time travel to a specific version.
    pub fn version(&self, v: i32) -> PyResult<Table> {
        Ok(Table {
            name: self.name.clone(),
            connection_uri: self.connection_uri.clone(),
            version_filter: Some(v),
            timestamp_filter: None,
        })
    }

    /// Time travel to a specific timestamp.
    pub fn at_timestamp(&self, ts: &str) -> PyResult<Table> {
        Ok(Table {
            name: self.name.clone(),
            connection_uri: self.connection_uri.clone(),
            version_filter: None,
            timestamp_filter: Some(ts.to_string()),
        })
    }

    /// Get current version number
    pub fn get_current_version(&self) -> PyResult<i32> {
        // TODO: Implement
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "Version retrieval not yet implemented",
        ))
    }

    /// Get table location
    pub fn get_location(&self) -> PyResult<String> {
        // TODO: Implement
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "Location retrieval not yet implemented",
        ))
    }

    pub fn __repr__(&self) -> String {
        let time_travel = if let Some(v) = self.version_filter {
            format!("@v{}", v)
        } else if let Some(ts) = &self.timestamp_filter {
            format!("@ts{}", ts)
        } else {
            String::new()
        };
        format!(
            "Table(name='{}'{}, uri='{}...', ...)",
            self.name,
            time_travel,
            &self.connection_uri[0..std::cmp::min(50, self.connection_uri.len())]
        )
    }
}

/// Represents a single-table transaction for writing.
#[pyclass]
pub struct Transaction {
    table_name: String,
    connection_uri: String,
    actions: Vec<String>, // Placeholder: in reality, strongly-typed actions
}

#[pymethods]
impl Transaction {
    /// Add a file to the transaction.
    pub fn add_file(&mut self, path: &str, size: i64, modification_time: i64) -> PyResult<()> {
        self.actions.push(format!(
            "add_file(path={}, size={}, mod_time={})",
            path, size, modification_time
        ));
        Ok(())
    }

    /// Remove a file from the transaction.
    pub fn remove_file(&mut self, path: &str) -> PyResult<()> {
        self.actions.push(format!("remove_file(path={})", path));
        Ok(())
    }

    /// Commit the transaction.
    ///
    /// Returns the new version number.
    pub fn commit(&self) -> PyResult<i32> {
        // TODO: Implement once Rust writers are integrated
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "Transaction commit not yet implemented",
        ))
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Transaction(table='{}', actions={}, uri='{}...')",
            self.table_name,
            self.actions.len(),
            &self.connection_uri[0..std::cmp::min(50, self.connection_uri.len())]
        )
    }
}

/// Transaction builder for multi-table atomic commits.
#[pyclass]
pub struct TransactionBuilder {
    connection_uri: String,
    staged_actions: std::collections::HashMap<String, Vec<String>>,
}

#[pymethods]
impl TransactionBuilder {
    /// Stage actions for a table.
    pub fn add_table_actions(&mut self, table_id: &str, actions: Vec<String>) -> PyResult<()> {
        self.staged_actions.insert(table_id.to_string(), actions);
        Ok(())
    }

    /// List staged tables.
    pub fn staged_tables(&self) -> PyResult<Vec<String>> {
        Ok(self.staged_actions.keys().cloned().collect())
    }

    /// Commit all staged tables atomically.
    pub fn commit(&self) -> PyResult<std::collections::HashMap<String, i32>> {
        // TODO: Implement once Rust multi-table support is integrated
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "Multi-table commit not yet implemented",
        ))
    }

    pub fn __repr__(&self) -> String {
        format!(
            "TransactionBuilder(tables={}, uri='{}...')",
            self.staged_actions.len(),
            &self.connection_uri[0..std::cmp::min(50, self.connection_uri.len())]
        )
    }
}
