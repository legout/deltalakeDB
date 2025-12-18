//! Python type bindings for core domain models

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde_json::{json, Value};
use uuid::Uuid;

/// Create the types submodule
pub fn create_module(py: Python) -> PyResult<PyModule> {
    let m = PyModule::new(py, "types")?;

    m.add_class::<ActiveFile>()?;
    m.add_class::<RemovedFile>()?;
    m.add_class::<Protocol>()?;
    m.add_class::<TableMetadata>()?;
    m.add_class::<Snapshot>()?;

    Ok(m)
}

/// Represents an active file in a Delta table.
#[pyclass]
pub struct ActiveFile {
    /// File path
    pub path: String,
    /// File size in bytes
    pub size: i64,
    /// Modification time in milliseconds since UNIX_EPOCH
    pub modification_time: i64,
    /// Partition values as JSON
    pub partition_values: Option<String>,
    /// Data change flag
    pub data_change: bool,
}

#[pymethods]
impl ActiveFile {
    /// Create a new active file.
    #[new]
    pub fn new(
        path: String,
        size: i64,
        modification_time: i64,
        partition_values: Option<String>,
        data_change: bool,
    ) -> Self {
        Self {
            path,
            size,
            modification_time,
            partition_values,
            data_change,
        }
    }

    /// String representation
    pub fn __repr__(&self) -> String {
        format!(
            "ActiveFile(path='{}', size={}, modification_time={})",
            self.path, self.size, self.modification_time
        )
    }

    /// Get path
    #[getter]
    pub fn get_path(&self) -> String {
        self.path.clone()
    }

    /// Get size
    #[getter]
    pub fn get_size(&self) -> i64 {
        self.size
    }

    /// Get modification time
    #[getter]
    pub fn get_modification_time(&self) -> i64 {
        self.modification_time
    }

    /// Get partition values
    #[getter]
    pub fn get_partition_values(&self) -> Option<String> {
        self.partition_values.clone()
    }

    /// Get data change flag
    #[getter]
    pub fn get_data_change(&self) -> bool {
        self.data_change
    }
}

/// Represents a removed file in a Delta table.
#[pyclass]
pub struct RemovedFile {
    /// File path
    pub path: String,
    /// Deletion time in milliseconds since UNIX_EPOCH
    pub deletion_time: i64,
    /// Partition values as JSON
    pub partition_values: Option<String>,
    /// Data change flag
    pub data_change: bool,
}

#[pymethods]
impl RemovedFile {
    /// Create a new removed file.
    #[new]
    pub fn new(
        path: String,
        deletion_time: i64,
        partition_values: Option<String>,
        data_change: bool,
    ) -> Self {
        Self {
            path,
            deletion_time,
            partition_values,
            data_change,
        }
    }

    /// String representation
    pub fn __repr__(&self) -> String {
        format!(
            "RemovedFile(path='{}', deletion_time={})",
            self.path, self.deletion_time
        )
    }

    /// Get path
    #[getter]
    pub fn get_path(&self) -> String {
        self.path.clone()
    }

    /// Get deletion time
    #[getter]
    pub fn get_deletion_time(&self) -> i64 {
        self.deletion_time
    }

    /// Get partition values
    #[getter]
    pub fn get_partition_values(&self) -> Option<String> {
        self.partition_values.clone()
    }

    /// Get data change flag
    #[getter]
    pub fn get_data_change(&self) -> bool {
        self.data_change
    }
}

/// Represents table protocol version constraints.
#[pyclass]
pub struct Protocol {
    /// Minimum reader version required to read this table
    pub min_reader_version: u32,
    /// Minimum writer version required to write to this table
    pub min_writer_version: u32,
}

#[pymethods]
impl Protocol {
    /// Create a new protocol specification.
    #[new]
    pub fn new(min_reader_version: u32, min_writer_version: u32) -> Self {
        Self {
            min_reader_version,
            min_writer_version,
        }
    }

    /// String representation
    pub fn __repr__(&self) -> String {
        format!(
            "Protocol(min_reader_version={}, min_writer_version={})",
            self.min_reader_version, self.min_writer_version
        )
    }

    /// Get minimum reader version
    #[getter]
    pub fn get_min_reader_version(&self) -> u32 {
        self.min_reader_version
    }

    /// Get minimum writer version
    #[getter]
    pub fn get_min_writer_version(&self) -> u32 {
        self.min_writer_version
    }
}

/// Represents table metadata (schema, properties).
#[pyclass]
pub struct TableMetadata {
    /// Schema as JSON string (Arrow schema in JSON format)
    pub schema_json: String,
    /// Partition columns
    pub partition_columns: Vec<String>,
    /// Table properties as JSON
    pub configuration: String,
}

#[pymethods]
impl TableMetadata {
    /// Create new table metadata.
    #[new]
    pub fn new(schema_json: String, partition_columns: Vec<String>, configuration: String) -> Self {
        Self {
            schema_json,
            partition_columns,
            configuration,
        }
    }

    /// String representation
    pub fn __repr__(&self) -> String {
        format!(
            "TableMetadata(partitions={}, schema_len={})",
            self.partition_columns.len(),
            self.schema_json.len()
        )
    }

    /// Get schema as JSON string
    #[getter]
    pub fn get_schema_json(&self) -> String {
        self.schema_json.clone()
    }

    /// Get partition columns
    #[getter]
    pub fn get_partition_columns(&self) -> Vec<String> {
        self.partition_columns.clone()
    }

    /// Get configuration as JSON string
    #[getter]
    pub fn get_configuration(&self) -> String {
        self.configuration.clone()
    }
}

/// Represents a table snapshot at a particular version.
#[pyclass]
pub struct Snapshot {
    /// Version number
    pub version: i32,
    /// Timestamp in milliseconds since UNIX_EPOCH
    pub timestamp_millis: i64,
    /// Protocol
    pub protocol: Protocol,
    /// Table metadata
    pub metadata: TableMetadata,
    /// Active files
    pub files: Vec<ActiveFile>,
}

#[pymethods]
impl Snapshot {
    /// Create a new snapshot.
    #[new]
    pub fn new(
        version: i32,
        timestamp_millis: i64,
        protocol: Protocol,
        metadata: TableMetadata,
        files: Vec<ActiveFile>,
    ) -> Self {
        Self {
            version,
            timestamp_millis,
            protocol,
            metadata,
            files,
        }
    }

    /// String representation
    pub fn __repr__(&self) -> String {
        format!(
            "Snapshot(version={}, files={}, timestamp={})",
            self.version,
            self.files.len(),
            self.timestamp_millis
        )
    }

    /// Get version
    #[getter]
    pub fn get_version(&self) -> i32 {
        self.version
    }

    /// Get timestamp
    #[getter]
    pub fn get_timestamp_millis(&self) -> i64 {
        self.timestamp_millis
    }

    /// Get protocol
    #[getter]
    pub fn get_protocol(&self) -> PyResult<Py<Protocol>> {
        Python::with_gil(|py| {
            Ok(Py::new(
                py,
                Protocol::new(
                    self.protocol.min_reader_version,
                    self.protocol.min_writer_version,
                ),
            )?)
        })
    }

    /// Get metadata
    #[getter]
    pub fn get_metadata(&self) -> PyResult<Py<TableMetadata>> {
        Python::with_gil(|py| {
            Ok(Py::new(
                py,
                TableMetadata::new(
                    self.metadata.schema_json.clone(),
                    self.metadata.partition_columns.clone(),
                    self.metadata.configuration.clone(),
                ),
            )?)
        })
    }

    /// Get files as list
    #[getter]
    pub fn get_files(&self) -> PyResult<Vec<Py<ActiveFile>>> {
        Python::with_gil(|py| {
            self.files
                .iter()
                .map(|f| {
                    Ok(Py::new(
                        py,
                        ActiveFile::new(
                            f.path.clone(),
                            f.size,
                            f.modification_time,
                            f.partition_values.clone(),
                            f.data_change,
                        ),
                    )?)
                })
                .collect()
        })
    }

    /// Iterate over files
    pub fn files(&self, py: Python) -> PyResult<Py<PyList>> {
        let files: Vec<Py<ActiveFile>> = self
            .files
            .iter()
            .map(|f| {
                Py::new(
                    py,
                    ActiveFile::new(
                        f.path.clone(),
                        f.size,
                        f.modification_time,
                        f.partition_values.clone(),
                        f.data_change,
                    ),
                )
            })
            .collect::<PyResult<_>>()?;

        Ok(PyList::new(py, files).into())
    }
}
