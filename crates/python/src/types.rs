//! Type system integration for Python bindings
//!
//! This module provides type conversion utilities and data type mappings
//! between Rust and Python for the SQL-Backed Delta Lake metadata system.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Delta Lake data types
#[pyclass]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DeltaDataType {
    Primitive(String),
    Struct(Vec<(String, DeltaDataType)>),
    Array(Box<DeltaDataType>),
    Map(Box<DeltaDataType>, Box<DeltaDataType>),
}

#[pymethods]
impl DeltaDataType {
    /// Get string representation of data type
    fn __str__(&self) -> String {
        match self {
            DeltaDataType::Primitive(name) => name.clone(),
            DeltaDataType::Struct(fields) => {
                let fields_str = fields.iter()
                    .map(|(name, dtype)| format!("{}: {}", name, dtype))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("struct<{}>", fields_str)
            },
            DeltaDataType::Array(element_type) => format!("array<{}>", element_type),
            DeltaDataType::Map(key_type, value_type) => format!("map<{}, {}>", key_type, value_type),
        }
    }

    /// Get Python representation
    fn to_py(&self, py: Python) -> PyResult<PyObject> {
        match self {
            DeltaDataType::Primitive(name) => Ok(name.to_object(py)),
            DeltaDataType::Struct(fields) => {
                let dict = PyDict::new(py);
                for (name, dtype) in fields {
                    dict.set_item(name, dtype.to_py(py)?)?;
                }
                Ok(dict.to_object(py))
            },
            DeltaDataType::Array(element_type) => {
                let list = PyList::new(py, [element_type.to_py(py)?]);
                Ok(list.to_object(py))
            },
            DeltaDataType::Map(key_type, value_type) => {
                let dict = PyDict::new(py);
                dict.set_item("key_type", key_type.to_py(py)?)?;
                dict.set_item("value_type", value_type.to_py(py)?)?;
                Ok(dict.to_object(py))
            },
        }
    }

    /// Create from Python object
    #[staticmethod]
    fn from_py(obj: &PyAny) -> PyResult<Self> {
        if let Ok(s) = obj.downcast::<PyString>() {
            Ok(DeltaDataType::Primitive(s.to_string_lossy().to_string()))
        } else if let Ok(dict) = obj.downcast::<PyDict>() {
            let mut fields = Vec::new();
            for (key, value) in dict.iter() {
                let name = key.extract::<String>()?;
                let dtype = DeltaDataType::from_py(value)?;
                fields.push((name, dtype));
            }
            Ok(DeltaDataType::Struct(fields))
        } else if let Ok(list) = obj.downcast::<PyList>() {
            if list.len() == 1 {
                let element_type = DeltaDataType::from_py(list.get_item(0)?)?;
                Ok(DeltaDataType::Array(Box::new(element_type)))
            } else {
                Err(pyo3::exceptions::PyValueError::new_err(
                    "Array type must have exactly one element type"
                ))
            }
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(
                "Cannot convert Python object to DeltaDataType"
            ))
        }
    }

    /// Check if type is primitive
    fn is_primitive(&self) -> bool {
        matches!(self, DeltaDataType::Primitive(_))
    }

    /// Check if type is complex
    fn is_complex(&self) -> bool {
        !self.is_primitive()
    }

    /// Get primitive name if applicable
    fn get_primitive_name(&self) -> Option<String> {
        match self {
            DeltaDataType::Primitive(name) => Some(name.clone()),
            _ => None,
        }
    }
}

/// Schema field definition
#[pyclass]
#[derive(Debug, Clone)]
pub struct SchemaField {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub data_type: DeltaDataType,
    #[pyo3(get)]
    pub nullable: bool,
    #[pyo3(get)]
    pub metadata: HashMap<String, String>,
}

#[pymethods]
impl SchemaField {
    /// Create a new schema field
    #[new]
    fn new(
        name: String,
        data_type: DeltaDataType,
        nullable: bool,
        metadata: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            name,
            data_type,
            nullable,
            metadata: metadata.unwrap_or_default(),
        }
    }

    /// Get field as dictionary
    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("data_type", self.data_type.to_py(py)?)?;
        dict.set_item("nullable", self.nullable)?;

        let metadata_dict = PyDict::new(py);
        for (key, value) in &self.metadata {
            metadata_dict.set_item(key, value)?;
        }
        dict.set_item("metadata", metadata_dict)?;

        Ok(dict.to_object(py))
    }

    /// Add metadata
    fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Remove metadata
    fn remove_metadata(&mut self, key: &str) -> Option<String> {
        self.metadata.remove(key)
    }

    fn __repr__(&self) -> String {
        format!(
            "SchemaField(name='{}', type={}, nullable={})",
            self.name, self.data_type, self.nullable
        )
    }
}

/// Table schema definition
#[pyclass]
#[derive(Debug, Clone)]
pub struct TableSchema {
    #[pyo3(get)]
    pub fields: Vec<SchemaField>,
    #[pyo3(get)]
    pub partition_columns: Vec<String>,
    #[pyo3(get)]
    pub schema_id: Option<String>,
}

#[pymethods]
impl TableSchema {
    /// Create a new table schema
    #[new]
    fn new(
        fields: Vec<SchemaField>,
        partition_columns: Option<Vec<String>>,
        schema_id: Option<String>,
    ) -> Self {
        Self {
            fields,
            partition_columns: partition_columns.unwrap_or_default(),
            schema_id,
        }
    }

    /// Add field to schema
    fn add_field(&mut self, field: SchemaField) {
        self.fields.push(field);
    }

    /// Remove field by name
    fn remove_field(&mut self, name: &str) -> Option<SchemaField> {
        let index = self.fields.iter().position(|f| f.name == name);
        if let Some(index) = index {
            Some(self.fields.remove(index))
        } else {
            None
        }
    }

    /// Get field by name
    fn get_field(&self, name: &str) -> Option<SchemaField> {
        self.fields.iter().find(|f| f.name == name).cloned()
    }

    /// Check if field exists
    fn has_field(&self, name: &str) -> bool {
        self.fields.iter().any(|f| f.name == name)
    }

    /// Get field names
    fn get_field_names(&self) -> Vec<String> {
        self.fields.iter().map(|f| f.name.clone()).collect()
    }

    /// Add partition column
    fn add_partition_column(&mut self, column: String) {
        if !self.partition_columns.contains(&column) {
            self.partition_columns.push(column);
        }
    }

    /// Remove partition column
    fn remove_partition_column(&mut self, column: &str) -> bool {
        let index = self.partition_columns.iter().position(|c| c == column);
        if let Some(index) = index {
            self.partition_columns.remove(index);
            true
        } else {
            false
        }
    }

    /// Check if column is partitioned
    fn is_partition_column(&self, column: &str) -> bool {
        self.partition_columns.contains(&column.to_string())
    }

    /// Get schema as JSON string
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("JSON serialization failed: {}", e)))
    }

    /// Create schema from JSON string
    #[staticmethod]
    fn from_json(json_str: &str) -> PyResult<Self> {
        serde_json::from_str(json_str)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("JSON deserialization failed: {}", e)))
    }

    /// Get schema as dictionary
    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);

        let fields_list = PyList::new(py, self.fields.iter().map(|f| f.to_dict(py)));
        dict.set_item("fields", fields_list)?;

        let partition_list = PyList::new(py, self.partition_columns.iter());
        dict.set_item("partition_columns", partition_list)?;

        if let Some(schema_id) = &self.schema_id {
            dict.set_item("schema_id", schema_id)?;
        }

        Ok(dict.to_object(py))
    }

    /// Validate schema
    fn validate(&self) -> PyResult<bool> {
        // Check for duplicate field names
        let field_names: std::collections::HashSet<_> = self.fields.iter().map(|f| &f.name).collect();
        if field_names.len() != self.fields.len() {
            return Ok(false);
        }

        // Check partition columns exist in fields
        for partition_col in &self.partition_columns {
            if !field_names.contains(partition_col) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Get validation errors
    fn get_validation_errors(&self) -> Vec<String> {
        let mut errors = Vec::new();

        // Check for duplicate field names
        let mut seen_names = std::collections::HashSet::new();
        for field in &self.fields {
            if seen_names.contains(&field.name) {
                errors.push(format!("Duplicate field name: {}", field.name));
            }
            seen_names.insert(&field.name);
        }

        // Check partition columns exist in fields
        let field_names: std::collections::HashSet<_> = self.fields.iter().map(|f| &f.name).collect();
        for partition_col in &self.partition_columns {
            if !field_names.contains(partition_col) {
                errors.push(format!("Partition column '{}' not found in schema fields", partition_col));
            }
        }

        errors
    }

    fn __repr__(&self) -> String {
        format!(
            "TableSchema(fields={}, partition_columns={})",
            self.fields.len(),
            self.partition_columns.len()
        )
    }
}

/// Type conversion utilities
#[pyclass]
#[derive(Debug)]
pub struct TypeConverter {
    #[pyo3(get)]
    pub supported_types: Vec<String>,
}

#[pymethods]
impl TypeConverter {
    /// Create new type converter
    #[new]
    fn new() -> Self {
        Self {
            supported_types: vec![
                "boolean".to_string(),
                "byte".to_string(),
                "short".to_string(),
                "integer".to_string(),
                "long".to_string(),
                "float".to_string(),
                "double".to_string(),
                "string".to_string(),
                "binary".to_string(),
                "date".to_string(),
                "timestamp".to_string(),
                "decimal".to_string(),
            ],
        }
    }

    /// Convert Python type to Delta data type
    fn python_to_delta(&self, py_type: &str) -> PyResult<DeltaDataType> {
        let delta_type = match py_type.to_lowercase().as_str() {
            "bool" | "boolean" => DeltaDataType::Primitive("boolean".to_string()),
            "int" | "int32" => DeltaDataType::Primitive("integer".to_string()),
            "int64" | "long" => DeltaDataType::Primitive("long".to_string()),
            "float" | "float32" => DeltaDataType::Primitive("float".to_string()),
            "float64" | "double" => DeltaDataType::Primitive("double".to_string()),
            "str" | "string" => DeltaDataType::Primitive("string".to_string()),
            "bytes" | "binary" => DeltaDataType::Primitive("binary".to_string()),
            "datetime" | "timestamp" => DeltaDataType::Primitive("timestamp".to_string()),
            "date" => DeltaDataType::Primitive("date".to_string()),
            _ => DeltaDataType::Primitive(py_type.to_string()),
        };
        Ok(delta_type)
    }

    /// Convert Delta data type to Python type
    fn delta_to_python(&self, delta_type: &DeltaDataType) -> PyResult<String> {
        let python_type = match delta_type {
            DeltaDataType::Primitive(name) => {
                match name.as_str() {
                    "boolean" => "bool".to_string(),
                    "byte" | "short" | "integer" => "int".to_string(),
                    "long" => "int".to_string(),
                    "float" => "float".to_string(),
                    "double" => "float".to_string(),
                    "string" => "str".to_string(),
                    "binary" => "bytes".to_string(),
                    "date" => "date".to_string(),
                    "timestamp" => "datetime".to_string(),
                    _ => name.clone(),
                }
            },
            DeltaDataType::Struct(_) => "dict".to_string(),
            DeltaDataType::Array(_) => "list".to_string(),
            DeltaDataType::Map(_, _) => "dict".to_string(),
        };
        Ok(python_type)
    }

    /// Check if type is supported
    fn is_type_supported(&self, type_name: &str) -> bool {
        self.supported_types.contains(&type_name.to_lowercase())
    }

    fn __repr__(&self) -> String {
        format!("TypeConverter(supported_types={})", self.supported_types.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::prepare_freethreaded_python;

    #[test]
    fn test_delta_data_type_primitive() {
        let dtype = DeltaDataType::Primitive("string".to_string());
        assert!(dtype.is_primitive());
        assert!(!dtype.is_complex());
        assert_eq!(dtype.get_primitive_name(), Some("string".to_string()));
        assert_eq!(dtype.__str__(), "string");
    }

    #[test]
    fn test_delta_data_type_struct() {
        let fields = vec![
            ("name".to_string(), DeltaDataType::Primitive("string".to_string())),
            ("age".to_string(), DeltaDataType::Primitive("integer".to_string())),
        ];
        let dtype = DeltaDataType::Struct(fields);
        assert!(!dtype.is_primitive());
        assert!(dtype.is_complex());
        assert!(dtype.__str__().contains("struct<"));
    }

    #[test]
    fn test_schema_field_creation() {
        let field = SchemaField::new(
            "test_field".to_string(),
            DeltaDataType::Primitive("string".to_string()),
            true,
            None,
        );
        assert_eq!(field.name, "test_field");
        assert!(field.nullable);
        assert!(field.metadata.is_empty());
    }

    #[test]
    fn test_table_schema_creation() {
        let fields = vec![
            SchemaField::new(
                "id".to_string(),
                DeltaDataType::Primitive("integer".to_string()),
                false,
                None,
            ),
            SchemaField::new(
                "name".to_string(),
                DeltaDataType::Primitive("string".to_string()),
                true,
                None,
            ),
        ];
        let schema = TableSchema::new(fields, Some(vec!["id".to_string()]), None);
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.partition_columns.len(), 1);
        assert!(schema.validate().unwrap());
    }

    #[test]
    fn test_type_converter() {
        let converter = TypeConverter::new();
        let delta_type = converter.python_to_delta("str").unwrap();
        assert_eq!(delta_type.__str__(), "string");

        let python_type = converter.delta_to_python(&delta_type).unwrap();
        assert_eq!(python_type, "str");
    }
}