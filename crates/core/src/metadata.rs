//! Delta table metadata and schema management.

use crate::{DeltaError, ValidationError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Represents the schema of a Delta table with column definitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    /// Unique identifier for this metadata configuration
    pub id: String,

    /// Optional human-readable name for the table
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Optional description of the table
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// The table schema in Delta format
    pub schema_string: String,

    /// The format of files in the table (typically "parquet")
    pub format: Format,

    /// Partition columns for the table
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_columns: Vec<String>,

    /// Table configuration properties
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub configuration: HashMap<String, String>,

    /// Timestamp when this metadata was created
    #[serde(rename = "createdTime")]
    pub created_time: Option<i64>,

    /// Additional metadata fields
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub additional_metadata: HashMap<String, serde_json::Value>,
}

/// Represents the file format configuration for a Delta table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Format {
    /// The file format provider (typically "parquet")
    pub provider: String,

    /// Additional format-specific options
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        }
    }
}

impl Metadata {
    /// Creates a new metadata instance with the given schema.
    ///
    /// # Arguments
    /// * `schema_string` - The table schema in Delta JSON format
    ///
    /// # Errors
    /// Returns `ValidationError` if the schema is invalid
    pub fn new(schema_string: String) -> Result<Self, ValidationError> {
        if schema_string.trim().is_empty() {
            return Err(ValidationError::InvalidSchema {
                schema: schema_string,
            });
        }

        // Validate that the schema is valid JSON
        serde_json::from_str::<serde_json::Value>(&schema_string)
            .map_err(|_| ValidationError::InvalidSchema {
                schema: schema_string.clone(),
            })?;

        Ok(Self {
            id: Uuid::new_v4().to_string(),
            name: None,
            description: None,
            schema_string,
            format: Format::default(),
            partition_columns: Vec::new(),
            configuration: HashMap::new(),
            created_time: Some(
                chrono::Utc::now()
                    .timestamp_millis(),
            ),
            additional_metadata: HashMap::new(),
        })
    }

    /// Creates a new metadata instance with a generated ID.
    pub fn with_id(schema_string: String, id: String) -> Result<Self, ValidationError> {
        let mut metadata = Self::new(schema_string)?;
        metadata.id = id;
        Ok(metadata)
    }

    /// Creates a new metadata instance with all fields specified.
    pub fn complete(
        id: String,
        name: Option<String>,
        description: Option<String>,
        schema_string: String,
        format: Format,
        partition_columns: Vec<String>,
        configuration: HashMap<String, String>,
        created_time: Option<i64>,
    ) -> Result<Self, ValidationError> {
        if schema_string.trim().is_empty() {
            return Err(ValidationError::InvalidSchema {
                schema: schema_string,
            });
        }

        // Validate that the schema is valid JSON
        serde_json::from_str::<serde_json::Value>(&schema_string)
            .map_err(|_| ValidationError::InvalidSchema {
                schema: schema_string.clone(),
            })?;

        // Validate partition column names
        for column in &partition_columns {
            if column.trim().is_empty() {
                return Err(ValidationError::InvalidPartitionValue {
                    column: column.clone(),
                    value: String::new(),
                });
            }
        }

        Ok(Self {
            id,
            name,
            description,
            schema_string,
            format,
            partition_columns,
            configuration,
            created_time,
            additional_metadata: HashMap::new(),
        })
    }

    /// Sets the table name.
    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// Sets the table description.
    pub fn with_description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// Sets the file format.
    pub fn with_format(mut self, format: Format) -> Self {
        self.format = format;
        self
    }

    /// Adds a partition column.
    pub fn add_partition_column(mut self, column: String) -> Self {
        if !column.trim().is_empty() && !self.partition_columns.contains(&column) {
            self.partition_columns.push(column);
        }
        self
    }

    /// Sets the partition columns.
    pub fn with_partition_columns(mut self, columns: Vec<String>) -> Self {
        self.partition_columns = columns
            .into_iter()
            .filter(|c| !c.trim().is_empty())
            .collect();
        self
    }

    /// Adds a configuration property.
    pub fn add_configuration(mut self, key: String, value: String) -> Self {
        self.configuration.insert(key, value);
        self
    }

    /// Sets multiple configuration properties.
    pub fn with_configuration(mut self, config: HashMap<String, String>) -> Self {
        self.configuration = config;
        self
    }

    /// Gets the schema as a parsed JSON value.
    pub fn schema(&self) -> Result<serde_json::Value, ValidationError> {
        serde_json::from_str(&self.schema_string).map_err(|_| ValidationError::InvalidSchema {
            schema: self.schema_string.clone(),
        })
    }

    /// Validates that this metadata is well-formed.
    pub fn validate(&self) -> Result<(), ValidationError> {
        // Validate ID is not empty
        if self.id.trim().is_empty() {
            return Err(ValidationError::InvalidTableId { id: self.id.clone() });
        }

        // Validate name if present
        if let Some(name) = &self.name {
            if name.trim().is_empty() {
                return Err(ValidationError::EmptyTableName);
            }
        }

        // Validate schema is valid JSON
        if let Err(_) = self.schema() {
            return Err(ValidationError::InvalidSchema {
                schema: self.schema_string.clone(),
            });
        }

        // Validate format provider
        if self.format.provider.trim().is_empty() {
            return Err(ValidationError::MissingField {
                field: "format.provider".to_string(),
            });
        }

        // Validate partition column names
        for column in &self.partition_columns {
            if column.trim().is_empty() {
                return Err(ValidationError::InvalidPartitionValue {
                    column: column.clone(),
                    value: String::new(),
                });
            }
        }

        // Validate created time if present
        if let Some(created_time) = self.created_time {
            if created_time < 0 {
                return Err(ValidationError::InvalidTimestamp {
                    timestamp: created_time,
                });
            }
        }

        Ok(())
    }

    /// Returns whether this metadata is partitioned.
    pub fn is_partitioned(&self) -> bool {
        !self.partition_columns.is_empty()
    }

    /// Gets the number of partition columns.
    pub fn partition_column_count(&self) -> usize {
        self.partition_columns.len()
    }

    /// Checks if a column is a partition column.
    pub fn is_partition_column(&self, column: &str) -> bool {
        self.partition_columns.contains(&column.to_string())
    }

    /// Gets a configuration value.
    pub fn get_configuration(&self, key: &str) -> Option<&String> {
        self.configuration.get(key)
    }

    /// Gets a mutable reference to a configuration value.
    pub fn get_configuration_mut(&mut self, key: &str) -> Option<&mut String> {
        self.configuration.get_mut(key)
    }

    /// Removes a configuration property.
    pub fn remove_configuration(&mut self, key: &str) -> Option<String> {
        self.configuration.remove(key)
    }

    /// Serializes the metadata to Delta JSON format.
    pub fn to_delta_json(&self) -> Result<String, DeltaError> {
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    /// Deserializes metadata from Delta JSON format.
    pub fn from_delta_json(json: &str) -> Result<Self, DeltaError> {
        let metadata: Metadata = serde_json::from_str(json)?;
        metadata.validate()?;
        Ok(metadata)
    }

    /// Gets the table name, falling back to ID if name is not set.
    pub fn display_name(&self) -> &str {
        self.name.as_ref().unwrap_or(&self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_metadata_new() {
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": false, "metadata": {}},
                {"name": "name", "type": "string", "nullable": true, "metadata": {}}
            ]
        });

        let metadata = Metadata::new(schema.to_string()).unwrap();
        assert!(!metadata.id.is_empty());
        assert_eq!(metadata.name, None);
        assert_eq!(metadata.description, None);
        assert_eq!(metadata.schema_string, schema.to_string());
        assert_eq!(metadata.format.provider, "parquet");
        assert!(metadata.partition_columns.is_empty());
        assert!(metadata.configuration.is_empty());
        assert!(metadata.created_time.is_some());
    }

    #[test]
    fn test_metadata_invalid_schema() {
        let result = Metadata::new("invalid json".to_string());
        assert!(result.is_err());

        let result = Metadata::new("".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_metadata_builder() {
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": false, "metadata": {}}
            ]
        });

        let metadata = Metadata::new(schema.to_string())
            .unwrap()
            .with_name("test_table".to_string())
            .with_description("A test table".to_string())
            .add_partition_column("date".to_string())
            .add_configuration("delta.enableChangeDataFeed".to_string(), "true".to_string());

        assert_eq!(metadata.name, Some("test_table".to_string()));
        assert_eq!(metadata.description, Some("A test table".to_string()));
        assert_eq!(metadata.partition_columns, vec!["date"]);
        assert_eq!(
            metadata.get_configuration("delta.enableChangeDataFeed"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn test_metadata_validation() {
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": false, "metadata": {}}
            ]
        });

        let mut metadata = Metadata::new(schema.to_string()).unwrap();
        assert!(metadata.validate().is_ok());

        // Test invalid name
        metadata.name = Some("".to_string());
        assert!(metadata.validate().is_err());

        // Test invalid partition column
        metadata.name = Some("valid".to_string());
        metadata.partition_columns = vec!["".to_string()];
        assert!(metadata.validate().is_err());
    }

    #[test]
    fn test_format_default() {
        let format = Format::default();
        assert_eq!(format.provider, "parquet");
        assert!(format.options.is_empty());
    }

    #[test]
    fn test_metadata_partition_operations() {
        let schema = json!({
            "type": "struct",
            "fields": []
        });

        let metadata = Metadata::new(schema.to_string())
            .unwrap()
            .add_partition_column("date".to_string())
            .add_partition_column("category".to_string())
            .add_partition_column("date".to_string()) // Duplicate should be ignored
            .add_partition_column("".to_string()); // Empty should be ignored

        assert_eq!(metadata.partition_columns, vec!["date", "category"]);
        assert!(metadata.is_partitioned());
        assert_eq!(metadata.partition_column_count(), 2);
        assert!(metadata.is_partition_column("date"));
        assert!(!metadata.is_partition_column("nonexistent"));
    }

    #[test]
    fn test_serialization() {
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": false, "metadata": {}}
            ]
        });

        let metadata = Metadata::new(schema.to_string()).unwrap();
        let json = metadata.to_delta_json().unwrap();
        let deserialized = Metadata::from_delta_json(&json).unwrap();
        assert_eq!(metadata, deserialized);
    }
}