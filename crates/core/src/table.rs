//! Delta table domain model representing table metadata and configuration.

use crate::{Commit, DeltaError, Metadata, Protocol, ValidationError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Represents a Delta Lake table with all its metadata and configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Table {
    /// Unique identifier for this table
    pub id: String,

    /// Human-readable name for the table
    pub name: String,

    /// Location where the table is stored (URI format)
    pub location: String,

    /// Current protocol version for this table
    pub protocol: Protocol,

    /// Current metadata for this table
    pub metadata: Metadata,

    /// Latest commit version for this table
    pub version: i64,

    /// Timestamp when the table was last modified
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<i64>,

    /// Created timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_time: Option<i64>,

    /// Table description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Table properties and configuration
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,

    /// Additional table-specific metadata
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub additional_metadata: HashMap<String, serde_json::Value>,
}

/// Represents a snapshot of the table at a specific version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSnapshot {
    /// The table this snapshot belongs to
    pub table_id: String,

    /// The version of this snapshot
    pub version: i64,

    /// The timestamp when this snapshot was taken
    pub timestamp: i64,

    /// The protocol version at this snapshot
    pub protocol: Protocol,

    /// The metadata at this snapshot
    pub metadata: Metadata,

    /// The actions that led to this snapshot
    pub actions: Vec<crate::Action>,
}

/// Represents table statistics computed from the current state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableStats {
    /// Total number of files in the table
    pub file_count: i64,

    /// Total size of all files in bytes
    pub total_size: i64,

    /// Total number of records in the table
    pub total_records: i64,

    /// Number of partitions if the table is partitioned
    pub partition_count: Option<usize>,

    /// Size statistics per partition (if available)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub partition_stats: HashMap<String, PartitionStats>,
}

/// Statistics for a specific partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionStats {
    /// Number of files in this partition
    pub file_count: i64,

    /// Total size of files in this partition
    pub total_size: i64,

    /// Total records in this partition
    pub total_records: i64,

    /// Partition values
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub values: HashMap<String, String>,
}

impl Table {
    /// Creates a new table with the specified name and location.
    ///
    /// # Arguments
    /// * `name` - Human-readable name for the table
    /// * `location` - Location where the table is stored (URI format)
    /// * `schema_string` - The table schema in Delta JSON format
    ///
    /// # Errors
    /// Returns `ValidationError` if required fields are invalid
    pub fn new(name: String, location: String, schema_string: String) -> Result<Self, ValidationError> {
        if name.trim().is_empty() {
            return Err(ValidationError::EmptyTableName);
        }
        if location.trim().is_empty() {
            return Err(ValidationError::InvalidTableLocation { location });
        }

        let metadata = Metadata::new(schema_string)?;
        let protocol = Protocol::default();
        let created_time = metadata.created_time;

        Ok(Self {
            id: Uuid::new_v4().to_string(),
            name,
            location,
            protocol,
            metadata,
            version: 0,
            last_modified: created_time,
            created_time,
            description: None,
            properties: HashMap::new(),
            additional_metadata: HashMap::new(),
        })
    }

    /// Creates a table with a specific ID.
    pub fn with_id(
        name: String,
        location: String,
        schema_string: String,
        id: String,
    ) -> Result<Self, ValidationError> {
        let mut table = Self::new(name, location, schema_string)?;
        table.id = id;
        Ok(table)
    }

    /// Creates a complete table with all fields specified.
    pub fn complete(
        id: String,
        name: String,
        location: String,
        protocol: Protocol,
        metadata: Metadata,
        version: i64,
        last_modified: Option<i64>,
        created_time: Option<i64>,
        description: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Self, ValidationError> {
        if name.trim().is_empty() {
            return Err(ValidationError::EmptyTableName);
        }
        if location.trim().is_empty() {
            return Err(ValidationError::InvalidTableLocation { location });
        }
        if version < 0 {
            return Err(ValidationError::InvalidCommitVersion { version });
        }

        // Validate protocol
        protocol.validate_supported_versions()?;

        // Validate metadata
        metadata.validate()?;

        Ok(Self {
            id,
            name,
            location,
            protocol,
            metadata,
            version,
            last_modified,
            created_time,
            description,
            properties,
            additional_metadata: HashMap::new(),
        })
    }

    /// Sets the table description.
    pub fn with_description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// Sets the table properties.
    pub fn with_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = properties;
        self
    }

    /// Adds a property to the table.
    pub fn add_property(mut self, key: String, value: String) -> Self {
        if !key.trim().is_empty() {
            self.properties.insert(key, value);
        }
        self
    }

    /// Updates the table metadata.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Updates the table protocol.
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Updates the table version.
    pub fn with_version(mut self, version: i64) -> Self {
        self.version = version;
        self.last_modified = Some(chrono::Utc::now().timestamp_millis());
        self
    }

    /// Gets the table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Gets the table location.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Gets the table ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Gets the current version.
    pub fn version(&self) -> i64 {
        self.version
    }

    /// Checks if the table is partitioned.
    pub fn is_partitioned(&self) -> bool {
        self.metadata.is_partitioned()
    }

    /// Gets the partition columns.
    pub fn partition_columns(&self) -> &[String] {
        &self.metadata.partition_columns
    }

    /// Gets a table property.
    pub fn get_property(&self, key: &str) -> Option<&String> {
        self.properties.get(key)
    }

    /// Gets a mutable reference to a table property.
    pub fn get_property_mut(&mut self, key: &str) -> Option<&mut String> {
        self.properties.get_mut(key)
    }

    /// Removes a table property.
    pub fn remove_property(&mut self, key: &str) -> Option<String> {
        self.properties.remove(key)
    }

    /// Gets the table schema as a parsed JSON value.
    pub fn schema(&self) -> Result<serde_json::Value, ValidationError> {
        self.metadata.schema()
    }

    /// Validates that this table is well-formed.
    pub fn validate(&self) -> Result<(), ValidationError> {
        // Validate table ID
        if self.id.trim().is_empty() {
            return Err(ValidationError::InvalidTableId { id: self.id.clone() });
        }

        // Validate table name
        if self.name.trim().is_empty() {
            return Err(ValidationError::EmptyTableName);
        }

        // Validate table location
        if self.location.trim().is_empty() {
            return Err(ValidationError::InvalidTableLocation {
                location: self.location.clone(),
            });
        }

        // Validate version
        if self.version < 0 {
            return Err(ValidationError::InvalidCommitVersion {
                version: self.version,
            });
        }

        // Validate timestamps
        if let Some(created_time) = self.created_time {
            if created_time < 0 {
                return Err(ValidationError::InvalidTimestamp {
                    timestamp: created_time,
                });
            }
        }

        if let Some(last_modified) = self.last_modified {
            if last_modified < 0 {
                return Err(ValidationError::InvalidTimestamp {
                    timestamp: last_modified,
                });
            }
        }

        // Validate protocol
        self.protocol.validate_supported_versions()?;

        // Validate metadata
        self.metadata.validate()?;

        Ok(())
    }

    /// Creates a snapshot of this table at the current version.
    pub fn create_snapshot(&self) -> TableSnapshot {
        TableSnapshot {
            table_id: self.id.clone(),
            version: self.version,
            timestamp: self.last_modified.unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
            protocol: self.protocol.clone(),
            metadata: self.metadata.clone(),
            actions: Vec::new(), // Would be populated from actual commits
        }
    }

    /// Applies a commit to this table, updating version and potentially metadata/protocol.
    pub fn apply_commit(&mut self, commit: &Commit) -> Result<(), ValidationError> {
        // Validate commit
        commit.validate()?;

        // Update version
        self.version = commit.version;
        self.last_modified = Some(commit.timestamp);

        // Process actions
        for action in &commit.actions {
            match action {
                crate::Action::Metadata(metadata) => {
                    self.metadata = metadata.clone();
                }
                crate::Action::Protocol(protocol) => {
                    self.protocol = protocol.clone();
                }
                _ => {
                    // Other actions (AddFile, RemoveFile, etc.) would be handled by state management
                }
            }
        }

        Ok(())
    }

    /// Serializes the table to Delta JSON format.
    pub fn to_delta_json(&self) -> Result<String, DeltaError> {
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    /// Deserializes a table from Delta JSON format.
    pub fn from_delta_json(json: &str) -> Result<Self, DeltaError> {
        let table: Table = serde_json::from_str(json)?;
        table.validate()?;
        Ok(table)
    }

    /// Gets a display-friendly representation of the table.
    pub fn display_name(&self) -> &str {
        &self.name
    }

    /// Gets the table age in milliseconds (if created_time is available).
    pub fn age_ms(&self) -> Option<i64> {
        self.created_time.map(|created| {
            chrono::Utc::now().timestamp_millis() - created
        })
    }
}

impl TableSnapshot {
    /// Creates a new table snapshot.
    pub fn new(
        table_id: String,
        version: i64,
        protocol: Protocol,
        metadata: Metadata,
        actions: Vec<crate::Action>,
    ) -> Self {
        Self {
            table_id,
            version,
            timestamp: chrono::Utc::now().timestamp_millis(),
            protocol,
            metadata,
            actions,
        }
    }

    /// Creates a snapshot with a specific timestamp.
    pub fn with_timestamp(
        table_id: String,
        version: i64,
        timestamp: i64,
        protocol: Protocol,
        metadata: Metadata,
        actions: Vec<crate::Action>,
    ) -> Self {
        Self {
            table_id,
            version,
            timestamp,
            protocol,
            metadata,
            actions,
        }
    }

    /// Validates the snapshot.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.version < 0 {
            return Err(ValidationError::InvalidCommitVersion {
                version: self.version,
            });
        }
        if self.timestamp < 0 {
            return Err(ValidationError::InvalidTimestamp {
                timestamp: self.timestamp,
            });
        }

        self.protocol.validate_supported_versions()?;
        self.metadata.validate()?;

        for action in &self.actions {
            action.validate()?;
        }

        Ok(())
    }

    /// Serializes the snapshot to Delta JSON format.
    pub fn to_delta_json(&self) -> Result<String, DeltaError> {
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(Into::into)
    }
}

impl Default for TableStats {
    fn default() -> Self {
        Self {
            file_count: 0,
            total_size: 0,
            total_records: 0,
            partition_count: None,
            partition_stats: HashMap::new(),
        }
    }
}

impl PartitionStats {
    /// Creates new partition statistics.
    pub fn new() -> Self {
        Self {
            file_count: 0,
            total_size: 0,
            total_records: 0,
            values: HashMap::new(),
        }
    }

    /// Adds a file to the partition statistics.
    pub fn add_file(&mut self, size: i64, records: i64) {
        self.file_count += 1;
        self.total_size += size;
        self.total_records += records;
    }

    /// Sets a partition value.
    pub fn set_value(&mut self, column: String, value: String) {
        self.values.insert(column, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_table_new() {
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": false, "metadata": {}},
                {"name": "name", "type": "string", "nullable": true, "metadata": {}}
            ]
        });

        let table = Table::new(
            "test_table".to_string(),
            "s3://bucket/test".to_string(),
            schema.to_string(),
        ).unwrap();

        assert_eq!(table.name, "test_table");
        assert_eq!(table.location, "s3://bucket/test");
        assert_eq!(table.version, 0);
        assert!(!table.is_partitioned());
        assert!(table.validate().is_ok());
    }

    #[test]
    fn test_table_validation() {
        let schema = json!({
            "type": "struct",
            "fields": []
        });

        // Test empty name
        let result = Table::new("".to_string(), "s3://bucket/test".to_string(), schema.to_string());
        assert!(result.is_err());

        // Test empty location
        let result = Table::new("test".to_string(), "".to_string(), schema.to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_table_builder() {
        let schema = json!({
            "type": "struct",
            "fields": []
        });

        let table = Table::new(
            "test_table".to_string(),
            "s3://bucket/test".to_string(),
            schema.to_string(),
        ).unwrap()
        .with_description("A test table".to_string())
        .add_property("key1".to_string(), "value1".to_string())
        .with_version(5);

        assert_eq!(table.description, Some("A test table".to_string()));
        assert_eq!(table.get_property("key1"), Some(&"value1".to_string()));
        assert_eq!(table.version, 5);
    }

    #[test]
    fn test_table_partitioned() {
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "date", "type": "string", "nullable": false, "metadata": {}},
                {"name": "value", "type": "long", "nullable": true, "metadata": {}}
            ]
        });

        let mut table = Table::new(
            "partitioned_table".to_string(),
            "s3://bucket/partitioned".to_string(),
            schema.to_string(),
        ).unwrap();

        // Add partition column through metadata
        table.metadata = table.metadata.with_partition_columns(vec!["date".to_string()]);

        assert!(table.is_partitioned());
        assert_eq!(table.partition_columns(), &["date"]);
    }

    #[test]
    fn test_table_serialization() {
        let schema = json!({
            "type": "struct",
            "fields": []
        });

        let table = Table::new(
            "test_table".to_string(),
            "s3://bucket/test".to_string(),
            schema.to_string(),
        ).unwrap();

        let json = table.to_delta_json().unwrap();
        let deserialized = Table::from_delta_json(&json).unwrap();
        assert_eq!(table, deserialized);
    }

    #[test]
    fn test_table_snapshot() {
        let schema = json!({
            "type": "struct",
            "fields": []
        });

        let table = Table::new(
            "test_table".to_string(),
            "s3://bucket/test".to_string(),
            schema.to_string(),
        ).unwrap();

        let snapshot = table.create_snapshot();
        assert_eq!(snapshot.table_id, table.id);
        assert_eq!(snapshot.version, table.version);
        assert_eq!(snapshot.protocol, table.protocol);
        assert_eq!(snapshot.metadata, table.metadata);
    }

    #[test]
    fn test_partition_stats() {
        let mut stats = PartitionStats::new();
        assert_eq!(stats.file_count, 0);
        assert_eq!(stats.total_size, 0);
        assert_eq!(stats.total_records, 0);

        stats.add_file(1024, 100);
        stats.set_value("date".to_string(), "2023-01-01".to_string());

        assert_eq!(stats.file_count, 1);
        assert_eq!(stats.total_size, 1024);
        assert_eq!(stats.total_records, 100);
        assert_eq!(stats.values.get("date"), Some(&"2023-01-01".to_string()));
    }
}