//! Delta Lake JSON commit file generator

use crate::error::{MirrorError, MirrorResult};
use crate::generators::{
    DeltaGenerator, DeltaFile, DeltaFormat, GenerationContext, DeltaGenerationData,
    GenerationOptions, json_file_name, validate_delta_json, sort_json_keys,
};
use deltalakedb_core::{Action, AddFile, RemoveFile};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

/// Delta Lake JSON commit file generator
pub struct DeltaJsonGenerator {
    options: GenerationOptions,
}

impl DeltaJsonGenerator {
    /// Create a new JSON generator with default options
    pub fn new() -> Self {
        Self {
            options: GenerationOptions::default(),
        }
    }

    /// Create a new JSON generator with custom options
    pub fn with_options(options: GenerationOptions) -> Self {
        Self { options }
    }

    /// Generate a Delta JSON commit file from actions
    pub fn generate_commit_file(
        &self,
        table_path: &str,
        version: i64,
        actions: &[Action],
    ) -> MirrorResult<DeltaFile> {
        let mut json_actions = Vec::new();

        for action in actions {
            let json_action = self.action_to_json(action)?;
            json_actions.push(json_action);
        }

        // Create the complete JSON structure
        let mut commit_json = json!(json_actions);

        // Apply formatting options
        if self.options.sort_keys {
            sort_json_keys(&mut commit_json);
        }

        // Serialize to string
        let json_str = if self.options.pretty_print {
            serde_json::to_string_pretty(&commit_json)
        } else {
            serde_json::to_string(&commit_json)
        }.map_err(|e| MirrorError::json_generation_error(
            format!("Failed to serialize JSON: {}", e)
        ))?;

        // Validate the generated JSON
        if self.options.validate_content {
            validate_delta_json(&commit_json)?;
        }

        let file_name = json_file_name(version);
        let content = json_str.into_bytes();

        let mut metadata = HashMap::new();
        metadata.insert("table_path".to_string(), table_path.to_string());
        metadata.insert("version".to_string(), version.to_string());
        metadata.insert("action_count".to_string(), actions.len().to_string());
        metadata.insert("generated_at".to_string(), chrono::Utc::now().to_rfc3339());

        Ok(DeltaFile::with_metadata(
            DeltaFormat::Json,
            file_name,
            content,
            metadata,
        ))
    }

    /// Generate a Delta JSON metadata file
    pub fn generate_metadata_file(
        &self,
        table_path: &str,
        version: i64,
        metadata: &deltalakedb_core::Metadata,
    ) -> MirrorResult<DeltaFile> {
        let metadata_json = self.metadata_to_json(metadata)?;

        if self.options.sort_keys {
            let mut sorted_json = metadata_json;
            sort_json_keys(&mut sorted_json);
            sorted_json
        } else {
            metadata_json
        };

        // Validate the generated JSON
        if self.options.validate_content {
            validate_delta_json(&sorted_json)?;
        }

        let json_str = if self.options.pretty_print {
            serde_json::to_string_pretty(&sorted_json)
        } else {
            serde_json::to_string(&sorted_json)
        }.map_err(|e| MirrorError::json_generation_error(
            format!("Failed to serialize metadata JSON: {}", e)
        ))?;

        let file_name = format!("{:020}.json", version);
        let content = json_str.into_bytes();

        let mut file_metadata = HashMap::new();
        file_metadata.insert("table_path".to_string(), table_path.to_string());
        file_metadata.insert("version".to_string(), version.to_string());
        file_metadata.insert("type".to_string(), "metadata".to_string());
        file_metadata.insert("table_id".to_string(), metadata.id.clone());
        file_metadata.insert("generated_at".to_string(), chrono::Utc::now().to_rfc3339());

        Ok(DeltaFile::with_metadata(
            DeltaFormat::Json,
            file_name,
            content,
            file_metadata,
        ))
    }

    /// Generate a Delta JSON protocol file
    pub fn generate_protocol_file(
        &self,
        table_path: &str,
        version: i64,
        protocol: &deltalakedb_core::Protocol,
    ) -> MirrorResult<DeltaFile> {
        let protocol_json = self.protocol_to_json(protocol)?;

        let mut sorted_json = protocol_json;
        if self.options.sort_keys {
            sort_json_keys(&mut sorted_json);
        }

        // Validate the generated JSON
        if self.options.validate_content {
            validate_delta_json(&sorted_json)?;
        }

        let json_str = if self.options.pretty_print {
            serde_json::to_string_pretty(&sorted_json)
        } else {
            serde_json::to_string(&sorted_json)
        }.map_err(|e| MirrorError::json_generation_error(
            format!("Failed to serialize protocol JSON: {}", e)
        ))?;

        let file_name = format!("{:020}.json", version);
        let content = json_str.into_bytes();

        let mut metadata = HashMap::new();
        metadata.insert("table_path".to_string(), table_path.to_string());
        metadata.insert("version".to_string(), version.to_string());
        metadata.insert("type".to_string(), "protocol".to_string());
        metadata.insert("min_reader_version".to_string(), protocol.min_reader_version.to_string());
        metadata.insert("min_writer_version".to_string(), protocol.min_writer_version.to_string());
        metadata.insert("generated_at".to_string(), chrono::Utc::now().to_rfc3339());

        Ok(DeltaFile::with_metadata(
            DeltaFormat::Json,
            file_name,
            content,
            metadata,
        ))
    }

    /// Convert an Action to Delta JSON format
    fn action_to_json(&self, action: &Action) -> MirrorResult<Value> {
        match action {
            Action::AddFile(add_file) => self.add_file_to_json(add_file),
            Action::RemoveFile(remove_file) => self.remove_file_to_json(remove_file),
            Action::Metadata(metadata) => self.custom_metadata_to_json(metadata),
        }
    }

    /// Convert AddFile action to JSON
    fn add_file_to_json(&self, add_file: &AddFile) -> MirrorResult<Value> {
        let mut add_obj = Map::new();

        add_obj.insert("path".to_string(), json!(add_file.path));
        add_obj.insert("size".to_string(), json!(add_file.size));
        add_obj.insert("modificationTime".to_string(), json!(add_file.modification_time));
        add_obj.insert("dataChange".to_string(), json!(add_file.data_change));

        // Add partition values if present
        if let Some(partition_values) = &add_file.partition_values {
            if !partition_values.is_empty() {
                add_obj.insert("partitionValues".to_string(), json!(partition_values));
            }
        }

        // Add stats if present
        if let Some(stats) = &add_file.stats {
            add_obj.insert("stats".to_string(), json!(stats));
        }

        // Add tags if present
        if let Some(tags) = &add_file.tags {
            if !tags.is_empty() {
                add_obj.insert("tags".to_string(), json!(tags));
            }
        }

        Ok(json!({"add": add_obj}))
    }

    /// Convert RemoveFile action to JSON
    fn remove_file_to_json(&self, remove_file: &RemoveFile) -> MirrorResult<Value> {
        let mut remove_obj = Map::new();

        remove_obj.insert("path".to_string(), json!(remove_file.path));
        remove_obj.insert("deletionTimestamp".to_string(), json!(remove_file.deletion_timestamp));
        remove_obj.insert("dataChange".to_string(), json!(remove_file.data_change));

        // Add extended file metadata if present
        if let Some(extended_metadata) = remove_file.extended_file_metadata {
            remove_obj.insert("extendedFileMetadata".to_string(), json!(extended_metadata));
        }

        // Add partition values if present
        if let Some(partition_values) = &remove_file.partition_values {
            if !partition_values.is_empty() {
                remove_obj.insert("partitionValues".to_string(), json!(partition_values));
            }
        }

        // Add tags if present
        if let Some(tags) = &remove_file.tags {
            if !tags.is_empty() {
                remove_obj.insert("tags".to_string(), json!(tags));
            }
        }

        Ok(json!({"remove": remove_obj}))
    }

    /// Convert custom metadata action to JSON
    fn custom_metadata_to_json(&self, metadata: &serde_json::Value) -> MirrorResult<Value> {
        Ok(json!({"metaData": metadata}))
    }

    /// Convert core metadata to Delta format
    fn metadata_to_json(&self, metadata: &deltalakedb_core::Metadata) -> MirrorResult<Value> {
        let mut meta_obj = Map::new();

        meta_obj.insert("id".to_string(), json!(metadata.id));
        meta_obj.insert("name".to_string(), json!(metadata.name));

        if let Some(description) = &metadata.description {
            meta_obj.insert("description".to_string(), json!(description));
        }

        // Format information
        let mut format_obj = Map::new();
        format_obj.insert("provider".to_string(), json!(metadata.format));
        meta_obj.insert("format".to_string(), json!(format_obj));

        // Schema information
        if let Some(schema_string) = &metadata.schema_string {
            meta_obj.insert("schemaString".to_string(), json!(schema_string));
        }

        // Partition columns
        if !metadata.partition_columns.is_empty() {
            meta_obj.insert("partitionColumns".to_string(), json!(metadata.partition_columns));
        }

        // Configuration
        if !metadata.configuration.is_null() {
            meta_obj.insert("configuration".to_string(), json!(metadata.configuration));
        }

        // Created time
        if let Some(created_time) = metadata.created_time {
            meta_obj.insert("createdTime".to_string(), json!(created_time));
        }

        Ok(json!(meta_obj))
    }

    /// Convert protocol to Delta format
    fn protocol_to_json(&self, protocol: &deltalakedb_core::Protocol) -> MirrorResult<Value> {
        let protocol_obj = json!({
            "minReaderVersion": protocol.min_reader_version,
            "minWriterVersion": protocol.min_writer_version
        });

        Ok(protocol_obj)
    }
}

impl Default for DeltaJsonGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl DeltaGenerator for DeltaJsonGenerator {
    fn generate_file(
        &self,
        context: &GenerationContext,
        data: &DeltaGenerationData,
    ) -> MirrorResult<DeltaFile> {
        // Validate context
        self.validate_context(context)?;

        // Generate commit file
        self.generate_commit_file(
            &context.table.table_path,
            context.version,
            &data.actions,
        )
    }

    fn validate_context(&self, context: &GenerationContext) -> MirrorResult<()> {
        if context.version < 0 {
            return Err(MirrorError::validation_error(
                format!("Invalid version: {}", context.version)
            ));
        }

        if context.table.table_path.is_empty() {
            return Err(MirrorError::validation_error("Table path cannot be empty"));
        }

        if !context.table.table_path.starts_with('/') {
            return Err(MirrorError::validation_error("Table path must be absolute"));
        }

        Ok(())
    }
}

/// Builder for creating Delta JSON files with fluent API
pub struct DeltaJsonBuilder {
    table_path: String,
    version: i64,
    actions: Vec<Action>,
    metadata: Option<deltalakedb_core::Metadata>,
    protocol: Option<deltalakedb_core::Protocol>,
    options: GenerationOptions,
}

impl DeltaJsonBuilder {
    /// Create a new builder
    pub fn new(table_path: &str, version: i64) -> Self {
        Self {
            table_path: table_path.to_string(),
            version,
            actions: Vec::new(),
            metadata: None,
            protocol: None,
            options: GenerationOptions::default(),
        }
    }

    /// Add an action to the builder
    pub fn add_action(mut self, action: Action) -> Self {
        self.actions.push(action);
        self
    }

    /// Add multiple actions to the builder
    pub fn add_actions(mut self, actions: Vec<Action>) -> Self {
        self.actions.extend(actions);
        self
    }

    /// Set table metadata
    pub fn with_metadata(mut self, metadata: deltalakedb_core::Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Set table protocol
    pub fn with_protocol(mut self, protocol: deltalakedb_core::Protocol) -> Self {
        self.protocol = Some(protocol);
        self
    }

    /// Set generation options
    pub fn with_options(mut self, options: GenerationOptions) -> Self {
        self.options = options;
        self
    }

    /// Enable pretty printing
    pub fn pretty_print(mut self) -> Self {
        self.options.pretty_print = true;
        self
    }

    /// Disable validation
    pub fn no_validation(mut self) -> Self {
        self.options.validate_content = false;
        self
    }

    /// Build the Delta JSON file
    pub fn build(self) -> MirrorResult<DeltaFile> {
        let generator = DeltaJsonGenerator::with_options(self.options);
        generator.generate_commit_file(&self.table_path, self.version, &self.actions)
    }

    /// Build a metadata file
    pub fn build_metadata(self) -> MirrorResult<DeltaFile> {
        let metadata = self.metadata.ok_or_else(|| {
            MirrorError::validation_error("Metadata is required to build metadata file")
        })?;

        let generator = DeltaJsonGenerator::with_options(self.options);
        generator.generate_metadata_file(&self.table_path, self.version, &metadata)
    }

    /// Build a protocol file
    pub fn build_protocol(self) -> MirrorResult<DeltaFile> {
        let protocol = self.protocol.ok_or_else(|| {
            MirrorError::validation_error("Protocol is required to build protocol file")
        })?;

        let generator = DeltaJsonGenerator::with_options(self.options);
        generator.generate_protocol_file(&self.table_path, self.version, &protocol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalakedb_core::{AddFile, RemoveFile, Metadata, Protocol};
    use std::collections::HashMap;
    use chrono::Utc;

    #[test]
    fn test_add_file_to_json() {
        let generator = DeltaJsonGenerator::new();
        let add_file = AddFile {
            path: "test.parquet".to_string(),
            size: 1024,
            modification_time: 1234567890,
            data_change: true,
            stats: Some("{\"numRecords\": 100}".to_string()),
            partition_values: Some(HashMap::from([
                ("year".to_string(), "2023".to_string())
            ])),
            tags: Some(HashMap::from([
                ("format".to_string(), "parquet".to_string())
            ])),
        };

        let result = generator.add_file_to_json(&add_file).unwrap();
        let json_obj = result.as_object().unwrap();
        let add_obj = json_obj.get("add").unwrap().as_object().unwrap();

        assert_eq!(add_obj.get("path").unwrap(), "test.parquet");
        assert_eq!(add_obj.get("size").unwrap(), 1024);
        assert_eq!(add_obj.get("dataChange").unwrap(), true);
    }

    #[test]
    fn test_remove_file_to_json() {
        let generator = DeltaJsonGenerator::new();
        let remove_file = RemoveFile {
            path: "old.parquet".to_string(),
            deletion_timestamp: 1234567890,
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(HashMap::new()),
            tags: Some(HashMap::new()),
        };

        let result = generator.remove_file_to_json(&remove_file).unwrap();
        let json_obj = result.as_object().unwrap();
        let remove_obj = json_obj.get("remove").unwrap().as_object().unwrap();

        assert_eq!(remove_obj.get("path").unwrap(), "old.parquet");
        assert_eq!(remove_obj.get("deletionTimestamp").unwrap(), 1234567890);
        assert_eq!(remove_obj.get("dataChange").unwrap(), true);
    }

    #[test]
    fn test_generate_commit_file() {
        let generator = DeltaJsonGenerator::new();
        let actions = vec![
            Action::AddFile(AddFile {
                path: "test.parquet".to_string(),
                size: 1024,
                modification_time: 1234567890,
                data_change: true,
                stats: None,
                partition_values: None,
                tags: None,
            })
        ];

        let result = generator.generate_commit_file("/table/path", 1, &actions).unwrap();
        assert_eq!(result.format, DeltaFormat::Json);
        assert_eq!(result.file_name, "00000000000000000001.json");
        assert!(result.size > 0);
    }

    #[test]
    fn test_generate_metadata_file() {
        let generator = DeltaJsonGenerator::new();
        let metadata = Metadata {
            id: "test-table".to_string(),
            name: "Test Table".to_string(),
            description: Some("A test table".to_string()),
            format: "parquet".to_string(),
            schema_string: None,
            partition_columns: vec!["year".to_string()],
            configuration: serde_json::json!({"key": "value"}),
            created_time: Some(1234567890),
        };

        let result = generator.generate_metadata_file("/table/path", 0, &metadata).unwrap();
        assert_eq!(result.format, DeltaFormat::Json);
        assert_eq!(result.file_name, "00000000000000000000.json");
        assert!(result.size > 0);
    }

    #[test]
    fn test_generate_protocol_file() {
        let generator = DeltaJsonGenerator::new();
        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        let result = generator.generate_protocol_file("/table/path", 0, &protocol).unwrap();
        assert_eq!(result.format, DeltaFormat::Json);
        assert_eq!(result.file_name, "00000000000000000000.json");
        assert!(result.size > 0);
    }

    #[test]
    fn test_json_builder() {
        let add_file = AddFile {
            path: "test.parquet".to_string(),
            size: 1024,
            modification_time: 1234567890,
            data_change: true,
            stats: None,
            partition_values: None,
            tags: None,
        };

        let result = DeltaJsonBuilder::new("/table/path", 1)
            .add_action(Action::AddFile(add_file))
            .pretty_print()
            .build()
            .unwrap();

        assert_eq!(result.format, DeltaFormat::Json);
        assert_eq!(result.file_name, "00000000000000000001.json");

        let json_str = result.as_string().unwrap();
        // Pretty printed JSON should have newlines and indentation
        assert!(json_str.contains('\n'));
        assert!(json_str.contains("  "));
    }

    #[test]
    fn test_json_builder_metadata() {
        let metadata = Metadata {
            id: "test-table".to_string(),
            name: "Test Table".to_string(),
            description: None,
            format: "parquet".to_string(),
            schema_string: None,
            partition_columns: vec![],
            configuration: serde_json::json!({}),
            created_time: None,
        };

        let result = DeltaJsonBuilder::new("/table/path", 0)
            .with_metadata(metadata)
            .build_metadata()
            .unwrap();

        assert_eq!(result.format, DeltaFormat::Json);
        assert_eq!(result.metadata.get("type").unwrap(), "metadata");
        assert_eq!(result.metadata.get("table_id").unwrap(), "test-table");
    }

    #[test]
    fn test_json_builder_protocol() {
        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        let result = DeltaJsonBuilder::new("/table/path", 0)
            .with_protocol(protocol)
            .build_protocol()
            .unwrap();

        assert_eq!(result.format, DeltaFormat::Json);
        assert_eq!(result.metadata.get("type").unwrap(), "protocol");
        assert_eq!(result.metadata.get("min_reader_version").unwrap(), "1");
        assert_eq!(result.metadata.get("min_writer_version").unwrap(), "2");
    }
}