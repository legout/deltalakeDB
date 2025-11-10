//! Delta Lake format generators

use crate::error::{MirrorError, MirrorResult};
use deltalakedb_core::{Action, Commit, Metadata, Protocol, Table};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};

pub mod json;
pub mod parquet;

pub use json::{DeltaJsonGenerator, DeltaJsonBuilder};
pub use parquet::DeltaParquetGenerator;

/// Delta Lake format types
#[derive(Debug, Clone, PartialEq)]
pub enum DeltaFormat {
    /// JSON commit file
    Json,
    /// Parquet checkpoint file
    Parquet,
}

/// Generated Delta file content
#[derive(Debug, Clone)]
pub struct DeltaFile {
    /// File format type
    pub format: DeltaFormat,
    /// File name (including version)
    pub file_name: String,
    /// File content
    pub content: Vec<u8>,
    /// File size in bytes
    pub size: usize,
    /// Compression applied
    pub compression: Option<String>,
    /// Generation timestamp
    pub generated_at: DateTime<Utc>,
    /// File metadata
    pub metadata: HashMap<String, String>,
}

/// Delta file generation context
#[derive(Debug, Clone)]
pub struct GenerationContext {
    /// Table information
    pub table: Table,
    /// Table protocol
    pub protocol: Option<Protocol>,
    /// Table metadata
    pub metadata: Option<Metadata>,
    /// Current commit version
    pub version: i64,
    /// Generation options
    pub options: GenerationOptions,
}

/// Delta file generation options
#[derive(Debug, Clone)]
pub struct GenerationOptions {
    /// Whether to include statistics in generated files
    pub include_statistics: bool,
    /// Whether to validate generated content
    pub validate_content: bool,
    /// Whether to pretty-print JSON output
    pub pretty_print: bool,
    /// Whether to sort JSON object keys
    pub sort_keys: bool,
    /// Whether to include tombstones for removed files
    pub include_tombstones: bool,
    /// Maximum number of actions per file
    pub max_actions_per_file: Option<usize>,
}

impl Default for GenerationOptions {
    fn default() -> Self {
        Self {
            include_statistics: true,
            validate_content: true,
            pretty_print: false,
            sort_keys: true,
            include_tombstones: true,
            max_actions_per_file: None,
        }
    }
}

/// Trait for generating Delta Lake format files
pub trait DeltaGenerator {
    /// Generate a Delta file from the given context and data
    fn generate_file(
        &self,
        context: &GenerationContext,
        data: &DeltaGenerationData,
    ) -> MirrorResult<DeltaFile>;

    /// Generate multiple Delta files from context and data
    fn generate_files(
        &self,
        context: &GenerationContext,
        data: &DeltaGenerationData,
    ) -> MirrorResult<Vec<DeltaFile>> {
        let file = self.generate_file(context, data)?;
        Ok(vec![file])
    }

    /// Validate that the generator can handle the given context
    fn validate_context(&self, context: &GenerationContext) -> MirrorResult<()> {
        if context.version < 0 {
            return Err(MirrorError::validation_error(
                format!("Invalid version: {}", context.version)
            ));
        }

        if context.table.table_path.is_empty() {
            return Err(MirrorError::validation_error("Table path cannot be empty"));
        }

        Ok(())
    }
}

/// Data available for Delta file generation
#[derive(Debug, Clone)]
pub struct DeltaGenerationData {
    /// Commit to generate
    pub commit: Commit,
    /// Actions included in the commit
    pub actions: Vec<Action>,
    /// Previous table metadata (for validation)
    pub previous_metadata: Option<Metadata>,
    /// Previous table protocol (for validation)
    pub previous_protocol: Option<Protocol>,
    /// Additional files to reference
    pub referenced_files: Vec<String>,
}

impl DeltaFile {
    /// Create a new Delta file
    pub fn new(
        format: DeltaFormat,
        file_name: String,
        content: Vec<u8>,
    ) -> Self {
        let size = content.len();
        Self {
            format,
            file_name,
            content,
            size,
            compression: None,
            generated_at: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    /// Create a new Delta file with metadata
    pub fn with_metadata(
        format: DeltaFormat,
        file_name: String,
        content: Vec<u8>,
        metadata: HashMap<String, String>,
    ) -> Self {
        let mut file = Self::new(format, file_name, content);
        file.metadata = metadata;
        file
    }

    /// Set compression information
    pub fn with_compression(mut self, compression: String) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Get file extension based on format
    pub fn extension(&self) -> &str {
        match self.format {
            DeltaFormat::Json => "json",
            DeltaFormat::Parquet => "parquet",
        }
    }

    /// Get MIME type for the file
    pub fn mime_type(&self) -> &str {
        match self.format {
            DeltaFormat::Json => "application/json",
            DeltaFormat::Parquet => "application/parquet",
        }
    }

    /// Get content as string (only valid for JSON files)
    pub fn as_string(&self) -> MirrorResult<&str> {
        match self.format {
            DeltaFormat::Json => {
                std::str::from_utf8(&self.content)
                    .map_err(|e| MirrorError::json_generation_error(
                        format!("Invalid UTF-8 in JSON content: {}", e)
                    ))
            }
            DeltaFormat::Parquet => {
                Err(MirrorError::json_generation_error(
                    "Cannot convert Parquet content to string"
                ))
            }
        }
    }

    /// Get content as JSON value (only valid for JSON files)
    pub fn as_json(&self) -> MirrorResult<Value> {
        let content_str = self.as_string()?;
        serde_json::from_str(content_str)
            .map_err(|e| MirrorError::json_generation_error(
                format!("Invalid JSON content: {}", e)
            ))
    }
}

/// Generate Delta JSON file name for a version
pub fn json_file_name(version: i64) -> String {
    format!("{:020}.json", version)
}

/// Generate Delta checkpoint file name for a version
pub fn checkpoint_file_name(version: i64) -> String {
    format!("{:020}.checkpoint.parquet", version)
}

/// Parse version from Delta file name
pub fn parse_version_from_filename(filename: &str) -> MirrorResult<i64> {
    let parts: Vec<&str> = filename.split('.').collect();
    if parts.len() < 2 {
        return Err(MirrorError::validation_error(
            format!("Invalid Delta file name: {}", filename)
        ));
    }

    parts[0].parse::<i64>()
        .map_err(|e| MirrorError::validation_error(
            format!("Cannot parse version from {}: {}", filename, e)
        ))
}

/// Validate Delta JSON structure
pub fn validate_delta_json(json: &Value) -> MirrorResult<()> {
    if let Some(array) = json.as_array() {
        // Validate commit file (array of actions)
        for action in array {
            validate_action_json(action)?;
        }
    } else if let Some(object) = json.as_object() {
        // Validate metadata or protocol file
        validate_metadata_or_protocol_json(object)?;
    } else {
        return Err(MirrorError::validation_error(
            "Delta JSON must be an array (commit) or object (metadata/protocol)"
        ));
    }

    Ok(())
}

/// Validate action JSON structure
fn validate_action_json(action: &Value) -> MirrorResult<()> {
    if let Some(action_obj) = action.as_object() {
        if !action_obj.contains_key("add") && !action_obj.contains_key("remove") && !action_obj.contains_key("metaData") {
            return Err(MirrorError::validation_error(
                "Action must contain 'add', 'remove', or 'metaData' field"
            ));
        }
    } else {
        return Err(MirrorError::validation_error(
            "Action must be a JSON object"
        ));
    }

    Ok(())
}

/// Validate metadata or protocol JSON structure
fn validate_metadata_or_protocol_json(json: &serde_json::Map<String, Value>) -> MirrorResult<()> {
    if json.contains_key("minReaderVersion") && json.contains_key("minWriterVersion") {
        // Protocol file
        let min_reader = json["minReaderVersion"].as_i64();
        let min_writer = json["minWriterVersion"].as_i64();

        if min_reader.is_none() || min_writer.is_none() {
            return Err(MirrorError::validation_error(
                "Protocol file must have valid minReaderVersion and minWriterVersion"
            ));
        }
    } else if json.contains_key("id") && json.contains_key("format") {
        // Metadata file
        // Basic validation - could be extended
        if !json["format"].as_object().and_then(|f| f.get("provider")).is_some() {
            return Err(MirrorError::validation_error(
                "Metadata file must have format.provider"
            ));
        }
    } else {
        return Err(MirrorError::validation_error(
            "JSON object must be either a protocol or metadata file"
        ));
    }

    Ok(())
}

/// Sort JSON object keys deterministically
pub fn sort_json_keys(json: &mut Value) {
    match json {
        Value::Object(ref mut map) => {
            // Sort keys alphabetically
            let mut sorted_keys: Vec<_> = map.keys().cloned().collect();
            sorted_keys.sort();

            // Create a new sorted map
            let mut sorted_map = serde_json::Map::new();
            for key in sorted_keys {
                let value = map.remove(&key).unwrap();
                sort_json_keys(&mut value.clone());
                sorted_map.insert(key, value);
            }

            *map = sorted_map;
        }
        Value::Array(ref mut arr) => {
            for item in arr.iter_mut() {
                sort_json_keys(item);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalakedb_core::{Action, AddFile};

    #[test]
    fn test_json_file_name() {
        assert_eq!(json_file_name(0), "00000000000000000000.json");
        assert_eq!(json_file_name(1), "00000000000000000001.json");
        assert_eq!(json_file_name(123), "00000000000000000123.json");
    }

    #[test]
    fn test_checkpoint_file_name() {
        assert_eq!(checkpoint_file_name(0), "00000000000000000000.checkpoint.parquet");
        assert_eq!(checkpoint_file_name(1), "00000000000000000001.checkpoint.parquet");
    }

    #[test]
    fn test_parse_version_from_filename() {
        assert_eq!(parse_version_from_filename("00000000000000000000.json").unwrap(), 0);
        assert_eq!(parse_version_from_filename("00000000000000000123.checkpoint.parquet").unwrap(), 123);
    }

    #[test]
    fn test_parse_version_invalid_filename() {
        assert!(parse_version_from_filename("invalid.json").is_err());
        assert!(parse_version_from_filename("abc.json").is_err());
    }

    #[test]
    fn test_delta_file() {
        let json_content = r#"{"test": "value"}"#.as_bytes().to_vec();
        let file = DeltaFile::new(DeltaFormat::Json, "test.json".to_string(), json_content);

        assert_eq!(file.format, DeltaFormat::Json);
        assert_eq!(file.file_name, "test.json");
        assert_eq!(file.extension(), "json");
        assert_eq!(file.mime_type(), "application/json");
        assert_eq!(file.size, 15); // Length of JSON content
        assert!(file.compression.is_none());
    }

    #[test]
    fn test_delta_file_as_string() {
        let json_content = r#"{"test": "value"}"#.as_bytes().to_vec();
        let file = DeltaFile::new(DeltaFormat::Json, "test.json".to_string(), json_content);

        assert_eq!(file.as_string().unwrap(), r#"{"test": "value"}"#);
    }

    #[test]
    fn test_delta_file_as_string_invalid_utf8() {
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let file = DeltaFile::new(DeltaFormat::Json, "test.json".to_string(), invalid_utf8);

        assert!(file.as_string().is_err());
    }

    #[test]
    fn test_validate_delta_json_commit() {
        let commit_json = serde_json::json!([
            {"add": {"path": "test.parquet", "size": 1024}},
            {"remove": {"path": "old.parquet"}}
        ]);

        assert!(validate_delta_json(&commit_json).is_ok());
    }

    #[test]
    fn test_validate_delta_json_protocol() {
        let protocol_json = serde_json::json!({
            "minReaderVersion": 1,
            "minWriterVersion": 2
        });

        assert!(validate_delta_json(&protocol_json).is_ok());
    }

    #[test]
    fn test_validate_delta_json_metadata() {
        let metadata_json = serde_json::json!({
            "id": "test-table",
            "format": {"provider": "parquet"},
            "schemaString": "{}"
        });

        assert!(validate_delta_json(&metadata_json).is_ok());
    }

    #[test]
    fn test_validate_delta_json_invalid() {
        let invalid_json = serde_json::json!({"invalid": "structure"});

        assert!(validate_delta_json(&invalid_json).is_err());
    }

    #[test]
    fn test_generation_options_default() {
        let options = GenerationOptions::default();
        assert!(options.include_statistics);
        assert!(options.validate_content);
        assert!(!options.pretty_print);
        assert!(options.sort_keys);
        assert!(options.include_tombstones);
        assert!(options.max_actions_per_file.is_none());
    }

    #[test]
    fn test_sort_json_keys() {
        let mut json = serde_json::json!({
            "z": 1,
            "a": 2,
            "m": {
                "c": 3,
                "b": 4
            }
        });

        sort_json_keys(&mut json);

        let expected = serde_json::json!({
            "a": 2,
            "m": {
                "b": 4,
                "c": 3
            },
            "z": 1
        });

        assert_eq!(json, expected);
    }
}