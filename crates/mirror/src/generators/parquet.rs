//! Delta Lake Parquet checkpoint file generator

use crate::error::{MirrorError, MirrorResult};
use crate::generators::{
    DeltaGenerator, DeltaFile, DeltaFormat, GenerationContext, DeltaGenerationData,
    GenerationOptions, checkpoint_file_name,
};
use deltalakedb_core::{Action, AddFile, RemoveFile};
use serde_json::Value;
use std::collections::HashMap;

/// Delta Lake Parquet checkpoint file generator
pub struct DeltaParquetGenerator {
    options: GenerationOptions,
}

impl DeltaParquetGenerator {
    /// Create a new Parquet generator with default options
    pub fn new() -> Self {
        Self {
            options: GenerationOptions::default(),
        }
    }

    /// Create a new Parquet generator with custom options
    pub fn with_options(options: GenerationOptions) -> Self {
        Self { options }
    }

    /// Generate a Delta Parquet checkpoint file
    pub fn generate_checkpoint_file(
        &self,
        table_path: &str,
        version: i64,
        actions: &[Action],
    ) -> MirrorResult<DeltaFile> {
        use arrow::{
            datatypes::{Schema, Field, DataType, Int64Type, StringType, BooleanType},
            record_batch::RecordBatch,
            array::{StringArray, Int64Array, BooleanArray, StructArray},
        };
        use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
        use parquet::file::properties::{WriterProperties, WriterVersion};
        use parquet::basic::Compression;
        use std::sync::Arc;
        use std::io::Cursor;

        // Convert actions to Arrow record batches
        let record_batches = self.actions_to_record_batches(actions)?;

        if record_batches.is_empty() {
            return Err(MirrorError::parquet_generation_error(
                "No actions to generate checkpoint".to_string()
            ));
        }

        // Create unified schema from first batch (or create empty schema)
        let schema = if !record_batches.is_empty() {
            record_batches[0].schema()
        } else {
            Arc::new(Schema::empty())
        };

        // Configure writer properties
        let compression = if let Some(comp_str) = &self.options.compression_algorithm {
            match comp_str.to_lowercase().as_str() {
                "snappy" => Compression::SNAPPY,
                "gzip" => Compression::GZIP,
                "brotli" => Compression::BROTLI,
                "lz4" => Compression::LZ4,
                "zstd" => Compression::ZSTD,
                _ => Compression::SNAPPY,
            }
        } else {
            Compression::SNAPPY
        };

        let row_group_size = self.options.max_batch_size.unwrap_or(1024);

        let writer_props = WriterProperties::builder()
            .set_compression(compression)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_max_row_group_size(row_group_size)
            .build();

        // Write to in-memory buffer
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(writer_props))
            .map_err(|e| MirrorError::parquet_generation_error(
                format!("Failed to create Parquet writer: {}", e)
            ))?;

        // Write record batches
        for batch in record_batches {
            writer.write(&batch)
                .map_err(|e| MirrorError::parquet_generation_error(
                    format!("Failed to write batch: {}", e)
                ))?;
        }

        writer.close()
            .map_err(|e| MirrorError::parquet_generation_error(
                format!("Failed to close Parquet writer: {}", e)
            ))?;

        let file_name = checkpoint_file_name(version);

        let mut metadata = HashMap::new();
        metadata.insert("table_path".to_string(), table_path.to_string());
        metadata.insert("version".to_string(), version.to_string());
        metadata.insert("action_count".to_string(), actions.len().to_string());
        metadata.insert("type".to_string(), "checkpoint".to_string());
        metadata.insert("format".to_string(), "parquet".to_string());
        metadata.insert("generated_at".to_string(), chrono::Utc::now().to_rfc3339());
        metadata.insert("compression".to_string(), format!("{:?}", compression));
        metadata.insert("row_group_size".to_string(), row_group_size.to_string());

        Ok(DeltaFile::with_metadata(
            DeltaFormat::Parquet,
            file_name,
            buffer,
            metadata,
        ))
    }

    /// Convert actions to Arrow record batches
    fn actions_to_record_batches(&self, actions: &[Action]) -> MirrorResult<Vec<RecordBatch>> {
        use arrow::{
            datatypes::{Schema, Field, DataType},
            record_batch::RecordBatch,
            array::{StringArray, Int64Array, BooleanArray, StructArray, Array},
        };
        use std::sync::Arc;

        let mut add_file_records = Vec::new();
        let mut remove_file_records = Vec::new();
        let mut metadata_records = Vec::new();
        let mut protocol_records = Vec::new();

        for action in actions {
            match action {
                Action::AddFile(add_file) => {
                    add_file_records.push(self.create_add_file_record(add_file)?);
                }
                Action::RemoveFile(remove_file) => {
                    remove_file_records.push(self.create_remove_file_record(remove_file)?);
                }
                Action::Metadata(metadata) => {
                    metadata_records.push(self.create_metadata_record(metadata)?);
                }
            }
        }

        let mut batches = Vec::new();

        // Create AddFile batch
        if !add_file_records.is_empty() {
            let schema = self.create_add_file_schema()?;
            let batch = RecordBatch::try_new(Arc::new(schema), add_file_records)
                .map_err(|e| MirrorError::parquet_generation_error(
                    format!("Failed to create AddFile batch: {}", e)
                ))?;
            batches.push(batch);
        }

        // Create RemoveFile batch
        if !remove_file_records.is_empty() {
            let schema = self.create_remove_file_schema()?;
            let batch = RecordBatch::try_new(Arc::new(schema), remove_file_records)
                .map_err(|e| MirrorError::parquet_generation_error(
                    format!("Failed to create RemoveFile batch: {}", e)
                ))?;
            batches.push(batch);
        }

        // Create Metadata batch
        if !metadata_records.is_empty() {
            let schema = self.create_metadata_schema()?;
            let batch = RecordBatch::try_new(Arc::new(schema), metadata_records)
                .map_err(|e| MirrorError::parquet_generation_error(
                    format!("Failed to create Metadata batch: {}", e)
                ))?;
            batches.push(batch);
        }

        // Create Protocol batch if needed (placeholder)
        if !protocol_records.is_empty() {
            let schema = self.create_protocol_schema()?;
            let batch = RecordBatch::try_new(Arc::new(schema), protocol_records)
                .map_err(|e| MirrorError::parquet_generation_error(
                    format!("Failed to create Protocol batch: {}", e)
                ))?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Create AddFile schema
    fn create_add_file_schema(&self) -> MirrorResult<Schema> {
        use arrow::datatypes::{Schema, Field, DataType};

        let fields = vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("size", DataType::Int64, false),
            Field::new("modification_time", DataType::Int64, false),
            Field::new("data_change", DataType::Boolean, false),
            Field::new("stats", DataType::Utf8, true),
            Field::new("partition_values", DataType::Utf8, true), // Simplified as JSON string
        ];

        Ok(Schema::new(fields))
    }

    /// Create RemoveFile schema
    fn create_remove_file_schema(&self) -> MirrorResult<Schema> {
        use arrow::datatypes::{Schema, Field, DataType};

        let fields = vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("deletion_timestamp", DataType::Int64, false),
            Field::new("data_change", DataType::Boolean, false),
            Field::new("extended_file_metadata", DataType::Boolean, true),
        ];

        Ok(Schema::new(fields))
    }

    /// Create Metadata schema
    fn create_metadata_schema(&self) -> MirrorResult<Schema> {
        use arrow::datatypes::{Schema, Field, DataType};

        let fields = vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("format", DataType::Utf8, false), // Simplified as JSON string
            Field::new("schema_string", DataType::Utf8, false),
            Field::new("partition_columns", DataType::Utf8, true), // Simplified as JSON string
            Field::new("configuration", DataType::Utf8, true), // Simplified as JSON string
            Field::new("created_time", DataType::Int64, false),
        ];

        Ok(Schema::new(fields))
    }

    /// Create Protocol schema (placeholder)
    fn create_protocol_schema(&self) -> MirrorResult<Schema> {
        use arrow::datatypes::{Schema, Field, DataType};

        let fields = vec![
            Field::new("min_reader_version", DataType::Int64, false),
            Field::new("min_writer_version", DataType::Int64, false),
        ];

        Ok(Schema::new(fields))
    }

    /// Create AddFile record
    fn create_add_file_record(&self, add_file: &AddFile) -> MirrorResult<Arc<dyn Array>> {
        use arrow::array::{StringArray, Int64Array, BooleanArray, StructArray};
        use std::sync::Arc;

        let path_array = StringArray::from(vec![add_file.path.as_str()]);
        let size_array = Int64Array::from(vec![add_file.size]);
        let modification_time_array = Int64Array::from(vec![add_file.modification_time]);
        let data_change_array = BooleanArray::from(vec![add_file.data_change]);

        let stats_json = add_file.stats.as_ref().cloned().unwrap_or_default();
        let stats_array = StringArray::from(vec![stats_json.as_str()]);

        let partition_values_json = add_file.partition_values.as_ref()
            .map(|pv| serde_json::to_string(pv).unwrap_or_default())
            .unwrap_or_default();
        let partition_values_array = StringArray::from(vec![partition_values_json.as_str()]);

        let struct_array = StructArray::try_new(
            vec![
                Arc::new(Field::new("path", arrow::datatypes::DataType::Utf8, false)),
                Arc::new(Field::new("size", arrow::datatypes::DataType::Int64, false)),
                Arc::new(Field::new("modification_time", arrow::datatypes::DataType::Int64, false)),
                Arc::new(Field::new("data_change", arrow::datatypes::DataType::Boolean, false)),
                Arc::new(Field::new("stats", arrow::datatypes::DataType::Utf8, true)),
                Arc::new(Field::new("partition_values", arrow::datatypes::DataType::Utf8, true)),
            ].into(),
            vec![
                Arc::new(path_array) as Arc<dyn Array>,
                Arc::new(size_array) as Arc<dyn Array>,
                Arc::new(modification_time_array) as Arc<dyn Array>,
                Arc::new(data_change_array) as Arc<dyn Array>,
                Arc::new(stats_array) as Arc<dyn Array>,
                Arc::new(partition_values_array) as Arc<dyn Array>,
            ],
            None,
        ).map_err(|e| MirrorError::parquet_generation_error(
            format!("Failed to create AddFile struct: {}", e)
        ))?;

        Ok(Arc::new(struct_array))
    }

    /// Create RemoveFile record
    fn create_remove_file_record(&self, remove_file: &RemoveFile) -> MirrorResult<Arc<dyn Array>> {
        use arrow::array::{StringArray, Int64Array, BooleanArray, StructArray};
        use std::sync::Arc;

        let path_array = StringArray::from(vec![remove_file.path.as_str()]);
        let deletion_timestamp_array = Int64Array::from(vec![remove_file.deletion_timestamp]);
        let data_change_array = BooleanArray::from(vec![remove_file.data_change]);
        let extended_file_metadata_array = BooleanArray::from(vec![
            remove_file.extended_file_metadata.unwrap_or(false)
        ]);

        let struct_array = StructArray::try_new(
            vec![
                Arc::new(Field::new("path", arrow::datatypes::DataType::Utf8, false)),
                Arc::new(Field::new("deletion_timestamp", arrow::datatypes::DataType::Int64, false)),
                Arc::new(Field::new("data_change", arrow::datatypes::DataType::Boolean, false)),
                Arc::new(Field::new("extended_file_metadata", arrow::datatypes::DataType::Boolean, true)),
            ].into(),
            vec![
                Arc::new(path_array) as Arc<dyn Array>,
                Arc::new(deletion_timestamp_array) as Arc<dyn Array>,
                Arc::new(data_change_array) as Arc<dyn Array>,
                Arc::new(extended_file_metadata_array) as Arc<dyn Array>,
            ],
            None,
        ).map_err(|e| MirrorError::parquet_generation_error(
            format!("Failed to create RemoveFile struct: {}", e)
        ))?;

        Ok(Arc::new(struct_array))
    }

    /// Create Metadata record
    fn create_metadata_record(&self, metadata: &Value) -> MirrorResult<Arc<dyn Array>> {
        use arrow::array::{StringArray, Int64Array, StructArray};
        use std::sync::Arc;

        // Extract metadata fields
        let id = metadata.get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let name = metadata.get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let description = metadata.get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let format_json = metadata.get("format")
            .map(|v| serde_json::to_string(v).unwrap_or_default())
            .unwrap_or_default();
        let schema_string = metadata.get("schemaString")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let partition_columns_json = metadata.get("partitionColumns")
            .map(|v| serde_json::to_string(v).unwrap_or_default())
            .unwrap_or_default();
        let configuration_json = metadata.get("configuration")
            .map(|v| serde_json::to_string(v).unwrap_or_default())
            .unwrap_or_default();
        let created_time = metadata.get("createdTime")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let id_array = StringArray::from(vec![id]);
        let name_array = StringArray::from(vec![name]);
        let description_array = StringArray::from(vec![description]);
        let format_array = StringArray::from(vec![format_json.as_str()]);
        let schema_string_array = StringArray::from(vec![schema_string]);
        let partition_columns_array = StringArray::from(vec![partition_columns_json.as_str()]);
        let configuration_array = StringArray::from(vec![configuration_json.as_str()]);
        let created_time_array = Int64Array::from(vec![created_time]);

        let struct_array = StructArray::try_new(
            vec![
                Arc::new(Field::new("id", arrow::datatypes::DataType::Utf8, false)),
                Arc::new(Field::new("name", arrow::datatypes::DataType::Utf8, true)),
                Arc::new(Field::new("description", arrow::datatypes::DataType::Utf8, true)),
                Arc::new(Field::new("format", arrow::datatypes::DataType::Utf8, false)),
                Arc::new(Field::new("schema_string", arrow::datatypes::DataType::Utf8, false)),
                Arc::new(Field::new("partition_columns", arrow::datatypes::DataType::Utf8, true)),
                Arc::new(Field::new("configuration", arrow::datatypes::DataType::Utf8, true)),
                Arc::new(Field::new("created_time", arrow::datatypes::DataType::Int64, false)),
            ].into(),
            vec![
                Arc::new(id_array) as Arc<dyn Array>,
                Arc::new(name_array) as Arc<dyn Array>,
                Arc::new(description_array) as Arc<dyn Array>,
                Arc::new(format_array) as Arc<dyn Array>,
                Arc::new(schema_string_array) as Arc<dyn Array>,
                Arc::new(partition_columns_array) as Arc<dyn Array>,
                Arc::new(configuration_array) as Arc<dyn Array>,
                Arc::new(created_time_array) as Arc<dyn Array>,
            ],
            None,
        ).map_err(|e| MirrorError::parquet_generation_error(
            format!("Failed to create Metadata struct: {}", e)
        ))?;

        Ok(Arc::new(struct_array))
    }

    /// Convert actions to checkpoint format
    fn actions_to_checkpoint_rows(&self, actions: &[Action]) -> MirrorResult<Vec<CheckpointRow>> {
        let mut rows = Vec::new();

        for action in actions {
            let row = match action {
                Action::AddFile(add_file) => CheckpointRow::AddFile {
                    path: add_file.path.clone(),
                    size: add_file.size,
                    modification_time: add_file.modification_time,
                    data_change: add_file.data_change,
                    stats: add_file.stats.clone(),
                    partition_values: add_file.partition_values.clone(),
                    tags: add_file.tags.clone(),
                },
                Action::RemoveFile(remove_file) => CheckpointRow::RemoveFile {
                    path: remove_file.path.clone(),
                    deletion_timestamp: remove_file.deletion_timestamp,
                    data_change: remove_file.data_change,
                    extended_file_metadata: remove_file.extended_file_metadata,
                    partition_values: remove_file.partition_values.clone(),
                    tags: remove_file.tags.clone(),
                },
                Action::Metadata(metadata) => CheckpointRow::Metadata(metadata.clone()),
            };

            rows.push(row);
        }

        Ok(rows)
    }
}

impl Default for DeltaParquetGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl DeltaGenerator for DeltaParquetGenerator {
    fn generate_file(
        &self,
        context: &GenerationContext,
        data: &DeltaGenerationData,
    ) -> MirrorResult<DeltaFile> {
        // Validate context
        self.validate_context(context)?;

        // Generate checkpoint file
        self.generate_checkpoint_file(
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

        // Checkpoint generation typically requires version >= checkpoint interval
        if context.version < 10 {
            tracing::warn!("Generating checkpoint for version {} (early checkpoint)", context.version);
        }

        Ok(())
    }
}

/// Checkpoint row representing an action in Parquet format
#[derive(Debug, Clone)]
pub enum CheckpointRow {
    /// AddFile action
    AddFile {
        path: String,
        size: i64,
        modification_time: i64,
        data_change: bool,
        stats: Option<String>,
        partition_values: Option<HashMap<String, String>>,
        tags: Option<HashMap<String, String>>,
    },
    /// RemoveFile action
    RemoveFile {
        path: String,
        deletion_timestamp: i64,
        data_change: bool,
        extended_file_metadata: Option<bool>,
        partition_values: Option<HashMap<String, String>>,
        tags: Option<HashMap<String, String>>,
    },
    /// Metadata action
    Metadata(Value),
}

/// Parquet schema definition for Delta checkpoints
#[derive(Debug)]
pub struct CheckpointSchema {
    /// Schema version
    pub version: i32,
    /// Column definitions
    pub columns: Vec<ColumnDefinition>,
}

/// Column definition in Parquet schema
#[derive(Debug)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,
    /// Column type
    pub data_type: ParquetDataType,
    /// Whether column is optional
    pub optional: bool,
    /// Column compression
    pub compression: Option<String>,
}

/// Parquet data types
#[derive(Debug, Clone)]
pub enum ParquetDataType {
    /// Boolean
    Boolean,
    /// Integer (32-bit)
    Int32,
    /// Integer (64-bit)
    Int64,
    /// String
    String,
    /// Binary data
    Binary,
    /// Float (32-bit)
    Float,
    /// Double (64-bit)
    Double,
    /// Timestamp (microseconds)
    Timestamp,
    /// Date
    Date,
    /// Decimal
    Decimal { precision: u8, scale: i8 },
    /// List
    List { item_type: Box<ParquetDataType> },
    /// Map
    Map { key_type: Box<ParquetDataType>, value_type: Box<ParquetDataType> },
    /// Struct
    Struct { fields: Vec<(String, ParquetDataType)> },
}

impl CheckpointSchema {
    /// Create the standard Delta checkpoint schema
    pub fn delta_checkpoint_schema() -> Self {
        Self {
            version: 1,
            columns: vec![
                // Common columns
                ColumnDefinition {
                    name: "action_type".to_string(),
                    data_type: ParquetDataType::String,
                    optional: false,
                    compression: Some("snappy".to_string()),
                },
                ColumnDefinition {
                    name: "file_path".to_string(),
                    data_type: ParquetDataType::String,
                    optional: true,
                    compression: Some("snappy".to_string()),
                },
                ColumnDefinition {
                    name: "file_size".to_string(),
                    data_type: ParquetDataType::Int64,
                    optional: true,
                    compression: Some("snappy".to_string()),
                },
                ColumnDefinition {
                    name: "modification_time".to_string(),
                    data_type: ParquetDataType::Timestamp,
                    optional: true,
                    compression: Some("snappy".to_string()),
                },
                ColumnDefinition {
                    name: "data_change".to_string(),
                    data_type: ParquetDataType::Boolean,
                    optional: true,
                    compression: Some("snappy".to_string()),
                },
                ColumnDefinition {
                    name: "stats".to_string(),
                    data_type: ParquetDataType::String,
                    optional: true,
                    compression: Some("snappy".to_string()),
                },
                // Additional columns would be added here in a full implementation
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalakedb_core::{AddFile, RemoveFile};
    use std::collections::HashMap;

    #[test]
    fn test_parquet_generator_new() {
        let generator = DeltaParquetGenerator::new();
        // Just test that it doesn't panic
        assert!(true);
    }

    #[test]
    fn test_generate_checkpoint_file() {
        let generator = DeltaParquetGenerator::new();
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

        let result = generator.generate_checkpoint_file("/table/path", 10, &actions).unwrap();
        assert_eq!(result.format, DeltaFormat::Parquet);
        assert_eq!(result.file_name, "00000000000000000010.checkpoint.parquet");
        assert!(result.size > 0);
        assert_eq!(result.metadata.get("action_count").unwrap(), "1");
    }

    #[test]
    fn test_checkpoint_schema() {
        let schema = CheckpointSchema::delta_checkpoint_schema();
        assert_eq!(schema.version, 1);
        assert!(!schema.columns.is_empty());

        // Check that common columns exist
        let column_names: Vec<String> = schema.columns.iter()
            .map(|col| col.name.clone())
            .collect();
        assert!(column_names.contains(&"action_type".to_string()));
        assert!(column_names.contains(&"file_path".to_string()));
    }

    #[test]
    fn test_actions_to_checkpoint_rows() {
        let generator = DeltaParquetGenerator::new();

        let add_file = AddFile {
            path: "test.parquet".to_string(),
            size: 1024,
            modification_time: 1234567890,
            data_change: true,
            stats: Some("{}".to_string()),
            partition_values: Some(HashMap::from([("year".to_string(), "2023".to_string())])),
            tags: None,
        };

        let actions = vec![Action::AddFile(add_file)];
        let rows = generator.actions_to_checkpoint_rows(&actions).unwrap();

        assert_eq!(rows.len(), 1);
        match &rows[0] {
            CheckpointRow::AddFile { path, size, data_change, .. } => {
                assert_eq!(path, "test.parquet");
                assert_eq!(*size, 1024);
                assert_eq!(*data_change, true);
            }
            _ => panic!("Expected AddFile row"),
        }
    }

    #[test]
    fn test_parquet_data_types() {
        let boolean_type = ParquetDataType::Boolean;
        let int_type = ParquetDataType::Int64;
        let string_type = ParquetDataType::String;
        let timestamp_type = ParquetDataType::Timestamp;

        // Just test that types can be created
        assert!(matches!(boolean_type, ParquetDataType::Boolean));
        assert!(matches!(int_type, ParquetDataType::Int64));
        assert!(matches!(string_type, ParquetDataType::String));
        assert!(matches!(timestamp_type, ParquetDataType::Timestamp));
    }

    #[test]
    fn test_parquet_generator_validation() {
        let generator = DeltaParquetGenerator::new();

        let valid_context = GenerationContext {
            table: deltalakedb_core::Table {
                id: uuid::Uuid::new_v4(),
                table_path: "/valid/path".to_string(),
                table_name: "valid_table".to_string(),
                table_uuid: uuid::Uuid::new_v4(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
            protocol: None,
            metadata: None,
            version: 10,
            options: GenerationOptions::default(),
        };

        assert!(generator.validate_context(&valid_context).is_ok());

        let invalid_context = GenerationContext {
            table: deltalakedb_core::Table {
                id: uuid::Uuid::new_v4(),
                table_path: "".to_string(), // Invalid: empty path
                table_name: "invalid_table".to_string(),
                table_uuid: uuid::Uuid::new_v4(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
            protocol: None,
            metadata: None,
            version: -1, // Invalid: negative version
            options: GenerationOptions::default(),
        };

        assert!(generator.validate_context(&invalid_context).is_err());
    }
}