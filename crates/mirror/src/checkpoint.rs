use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBufferBuilder, BooleanBuilder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
};
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use deltalakedb_core::{
    delta::{MetaDataPayload, ProtocolPayload},
    txn_log::{ActiveFile, Protocol, TableMetadata},
};
use serde_json;

use crate::MirrorError;

/// Serializes table snapshots into Delta-compliant Parquet checkpoints.
pub struct CheckpointSerializer;

impl CheckpointSerializer {
    /// Produces Parquet bytes for the provided snapshot rows.
    pub fn serialize(
        protocol: &Protocol,
        metadata: &TableMetadata,
        files: &[ActiveFile],
        _properties: &HashMap<String, String>,
    ) -> Result<Vec<u8>, MirrorError> {
        let mut actions = Vec::new();
        actions.push(CheckpointAction::Protocol(ProtocolPayload::from(protocol)));
        actions.push(CheckpointAction::MetaData(MetaDataPayload {
            schema_string: metadata.schema_json.clone(),
            partition_columns: metadata.partition_columns.clone(),
            configuration: metadata.configuration.clone(),
        }));
        for file in files {
            actions.push(CheckpointAction::Add(file.clone()));
        }

        let batch = build_record_batch(&actions)?;
        let level = ZstdLevel::try_new(3).unwrap_or_default();
        let props = WriterProperties::builder().set_compression(Compression::ZSTD(level));
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props.build()))
                .map_err(|err| MirrorError::InvalidState(err.to_string()))?;
            writer
                .write(&batch)
                .map_err(|err| MirrorError::InvalidState(err.to_string()))?;
            writer
                .close()
                .map_err(|err| MirrorError::InvalidState(err.to_string()))?;
        }
        Ok(buffer)
    }
}

#[derive(Clone)]
enum CheckpointAction {
    Protocol(ProtocolPayload),
    MetaData(MetaDataPayload),
    Add(ActiveFile),
}

fn build_record_batch(actions: &[CheckpointAction]) -> Result<RecordBatch, MirrorError> {
    let len = actions.len();
    let mut add_path = StringBuilder::with_capacity(len, len * 10);
    let mut add_size = Int64Builder::with_capacity(len);
    let mut add_partition = StringBuilder::with_capacity(len, len * 10);
    let mut add_stats = StringBuilder::with_capacity(len, len * 4);
    let mut add_mod_time = Int64Builder::with_capacity(len);
    let mut add_data_change = BooleanBuilder::new();
    let mut add_validity = BooleanBufferBuilder::new(len);

    let mut remove_path = StringBuilder::with_capacity(len, len * 10);
    let mut remove_del_ts = Int64Builder::with_capacity(len);
    let mut remove_data_change = BooleanBuilder::new();
    let mut remove_validity = BooleanBufferBuilder::new(len);

    let mut metadata_schema = StringBuilder::with_capacity(len, len * 20);
    let mut metadata_partitions = ListBuilder::new(StringBuilder::new());
    let mut metadata_config = StringBuilder::with_capacity(len, len * 20);
    let mut metadata_validity = BooleanBufferBuilder::new(len);

    let mut protocol_reader = Int32Builder::with_capacity(len);
    let mut protocol_writer = Int32Builder::with_capacity(len);
    let mut protocol_validity = BooleanBufferBuilder::new(len);

    for action in actions {
        match action {
            CheckpointAction::Add(add) => {
                add_path.append_value(&add.path);
                add_size.append_value(add.size_bytes as i64);
                add_partition.append_value(
                    serde_json::to_string(&add.partition_values)
                        .map_err(|err| MirrorError::Serialization(err))?,
                );
                add_stats.append_null();
                add_mod_time.append_value(add.modification_time);
                add_data_change.append_value(true);
                add_validity.append(true);

                remove_path.append_null();
                remove_del_ts.append_null();
                remove_data_change.append_null();
                remove_validity.append(false);

                metadata_schema.append_null();
                metadata_config.append_null();
                metadata_partitions.append(false);
                metadata_validity.append(false);

                protocol_reader.append_null();
                protocol_writer.append_null();
                protocol_validity.append(false);
            }
            CheckpointAction::MetaData(meta) => {
                add_path.append_null();
                add_size.append_null();
                add_partition.append_null();
                add_stats.append_null();
                add_mod_time.append_null();
                add_data_change.append_null();
                add_validity.append(false);

                remove_path.append_null();
                remove_del_ts.append_null();
                remove_data_change.append_null();
                remove_validity.append(false);

                metadata_schema.append_value(&meta.schema_string);
                for part in &meta.partition_columns {
                    metadata_partitions.values().append_value(part);
                }
                metadata_partitions.append(true);
                let config_json = serde_json::to_string(&meta.configuration)
                    .map_err(|err| MirrorError::Serialization(err))?;
                metadata_config.append_value(config_json);
                metadata_validity.append(true);

                protocol_reader.append_null();
                protocol_writer.append_null();
                protocol_validity.append(false);
            }
            CheckpointAction::Protocol(proto) => {
                add_path.append_null();
                add_size.append_null();
                add_partition.append_null();
                add_stats.append_null();
                add_mod_time.append_null();
                add_data_change.append_null();
                add_validity.append(false);

                remove_path.append_null();
                remove_del_ts.append_null();
                remove_data_change.append_null();
                remove_validity.append(false);

                metadata_schema.append_null();
                metadata_partitions.append(false);
                metadata_config.append_null();
                metadata_validity.append(false);

                protocol_reader.append_value(proto.min_reader_version as i32);
                protocol_writer.append_value(proto.min_writer_version as i32);
                protocol_validity.append(true);
            }
        }
    }

    let add_fields = vec![
        Arc::new(Field::new("path", DataType::Utf8, true)),
        Arc::new(Field::new("size", DataType::Int64, true)),
        Arc::new(Field::new("partitionValues", DataType::Utf8, true)),
        Arc::new(Field::new("stats", DataType::Utf8, true)),
        Arc::new(Field::new("modificationTime", DataType::Int64, true)),
        Arc::new(Field::new("dataChange", DataType::Boolean, true)),
    ];
    let add_array = StructArray::new(
        Fields::from(add_fields),
        vec![
            Arc::new(add_path.finish()) as ArrayRef,
            Arc::new(add_size.finish()) as ArrayRef,
            Arc::new(add_partition.finish()) as ArrayRef,
            Arc::new(add_stats.finish()) as ArrayRef,
            Arc::new(add_mod_time.finish()) as ArrayRef,
            Arc::new(add_data_change.finish()) as ArrayRef,
        ],
        Some(add_validity.finish().into()),
    );

    let remove_fields = vec![
        Arc::new(Field::new("path", DataType::Utf8, true)),
        Arc::new(Field::new("deletionTimestamp", DataType::Int64, true)),
        Arc::new(Field::new("dataChange", DataType::Boolean, true)),
    ];
    let remove_array = StructArray::new(
        Fields::from(remove_fields),
        vec![
            Arc::new(remove_path.finish()) as ArrayRef,
            Arc::new(remove_del_ts.finish()) as ArrayRef,
            Arc::new(remove_data_change.finish()) as ArrayRef,
        ],
        Some(remove_validity.finish().into()),
    );

    let metadata_partitions_list = metadata_partitions.finish();
    let metadata_fields = vec![
        Arc::new(Field::new("schemaString", DataType::Utf8, true)),
        Arc::new(Field::new(
            "partitionColumns",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )),
        Arc::new(Field::new("configuration", DataType::Utf8, true)),
    ];
    let metadata_array = StructArray::new(
        Fields::from(metadata_fields),
        vec![
            Arc::new(metadata_schema.finish()) as ArrayRef,
            Arc::new(metadata_partitions_list) as ArrayRef,
            Arc::new(metadata_config.finish()) as ArrayRef,
        ],
        Some(metadata_validity.finish().into()),
    );

    let protocol_fields = vec![
        Arc::new(Field::new("minReaderVersion", DataType::Int32, true)),
        Arc::new(Field::new("minWriterVersion", DataType::Int32, true)),
    ];
    let protocol_array = StructArray::new(
        Fields::from(protocol_fields),
        vec![
            Arc::new(protocol_reader.finish()) as ArrayRef,
            Arc::new(protocol_writer.finish()) as ArrayRef,
        ],
        Some(protocol_validity.finish().into()),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("add", add_array.data_type().clone(), true),
        Field::new("remove", remove_array.data_type().clone(), true),
        Field::new("metaData", metadata_array.data_type().clone(), true),
        Field::new("protocol", protocol_array.data_type().clone(), true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(add_array) as ArrayRef,
            Arc::new(remove_array) as ArrayRef,
            Arc::new(metadata_array) as ArrayRef,
            Arc::new(protocol_array) as ArrayRef,
        ],
    )
    .map_err(|err| MirrorError::InvalidState(err.to_string()))
}
