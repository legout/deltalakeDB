//! Canonical JSON serialization for Delta Lake artifacts.
//!
//! Produces byte-for-byte compatible Delta JSON following the specification:
//! - Newline-delimited format (one JSON object per line)
//! - Deterministic field ordering (alphabetical)
//! - Timestamps as integer milliseconds
//! - Optional fields omitted (not serialized as null)

use crate::error::{MirrorError, MirrorResult};
use deltalakedb_core::types::{Action, AddFile, MetadataUpdate, ProtocolUpdate, RemoveFile, TxnAction};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Serialize a list of actions to newline-delimited JSON format.
///
/// # Arguments
///
/// * `actions` - The actions to serialize in order
///
/// # Returns
///
/// Newline-delimited JSON string where each line is a valid JSON object
///
/// # Example
///
/// ```ignore
/// let json = serialize_actions(&[action1, action2])?;
/// // Output:
/// // {"add":{"path":"file1.parquet",...}}
/// // {"remove":{"path":"old.parquet",...}}
/// ```
pub fn serialize_actions(actions: &[Action]) -> MirrorResult<String> {
    let mut lines = Vec::new();

    // Sort actions by type: Protocol, Metadata, Txn, Add, Remove
    let mut sorted_actions = actions.to_vec();
    sorted_actions.sort_by_key(|a| action_priority(a));

    for action in sorted_actions {
        let json = match &action {
            Action::Add(file) => {
                json!({
                    "add": serialize_add_file(file)?
                })
            }
            Action::Remove(file) => {
                json!({
                    "remove": serialize_remove_file(file)?
                })
            }
            Action::Metadata(meta) => {
                json!({
                    "metaData": serialize_metadata(meta)?
                })
            }
            Action::Protocol(proto) => {
                json!({
                    "protocol": serialize_protocol(proto)?
                })
            }
            Action::Txn(txn) => {
                json!({
                    "txn": serialize_txn(txn)?
                })
            }
        };

        // Convert to canonical form (sorted keys)
        let canonical = canonicalize_json(&json)?;
        lines.push(canonical.to_string());
    }

    Ok(lines.join("\n"))
}

/// Serialize an AddFile action to JSON.
fn serialize_add_file(file: &AddFile) -> MirrorResult<Value> {
    let mut map = BTreeMap::new();

    map.insert("path".to_string(), Value::String(file.path.clone()));
    map.insert("size".to_string(), Value::Number(file.size.into()));
    map.insert(
        "modificationTime".to_string(),
        Value::Number(file.modification_time.into()),
    );

    if let Some(stats) = &file.stats {
        // Parse stats JSON string to value
        let stats_val: Value = serde_json::from_str(stats)
            .unwrap_or_else(|_| Value::String(stats.clone()));
        map.insert("stats".to_string(), stats_val);
    }

    if let Some(truncated) = file.stats_truncated {
        map.insert("statsStruncated".to_string(), Value::Bool(truncated));
    }

    if let Some(tags) = &file.tags {
        let tags_map: BTreeMap<String, Value> = tags
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        map.insert("tags".to_string(), Value::Object(tags_map.into()));
    }

    map.insert(
        "dataChangeVersion".to_string(),
        Value::Number(file.data_change_version.into()),
    );

    Ok(Value::Object(map.into()))
}

/// Serialize a RemoveFile action to JSON.
fn serialize_remove_file(file: &RemoveFile) -> MirrorResult<Value> {
    let mut map = BTreeMap::new();

    map.insert("path".to_string(), Value::String(file.path.clone()));
    map.insert(
        "deletionTimestamp".to_string(),
        Value::Number(file.deletion_timestamp.into()),
    );
    map.insert("dataChange".to_string(), Value::Bool(file.data_change));

    if let Some(extended) = file.extended_file_metadata {
        map.insert("extendedFileMetadata".to_string(), Value::Bool(extended));
    }

    if let Some(dv) = &file.deletion_vector {
        let dv_val: Value = serde_json::from_str(dv)
            .unwrap_or_else(|_| Value::String(dv.clone()));
        map.insert("deletionVector".to_string(), dv_val);
    }

    Ok(Value::Object(map.into()))
}

/// Serialize a Metadata action to JSON.
fn serialize_metadata(meta: &MetadataUpdate) -> MirrorResult<Value> {
    let mut map = BTreeMap::new();

    if let Some(desc) = &meta.description {
        map.insert("description".to_string(), Value::String(desc.clone()));
    }

    if let Some(schema) = &meta.schema {
        let schema_val: Value = serde_json::from_str(schema)
            .unwrap_or_else(|_| Value::String(schema.clone()));
        map.insert("schemaString".to_string(), schema_val);
    }

    if let Some(partitions) = &meta.partition_columns {
        let partition_array: Vec<Value> =
            partitions.iter().map(|p| Value::String(p.clone())).collect();
        map.insert("partitionColumns".to_string(), Value::Array(partition_array));
    }

    if let Some(config) = &meta.configuration {
        let config_map: BTreeMap<String, Value> = config
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        map.insert("configuration".to_string(), Value::Object(config_map.into()));
    }

    if let Some(created_time) = meta.created_time {
        map.insert("createdTime".to_string(), Value::Number(created_time.into()));
    }

    Ok(Value::Object(map.into()))
}

/// Serialize a Protocol action to JSON.
fn serialize_protocol(proto: &ProtocolUpdate) -> MirrorResult<Value> {
    let mut map = BTreeMap::new();

    if let Some(reader) = proto.min_reader_version {
        map.insert("minReaderVersion".to_string(), Value::Number(reader.into()));
    }

    if let Some(writer) = proto.min_writer_version {
        map.insert("minWriterVersion".to_string(), Value::Number(writer.into()));
    }

    Ok(Value::Object(map.into()))
}

/// Serialize a Transaction action to JSON.
fn serialize_txn(txn: &TxnAction) -> MirrorResult<Value> {
    let mut map = BTreeMap::new();

    map.insert("appId".to_string(), Value::String(txn.app_id.clone()));
    map.insert("version".to_string(), Value::Number(txn.version.into()));
    map.insert("lastUpdate".to_string(), Value::Number(txn.timestamp.into()));

    Ok(Value::Object(map.into()))
}

/// Determine action priority for deterministic ordering.
/// Order: Protocol (0), Metadata (1), Txn (2), Add (3), Remove (4)
fn action_priority(action: &Action) -> u8 {
    match action {
        Action::Protocol(_) => 0,
        Action::Metadata(_) => 1,
        Action::Txn(_) => 2,
        Action::Add(_) => 3,
        Action::Remove(_) => 4,
    }
}

/// Canonicalize JSON by ensuring consistent key ordering.
fn canonicalize_json(value: &Value) -> MirrorResult<Value> {
    match value {
        Value::Object(map) => {
            let mut canonical = BTreeMap::new();
            for (k, v) in map {
                canonical.insert(k.clone(), canonicalize_json(v)?);
            }
            Ok(Value::Object(canonical.into()))
        }
        Value::Array(arr) => {
            let canonical: Result<Vec<_>, _> =
                arr.iter().map(canonicalize_json).collect();
            Ok(Value::Array(canonical?))
        }
        other => Ok(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_file_serialization() {
        let file = AddFile {
            path: "s3://bucket/file.parquet".to_string(),
            size: 1024,
            modification_time: 1704067200000,
            data_change_version: 1,
            stats: None,
            stats_truncated: None,
            tags: None,
        };

        let result = serialize_add_file(&file);
        assert!(result.is_ok());

        let json = result.unwrap();
        assert_eq!(json.get("path").and_then(|v| v.as_str()), Some("s3://bucket/file.parquet"));
        assert_eq!(json.get("size").and_then(|v| v.as_i64()), Some(1024));
    }

    #[test]
    fn test_action_ordering() {
        let protocol = Action::Protocol(ProtocolUpdate {
            min_reader_version: Some(1),
            min_writer_version: Some(2),
        });

        let metadata = Action::Metadata(MetadataUpdate {
            description: None,
            schema: None,
            partition_columns: None,
            created_time: None,
            configuration: None,
        });

        let file = AddFile {
            path: "file.parquet".to_string(),
            size: 1024,
            modification_time: 0,
            data_change_version: 1,
            stats: None,
            stats_truncated: None,
            tags: None,
        };
        let add = Action::Add(file);

        // Protocol should have priority 0, Metadata 1, Add 3
        assert_eq!(action_priority(&protocol), 0);
        assert_eq!(action_priority(&metadata), 1);
        assert_eq!(action_priority(&add), 3);
    }

    #[test]
    fn test_serialize_actions_newline_delimited() {
        let file1 = AddFile {
            path: "file1.parquet".to_string(),
            size: 1024,
            modification_time: 0,
            data_change_version: 1,
            stats: None,
            stats_truncated: None,
            tags: None,
        };

        let file2 = AddFile {
            path: "file2.parquet".to_string(),
            size: 2048,
            modification_time: 0,
            data_change_version: 1,
            stats: None,
            stats_truncated: None,
            tags: None,
        };

        let result = serialize_actions(&[Action::Add(file1), Action::Add(file2)]);
        assert!(result.is_ok());

        let json = result.unwrap();
        let lines: Vec<&str> = json.lines().collect();
        assert_eq!(lines.len(), 2);

        // Each line should be valid JSON
        for line in lines {
            assert!(serde_json::from_str::<Value>(line).is_ok());
        }
    }
}
