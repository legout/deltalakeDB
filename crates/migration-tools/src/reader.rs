//! Reader for Delta Lake transaction logs.
//!
//! Reads checkpoint files and JSON commits from `_delta_log` to reconstruct
//! the complete version history of a Delta table.

use crate::error::{MigrationError, MigrationResult};
use deltalakedb_core::types::Action;
use object_store::ObjectStore;
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Reads Delta Lake transaction logs from object storage.
pub struct DeltaLogReader {
    object_store: Arc<dyn ObjectStore>,
    table_path: String,
}

impl DeltaLogReader {
    /// Create a new Delta log reader.
    pub fn new(object_store: Arc<dyn ObjectStore>, table_path: String) -> Self {
        DeltaLogReader {
            object_store,
            table_path,
        }
    }

    /// Get the path to the _delta_log directory.
    fn delta_log_path(&self) -> String {
        format!("{}/_delta_log", self.table_path)
    }

    /// Find the latest checkpoint version in _delta_log.
    pub async fn find_latest_checkpoint(&self) -> MigrationResult<Option<i64>> {
        let delta_log = self.delta_log_path();
        debug!("Scanning for checkpoints in: {}", delta_log);

        let entries = self
            .object_store
            .list(Some(&delta_log.clone().into()))
            .await
            .map_err(|e| MigrationError::ObjectStoreError(e.to_string()))?;

        let mut checkpoints: Vec<i64> = Vec::new();

        for entry in entries {
            let path = entry.path.as_ref();
            if let Some(name) = path.file_name.as_ref() {
                if let Some(name_str) = name.as_ref().split('/').last() {
                    // Look for checkpoint files: NNNNNNNNNN.checkpoint.parquet
                    if name_str.ends_with(".checkpoint.parquet") {
                        if let Ok(version_str) =
                            name_str.trim_end_matches(".checkpoint.parquet").parse::<i64>()
                        {
                            checkpoints.push(version_str);
                        }
                    }
                }
            }
        }

        if checkpoints.is_empty() {
            debug!("No checkpoints found");
            Ok(None)
        } else {
            checkpoints.sort();
            let latest = checkpoints.last().copied();
            info!("Found checkpoint at version: {:?}", latest);
            Ok(latest)
        }
    }

    /// Find the latest version number in _delta_log.
    pub async fn find_latest_version(&self) -> MigrationResult<i64> {
        let delta_log = self.delta_log_path();
        debug!("Scanning for latest JSON commit in: {}", delta_log);

        let entries = self
            .object_store
            .list(Some(&delta_log.clone().into()))
            .await
            .map_err(|e| MigrationError::ObjectStoreError(e.to_string()))?;

        let mut versions: Vec<i64> = Vec::new();

        for entry in entries {
            let path = entry.path.as_ref();
            if let Some(name) = path.file_name.as_ref() {
                if let Some(name_str) = name.as_ref().split('/').last() {
                    // Look for JSON commit files: NNNNNNNNNN.json (but not checkpoints)
                    if name_str.ends_with(".json") && !name_str.contains("checkpoint") {
                        if let Ok(version_str) = name_str.trim_end_matches(".json").parse::<i64>() {
                            versions.push(version_str);
                        }
                    }
                }
            }
        }

        if versions.is_empty() {
            Err(MigrationError::DeltaLogError(
                "No JSON commits found in _delta_log".to_string(),
            ))
        } else {
            versions.sort();
            let latest = versions.last().copied().unwrap();
            info!("Found latest version: {}", latest);
            Ok(latest)
        }
    }

    /// Read a JSON commit file and parse actions.
    pub async fn read_json_commit(&self, version: i64) -> MigrationResult<Vec<Action>> {
        let path = format!("{}/_delta_log/{:020}.json", self.table_path, version);
        debug!("Reading JSON commit: {}", path);

        let data = self
            .object_store
            .get(&path.clone().into())
            .await
            .map_err(|e| {
                MigrationError::DeltaLogError(format!("Failed to read {}: {}", path, e))
            })?;

        let content = String::from_utf8(data.to_vec()).map_err(|e| {
            MigrationError::ParseError(format!("Invalid UTF-8 in {}: {}", path, e))
        })?;

        let mut actions = Vec::new();

        // Parse newline-delimited JSON
        for (line_num, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }

            let json: Value = serde_json::from_str(line).map_err(|e| {
                MigrationError::ParseError(format!(
                    "Failed to parse line {} in {}: {}",
                    line_num + 1,
                    path,
                    e
                ))
            })?;

            // Convert JSON to Action
            let action = self.parse_action(&json, version)?;
            actions.push(action);
        }

        info!("Parsed {} actions from version {}", actions.len(), version);
        Ok(actions)
    }

    /// Parse a JSON action object into an Action.
    fn parse_action(&self, json: &Value, _version: i64) -> MigrationResult<Action> {
        // Try each action type
        if let Some(add) = json.get("add") {
            self.parse_add_action(add)
        } else if let Some(remove) = json.get("remove") {
            self.parse_remove_action(remove)
        } else if let Some(metadata) = json.get("metaData") {
            self.parse_metadata_action(metadata)
        } else if let Some(protocol) = json.get("protocol") {
            self.parse_protocol_action(protocol)
        } else if let Some(txn) = json.get("txn") {
            self.parse_txn_action(txn)
        } else {
            Err(MigrationError::ParseError(
                "Unknown action type in JSON".to_string(),
            ))
        }
    }

    fn parse_add_action(&self, json: &Value) -> MigrationResult<Action> {
        use deltalakedb_core::types::AddFile;

        let path = json
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| MigrationError::ParseError("Missing 'path' in add action".to_string()))?
            .to_string();

        let size = json
            .get("size")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| {
                MigrationError::ParseError("Missing 'size' in add action".to_string())
            })?;

        let modification_time = json
            .get("modificationTime")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| {
                MigrationError::ParseError("Missing 'modificationTime' in add action".to_string())
            })?;

        let data_change_version = json
            .get("dataChangeVersion")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let stats = json.get("stats").map(|v| v.to_string());
        let stats_truncated = json.get("statsStruncated").and_then(|v| v.as_bool());
        let tags = json.get("tags").and_then(|v| v.as_object()).map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect()
        });

        Ok(Action::Add(AddFile {
            path,
            size,
            modification_time,
            data_change_version,
            stats,
            stats_truncated,
            tags,
        }))
    }

    fn parse_remove_action(&self, json: &Value) -> MigrationResult<Action> {
        use deltalakedb_core::types::RemoveFile;

        let path = json
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                MigrationError::ParseError("Missing 'path' in remove action".to_string())
            })?
            .to_string();

        let deletion_timestamp = json
            .get("deletionTimestamp")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| {
                MigrationError::ParseError("Missing 'deletionTimestamp' in remove action".to_string())
            })?;

        let data_change = json
            .get("dataChange")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let extended_file_metadata = json.get("extendedFileMetadata").and_then(|v| v.as_bool());
        let deletion_vector = json.get("deletionVector").map(|v| v.to_string());

        Ok(Action::Remove(RemoveFile {
            path,
            deletion_timestamp,
            data_change,
            extended_file_metadata,
            deletion_vector,
        }))
    }

    fn parse_metadata_action(&self, json: &Value) -> MigrationResult<Action> {
        use deltalakedb_core::types::MetadataUpdate;

        let description = json.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());
        let schema = json.get("schemaString").map(|v| v.to_string());
        let partition_columns = json
            .get("partitionColumns")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            });
        let created_time = json.get("createdTime").and_then(|v| v.as_i64());
        let configuration = json.get("configuration").and_then(|v| v.as_object()).map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect()
        });

        Ok(Action::Metadata(MetadataUpdate {
            description,
            schema,
            partition_columns,
            created_time,
            configuration,
        }))
    }

    fn parse_protocol_action(&self, json: &Value) -> MigrationResult<Action> {
        use deltalakedb_core::types::ProtocolUpdate;

        let min_reader_version = json.get("minReaderVersion").and_then(|v| v.as_i64());
        let min_writer_version = json.get("minWriterVersion").and_then(|v| v.as_i64());

        Ok(Action::Protocol(ProtocolUpdate {
            min_reader_version,
            min_writer_version,
        }))
    }

    fn parse_txn_action(&self, json: &Value) -> MigrationResult<Action> {
        use deltalakedb_core::types::TxnAction;

        let app_id = json
            .get("appId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                MigrationError::ParseError("Missing 'appId' in txn action".to_string())
            })?
            .to_string();

        let version = json
            .get("version")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| {
                MigrationError::ParseError("Missing 'version' in txn action".to_string())
            })?;

        let timestamp = json
            .get("lastUpdate")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        Ok(Action::Txn(TxnAction {
            app_id,
            version,
            timestamp,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_path_parsing() {
        // Checkpoint files should have format: NNNNNNNNNN.checkpoint.parquet
        assert!("00000000000000000010.checkpoint.parquet".ends_with(".checkpoint.parquet"));
        assert!("00000000000000000010.json".ends_with(".json"));
    }
}
