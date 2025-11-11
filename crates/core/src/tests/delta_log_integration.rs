//! Integration tests for domain models with sample Delta logs

#[cfg(test)]
mod tests {
    use crate::actions::*;
    use crate::commit::*;
    use crate::metadata::*;
    use crate::protocol::*;
    use crate::table::*;
    use serde_json;

    /// Sample Delta protocol version JSON
    const SAMPLE_PROTOCOL_JSON: &str = r#"
    {
        "minReaderVersion": 1,
        "minWriterVersion": 2
    }
    "#;

    /// Sample Delta metadata JSON
    const SAMPLE_METADATA_JSON: &str = r#"
    {
        "id": "821b5c8b-079c-4e23-a179-456714abfd0f",
        "format": {
            "provider": "parquet",
            "options": {}
        },
        "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"data\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
        "partitionColumns": ["year", "month"],
        "configuration": {"delta.autoOptimize.optimizeWrite": "true"},
        "createdTime": 1564524294372
    }
    "#;

    /// Sample add file action JSON
    const SAMPLE_ADD_FILE_JSON: &str = r#"
    {
        "add": {
            "path": "part-00000-cb6b150b-30b8-4334-9951-cc41e85a0171-c000.snappy.parquet",
            "size": 380,
            "modificationTime": 1564524294000,
            "dataChange": true,
            "stats": "{\"numRecords\": 1, \"minValues\": {\"id\": 0}, \"maxValues\": {\"id\": 0}, \"nullCount\": {\"id\": 0}}",
            "partitionValues": {"year": "2019", "month": "7"}
        }
    }
    "#;

    /// Sample commit info JSON
    const SAMPLE_COMMIT_INFO_JSON: &str = r#"
    {
        "timestamp": 1564524294000,
        "userId": "1234567890",
        "userName": "user",
        "operation": "WRITE",
        "operationParameters": {"mode": "Append", "partitionBy": "[\"year\",\"month\"]"},
        "notebook": {"notebookId": "1234567890"},
        "clusterId": "cluster1",
        "readVersion": 0,
        "isolationLevel": "WriteSerializable",
        "isBlindAppend": true,
        "operationMetrics": {"numFiles": "1", "numOutputBytes": "380", "numOutputRows": "1"},
        "tags": {"RESTORE_TIMESTAMP": "1564524294000"}
    }
    "#;

    /// Sample commit file JSON with multiple actions
    const SAMPLE_COMMIT_FILE_JSON: &str = r#"
    [
        {
            "commitInfo": {
                "timestamp": 1564524294000,
                "operation": "WRITE",
                "operationParameters": {"mode": "Append"}
            }
        },
        {
            "protocol": {
                "minReaderVersion": 1,
                "minWriterVersion": 2
            }
        },
        {
            "metaData": {
                "id": "test-table-id",
                "format": {"provider": "parquet"},
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false}]}",
                "partitionColumns": []
            }
        },
        {
            "add": {
                "path": "test-file.parquet",
                "size": 1000,
                "modificationTime": 1564524294000,
                "dataChange": true
            }
        }
    ]
    "#;

    #[test]
    fn test_parse_protocol_from_sample_delta_log() {
        let protocol: Protocol = serde_json::from_str(SAMPLE_PROTOCOL_JSON)
            .expect("Should parse sample protocol JSON");

        assert_eq!(protocol.min_reader_version, 1);
        assert_eq!(protocol.min_writer_version, 2);

        // Test round-trip serialization
        let serialized = serde_json::to_string(&protocol)
            .expect("Should serialize protocol");
        let parsed_again: Protocol = serde_json::from_str(&serialized)
            .expect("Should parse serialized protocol");

        assert_eq!(protocol.min_reader_version, parsed_again.min_reader_version);
        assert_eq!(protocol.min_writer_version, parsed_again.min_writer_version);
    }

    #[test]
    fn test_parse_metadata_from_sample_delta_log() {
        let metadata: Metadata = serde_json::from_str(SAMPLE_METADATA_JSON)
            .expect("Should parse sample metadata JSON");

        assert_eq!(metadata.id, "821b5c8b-079c-4e23-a179-456714abfd0f");
        assert_eq!(metadata.format.provider, "parquet");
        assert_eq!(metadata.partition_columns, vec!["year".to_string(), "month".to_string()]);
        assert!(metadata.configuration.contains_key("delta.autoOptimize.optimizeWrite"));
        assert_eq!(metadata.created_time, 1564524294372);

        // Validate schema is parseable
        let schema: serde_json::Value = serde_json::from_str(&metadata.schema_string)
            .expect("Should parse schema string");

        assert_eq!(schema["type"], "struct");
        assert!(schema["fields"].as_array().unwrap().len() > 0);
    }

    #[test]
    fn test_parse_add_file_action_from_sample_delta_log() {
        let action_value: serde_json::Value = serde_json::from_str(SAMPLE_ADD_FILE_JSON)
            .expect("Should parse add file action JSON");

        let action: Action = serde_json::from_str(SAMPLE_ADD_FILE_JSON)
            .expect("Should parse as Action enum");

        match action {
            Action::AddFile(add_file) => {
                assert_eq!(add_file.path, "part-00000-cb6b150b-30b8-4334-9951-cc41e85a0171-c000.snappy.parquet");
                assert_eq!(add_file.size, 380);
                assert_eq!(add_file.modification_time, 1564524294000);
                assert!(add_file.data_change);
                assert_eq!(add_file.partition_values.get("year"), Some(&"2019".to_string()));
                assert_eq!(add_file.partition_values.get("month"), Some(&"7".to_string()));

                // Validate stats JSON is present and parseable
                if let Some(stats) = &add_file.stats {
                    let stats_value: serde_json::Value = serde_json::from_str(stats)
                        .expect("Should parse stats JSON");
                    assert_eq!(stats_value["numRecords"], 1);
                }
            }
            _ => panic!("Expected AddFile action"),
        }
    }

    #[test]
    fn test_parse_commit_info_from_sample_delta_log() {
        let commit_info: CommitInfo = serde_json::from_str(SAMPLE_COMMIT_INFO_JSON)
            .expect("Should parse sample commit info JSON");

        assert_eq!(commit_info.timestamp, 1564524294000);
        assert_eq!(commit_info.user_id, Some("1234567890".to_string()));
        assert_eq!(commit_info.user_name, Some("user".to_string()));
        assert_eq!(commit_info.operation, "WRITE".to_string());
        assert_eq!(commit_info.read_version, Some(0));
        assert_eq!(commit_info.isolation_level, Some("WriteSerializable".to_string()));
        assert_eq!(commit_info.is_blind_append, Some(true));

        // Check operation parameters
        assert!(commit_info.operation_parameters.contains_key("mode"));
        assert_eq!(commit_info.operation_parameters["mode"], "Append");

        // Check operation metrics
        assert!(commit_info.operation_metrics.contains_key("numFiles"));
        assert_eq!(commit_info.operation_metrics["numFiles"], "1");

        // Check tags
        assert!(commit_info.tags.contains_key("RESTORE_TIMESTAMP"));
    }

    #[test]
    fn test_parse_complete_commit_file_from_sample_delta_log() {
        let actions: Vec<Action> = serde_json::from_str(SAMPLE_COMMIT_FILE_JSON)
            .expect("Should parse sample commit file JSON");

        assert_eq!(actions.len(), 4);

        let mut found_commit_info = false;
        let mut found_protocol = false;
        let mut found_metadata = false;
        let mut found_add_file = false;

        for action in actions {
            match action {
                Action::CommitInfo(_) => found_commit_info = true,
                Action::Protocol(_) => found_protocol = true,
                Action::Metadata(_) => found_metadata = true,
                Action::AddFile(_) => found_add_file = true,
                _ => {}
            }
        }

        assert!(found_commit_info, "Should find CommitInfo action");
        assert!(found_protocol, "Should find Protocol action");
        assert!(found_metadata, "Should find Metadata action");
        assert!(found_add_file, "Should find AddFile action");
    }

    #[test]
    fn test_create_table_from_sample_delta_log() {
        let actions: Vec<Action> = serde_json::from_str(SAMPLE_COMMIT_FILE_JSON)
            .expect("Should parse sample commit file JSON");

        let mut table = Table::test_new("test-table");
        let mut add_files = Vec::new();
        let mut commit_info_found = None;

        // Extract actions from the commit
        for action in actions {
            match action {
                Action::Protocol(protocol) => {
                    table.protocol = protocol;
                }
                Action::Metadata(metadata) => {
                    table.metadata = metadata;
                }
                Action::AddFile(add_file) => {
                    add_files.push(add_file);
                }
                Action::CommitInfo(commit_info) => {
                    commit_info_found = Some(commit_info);
                }
                _ => {}
            }
        }

        // Verify table state
        assert_eq!(table.name, "test-table");
        assert_eq!(table.protocol.min_reader_version, 1);
        assert_eq!(table.protocol.min_writer_version, 2);
        assert_eq!(table.metadata.id, "test-table-id");
        assert_eq!(add_files.len(), 1);
        assert!(commit_info_found.is_some());

        // Verify file details
        let file = &add_files[0];
        assert_eq!(file.path, "test-file.parquet");
        assert_eq!(file.size, 1000);
    }

    #[test]
    fn test_round_trip_serialization_with_sample_data() {
        let original_protocol: Protocol = serde_json::from_str(SAMPLE_PROTOCOL_JSON)
            .expect("Should parse original protocol");

        let original_metadata: Metadata = serde_json::from_str(SAMPLE_METADATA_JSON)
            .expect("Should parse original metadata");

        let original_commit_info: CommitInfo = serde_json::from_str(SAMPLE_COMMIT_INFO_JSON)
            .expect("Should parse original commit info");

        // Serialize and deserialize to test round-trip compatibility
        let serialized_protocol = serde_json::to_string(&original_protocol)
            .expect("Should serialize protocol");
        let roundtrip_protocol: Protocol = serde_json::from_str(&serialized_protocol)
            .expect("Should deserialize protocol");

        let serialized_metadata = serde_json::to_string(&original_metadata)
            .expect("Should serialize metadata");
        let roundtrip_metadata: Metadata = serde_json::from_str(&serialized_metadata)
            .expect("Should deserialize metadata");

        let serialized_commit_info = serde_json::to_string(&original_commit_info)
            .expect("Should serialize commit info");
        let roundtrip_commit_info: CommitInfo = serde_json::from_str(&serialized_commit_info)
            .expect("Should deserialize commit info");

        // Verify round-trip results match originals
        assert_eq!(original_protocol.min_reader_version, roundtrip_protocol.min_reader_version);
        assert_eq!(original_protocol.min_writer_version, roundtrip_protocol.min_writer_version);

        assert_eq!(original_metadata.id, roundtrip_metadata.id);
        assert_eq!(original_metadata.partition_columns, roundtrip_metadata.partition_columns);

        assert_eq!(original_commit_info.operation, roundtrip_commit_info.operation);
        assert_eq!(original_commit_info.timestamp, roundtrip_commit_info.timestamp);
    }

    #[test]
    fn test_protocol_version_compliance() {
        let protocol: Protocol = serde_json::from_str(SAMPLE_PROTOCOL_JSON)
            .expect("Should parse sample protocol JSON");

        // Test protocol version validation
        assert!(protocol.min_reader_version >= 1, "Reader version should be >= 1");
        assert!(protocol.min_writer_version >= 1, "Writer version should be >= 1");

        // Test common protocol version combinations
        assert!(protocol.supports_reader_version(1));
        assert!(protocol.supports_reader_version(2));
        assert!(!protocol.supports_reader_version(0));

        assert!(protocol.supports_writer_version(2));
        assert!(protocol.supports_writer_version(3));
        assert!(!protocol.supports_writer_version(1));
    }

    #[test]
    fn test_metadata_validation() {
        let metadata: Metadata = serde_json::from_str(SAMPLE_METADATA_JSON)
            .expect("Should parse sample metadata JSON");

        // Validate required fields
        assert!(!metadata.id.is_empty(), "Metadata should have a valid ID");
        assert!(!metadata.schema_string.is_empty(), "Metadata should have a schema");
        assert!(metadata.created_time > 0, "Metadata should have a valid creation time");

        // Validate schema structure
        let schema: serde_json::Value = serde_json::from_str(&metadata.schema_string)
            .expect("Should parse schema");

        assert_eq!(schema["type"], "struct");
        assert!(schema["fields"].as_array().unwrap().len() > 0);

        // Validate partition columns exist in schema if specified
        if !metadata.partition_columns.is_empty() {
            let fields = schema["fields"].as_array().unwrap();
            for partition_col in &metadata.partition_columns {
                let found = fields.iter().any(|field| {
                    field["name"].as_str() == Some(partition_col.as_str())
                });
                assert!(found, "Partition column '{}' should exist in schema", partition_col);
            }
        }
    }

    #[test]
    fn test_file_action_validation() {
        let action_value: serde_json::Value = serde_json::from_str(SAMPLE_ADD_FILE_JSON)
            .expect("Should parse add file action JSON");

        let action: Action = serde_json::from_str(SAMPLE_ADD_FILE_JSON)
            .expect("Should parse as Action enum");

        if let Action::AddFile(add_file) = action {
            // Validate required fields
            assert!(!add_file.path.is_empty(), "File path should not be empty");
            assert!(add_file.size > 0, "File size should be positive");
            assert!(add_file.modification_time > 0, "Modification time should be valid");

            // Validate partition values if present
            if !add_file.partition_values.is_empty() {
                assert!(add_file.partition_values.contains_key("year"));
                assert!(add_file.partition_values.contains_key("month"));
            }

            // Validate stats if present
            if let Some(stats) = &add_file.stats {
                let stats_value: serde_json::Value = serde_json::from_str(stats)
                    .expect("Should parse stats JSON");

                assert!(stats_value.get("numRecords").is_some(), "Stats should contain numRecords");
            }
        } else {
            panic!("Expected AddFile action");
        }
    }

    #[test]
    fn test_large_delta_log_parsing() {
        // Create a larger sample Delta log with multiple actions
        let large_commit_json = r#"
        [
            {
                "commitInfo": {
                    "timestamp": 1564524294000,
                    "operation": "WRITE",
                    "operationParameters": {"mode": "Append"}
                }
            },
            {
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 2
                }
            },
            {
                "metaData": {
                    "id": "large-test-table-id",
                    "format": {"provider": "parquet"},
                    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false},{\"name\":\"data\",\"type\":\"string\",\"nullable\":true},{\"name\":\"timestamp\",\"type\":\"timestamp\",\"nullable\":false}]}",
                    "partitionColumns": ["year"]
                }
            }
        ]
        "#;

        let actions: Vec<Action> = serde_json::from_str(large_commit_json)
            .expect("Should parse large commit JSON");

        assert_eq!(actions.len(), 3);

        // Find and validate metadata
        let metadata_action = actions.iter()
            .find(|a| matches!(a, Action::Metadata(_)))
            .expect("Should find Metadata action");

        if let Action::Metadata(metadata) = metadata_action {
            assert_eq!(metadata.id, "large-test-table-id");
            assert_eq!(metadata.partition_columns, vec!["year".to_string()]);

            // Validate schema has more fields
            let schema: serde_json::Value = serde_json::from_str(&metadata.schema_string)
                .expect("Should parse schema");
            assert_eq!(schema["fields"].as_array().unwrap().len(), 3);
        }
    }
}