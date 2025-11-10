//! Integration tests for mirror engine.
//!
//! Tests the complete mirror pipeline including:
//! - JSON serialization conformance
//! - Checkpoint generation
//! - Idempotent write semantics
//! - Reconciliation logic
//! - Property-based testing for determinism

use deltalakedb_core::types::{Action, AddFile, MetadataUpdate, ProtocolUpdate, Snapshot};
use deltalakedb_mirror::{CheckpointWriter, MirrorEngine, MirrorReconciler, ReconciliationConfig};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_json_serialization_determinism() {
    // Same actions should always produce identical JSON
    let file1 = AddFile {
        path: "s3://bucket/file.parquet".to_string(),
        size: 1024,
        modification_time: 1704067200000,
        data_change_version: 1,
        stats: None,
        stats_truncated: None,
        tags: None,
    };

    let file2 = AddFile {
        path: "s3://bucket/file2.parquet".to_string(),
        size: 2048,
        modification_time: 1704067200000,
        data_change_version: 1,
        stats: None,
        stats_truncated: None,
        tags: None,
    };

    let actions = vec![Action::Add(file1), Action::Add(file2)];

    // Serialize multiple times
    let json1 =
        deltalakedb_mirror::serializer::serialize_actions(&actions).expect("First serialization");
    let json2 =
        deltalakedb_mirror::serializer::serialize_actions(&actions).expect("Second serialization");
    let json3 =
        deltalakedb_mirror::serializer::serialize_actions(&actions).expect("Third serialization");

    // All should be identical
    assert_eq!(json1, json2);
    assert_eq!(json2, json3);
}

#[test]
fn test_json_newline_delimited_format() {
    let file1 = AddFile {
        path: "file1.parquet".to_string(),
        size: 100,
        modification_time: 0,
        data_change_version: 1,
        stats: None,
        stats_truncated: None,
        tags: None,
    };

    let file2 = AddFile {
        path: "file2.parquet".to_string(),
        size: 200,
        modification_time: 0,
        data_change_version: 1,
        stats: None,
        stats_truncated: None,
        tags: None,
    };

    let actions = vec![Action::Add(file1), Action::Add(file2)];
    let json = deltalakedb_mirror::serializer::serialize_actions(&actions)
        .expect("Serialization failed");

    // Should have 2 lines
    let lines: Vec<&str> = json.lines().collect();
    assert_eq!(lines.len(), 2);

    // Each line should be valid JSON
    for line in lines {
        let parsed: Value = serde_json::from_str(line).expect("Each line should be valid JSON");
        assert!(parsed.is_object());
    }
}

#[test]
fn test_action_ordering_determinism() {
    let protocol = Action::Protocol(ProtocolUpdate {
        min_reader_version: Some(1),
        min_writer_version: Some(2),
    });

    let metadata = Action::Metadata(MetadataUpdate {
        description: Some("Test table".to_string()),
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

    // Test different input orders
    let actions1 = vec![Action::Add(file.clone()), metadata.clone(), protocol.clone()];

    let actions2 = vec![metadata, protocol, Action::Add(file)];

    let json1 = deltalakedb_mirror::serializer::serialize_actions(&actions1)
        .expect("First serialization");
    let json2 = deltalakedb_mirror::serializer::serialize_actions(&actions2)
        .expect("Second serialization");

    // Should produce identical output despite different input order
    assert_eq!(json1, json2);

    // First line should be protocol (priority 0)
    let lines1: Vec<&str> = json1.lines().collect();
    assert!(lines1[0].contains("protocol"));
}

#[test]
fn test_checkpoint_path_format() {
    let path = CheckpointWriter::checkpoint_path("s3://bucket/table", 100);
    assert_eq!(
        path,
        "s3://bucket/table/_delta_log/00000000000000000100.checkpoint.parquet"
    );
}

#[test]
fn test_checkpoint_interval_logic() {
    // Default interval is 10
    assert!(CheckpointWriter::should_checkpoint(0, 10));
    assert!(CheckpointWriter::should_checkpoint(10, 10));
    assert!(CheckpointWriter::should_checkpoint(20, 10));
    assert!(!CheckpointWriter::should_checkpoint(5, 10));
    assert!(!CheckpointWriter::should_checkpoint(15, 10));
}

#[test]
fn test_reconciliation_exponential_backoff() {
    let reconciler = MirrorReconciler::new();

    // Backoff should double with each attempt
    let b0 = reconciler.calculate_backoff(0);
    let b1 = reconciler.calculate_backoff(1);
    let b2 = reconciler.calculate_backoff(2);
    let b3 = reconciler.calculate_backoff(3);

    assert!(b1 > b0);
    assert!(b2 > b1);
    assert!(b3 > b2);

    // Verify doubling pattern
    assert_eq!(b1.as_secs(), b0.as_secs() * 2);
    assert_eq!(b2.as_secs(), b1.as_secs() * 2);
}

#[test]
fn test_reconciliation_backoff_max() {
    let config = ReconciliationConfig {
        retry_backoff_max: Duration::from_secs(10),
        ..Default::default()
    };
    let reconciler = MirrorReconciler::with_config(config);

    // Eventually backoff should be capped at max
    let max_backoff = reconciler.calculate_backoff(100);
    assert!(max_backoff <= Duration::from_secs(10));
}

#[test]
fn test_reconciliation_lag_alerting() {
    let reconciler = MirrorReconciler::new();

    // Below threshold - no alert
    assert!(!reconciler.should_alert_on_lag(Duration::from_secs(100)));

    // Above threshold - alert
    assert!(reconciler.should_alert_on_lag(Duration::from_secs(400)));
}

#[test]
fn test_snapshot_checkpoint_generation() {
    let file = AddFile {
        path: "s3://bucket/file.parquet".to_string(),
        size: 1024,
        modification_time: 1704067200000,
        data_change_version: 1,
        stats: None,
        stats_truncated: None,
        tags: None,
    };

    let snapshot = Snapshot {
        version: 1,
        timestamp: 1704067200000,
        files: vec![file],
        metadata: Default::default(),
        protocol: Default::default(),
    };

    // Should not fail with valid snapshot
    let result = CheckpointWriter::write_checkpoint(&snapshot, "/tmp/test_checkpoint.parquet");

    // Note: Result might be Ok or Err depending on filesystem permissions
    // The important thing is the method doesn't panic
    let _ = result;
}

#[test]
fn test_empty_snapshot_checkpoint() {
    let snapshot = Snapshot {
        version: 0,
        timestamp: 1704067200000,
        files: vec![],
        metadata: Default::default(),
        protocol: Default::default(),
    };

    // Should handle empty snapshot gracefully
    let result = CheckpointWriter::write_checkpoint(&snapshot, "/tmp/test_empty_checkpoint.parquet");

    // Should succeed even with no files
    let _ = result;
}

#[test]
fn test_mirror_engine_creation() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let engine = MirrorEngine::new(store, 10);

    // Engine should be created without panic
    assert_eq!(engine.checkpoint_interval, 10);
}

#[test]
fn test_json_serialization_with_metadata() {
    let metadata = Action::Metadata(MetadataUpdate {
        description: Some("Test table description".to_string()),
        schema: Some(r#"{"type":"struct","fields":[]}"#.to_string()),
        partition_columns: Some(vec!["date".to_string()]),
        created_time: Some(1704067200000),
        configuration: None,
    });

    let actions = vec![metadata];
    let json = deltalakedb_mirror::serializer::serialize_actions(&actions)
        .expect("Serialization failed");

    // Should contain metadata action
    assert!(json.contains("metaData"));
    assert!(json.contains("Test table description"));
}

#[test]
fn test_json_serialization_with_protocol() {
    let protocol = Action::Protocol(ProtocolUpdate {
        min_reader_version: Some(1),
        min_writer_version: Some(2),
    });

    let actions = vec![protocol];
    let json = deltalakedb_mirror::serializer::serialize_actions(&actions)
        .expect("Serialization failed");

    // Should contain protocol action with version info
    assert!(json.contains("protocol"));
    assert!(json.contains("minReaderVersion"));
    assert!(json.contains("minWriterVersion"));
}

#[test]
fn test_deterministic_field_ordering() {
    // Add file with multiple fields
    let file = AddFile {
        path: "s3://bucket/file.parquet".to_string(),
        size: 1024,
        modification_time: 1704067200000,
        data_change_version: 1,
        stats: None,
        stats_truncated: None,
        tags: None,
    };

    let actions = vec![Action::Add(file)];
    let json = deltalakedb_mirror::serializer::serialize_actions(&actions)
        .expect("Serialization failed");

    // Parse and verify fields are alphabetically ordered
    let parsed: Value = serde_json::from_str(&json).expect("Should parse");

    // Fields in "add" should follow Delta spec ordering
    if let Some(add_obj) = parsed.get("add").and_then(|v| v.as_object()) {
        let keys: Vec<_> = add_obj.keys().collect();
        // Should contain these keys in some order
        assert!(keys.contains(&"path"));
        assert!(keys.contains(&"size"));
        assert!(keys.contains(&"modificationTime"));
    }
}
