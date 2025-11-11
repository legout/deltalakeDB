//! Test JSON serialization functionality for Delta actions

use deltalakedb_core::{DeltaAction, AddFile, RemoveFile, Metadata, Protocol, Txn, Format};
use deltalakedb_sql::mirror::{serialize_delta_json, serialize_single_action, create_delta_commit_file, delta_commit_path};

#[test]
fn test_serialize_add_file() {
    let add_file = AddFile::new(
        "test.parquet".to_string(),
        1024,
        1234567890,
        true,
    ).with_partition_value("date".to_string(), "2023-01-01".to_string());

    let action = DeltaAction::Add(add_file);
    let json = serialize_single_action(&action).unwrap();
    
    assert!(json.contains("\"add\""));
    assert!(json.contains("\"test.parquet\""));
    assert!(json.contains("\"date\":\"2023-01-01\""));
    
    println!("AddFile JSON: {}", json);
}

#[test]
fn test_serialize_remove_file() {
    let remove_file = RemoveFile::new("test.parquet".to_string(), true)
        .with_deletion_timestamp(1234567890);

    let action = DeltaAction::Remove(remove_file);
    let json = serialize_single_action(&action).unwrap();
    
    assert!(json.contains("\"remove\""));
    assert!(json.contains("\"test.parquet\""));
    assert!(json.contains("\"deletionTimestamp\":1234567890"));
    
    println!("RemoveFile JSON: {}", json);
}

#[test]
fn test_serialize_metadata() {
    let format = Format::new("parquet".to_string());
    let metadata = Metadata::new(
        "test-id".to_string(),
        "{\"type\":\"struct\",\"fields\":[]}".to_string(),
        format,
    ).with_partition_column("date".to_string());

    let action = DeltaAction::Metadata(metadata);
    let json = serialize_single_action(&action).unwrap();
    
    assert!(json.contains("\"metaData\""));
    assert!(json.contains("\"test-id\""));
    assert!(json.contains("\"partitionColumns\":[\"date\"]"));
    
    println!("Metadata JSON: {}", json);
}

#[test]
fn test_serialize_protocol() {
    let protocol = Protocol::new(2, 3);
    let action = DeltaAction::Protocol(protocol);
    let json = serialize_single_action(&action).unwrap();
    
    assert!(json.contains("\"protocol\""));
    assert!(json.contains("\"minReaderVersion\":2"));
    assert!(json.contains("\"minWriterVersion\":3"));
    
    println!("Protocol JSON: {}", json);
}

#[test]
fn test_serialize_transaction() {
    let txn = Txn::new("app-123".to_string(), 10, 1234567890);
    let action = DeltaAction::Transaction(txn);
    let json = serialize_single_action(&action).unwrap();
    
    assert!(json.contains("\"txn\""));
    assert!(json.contains("\"app-123\""));
    assert!(json.contains("\"version\":10"));
    
    println!("Transaction JSON: {}", json);
}

#[test]
fn test_serialize_multiple_actions() {
    let actions = vec![
        DeltaAction::Protocol(Protocol::new(1, 2)),
        DeltaAction::Metadata(Metadata::new(
            "test-id".to_string(),
            "{\"type\":\"struct\"}".to_string(),
            Format::default(),
        )),
        DeltaAction::Add(AddFile::new("data.parquet".to_string(), 2048, 1234567890, true)),
    ];

    let json = serialize_delta_json(&actions).unwrap();
    let lines: Vec<&str> = json.split('\n').collect();
    
    assert_eq!(lines.len(), 3);
    assert!(lines[0].contains("\"protocol\""));
    assert!(lines[1].contains("\"metaData\""));
    assert!(lines[2].contains("\"add\""));
    
    println!("Multiple actions JSON:\n{}", json);
}

#[test]
fn test_create_delta_commit_file() {
    let actions = vec![
        DeltaAction::Protocol(Protocol::default()),
        DeltaAction::Add(AddFile::new("test.parquet".to_string(), 1024, 1234567890, true)),
    ];

    let content = create_delta_commit_file("test_table", 1, &actions).unwrap();
    let lines: Vec<&str> = content.split('\n').collect();
    
    assert_eq!(lines.len(), 2);
    assert!(content.contains("\"protocol\""));
    assert!(content.contains("\"add\""));
    
    println!("Delta commit file content:\n{}", content);
}

#[test]
fn test_create_delta_commit_file_empty_actions() {
    let actions = vec![];
    let result = create_delta_commit_file("test_table", 1, &actions);
    assert!(result.is_err());
}

#[test]
fn test_delta_commit_path() {
    let path = delta_commit_path("test_table", 1);
    assert_eq!(path, "_delta_log/00000000000000000001.json");
    
    let path = delta_commit_path("test_table", 123);
    assert_eq!(path, "_delta_log/00000000000000000123.json");
    
    println!("Delta commit path: {}", path);
}

fn main() {
    println!("Testing Delta JSON serialization...");
    
    test_serialize_add_file();
    test_serialize_remove_file();
    test_serialize_metadata();
    test_serialize_protocol();
    test_serialize_transaction();
    test_serialize_multiple_actions();
    test_create_delta_commit_file();
    test_create_delta_commit_file_empty_actions();
    test_delta_commit_path();
    
    println!("All tests passed!");
}