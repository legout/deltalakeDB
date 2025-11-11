//! Simple test for JSON serialization functionality

use deltalakedb_core::{DeltaAction, AddFile, RemoveFile, Metadata, Protocol, Txn, Format};
use serde_json;

fn serialize_single_action(action: &DeltaAction) -> Result<String, Box<dyn std::error::Error>> {
    match action {
        DeltaAction::Add(add_file) => {
            let mut json = serde_json::Map::new();
            json.insert("add".to_string(), serde_json::to_value(add_file)?);
            Ok(serde_json::to_string(&json)?)
        }
        DeltaAction::Remove(remove_file) => {
            let mut json = serde_json::Map::new();
            json.insert("remove".to_string(), serde_json::to_value(remove_file)?);
            Ok(serde_json::to_string(&json)?)
        }
        DeltaAction::Metadata(metadata) => {
            let mut json = serde_json::Map::new();
            json.insert("metaData".to_string(), serde_json::to_value(metadata)?);
            Ok(serde_json::to_string(&json)?)
        }
        DeltaAction::Protocol(protocol) => {
            let mut json = serde_json::Map::new();
            json.insert("protocol".to_string(), serde_json::to_value(protocol)?);
            Ok(serde_json::to_string(&json)?)
        }
        DeltaAction::Transaction(txn) => {
            let mut json = serde_json::Map::new();
            json.insert("txn".to_string(), serde_json::to_value(txn)?);
            Ok(serde_json::to_string(&json)?)
        }
    }
}

fn main() {
    println!("Testing Delta JSON serialization...");
    
    // Test AddFile
    let add_file = AddFile::new(
        "test.parquet".to_string(),
        1024,
        1234567890,
        true,
    ).with_partition_value("date".to_string(), "2023-01-01".to_string());

    let action = DeltaAction::Add(add_file);
    let json = serialize_single_action(&action).unwrap();
    println!("AddFile JSON: {}", json);
    assert!(json.contains("\"add\""));
    assert!(json.contains("\"test.parquet\""));
    assert!(json.contains("\"date\":\"2023-01-01\""));
    
    // Test Protocol
    let protocol = Protocol::new(2, 3);
    let action = DeltaAction::Protocol(protocol);
    let json = serialize_single_action(&action).unwrap();
    println!("Protocol JSON: {}", json);
    assert!(json.contains("\"protocol\""));
    assert!(json.contains("\"minReaderVersion\":2"));
    assert!(json.contains("\"minWriterVersion\":3"));
    
    // Test Metadata
    let format = Format::new("parquet".to_string());
    let metadata = Metadata::new(
        "test-id".to_string(),
        "{\"type\":\"struct\",\"fields\":[]}".to_string(),
        format,
    ).with_partition_column("date".to_string());

    let action = DeltaAction::Metadata(metadata);
    let json = serialize_single_action(&action).unwrap();
    println!("Metadata JSON: {}", json);
    assert!(json.contains("\"metaData\""));
    assert!(json.contains("\"test-id\""));
    assert!(json.contains("\"partitionColumns\":[\"date\"]"));
    
    println!("✅ All JSON serialization tests passed!");
}