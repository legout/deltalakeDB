## Why
The SQL-Backed Delta Metadata system needs a robust, type-safe foundation of Delta Lake domain models to represent tables, files, commits, and protocol information in Rust. These models will serve as the core data structures used throughout the SQL adapters, mirroring engine, and Python bindings.

## What Changes
- Add comprehensive Rust domain models for Delta Lake entities
- Implement Delta JSON format serialization/deserialization
- Add validation and error handling for Delta protocol compliance
- Include comprehensive unit tests and property-based testing

## Impact
- Affected specs: rust-workspace
- Affected code: crates/core/src/lib.rs (will be expanded significantly)
- Dependencies: serde, serde_json, uuid, chrono, thiserror