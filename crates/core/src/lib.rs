//! deltalakedb-core
//!
//! Core domain models and actions for the SQL-backed Delta Lake metadata plane.

#![warn(missing_docs)]

/// Shared Delta JSON action definitions.
pub mod delta;
/// Transaction log abstractions shared across metadata implementations.
pub mod txn_log;
