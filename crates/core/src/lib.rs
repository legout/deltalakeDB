//! deltalakedb-core
//!
//! Core domain models and actions for the SQL-backed Delta Lake metadata plane.

#![warn(missing_docs)]

mod error;
mod table;
mod actions;
mod protocol;
mod metadata;
mod commit;

#[cfg(test)]
mod tests;

pub use error::{DeltaError, ProtocolError, ValidationError};
pub use table::Table;
pub use actions::{Action, AddFile, RemoveFile};
pub use protocol::Protocol;
pub use metadata::Metadata;
pub use commit::Commit;
