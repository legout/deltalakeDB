//! deltalakedb-mirror
//!
//! Deterministic serializer and worker for mirroring committed actions to `_delta_log`.

#![warn(missing_docs)]

mod error;
mod json;
mod object_store;
mod worker;

pub use error::MirrorError;
pub use object_store::{LocalFsObjectStore, ObjectStore};
pub use worker::{MirrorOutcome, MirrorRunner, MirrorStatus};
