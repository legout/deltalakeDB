//! deltalakedb-mirror
//!
//! Deterministic serializer and worker for mirroring committed actions to `_delta_log`.

#![warn(missing_docs)]

mod checkpoint;
mod error;
mod json;
mod object_store;
mod service;
mod worker;

pub use checkpoint::CheckpointSerializer;
pub use error::MirrorError;
pub use object_store::{LocalFsObjectStore, ObjectStore};
pub use service::{AlertSink, LagAlert, LagSeverity, MirrorService};
pub use worker::{MirrorOutcome, MirrorRunner, MirrorStatus};
