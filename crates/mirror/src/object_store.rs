use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::MirrorError;

/// Minimal interface for writing mirrored artifacts to an object/object-like store.
#[async_trait]
pub trait ObjectStore: Send + Sync + Clone {
    /// Writes bytes to the `_delta_log` key for the provided table location.
    async fn put_file(
        &self,
        table_location: &str,
        file_name: &str,
        bytes: &[u8],
    ) -> Result<(), MirrorError>;
}

/// Object store implementation that targets the local filesystem.
#[derive(Debug, Default, Clone)]
pub struct LocalFsObjectStore;

#[async_trait]
impl ObjectStore for LocalFsObjectStore {
    async fn put_file(
        &self,
        table_location: &str,
        file_name: &str,
        bytes: &[u8],
    ) -> Result<(), MirrorError> {
        let base = parse_table_location(table_location)?;
        let log_dir = base.join("_delta_log");
        fs::create_dir_all(&log_dir).map_err(|err| MirrorError::ObjectStore(err.to_string()))?;
        let path = log_dir.join(file_name);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|err| MirrorError::ObjectStore(err.to_string()))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|err| MirrorError::ObjectStore(err.to_string()))?;
        Ok(())
    }
}

fn parse_table_location(location: &str) -> Result<PathBuf, MirrorError> {
    if let Some(stripped) = location.strip_prefix("file://") {
        return Ok(Path::new(stripped).to_path_buf());
    }

    if location.contains("://") {
        return Err(MirrorError::InvalidLocation {
            location: location.to_string(),
            message: "unsupported scheme".into(),
        });
    }

    let path = Path::new(location);
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    Ok(std::env::current_dir()
        .map_err(|err| MirrorError::InvalidLocation {
            location: location.to_string(),
            message: err.to_string(),
        })?
        .join(location))
}
