use deltalakedb_core::delta::DeltaAction;

use crate::MirrorError;

/// Canonical serializer for Delta JSON commits.
pub struct JsonCommitSerializer;

impl JsonCommitSerializer {
    /// Serializes the provided action list into newline-delimited JSON bytes.
    pub fn serialize(actions: &[DeltaAction]) -> Result<Vec<u8>, MirrorError> {
        let mut buf = Vec::new();
        for (idx, action) in actions.iter().enumerate() {
            let line = serde_json::to_vec(action)?;
            buf.extend_from_slice(&line);
            if idx + 1 < actions.len() {
                buf.push(b'\n');
            }
        }
        buf.push(b'\n');
        Ok(buf)
    }
}
