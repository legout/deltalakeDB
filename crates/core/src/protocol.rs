//! Delta protocol versioning and compatibility.

use crate::{ProtocolError, ValidationError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Current supported protocol versions
pub const MIN_SUPPORTED_READER_VERSION: i32 = 1;
pub const MAX_SUPPORTED_READER_VERSION: i32 = 3;
pub const MIN_SUPPORTED_WRITER_VERSION: i32 = 1;
pub const MAX_SUPPORTED_WRITER_VERSION: i32 = 7;

/// Represents the Delta Lake protocol version and feature compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    /// Minimum reader version required to read the table
    #[serde(rename = "minReaderVersion")]
    pub min_reader_version: i32,

    /// Minimum writer version required to write to the table
    #[serde(rename = "minWriterVersion")]
    pub min_writer_version: i32,

    /// Optional feature-specific reader features (protocol version 3+)
    #[serde(rename = "readerFeatures", skip_serializing_if = "Option::is_none")]
    pub reader_features: Option<Vec<String>>,

    /// Optional feature-specific writer features (protocol version 7+)
    #[serde(rename = "writerFeatures", skip_serializing_if = "Option::is_none")]
    pub writer_features: Option<Vec<String>>,

    /// Additional protocol-specific metadata
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Default for Protocol {
    fn default() -> Self {
        Self {
            min_reader_version: MIN_SUPPORTED_READER_VERSION,
            min_writer_version: MIN_SUPPORTED_WRITER_VERSION,
            reader_features: None,
            writer_features: None,
            metadata: HashMap::new(),
        }
    }
}

impl Protocol {
    /// Creates a new protocol with the specified versions.
    ///
    /// # Arguments
    /// * `min_reader_version` - Minimum reader version required
    /// * `min_writer_version` - Minimum writer version required
    ///
    /// # Errors
    /// Returns `ValidationError` if versions are negative or unsupported
    pub fn new(min_reader_version: i32, min_writer_version: i32) -> Result<Self, ValidationError> {
        if min_reader_version < 0 {
            return Err(ValidationError::InvalidCommitVersion {
                version: min_reader_version as i64
            });
        }
        if min_writer_version < 0 {
            return Err(ValidationError::InvalidCommitVersion {
                version: min_writer_version as i64
            });
        }

        let protocol = Self {
            min_reader_version,
            min_writer_version,
            reader_features: None,
            writer_features: None,
            metadata: HashMap::new(),
        };

        // Validate that the protocol is supported
        protocol.validate_supported_versions()?;
        Ok(protocol)
    }

    /// Creates a new protocol with all specified versions and features.
    pub fn with_features(
        min_reader_version: i32,
        min_writer_version: i32,
        reader_features: Option<Vec<String>>,
        writer_features: Option<Vec<String>>,
    ) -> Result<Self, ValidationError> {
        let mut protocol = Self::new(min_reader_version, min_writer_version)?;
        protocol.reader_features = reader_features;
        protocol.writer_features = writer_features;
        protocol.validate_supported_versions()?;
        Ok(protocol)
    }

    /// Validates that this protocol version is supported by the current implementation.
    pub fn validate_supported_versions(&self) -> Result<(), ProtocolError> {
        if self.min_reader_version > MAX_SUPPORTED_READER_VERSION {
            return Err(ProtocolError::UnsupportedReaderVersion {
                version: self.min_reader_version,
                max_supported: MAX_SUPPORTED_READER_VERSION,
            });
        }

        if self.min_writer_version > MAX_SUPPORTED_WRITER_VERSION {
            return Err(ProtocolError::UnsupportedWriterVersion {
                version: self.min_writer_version,
                max_supported: MAX_SUPPORTED_WRITER_VERSION,
            });
        }

        // Validate reader features if present
        if let Some(features) = &self.reader_features {
            if !features.is_empty() && self.min_reader_version < 3 {
                return Err(ProtocolError::UnsupportedFeatures {
                    features: features.clone(),
                });
            }
        }

        // Validate writer features if present
        if let Some(features) = &self.writer_features {
            if !features.is_empty() && self.min_writer_version < 7 {
                return Err(ProtocolError::UnsupportedFeatures {
                    features: features.clone(),
                });
            }
        }

        Ok(())
    }

    /// Checks if this protocol is compatible with another protocol for reading.
    pub fn is_compatible_for_reading(&self, other: &Protocol) -> bool {
        self.min_reader_version <= other.min_reader_version
            && self.features_compatible_for_reading(other)
    }

    /// Checks if this protocol is compatible with another protocol for writing.
    pub fn is_compatible_for_writing(&self, other: &Protocol) -> bool {
        self.min_writer_version <= other.min_writer_version
            && self.features_compatible_for_writing(other)
    }

    /// Gets the effective minimum reader version considering features.
    pub fn effective_reader_version(&self) -> i32 {
        let mut version = self.min_reader_version;

        // Reader features require version 3+
        if self.reader_features.as_ref().map_or(false, |f| !f.is_empty()) {
            version = version.max(3);
        }

        version
    }

    /// Gets the effective minimum writer version considering features.
    pub fn effective_writer_version(&self) -> i32 {
        let mut version = self.min_writer_version;

        // Writer features require version 7+
        if self.writer_features.as_ref().map_or(false, |f| !f.is_empty()) {
            version = version.max(7);
        }

        // Reader features require version 3+
        if self.reader_features.as_ref().map_or(false, |f| !f.is_empty()) {
            version = version.max(3);
        }

        version
    }

    /// Checks if a specific reader feature is supported.
    pub fn has_reader_feature(&self, feature: &str) -> bool {
        self.reader_features
            .as_ref()
            .map_or(false, |features| features.contains(&feature.to_string()))
    }

    /// Checks if a specific writer feature is supported.
    pub fn has_writer_feature(&self, feature: &str) -> bool {
        self.writer_features
            .as_ref()
            .map_or(false, |features| features.contains(&feature.to_string()))
    }

    /// Adds a reader feature to the protocol.
    pub fn add_reader_feature(&mut self, feature: String) {
        if self.reader_features.is_none() {
            self.reader_features = Some(vec![feature]);
        } else {
            self.reader_features.as_mut().unwrap().push(feature);
        }

        // Update minimum version if needed
        self.min_reader_version = self.min_reader_version.max(3);
    }

    /// Adds a writer feature to the protocol.
    pub fn add_writer_feature(&mut self, feature: String) {
        if self.writer_features.is_none() {
            self.writer_features = Some(vec![feature]);
        } else {
            self.writer_features.as_mut().unwrap().push(feature);
        }

        // Update minimum version if needed
        self.min_writer_version = self.min_writer_version.max(7);
    }

    /// Serializes the protocol to Delta JSON format.
    pub fn to_delta_json(&self) -> Result<String, crate::DeltaError> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    /// Deserializes a protocol from Delta JSON format.
    pub fn from_delta_json(json: &str) -> Result<Self, crate::DeltaError> {
        let protocol: Protocol = serde_json::from_str(json)?;
        protocol.validate_supported_versions()?;
        Ok(protocol)
    }

    fn features_compatible_for_reading(&self, other: &Protocol) -> bool {
        match (&self.reader_features, &other.reader_features) {
            (None, _) => true,
            (Some(self_features), Some(other_features)) => {
                self_features.iter().all(|f| other_features.contains(f))
            }
            (Some(_), None) => false,
        }
    }

    fn features_compatible_for_writing(&self, other: &Protocol) -> bool {
        match (&self.writer_features, &other.writer_features) {
            (None, _) => true,
            (Some(self_features), Some(other_features)) => {
                self_features.iter().all(|f| other_features.contains(f))
            }
            (Some(_), None) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_default() {
        let protocol = Protocol::default();
        assert_eq!(protocol.min_reader_version, 1);
        assert_eq!(protocol.min_writer_version, 1);
        assert!(protocol.reader_features.is_none());
        assert!(protocol.writer_features.is_none());
    }

    #[test]
    fn test_protocol_new() {
        let protocol = Protocol::new(2, 3).unwrap();
        assert_eq!(protocol.min_reader_version, 2);
        assert_eq!(protocol.min_writer_version, 3);
    }

    #[test]
    fn test_protocol_invalid_versions() {
        let result = Protocol::new(-1, 1);
        assert!(result.is_err());

        let result = Protocol::new(1, -1);
        assert!(result.is_err());
    }

    #[test]
    fn test_protocol_validation() {
        let protocol = Protocol::new(MAX_SUPPORTED_READER_VERSION + 1, 1);
        assert!(protocol.is_err());

        let protocol = Protocol::new(1, MAX_SUPPORTED_WRITER_VERSION + 1);
        assert!(protocol.is_err());
    }

    #[test]
    fn test_protocol_features() {
        let mut protocol = Protocol::new(1, 1).unwrap();
        protocol.add_reader_feature("deletionVectors".to_string());
        assert_eq!(protocol.min_reader_version, 3);
        assert!(protocol.has_reader_feature("deletionVectors"));

        protocol.add_writer_feature("columnMapping".to_string());
        assert_eq!(protocol.min_writer_version, 7);
        assert!(protocol.has_writer_feature("columnMapping"));
    }

    #[test]
    fn test_effective_versions() {
        let mut protocol = Protocol::new(1, 1).unwrap();
        assert_eq!(protocol.effective_reader_version(), 1);
        assert_eq!(protocol.effective_writer_version(), 1);

        protocol.add_reader_feature("feature".to_string());
        assert_eq!(protocol.effective_reader_version(), 3);
        assert_eq!(protocol.effective_writer_version(), 3);

        protocol.add_writer_feature("feature".to_string());
        assert_eq!(protocol.effective_reader_version(), 3);
        assert_eq!(protocol.effective_writer_version(), 7);
    }

    #[test]
    fn test_compatibility() {
        let protocol1 = Protocol::new(1, 1).unwrap();
        let protocol2 = Protocol::new(2, 2).unwrap();

        assert!(protocol1.is_compatible_for_reading(&protocol2));
        assert!(protocol1.is_compatible_for_writing(&protocol2));

        assert!(!protocol2.is_compatible_for_reading(&protocol1));
        assert!(!protocol2.is_compatible_for_writing(&protocol1));
    }

    #[test]
    fn test_serialization() {
        let protocol = Protocol::new(1, 2).unwrap();
        let json = protocol.to_delta_json().unwrap();
        let deserialized = Protocol::from_delta_json(&json).unwrap();
        assert_eq!(protocol, deserialized);
    }
}