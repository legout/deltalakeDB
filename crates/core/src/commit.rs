//! Delta commit domain model for transaction log entries.

use crate::{Action, DeltaError, ValidationError};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents a Delta Lake commit - a single transaction log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Commit {
    /// Unique identifier for this commit
    pub id: String,

    /// The commit version (monotonically increasing)
    pub version: i64,

    /// When this commit was created (milliseconds since epoch)
    pub timestamp: i64,

    /// The operation performed in this commit (WRITE, UPDATE, DELETE, etc.)
    pub operation: Option<String>,

    /// User who performed the operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    /// Username of the committer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,

    /// The actions that make up this commit
    pub actions: Vec<Action>,

    /// Additional commit metadata
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: std::collections::HashMap<String, serde_json::Value>,

    /// Isolation level for this commit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isolation_level: Option<String>,

    /// Whether this commit contains only blind appends
    #[serde(default)]
    pub is_blind_append: bool,

    /// Read version this commit was built on
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_version: Option<i64>,

    /// Additional operation parameters
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub operation_parameters: std::collections::HashMap<String, serde_json::Value>,
}

/// Represents a Delta commit file (.json) in the _delta_log directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitFile {
    /// The commit version (derived from filename)
    pub version: i64,

    /// The actions contained in this commit
    pub actions: Vec<Action>,

    /// Commit information if present
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_info: Option<crate::actions::CommitInfo>,
}

impl Commit {
    /// Creates a new commit with the specified version and actions.
    ///
    /// # Arguments
    /// * `version` - The commit version
    /// * `actions` - The actions to include in this commit
    ///
    /// # Errors
    /// Returns `ValidationError` if version is negative or actions list is empty
    pub fn new(version: i64, actions: Vec<Action>) -> Result<Self, ValidationError> {
        if version < 0 {
            return Err(ValidationError::InvalidCommitVersion { version });
        }
        if actions.is_empty() {
            return Err(ValidationError::EmptyCommitActions);
        }

        // Validate all actions
        for action in &actions {
            action.validate()?;
        }

        Ok(Self {
            id: Uuid::new_v4().to_string(),
            version,
            timestamp: chrono::Utc::now().timestamp_millis(),
            operation: None,
            user_id: None,
            user_name: None,
            actions,
            metadata: std::collections::HashMap::new(),
            isolation_level: None,
            is_blind_append: false,
            read_version: None,
            operation_parameters: std::collections::HashMap::new(),
        })
    }

    /// Creates a new commit with a generated ID.
    pub fn with_id(version: i64, actions: Vec<Action>, id: String) -> Result<Self, ValidationError> {
        let mut commit = Self::new(version, actions)?;
        commit.id = id;
        Ok(commit)
    }

    /// Creates a complete commit with all fields specified.
    pub fn complete(
        id: String,
        version: i64,
        timestamp: i64,
        operation: Option<String>,
        user_id: Option<String>,
        user_name: Option<String>,
        actions: Vec<Action>,
        metadata: std::collections::HashMap<String, serde_json::Value>,
        isolation_level: Option<String>,
        is_blind_append: bool,
        read_version: Option<i64>,
        operation_parameters: std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Self, ValidationError> {
        if version < 0 {
            return Err(ValidationError::InvalidCommitVersion { version });
        }
        if actions.is_empty() {
            return Err(ValidationError::EmptyCommitActions);
        }
        if timestamp < 0 {
            return Err(ValidationError::InvalidTimestamp { timestamp });
        }

        // Validate all actions
        for action in &actions {
            action.validate()?;
        }

        Ok(Self {
            id,
            version,
            timestamp,
            operation,
            user_id,
            user_name,
            actions,
            metadata,
            isolation_level,
            is_blind_append,
            read_version,
            operation_parameters,
        })
    }

    /// Sets the operation for this commit.
    pub fn with_operation(mut self, operation: String) -> Self {
        self.operation = Some(operation);
        self
    }

    /// Sets the user information for this commit.
    pub fn with_user(mut self, user_id: Option<String>, user_name: Option<String>) -> Self {
        self.user_id = user_id;
        self.user_name = user_name;
        self
    }

    /// Sets the isolation level for this commit.
    pub fn with_isolation_level(mut self, isolation_level: String) -> Self {
        self.isolation_level = Some(isolation_level);
        self
    }

    /// Sets whether this commit is a blind append.
    pub fn with_blind_append(mut self, is_blind_append: bool) -> Self {
        self.is_blind_append = is_blind_append;
        self
    }

    /// Sets the read version this commit was built on.
    pub fn with_read_version(mut self, read_version: i64) -> Self {
        self.read_version = Some(read_version);
        self
    }

    /// Adds an operation parameter.
    pub fn add_operation_parameter(mut self, key: String, value: serde_json::Value) -> Self {
        if !key.trim().is_empty() {
            self.operation_parameters.insert(key, value);
        }
        self
    }

    /// Adds metadata to the commit.
    pub fn add_metadata(mut self, key: String, value: serde_json::Value) -> Self {
        if !key.trim().is_empty() {
            self.metadata.insert(key, value);
        }
        self
    }

    /// Gets the number of actions in this commit.
    pub fn action_count(&self) -> usize {
        self.actions.len()
    }

    /// Gets all AddFile actions from this commit.
    pub fn add_files(&self) -> Vec<&crate::actions::AddFile> {
        self.actions
            .iter()
            .filter_map(|action| {
                if let crate::actions::Action::AddFile(add_file) = action {
                    Some(add_file)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Gets all RemoveFile actions from this commit.
    pub fn remove_files(&self) -> Vec<&crate::actions::RemoveFile> {
        self.actions
            .iter()
            .filter_map(|action| {
                if let crate::actions::Action::RemoveFile(remove_file) = action {
                    Some(remove_file)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Gets the Metadata action if present.
    pub fn metadata_action(&self) -> Option<&crate::actions::Metadata> {
        self.actions.iter().find_map(|action| {
            if let crate::actions::Action::Metadata(metadata) = action {
                Some(metadata)
            } else {
                None
            }
        })
    }

    /// Gets the Protocol action if present.
    pub fn protocol_action(&self) -> Option<&crate::actions::Protocol> {
        self.actions.iter().find_map(|action| {
            if let crate::actions::Action::Protocol(protocol) = action {
                Some(protocol)
            } else {
                None
            }
        })
    }

    /// Gets all CommitInfo actions from this commit.
    pub fn commit_infos(&self) -> Vec<&crate::actions::CommitInfo> {
        self.actions
            .iter()
            .filter_map(|action| {
                if let crate::actions::Action::CommitInfo(commit_info) = action {
                    Some(commit_info)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Checks if this commit contains data changes (AddFile or RemoveFile with data_change=true).
    pub fn has_data_changes(&self) -> bool {
        self.actions.iter().any(|action| match action {
            crate::actions::Action::AddFile(add_file) => add_file.data_change,
            crate::actions::Action::RemoveFile(remove_file) => remove_file.data_change,
            _ => false,
        })
    }

    /// Gets the total size of all files added in this commit.
    pub fn total_added_size(&self) -> i64 {
        self.add_files().iter().map(|add_file| add_file.size).sum()
    }

    /// Gets the total number of records added in this commit.
    pub fn total_added_records(&self) -> i64 {
        self.add_files()
            .iter()
            .map(|add_file| add_file.num_records.unwrap_or(0))
            .sum()
    }

    /// Validates the commit.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.version < 0 {
            return Err(ValidationError::InvalidCommitVersion {
                version: self.version,
            });
        }
        if self.timestamp < 0 {
            return Err(ValidationError::InvalidTimestamp {
                timestamp: self.timestamp,
            });
        }
        if self.actions.is_empty() {
            return Err(ValidationError::EmptyCommitActions);
        }

        // Validate all actions
        for action in &self.actions {
            action.validate()?;
        }

        Ok(())
    }

    /// Serializes the commit to Delta JSON format.
    pub fn to_delta_json(&self) -> Result<String, DeltaError> {
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    /// Deserializes a commit from Delta JSON format.
    pub fn from_delta_json(json: &str) -> Result<Self, DeltaError> {
        let commit: Commit = serde_json::from_str(json)?;
        commit.validate()?;
        Ok(commit)
    }

    /// Converts this commit to a CommitFile representation.
    pub fn to_commit_file(&self) -> CommitFile {
        let commit_info = self.commit_infos().first().cloned();

        CommitFile {
            version: self.version,
            actions: self.actions.clone(),
            commit_info: commit_info.cloned(),
        }
    }
}

impl CommitFile {
    /// Creates a new CommitFile from version and actions.
    pub fn new(version: i64, actions: Vec<Action>) -> Result<Self, ValidationError> {
        if version < 0 {
            return Err(ValidationError::InvalidCommitVersion { version });
        }
        if actions.is_empty() {
            return Err(ValidationError::EmptyCommitActions);
        }

        // Validate all actions
        for action in &actions {
            action.validate()?;
        }

        Ok(Self {
            version,
            actions,
            commit_info: None,
        })
    }

    /// Creates a CommitFile from JSON content.
    pub fn from_delta_json(json: &str, version: i64) -> Result<Self, DeltaError> {
        if version < 0 {
            return Err(ValidationError::InvalidCommitVersion { version }.into());
        }

        let actions: Vec<Action> = serde_json::from_str(json)?;

        if actions.is_empty() {
            return Err(ValidationError::EmptyCommitActions.into());
        }

        // Look for CommitInfo in actions
        let commit_info = actions.iter().find_map(|action| {
            if let Action::CommitInfo(info) = action {
                Some(info.clone())
            } else {
                None
            }
        });

        Ok(Self {
            version,
            actions,
            commit_info,
        })
    }

    /// Serializes the actions to Delta JSON format.
    pub fn actions_to_json(&self) -> Result<String, DeltaError> {
        serde_json::to_string_pretty(&self.actions).map_err(Into::into)
    }

    /// Gets the number of actions in this commit file.
    pub fn action_count(&self) -> usize {
        self.actions.len()
    }

    /// Validates the commit file.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.version < 0 {
            return Err(ValidationError::InvalidCommitVersion {
                version: self.version,
            });
        }
        if self.actions.is_empty() {
            return Err(ValidationError::EmptyCommitActions);
        }

        // Validate all actions
        for action in &self.actions {
            action.validate()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::{AddFile, Action, Protocol};

    #[test]
    fn test_commit_new() {
        let add_file = AddFile::new("data/file1.parquet".to_string(), 1024).unwrap();
        let actions = vec![Action::AddFile(add_file)];

        let commit = Commit::new(0, actions).unwrap();
        assert_eq!(commit.version, 0);
        assert_eq!(commit.action_count(), 1);
        assert!(commit.has_data_changes());
        assert!(commit.validate().is_ok());
    }

    #[test]
    fn test_commit_invalid_version() {
        let actions = vec![];
        let result = Commit::new(-1, actions);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_empty_actions() {
        let actions = vec![];
        let result = Commit::new(0, actions);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_builder() {
        let add_file = AddFile::new("data/file1.parquet".to_string(), 1024).unwrap();
        let actions = vec![Action::AddFile(add_file)];

        let commit = Commit::new(0, actions)
            .unwrap()
            .with_operation("WRITE".to_string())
            .with_user(Some("user123".to_string()), Some("testuser".to_string()))
            .with_isolation_level("Serializable".to_string())
            .with_blind_append(true)
            .with_read_version(-1)
            .add_operation_parameter("mode".to_string(), serde_json::Value::String("overwrite".to_string()));

        assert_eq!(commit.operation, Some("WRITE".to_string()));
        assert_eq!(commit.user_id, Some("user123".to_string()));
        assert_eq!(commit.user_name, Some("testuser".to_string()));
        assert_eq!(commit.isolation_level, Some("Serializable".to_string()));
        assert!(commit.is_blind_append);
        assert_eq!(commit.read_version, Some(-1));
        assert_eq!(
            commit.operation_parameters.get("mode"),
            Some(&serde_json::Value::String("overwrite".to_string()))
        );
    }

    #[test]
    fn test_commit_action_accessors() {
        let add_file = AddFile::new("data/file1.parquet".to_string(), 1024).unwrap();
        let remove_file = crate::actions::RemoveFile::new("data/file2.parquet".to_string(), 1234567890).unwrap();
        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
            metadata: std::collections::HashMap::new(),
        };

        let actions = vec![
            Action::AddFile(add_file.clone()),
            Action::RemoveFile(remove_file),
            Action::Protocol(protocol.clone()),
        ];

        let commit = Commit::new(0, actions).unwrap();

        assert_eq!(commit.add_files().len(), 1);
        assert_eq!(commit.remove_files().len(), 1);
        assert!(commit.metadata_action().is_none());
        assert_eq!(commit.protocol_action(), Some(&protocol));

        assert_eq!(commit.total_added_size(), 1024);
        assert_eq!(commit.total_added_records(), 0);
    }

    #[test]
    fn test_commit_serialization() {
        let add_file = AddFile::new("data/file1.parquet".to_string(), 1024).unwrap();
        let actions = vec![Action::AddFile(add_file)];

        let commit = Commit::new(0, actions).unwrap();
        let json = commit.to_delta_json().unwrap();
        let deserialized = Commit::from_delta_json(&json).unwrap();
        assert_eq!(commit, deserialized);
    }

    #[test]
    fn test_commit_file() {
        let add_file = AddFile::new("data/file1.parquet".to_string(), 1024).unwrap();
        let actions = vec![Action::AddFile(add_file)];

        let commit_file = CommitFile::new(0, actions).unwrap();
        assert_eq!(commit_file.version, 0);
        assert_eq!(commit_file.action_count(), 1);
        assert!(commit_file.validate().is_ok());
    }

    #[test]
    fn test_commit_file_from_json() {
        let add_file = AddFile::new("data/file1.parquet".to_string(), 1024).unwrap();
        let actions = vec![Action::AddFile(add_file)];

        let json = serde_json::to_string(&actions).unwrap();
        let commit_file = CommitFile::from_delta_json(&json, 0).unwrap();
        assert_eq!(commit_file.version, 0);
        assert_eq!(commit_file.action_count(), 1);
    }
}