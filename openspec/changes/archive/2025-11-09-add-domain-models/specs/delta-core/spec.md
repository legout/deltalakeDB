## ADDED Requirements

### Requirement: Delta Table Domain Model
The system SHALL provide a comprehensive Table domain model that represents Delta Lake table metadata and configuration.

#### Scenario: Table creation with metadata
- **WHEN** a new Table is created with id, name, location, and protocol version
- **THEN** the Table struct SHALL store all metadata fields with proper types
- **AND** validation SHALL ensure required fields are present

#### Scenario: Table protocol validation
- **WHEN** a Table's protocol is checked for compliance
- **THEN** the system SHALL validate reader and writer versions against supported Delta protocol versions
- **AND** SHALL return an error if the protocol is not supported

### Requirement: Delta File Actions Domain Model
The system SHALL provide domain models for Delta file actions (AddFile and RemoveFile) with all required metadata fields.

#### Scenario: AddFile action creation
- **WHEN** an AddFile action is created with path, size, partition values, and statistics
- **THEN** the AddFile struct SHALL store all fields with proper types
- **AND** SHALL validate that file size is non-negative and path is non-empty

#### Scenario: RemoveFile action creation
- **WHEN** a RemoveFile action is created with path and deletion timestamp
- **THEN** the RemoveFile struct SHALL store the required fields
- **AND** SHALL validate that the deletion timestamp is valid

### Requirement: Delta Commit Domain Model
The system SHALL provide a Commit domain model that represents Delta transaction log entries with versioning and operation tracking.

#### Scenario: Commit creation with actions
- **WHEN** a Commit is created with version, timestamp, operation, and actions
- **THEN** the Commit struct SHALL store all fields with proper types
- **AND** SHALL validate that version is non-negative and actions list is not empty

#### Scenario: Commit serialization to Delta JSON
- **WHEN** a Commit is serialized to Delta JSON format
- **THEN** the output SHALL match Delta protocol specification exactly
- **AND** SHALL include all required fields in correct format

### Requirement: Delta Protocol Versioning
The system SHALL provide a Protocol domain model that handles Delta protocol version compatibility checking.

#### Scenario: Protocol version validation
- **WHEN** a Protocol is checked against Delta specification
- **THEN** the system SHALL validate minimum reader and writer versions
- **AND** SHALL determine if the protocol is supported by current implementation

#### Scenario: Protocol compatibility checking
- **WHEN** two Protocol versions are compared for compatibility
- **THEN** the system SHALL return whether they are compatible
- **AND** SHALL provide specific reasons for incompatibility

### Requirement: Delta JSON Serialization
The system SHALL provide serialization and deserialization of Delta domain models to/from Delta JSON format.

#### Scenario: Domain model serialization
- **WHEN** any domain model is serialized to JSON
- **THEN** the output SHALL conform to Delta Lake JSON specification
- **AND** SHALL handle all field types correctly including nested structures

#### Scenario: Delta JSON deserialization
- **WHEN** Delta JSON is deserialized into domain models
- **THEN** the system SHALL validate JSON structure and required fields
- **AND** SHALL return appropriate errors for malformed input

### Requirement: Domain Model Validation
The system SHALL provide comprehensive validation for all Delta domain models to ensure protocol compliance.

#### Scenario: Table metadata validation
- **WHEN** Table metadata is validated
- **THEN** the system SHALL check all required fields are present and valid
- **AND** SHALL validate table location format and protocol compatibility

#### Scenario: File action validation
- **WHEN** AddFile or RemoveFile actions are validated
- **THEN** the system SHALL check file paths, sizes, and timestamps
- **AND** SHALL validate partition values and statistics format

### Requirement: Error Handling for Domain Models
The system SHALL provide specific error types for domain model validation and protocol compliance failures.

#### Scenario: Invalid Delta protocol error
- **WHEN** an unsupported Delta protocol version is encountered
- **THEN** the system SHALL return a specific ProtocolError
- **AND** SHALL include details about supported versions

#### Scenario: Validation error reporting
- **WHEN** domain model validation fails
- **THEN** the system SHALL return detailed ValidationError
- **AND** SHALL include specific field and validation rule information