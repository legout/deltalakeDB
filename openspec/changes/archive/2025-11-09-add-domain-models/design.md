## Context
The SQL-Backed Delta Metadata system needs a robust domain model layer that accurately represents Delta Lake's transaction log structure. This will serve as the foundation for SQL adapters, mirroring engines, and Python bindings. The models must be type-safe, serializable, and compliant with the Delta Lake protocol specification.

## Goals / Non-Goals
- Goals:
  - Type-safe Rust representations of all Delta Lake entities
  - Accurate Delta JSON format serialization/deserialization
  - Protocol compliance validation
  - Comprehensive test coverage
- Non-Goals:
  - Database-specific storage implementations (handled by SQL adapters)
  - Performance optimizations at this layer (focus on correctness)
  - Delta log file reading/writing (handled by separate components)

## Decisions
- Decision: Use serde for JSON serialization with custom field mappings for Delta compatibility
  - Rationale: serde is the de-facto standard for JSON serialization in Rust, handles edge cases well
  - Alternatives considered: manual serialization (too error-prone), other serialization crates (less ecosystem support)

- Decision: Represent Delta actions as an enum with variants for each action type
  - Rationale: Type safety, exhaustive pattern matching, clear semantic meaning
  - Alternatives considered: generic map/dictionary (loses type safety), separate structs (harder to work with)

- Decision: Use UUID for table identifiers and i64 for versions
  - Rationale: Matches Delta protocol specification and provides uniqueness guarantees
  - Alternatives considered: string identifiers (less efficient), custom types (unnecessary complexity)

- Decision: Include comprehensive validation in domain models
  - Rationale: Early error detection, protocol compliance, better debugging
  - Alternatives considered: validation only at higher layers (errors discovered too late)

## Risks / Trade-offs
- Risk: Delta protocol evolution may require breaking changes to domain models
  - Mitigation: Use versioned models, maintain backward compatibility where possible
- Trade-off: Rich domain models vs. simple data structures
  - Decision: Favor rich models with validation for correctness and developer experience
- Risk: Performance overhead of validation and serialization
  - Mitigation: Profile critical paths, consider optional validation in hot paths

## Migration Plan
- No migration required - this is new code added to existing crates
- Existing placeholder code in crates/core will be replaced
- Backward compatibility maintained through careful API design

## Open Questions
- Should we support multiple Delta protocol versions simultaneously?
- How should we handle Delta log files with unknown action types?
- What level of validation should be performed by default vs. optional?