# rust-workspace Specification

## Purpose
TBD - created by archiving change add-repo-skeleton. Update Purpose after archive.
## Requirements
### Requirement: Cargo Workspace
The repository SHALL define a Cargo workspace at the root that includes planned crates for core domain, SQL adapters, and log mirroring.

#### Scenario: Workspace members declared
- **WHEN** viewing `Cargo.toml` at the repository root
- **THEN** a `[workspace]` with `members = ["crates/core", "crates/sql", "crates/mirror"]` is present

#### Scenario: Minimal compilable crates
- **WHEN** running `cargo check`
- **THEN** each workspace member compiles with a minimal `lib.rs` that builds successfully

### Requirement: Crate Naming and Structure
Crate names and directories MUST follow the planned architecture.

#### Scenario: Crate names
- **WHEN** viewing `crates/*/Cargo.toml`
- **THEN** crates are named `deltalakedb-core`, `deltalakedb-sql`, and `deltalakedb-mirror`

