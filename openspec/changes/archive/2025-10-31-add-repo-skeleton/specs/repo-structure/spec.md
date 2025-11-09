## ADDED Requirements
### Requirement: Repository Structure
The repository SHALL provide a minimal, conventional layout to host Rust crates and a Python package, following `openspec/project.md` conventions.

#### Scenario: Root folders exist
- **WHEN** the repository is initialized
- **THEN** the following directories exist at the root: `crates/`, `src/`, `.github/`, `scripts/`

#### Scenario: Top-level metadata files
- **WHEN** the repository is initialized
- **THEN** top-level files exist for `README.md`, `CONTRIBUTING.md`, `.gitignore`, and `pyproject.toml`

### Requirement: Tooling Baseline
The repository MUST document baseline developer tooling for formatting and linting per project conventions.

#### Scenario: Python tool configs present
- **WHEN** developers open the repository
- **THEN** Python tooling configs exist in `pyproject.toml` for black, isort, ruff, and mypy with Python 3.12 baseline

#### Scenario: Rust toolchain expectations
- **WHEN** developers open the repository
- **THEN** Rustfmt and Clippy are expected to run via CI or local tooling; configuration may be default initially
