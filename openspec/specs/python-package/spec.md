# python-package Specification

## Purpose
TBD - created by archiving change add-repo-skeleton. Update Purpose after archive.
## Requirements
### Requirement: Python Package Skeleton
The repository SHALL include a Python package using `src/` layout, aligned with Python 3.12 and project conventions.

#### Scenario: Package layout exists
- **WHEN** listing the `src/` directory
- **THEN** a package `delkalakedb` exists with `__init__.py`

#### Scenario: pyproject configuration
- **WHEN** viewing `pyproject.toml`
- **THEN** project metadata exists and tool sections configure black, isort, ruff, and mypy targeting Python 3.12; project management uses `uv` (documented commands: `uv sync`, `uv run`, `uvx`)

### Requirement: Naming Consistency
The Python package MUST use the name `delkalakedb` as per project conventions.

#### Scenario: Import name
- **WHEN** running `python -c "import delkalakedb; print(delkalakedb.__name__)"`
- **THEN** the module imports successfully and reports `delkalakedb`

### Requirement: Migration CLI Interface
The Python package SHALL provide a command-line interface for migrating existing Delta tables to SQL-backed metadata.

#### Scenario: CLI import command
- **WHEN** `deltalakedb import` is executed with table path and database connection
- **THEN** it migrates the table metadata and reports success/failure

#### Scenario: Migration validation command
- **WHEN** `deltalakedb validate` is executed on a migrated table
- **THEN** it compares SQL and file-based snapshots and reports any drift

#### Scenario: Migration status
- **WHEN** `deltalakedb migration-status` is executed
- **THEN** it shows the migration progress and any issues encountered

