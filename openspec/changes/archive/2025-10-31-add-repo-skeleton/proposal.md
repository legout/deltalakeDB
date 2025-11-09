## Why
Establish a minimal, consistent repository skeleton so implementation across Rust and Python can start aligned with project conventions and the PRD.

## What Changes
- Define the base repository layout (root folders and key files)
- Add a Cargo workspace skeleton for planned Rust crates (core, sql, mirror)
- Add a Python package skeleton using `src/` layout for `delkalakedb` and Python 3.12 baseline
- Adopt `uv` as the Python project manager for dependency/env management and tooling execution (via `uv sync` / `uv run` / `uvx`)
- Document minimal developer tooling hooks in specs (formatters/linters), leaving full CI setup for a later change

## Impact
- Affected specs: repo-structure, rust-workspace, python-package
- Affected code: root layout, Cargo workspace, crates/ scaffolding, Python `src/delkalakedb/` scaffolding, `pyproject.toml` baseline

## Reference: Minimal pyproject.toml
```toml
[build-system]
requires = ["hatchling>=1.24.2"]
build-backend = "hatchling.build"

[project]
name = "delkalakedb"
version = "0.0.0"
description = "SQL-backed metadata plane for Delta Lake"
readme = "README.md"
requires-python = ">=3.12"
# License TBD; propose adding via a later change
# license = { text = "" }
authors = [{ name = "Project Authors" }]
dependencies = []

[project.optional-dependencies]
# Install with: `uv sync --extra dev`
dev = [
  "ruff>=0.6.0",
  "black>=24.0.0",
  "isort>=5.13.0",
  "mypy>=1.8.0",
  "pytest>=8.0.0",
]

[tool.ruff]
target-version = "py312"
line-length = 88

[tool.black]
line-length = 88
target-version = ["py312"]

[tool.isort]
profile = "black"

[tool.mypy]
python_version = "3.12"
warn_unused_ignores = true
warn_redundant_casts = true
strict = true
```

## Reference: uv usage
- Create/refresh env and install deps: `uv sync --extra dev`
- Run tools within env: `uv run ruff check .`, `uv run black --check .`, `uv run pytest`
- Use standalone executables when not in venv: `uvx ruff@latest .`

## Notes on Rust compatibility
- Rust uses Cargo and is unaffected by `uv`. Workflows are parallel: `cargo build/test` for Rust; `uv` for Python.
- When pyo3/maturin is introduced, we will update `[build-system]` in a separate change to use `maturin` as build backend while continuing to manage Python deps with `uv`.
