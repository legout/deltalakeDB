# OpenSpec Proposal: add-repo-skeleton

## Why
We need a minimal, working Rust + Python skeleton so contributors can build, test, and lint from day one while aligning with the project conventions in openspec/project.md.

## What Changes
- Add Rust workspace with a single `core` crate that builds and passes a trivial test (`cargo build && cargo test`).
- Add Python package `delkalakedb` in `src/` with `pyproject.toml` (src-layout) and a trivial import test (`pytest -q`).
- Add minimal lint/type configs: Rust (rustfmt + clippy), Python (ruff + black + mypy; line length 88).
- Add .gitignore for Rust/Python artifacts; optional pre-commit hooks wiring.
- Optional CI (GitHub Actions) running build, tests, and linters; can be enabled once a remote exists.

## Impact
- New capability: `repo-structure` (initial project infra). No breaking runtime behavior; purely scaffolding and tooling.

---

## Spec Deltas (to be added at openspec/changes/add-repo-skeleton/specs/repo-structure/spec.md)

```markdown
## ADDED Requirements
### Requirement: Repository structure and bootstrap
The repository SHALL provide a minimal Rust+Python layout that builds, tests, and lints successfully on a clean clone.

#### Scenario: Rust workspace builds and tests
- **WHEN** a developer runs `cargo build && cargo test` at repo root
- **THEN** the build succeeds and at least one test passes in the `core` crate

#### Scenario: Python package imports and tests
- **WHEN** a developer runs `pytest -q`
- **THEN** tests pass, including a trivial import test for `delkalakedb`

#### Scenario: Linters configured
- **WHEN** `ruff`, `black --check`, and `mypy` are run for Python, and `cargo fmt --check` and `cargo clippy -D warnings` are run for Rust
- **THEN** all checks pass on a fresh clone without additional setup beyond documented prerequisites

### Requirement: Minimal tooling files
The repository SHALL include essential tooling files for day-1 development.

#### Scenario: Ignore files and configs present
- **WHEN** viewing the repo
- **THEN** `.gitignore` (Rust+Python), Python `pyproject.toml` (src layout, requires-python>=3.12), and Rust workspace `Cargo.toml` are present
```

---

## Tasks (openspec/changes/add-repo-skeleton/tasks.md)
1. Create `.gitignore` covering `target/`, `.venv/`, `__pycache__/`, `.mypy_cache/`, `.ruff_cache/`, `.pytest_cache/`.
2. Add `Cargo.toml` workspace at repo root and scaffold `crates/core` with `Cargo.toml` and `src/lib.rs` (contains one trivial test).
3. Add Python `src/delkalakedb/__init__.py` (expose `__version__ = "0.0.0"`).
4. Add `pyproject.toml` using src-layout (`project.name = "delkalakedb"`, `requires-python = ">=3.12"`, dev tools: ruff/black/mypy). We will switch to `maturin` when Rust bindings land.
5. Add `tests/test_import.py` asserting `import delkalakedb` succeeds.
6. Add lint/type configs: ruff (line-length 88, select error-first), black (88), mypy (strict for package, loose for tests).
7. Add Rust formatting (rustfmt) and clippy settings (treat warnings as errors in CI).
8. Optional: add `.pre-commit-config.yaml` wiring ruff/black/mypy.
9. Optional: add `.github/workflows/ci.yml` running cargo build/test/clippy/fmt and pytest/ruff/mypy.
10. Validate the change with `openspec validate add-repo-skeleton --strict` and fix any issues.

## Notes / Design
- No `design.md` needed; this is pure scaffolding following project conventions.

## Open Questions
1) Confirm Python package name `delkalakedb` (matches project.md) despite repo name `deltalakeDB`.
2) OK to use `hatchling` as temporary Python build backend for src-layout until `maturin` is introduced, or do you prefer a different placeholder (e.g., setuptools)?
3) Do you want initial CI included now (will activate once pushed to GitHub), or defer it to a follow-up change?
4) Rust crates: start with only `core` now and add `sql`/`mirror` later—agree?
