# Contributing to deltalakedb

We welcome contributions! This document provides guidelines for contributing to `deltalakedb`.

## Development Workflow

1. **Set up your environment**
   ```bash
   # Clone the repository
   git clone <repo-url>
   cd deltalakedb

   # Install Python dependencies with uv
   uv sync --extra dev

   # Verify Rust toolchain
   cargo --version
   ```

2. **Make changes**
   - Follow the project conventions in `openspec/project.md`.
   - For new features or breaking changes, create an OpenSpec proposal first (see `openspec/AGENTS.md`).

3. **Run checks**
   ```bash
   # Python linting and formatting
   uv run ruff check .
   uv run black --check .
   uv run mypy src/

   # Rust checks
   cargo fmt --check
   cargo clippy -- -D warnings
cargo test
    ```

4. **Integration testing (optional)**
   ```bash
   # Start Postgres and multiple S3-compatible object stores for integration tests
   ./scripts/test-env.sh start

   # Run integration tests (when available)
   # cargo test --features integration
   # uv run pytest tests/integration/

   # Stop services when done
   ./scripts/test-env.sh stop
   ```

   The test environment includes:
   - Postgres database
   - MinIO S3-compatible object store
   - SeaweedFS S3 gateway
   - RustFS object store
   - Garage S3-compatible object store

   See README.md for detailed endpoints and credentials.

5. **Submit a pull request**
   - Ensure CI passes.
   - Include tests for new functionality.
   - Update documentation as needed.

## Code Style

- Rust: `rustfmt` and `clippy` enforced.
- Python: Black (88 chars), isort, Ruff, and MyPy.
- Use conventional commits (`feat:`, `fix:`, `refactor:`, `docs:`).

## Questions?

Open an issue or discussion for questions.