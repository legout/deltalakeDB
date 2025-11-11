## Why
The SQL-Backed Delta Metadata system needs Python bindings to provide seamless integration with the existing Python data ecosystem. This includes compatibility with the `deltalake` Python package, support for `deltasql://` URIs, and CLI utilities for table operations. The Python layer will expose Rust functionality through pyo3 bindings while maintaining a Pythonic API.

## What Changes
- Add pyo3 bindings for Rust core functionality
- Implement `deltalake` compatibility layer for existing Python workflows
- Add CLI utilities for table operations and management
- Include support for `deltasql://` URI scheme and connection configuration

## Impact
- Affected specs: python-package
- Affected code: src/deltalakedb/ (will be expanded significantly)
- Dependencies: pyo3, deltalake, click, url-py, pydantic