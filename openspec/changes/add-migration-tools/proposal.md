## Why
To adopt SQL-backed metadata for existing Delta tables, users need tooling to bootstrap the SQL catalog from existing `_delta_log/` files and validate the migration.

## What Changes
- Create CLI tool for importing existing Delta tables to SQL
- Implement checkpoint and JSON log replay logic
- Add validation and drift detection between SQL and file-based state
- Create rollback procedures for failed migrations
- Add progress reporting and resume capability

## Impact
- Affected specs: rust-workspace, python-package
- Affected code: New CLI tool and migration utilities
- Enables adoption of SQL-backed metadata for existing Delta tables
- Critical for production rollout and user confidence
