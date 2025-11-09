## Why
Provide developers with a quick, reproducible way to spin up Postgres and MinIO for integration testing without manual setup, aligning with the project’s testing strategy.

## What Changes
- Add a Docker Compose configuration that starts Postgres and MinIO services with minimal defaults.
- Include a helper script to start/stop the test environment.
- Document connection endpoints and credentials for use in tests and local development.
- Keep the setup minimal and optional; CI can use testcontainers, but local developers get a single-command environment.

## Impact
- Affected specs: test-infra
- Affected code: root `docker-compose.yml`, helper script in `scripts/`, documentation updates in README/CONTRIBUTING
