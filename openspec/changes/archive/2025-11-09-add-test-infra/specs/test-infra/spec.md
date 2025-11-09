## ADDED Requirements
### Requirement: Docker Compose Test Environment
The repository SHALL provide a Docker Compose configuration that starts Postgres and MinIO for local integration testing.

#### Scenario: Services start and become healthy
- **WHEN** a developer runs `docker compose up -d`
- **THEN** Postgres and MinIO services start and become healthy within a reasonable time

#### Scenario: Default endpoints and credentials
- **WHEN** the environment is running
- **THEN** Postgres is reachable at `localhost:5432` with user `postgres` and password `postgres`
- **THEN** MinIO is reachable at `localhost:9000` with access key `minioadmin` and secret key `minioadmin`

### Requirement: Helper Script
The repository SHALL include a helper script to start/stop the test environment with clear output.

#### Scenario: Start and stop commands
- **WHEN** a developer runs `scripts/test-env.sh start`
- **THEN** the Docker Compose services are started and status is printed
- **WHEN** a developer runs `scripts/test-env.sh stop`
- **THEN** the Docker Compose services are stopped and removed

### Requirement: Documentation
The repository SHALL document how to use the test environment in README and CONTRIBUTING.

#### Scenario: README includes Local Testing section
- **WHEN** viewing README.md
- **THEN** a “Local Testing” section explains how to start the environment and lists endpoints/credentials

#### Scenario: CONTRIBUTING mentions test setup
- **WHEN** viewing CONTRIBUTING.md
- **THEN** it mentions the Docker Compose setup for integration testing
