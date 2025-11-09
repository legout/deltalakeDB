# test-infra Specification

## Purpose
Provide a comprehensive local testing environment with multiple S3-compatible object stores to validate Delta Lake mirroring and S3 compatibility across different backends.
## Requirements
### Requirement: Docker Compose Test Environment
The repository SHALL provide a Docker Compose configuration that starts Postgres, MinIO, SeaweedFS S3, a Rust‑based S3 backend (RustFS from `github.com/rustfs/rustfs`), and Garage for local integration testing.

#### Scenario: Services start and become healthy
- **WHEN** a developer runs `docker compose up -d`
- **THEN** Postgres, MinIO, SeaweedFS S3, RustFS, and Garage services start and become healthy within a reasonable time

#### Scenario: Default endpoints and credentials
- **WHEN** the environment is running
- **THEN** Postgres is reachable at `localhost:5432` with user `postgres` and password `postgres`
- **THEN** MinIO is reachable at `localhost:9000` with access key `minioadmin` and secret key `minioadmin`
- **THEN** SeaweedFS S3 is reachable at `localhost:8333` with access key `seaweedadmin` and secret key `seaweedsecret`
- **THEN** RustFS S3 is reachable at `localhost:9100` and its web console at `localhost:9101` with access key `rustfsadmin` and secret key `rustfsadmin`
- **THEN** Garage S3 is reachable at `localhost:3900` and its admin at `localhost:3901` with access key `garageadmin` and secret key `garagesecret`

### Requirement: Helper Script
The repository SHALL include a helper script to start/stop the test environment with clear output.

#### Scenario: Start and stop commands
- **WHEN** a developer runs `scripts/test-env.sh start`
- **THEN** the Docker Compose services are started and comprehensive status is printed for all services
- **WHEN** a developer runs `scripts/test-env.sh stop`
- **THEN** the Docker Compose services are stopped and removed

#### Scenario: Status output
- **WHEN** a developer runs `scripts/test-env.sh start`
- **THEN** the output includes endpoints and credentials for all S3-compatible backends

### Requirement: Documentation
The repository SHALL document how to use the test environment in README and CONTRIBUTING.

#### Scenario: README includes Local Testing section
- **WHEN** viewing README.md
- **THEN** a "Local Testing" section explains how to start the environment and lists endpoints/credentials for all S3 backends

#### Scenario: CONTRIBUTING mentions test setup
- **WHEN** viewing CONTRIBUTING.md
- **THEN** it mentions the expanded Docker Compose setup with multiple S3-compatible object stores for integration testing

### Requirement: Data Persistence
The repository SHALL ensure data persists across container restarts.

#### Scenario: Volume persistence
- **WHEN** services are stopped and restarted
- **THEN** all data volumes persist and services maintain their state

