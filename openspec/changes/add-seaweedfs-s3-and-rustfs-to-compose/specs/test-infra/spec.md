## MODIFIED Requirements
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
