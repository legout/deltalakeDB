## 1. Implementation
- [x] 1.1 Add `docker-compose.yml` at root with Postgres and MinIO services
- [x] 1.2 Add helper script `scripts/test-env.sh` to start/stop the environment
- [x] 1.3 Add a `.env.example` file with default credentials and endpoints
- [x] 1.4 Update README.md with a “Local Testing” section referencing the Docker Compose setup
- [x] 1.5 Update CONTRIBUTING.md to mention the test environment setup

## 2. Validation
- [x] 2.1 `docker compose up -d` start services and they become healthy
- [x] 2.2 `scripts/test-env.sh start` and `scripts/test-env.sh stop` work
- [x] 2.3 Postgres is reachable at `localhost:5432` with the configured user/password
- [x] 2.4 MinIO is reachable at `localhost:9000` with the configured access key
- [x] 2.5 Repo passes `openspec validate add-test-infra --strict`

## 3. Dependencies
- Docker and Docker Compose must be installed on the developer’s machine.
- No code changes to Rust/Python are required in this change.
