## 1. Implementation
- [x] 1.1 Add `seaweedfs-s3` service to `docker-compose.yml` (image, command, ports, healthcheck, volume)
- [x] 1.2 Add `rustfs` service (from `github.com/rustfs/rustfs`) to `docker-compose.yml` (image or local build, config, ports, healthcheck, volume)
  - Use ports: S3 `9100`, web console `9101`; credentials `rustfsadmin` / `rustfsadmin`.
- [x] 1.3 Add `garage` service to `docker-compose.yml` (image, config, ports, healthcheck, volume)
  - Use ports: S3 `3900`, admin `3901`; credentials `garageadmin` / `garagesecret`.
- [x] 1.4 Ensure unique ports and compose service names; add named volumes
- [x] 1.5 Add minimal config files if needed (e.g., `seaweedfs/s3.json`, `rustfs/config.*`, `garage/garage.toml`) and mount them

## 2. Tooling/Docs
- [x] 2.1 Update `README.md` Local Testing with endpoints and credentials for all services
- [x] 2.2 Update `CONTRIBUTING.md` to reference the expanded compose environment
- [x] 2.3 If present, update `scripts/test-env.sh` to print status for new services

## 3. Verification
- [x] 3.1 `docker compose up -d` brings all services healthy
- [x] 3.2 Verify S3 access against MinIO, SeaweedFS, RustFS, and Garage with sample bucket ops
- [x] 3.3 Confirm volumes persist data across restarts
