## Context
Extend local test infrastructure with additional S3‑compatible stores to broaden coverage beyond MinIO. Selected backends: SeaweedFS S3 gateway, RustFS, and Garage.

## Goals / Non-Goals
- Goals: Easy local startup; distinct ports; predictable default credentials; health checks.
- Non-Goals: Multi‑node clustering, production‑grade HA, or benchmarking.

## Decisions
- SeaweedFS: Use single‑process server with S3 enabled for simplicity (`chrislusf/seaweedfs` image). Provide `s3.json` for credentials; expose S3 on `8333`.
- RustFS: Add service based on the upstream repo `github.com/rustfs/rustfs`. If an official image exists, use it; otherwise add a small Dockerfile to build a single‑node instance suitable for local testing. Expose S3 on `9100` and the web console on `9101`. Use credentials `rustfsadmin` / `rustfsadmin`.
- Garage: Include `ghcr.io/garagehq/garage` image in single‑node mode. Expose S3 on `3900` and admin on `3901`. Use credentials `garageadmin` / `garagesecret`.
- Credentials: Use clearly non‑production defaults unique per backend to avoid confusion with MinIO.
- Health: Add HTTP healthchecks where available (MinIO admin, Garage admin). Use TCP checks for services without HTTP health endpoints.

## Risks / Trade-offs
- SeaweedFS and RustFS (and Garage, if included) may require small config files; we will version minimal configs under a `dev/` folder and mount them.
- Port collisions: ensure ports do not overlap with existing MinIO (`9000/9001`).

## Migration Plan
1) Add compose services with unique names (`seaweedfs-s3`, `rustfs`, `garage`).
2) Add named volumes for persistence.
3) Add minimal config files and mounts (e.g., `seaweedfs/s3.json`, `rustfs/config.*`, `garage/garage.toml`).
4) Update docs and helper scripts.
5) Verify health and basic S3 operations across all backends.

## Open Questions
- Should services be optional via env flags (e.g., `ENABLE_SEAWEEDFS`, `ENABLE_GARAGE`, `ENABLE_RUSTFS`)? Default: run all.
- RustFS containerization: use upstream image if available; otherwise, build from a local Dockerfile.
