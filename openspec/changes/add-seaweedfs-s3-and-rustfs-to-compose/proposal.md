## Why
Developers need to validate Delta mirroring and S3‑compatible behavior against multiple object stores beyond MinIO. Adding SeaweedFS S3 and a Rust‑based S3 backend improves compatibility coverage and reduces vendor bias in tests.

## What Changes
- Add `seaweedfs-s3` service to `docker-compose.yml` with healthcheck, volume, and default credentials.
- Add `rustfs` service (https://github.com/rustfs/rustfs) to `docker-compose.yml` with healthcheck, volume, and default credentials.
  - RustFS S3 endpoint on `9100`, web console on `9101`, credentials `rustfsadmin` / `rustfsadmin`.
- Add a separate `garage` service (Garage S3) for broader S3 coverage.
  - Garage S3 endpoint on `3900`, admin on `3901`, credentials `garageadmin` / `garagesecret`.
- Update test‑infra spec to include the additional S3‑compatible backends and document endpoints/credentials.
- Update local testing docs to list all services and ports.

## Impact
- Affected specs: `openspec/specs/test-infra/spec.md`
- Affected code: `docker-compose.yml`, `README.md`, `CONTRIBUTING.md`, optionally `scripts/test-env.sh`
