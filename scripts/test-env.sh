#!/usr/bin/env bash
# Helper script to start/stop the test environment (Postgres + MinIO).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yml"

usage() {
  echo "Usage: $0 {start|stop|status}"
  echo "  start  - Start Postgres and S3-compatible object stores (MinIO, SeaweedFS, RustFS, Garage)"
  echo "  stop   - Stop and remove services"
  echo "  status - Show service status"
  exit 1
}

start() {
  echo "Starting test environment..."
  cd "$PROJECT_ROOT"
  docker compose -f "$COMPOSE_FILE" up -d
  echo "Waiting for services to be healthy..."
  # Note: docker compose wait does not support --timeout on all versions; we poll briefly
  for i in {1..30}; do
    if docker compose -f "$COMPOSE_FILE" ps | grep -q "healthy"; then
      break
    fi
    sleep 2
  done
  echo "Test environment started."
  echo ""
  echo "Database:"
  echo "  Postgres: localhost:5432 (user: postgres, password: postgres, db: deltalakedb)"
  echo ""
  echo "S3-Compatible Object Stores:"
  echo "  MinIO: http://localhost:9000 (access key: minioadmin, secret key: minioadmin)"
  echo "  MinIO Console: http://localhost:9001"
  echo "  SeaweedFS S3: http://localhost:8333 (access key: seaweedfsadmin, secret key: seaweedfsadmin)"
  echo "  SeaweedFS Master: http://localhost:9333"
  echo "  RustFS: http://localhost:9100 (access key: rustfsadmin, secret key: rustfsadmin)"
  echo "  RustFS Console: http://localhost:9101"
  echo "  Garage: http://localhost:3900 (configure keys via Garage CLI)"
  echo "  Garage Admin: http://localhost:3903 (admin token: garageadmin)"
}

stop() {
  echo "Stopping test environment..."
  cd "$PROJECT_ROOT"
  docker compose -f "$COMPOSE_FILE" down
  echo "Test environment stopped."
}

status() {
  echo "Test environment status:"
  cd "$PROJECT_ROOT"
  docker compose -f "$COMPOSE_FILE" ps
}

case "${1:-}" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  status)
    status
    ;;
  *)
    usage
    ;;
esac