#!/usr/bin/env bash
#
# Tear down the shared Apache Polaris + MinIO stack, including its volumes.
# Dropping the volumes is deliberate: the catalog's storage config (in
# particular the advertised S3 endpoint) is fixed at creation, so a fresh start
# is the only way to pick up a changed host IP.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.polaris.yml"

docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans || true
