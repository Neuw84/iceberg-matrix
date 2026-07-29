#!/usr/bin/env bash
#
# Tear down the shared Lakekeeper + MinIO stack, including its volumes so the
# next run starts from an empty catalog and bucket.
#
# Usage:
#   tests/docker/stop-lakekeeper.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.lakekeeper.yml"

docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans || true
