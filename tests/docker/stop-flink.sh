#!/usr/bin/env bash
#
# Tear down the Dockerized Flink cluster started by start-flink.sh.
#
# Usage:
#   tests/docker/stop-flink.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.flink.yml"

echo "[flink] stopping cluster"
docker compose -f "${COMPOSE_FILE}" down --remove-orphans --volumes || true
rm -rf "${SCRIPT_DIR}/flink-work"
