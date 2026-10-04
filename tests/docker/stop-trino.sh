#!/usr/bin/env bash
# Tear down the Trino coordinator started by start-trino.sh.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
docker compose -f "${SCRIPT_DIR}/docker-compose.trino.yml" down --remove-orphans || true
