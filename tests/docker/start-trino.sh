#!/usr/bin/env bash
#
# Start the single-node Trino coordinator used by tests/trino_feature_tests.py
# and wait until it reports healthy and the "iceberg" catalog resolves.
#
# Requires the Polaris + RustFS stack to be up first:
#   tests/docker/start-polaris.sh
#   tests/docker/start-trino.sh
#   python tests/trino_feature_tests.py
#
# Environment overrides (all optional):
#   TRINO_IMAGE    base Trino image     (default: trinodb/trino:483)
#   TRINO_HOST_IP  host address the container uses to reach the catalog/RustFS

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.trino.yml"
# shellcheck source=tests/docker/host-ip.sh
source "${SCRIPT_DIR}/host-ip.sh"

export TRINO_IMAGE="${TRINO_IMAGE:-trinodb/trino:483}"

# The container reaches Polaris and RustFS at the host's own IP -- the same
# address Polaris advertises as the S3 endpoint (see start-polaris.sh).
TRINO_HOST_IP="${TRINO_HOST_IP:-${POLARIS_S3_HOST:-$(detect_host_ip)}}"
if [[ -z "${TRINO_HOST_IP}" ]]; then
  echo "[trino] could not determine this host's IP address; set TRINO_HOST_IP" >&2
  exit 1
fi
export TRINO_HOST_IP

echo "[trino] starting Trino (${TRINO_IMAGE}); catalog -> http://${TRINO_HOST_IP}:8181/api/catalog"
docker compose -f "${COMPOSE_FILE}" up -d

wait_for_catalog() {
  for _ in $(seq 1 80); do
    if curl -sf http://127.0.0.1:8080/v1/info >/dev/null 2>&1; then
      # /v1/info is up; confirm the iceberg catalog actually loaded by listing
      # its schemas through the HTTP statement API via the trino client would
      # need the client installed, so probe the server's catalog list instead.
      if docker compose -f "${COMPOSE_FILE}" exec -T trino \
          trino --execute "SHOW SCHEMAS FROM iceberg" >/dev/null 2>&1; then
        echo "[trino] ready: iceberg catalog resolves"
        return 0
      fi
    fi
    sleep 2
  done
  echo "[trino] timed out waiting for the iceberg catalog" >&2
  docker compose -f "${COMPOSE_FILE}" logs --tail 120 >&2 || true
  return 1
}

wait_for_catalog

cat <<EOF
[trino] up. Run the feature tests with:

  export TRINO_HOST=127.0.0.1 TRINO_PORT=8080
  uv run --with trino python tests/trino_feature_tests.py

(the suite also needs the Spark fixture; see tests/trino_feature_tests.py)
EOF
