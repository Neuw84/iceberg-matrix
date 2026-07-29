#!/usr/bin/env bash
#
# Build and start the Dockerized Flink cluster used by
# tests/flink_feature_tests.py, then wait until a TaskManager has registered so
# the SQL client can actually run jobs.
#
# Requires the Lakekeeper + MinIO stack to be up first:
#   tests/docker/start-lakekeeper.sh
#   tests/docker/start-flink.sh
#   python tests/flink_feature_tests.py
#
# Environment overrides (all optional):
#   FLINK_IMAGE            base Flink image        (default: flink:2.3.0-scala_2.12-java17)
#   FLINK_VERSION          Flink engine version    (default: 2.3.0)
#   ICEBERG_VERSION        Iceberg version         (default: 1.11.0)
#   ICEBERG_FLINK_MAJOR    Iceberg runtime's Flink minor (default: 2.1)
#   FLINK_HOST_IP          host address containers use to reach the catalog/MinIO

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.flink.yml"
# shellcheck source=tests/docker/host-ip.sh
source "${SCRIPT_DIR}/host-ip.sh"

export FLINK_IMAGE="${FLINK_IMAGE:-flink:2.3.0-scala_2.12-java17}"
export FLINK_VERSION="${FLINK_VERSION:-2.3.0}"
export ICEBERG_VERSION="${ICEBERG_VERSION:-1.11.0}"
export ICEBERG_FLINK_MAJOR="${ICEBERG_FLINK_MAJOR:-2.1}"

FLINK_HOST_IP="${FLINK_HOST_IP:-${LAKEKEEPER_S3_HOST:-$(detect_host_ip)}}"
if [[ -z "${FLINK_HOST_IP}" ]]; then
  echo "[flink] could not determine this host's IP address; set FLINK_HOST_IP" >&2
  exit 1
fi

# The harness hands SQL scripts to the cluster through this bind mount.
mkdir -p "${SCRIPT_DIR}/flink-work"

echo "[flink] building image (Flink ${FLINK_VERSION}, Iceberg ${ICEBERG_VERSION}, runtime ${ICEBERG_FLINK_MAJOR})"
docker compose -f "${COMPOSE_FILE}" build

echo "[flink] starting cluster"
docker compose -f "${COMPOSE_FILE}" up -d

wait_for_slots() {
  local overview slots
  for _ in $(seq 1 60); do
    overview="$(curl -sf http://127.0.0.1:8081/overview 2>/dev/null || true)"
    if [[ -n "${overview}" ]]; then
      slots="$(printf '%s' "${overview}" \
        | python3 -c 'import json,sys; print(json.load(sys.stdin).get("slots-total", 0))' 2>/dev/null || echo 0)"
      if [[ "${slots}" -gt 0 ]]; then
        echo "[flink] cluster ready (${slots} task slots)"
        return 0
      fi
    fi
    sleep 2
  done
  echo "[flink] timed out waiting for a TaskManager to register" >&2
  docker compose -f "${COMPOSE_FILE}" logs --tail 100 >&2 || true
  return 1
}

wait_for_slots

cat <<EOF
[flink] cluster is up. Run the feature tests with:

  export ICEBERG_REST_URI=http://${FLINK_HOST_IP}:8181/catalog
  export ICEBERG_S3_ENDPOINT=http://${FLINK_HOST_IP}:9000
  python tests/flink_feature_tests.py

(tests/flink_feature_tests.py detects the Docker cluster automatically and
falls back to these same defaults.)
EOF
