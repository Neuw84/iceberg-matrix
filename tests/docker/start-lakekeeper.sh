#!/usr/bin/env bash
#
# Bring up the shared Lakekeeper + MinIO stack used by the engine feature-test
# suites and wait until the Iceberg REST catalog is usable from the host.
#
# Usage:
#   tests/docker/start-lakekeeper.sh
#
# Environment overrides (all optional, defaults match tests/catalog_config.py):
#   LAKEKEEPER_WAREHOUSE   warehouse name to create        (default: demo)
#   LAKEKEEPER_BUCKET      MinIO bucket for the warehouse  (default: warehouse)
#   LAKEKEEPER_S3_KEY_ID   MinIO access key                (default: minio)
#   LAKEKEEPER_S3_SECRET   MinIO secret key                (default: minio12345)
#   LAKEKEEPER_S3_REGION   region reported to clients      (default: us-east-1)
#   LAKEKEEPER_URI         host-visible Lakekeeper base URI (default: http://127.0.0.1:8181)
#   LAKEKEEPER_S3_HOST     host address the warehouse advertises for MinIO
#                          (default: this host's detected IP)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.lakekeeper.yml"

export LAKEKEEPER_WAREHOUSE="${LAKEKEEPER_WAREHOUSE:-demo}"
export LAKEKEEPER_BUCKET="${LAKEKEEPER_BUCKET:-warehouse}"
export LAKEKEEPER_S3_KEY_ID="${LAKEKEEPER_S3_KEY_ID:-minio}"
export LAKEKEEPER_S3_SECRET="${LAKEKEEPER_S3_SECRET:-minio12345}"
export LAKEKEEPER_S3_REGION="${LAKEKEEPER_S3_REGION:-us-east-1}"
LAKEKEEPER_URI="${LAKEKEEPER_URI:-http://127.0.0.1:8181}"

# ---------------------------------------------------------------------------
# The warehouse must advertise an S3 endpoint that both the Lakekeeper container
# and host-side engines can reach, because Iceberg's Java REST client applies
# the storage config the server returns on loadTable over the client's own.
# A compose-internal name (http://minio:9000) fails on the host with
# UnknownHostException, so use the host's own IP: containers reach it through
# MinIO's published port, and the host reaches itself. No /etc/hosts entry and
# no root access required.
# ---------------------------------------------------------------------------
detect_host_ip() {
  local ip=""
  if command -v ipconfig >/dev/null 2>&1; then
    for iface in en0 en1 en2 en3; do
      ip="$(ipconfig getifaddr "${iface}" 2>/dev/null || true)"
      [[ -n "${ip}" ]] && break
    done
  fi
  if [[ -z "${ip}" ]]; then
    ip="$(hostname -I 2>/dev/null | awk '{print $1}' || true)"
  fi
  if [[ -z "${ip}" ]]; then
    # No traffic is sent; this just asks the kernel which local address would
    # be used to reach an external host.
    ip="$(python3 -c 'import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
try:
    s.connect(("8.8.8.8", 80))
    print(s.getsockname()[0])
finally:
    s.close()' 2>/dev/null || true)"
  fi
  printf '%s' "${ip}"
}

wait_for_job() {
  local service="$1" cid status exit_code
  for _ in $(seq 1 90); do
    cid="$(docker compose -f "${COMPOSE_FILE}" ps -aq "${service}" 2>/dev/null || true)"
    if [[ -n "${cid}" ]]; then
      status="$(docker inspect -f '{{.State.Status}}' "${cid}")"
      if [[ "${status}" == "exited" ]]; then
        exit_code="$(docker inspect -f '{{.State.ExitCode}}' "${cid}")"
        if [[ "${exit_code}" == "0" ]]; then
          echo "[lakekeeper] ${service} completed"
          return 0
        fi
        echo "[lakekeeper] ${service} failed (exit ${exit_code}):" >&2
        docker logs "${cid}" >&2 || true
        return 1
      fi
    fi
    sleep 2
  done
  echo "[lakekeeper] timed out waiting for ${service}" >&2
  docker compose -f "${COMPOSE_FILE}" logs "${service}" >&2 || true
  return 1
}

wait_for_catalog() {
  local url="${LAKEKEEPER_URI}/catalog/v1/config?warehouse=${LAKEKEEPER_WAREHOUSE}"
  for _ in $(seq 1 45); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "[lakekeeper] REST catalog ready at ${LAKEKEEPER_URI}/catalog"
      curl -fsS "${url}" || true
      echo
      return 0
    fi
    sleep 2
  done
  echo "[lakekeeper] timed out waiting for ${url}" >&2
  docker compose -f "${COMPOSE_FILE}" logs lakekeeper >&2 || true
  return 1
}

export LAKEKEEPER_S3_HOST="${LAKEKEEPER_S3_HOST:-$(detect_host_ip)}"
if [[ -z "${LAKEKEEPER_S3_HOST}" ]]; then
  echo "[lakekeeper] could not determine this host's IP address; set LAKEKEEPER_S3_HOST" >&2
  exit 1
fi
echo "[lakekeeper] warehouse will advertise S3 endpoint http://${LAKEKEEPER_S3_HOST}:9000"

echo "[lakekeeper] starting stack (warehouse '${LAKEKEEPER_WAREHOUSE}' on bucket '${LAKEKEEPER_BUCKET}')"
docker compose -f "${COMPOSE_FILE}" up -d

wait_for_job bootstrap
wait_for_job warehouse-init
wait_for_catalog
