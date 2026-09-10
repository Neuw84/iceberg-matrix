#!/usr/bin/env bash
#
# Bring up the shared Apache Polaris + MinIO stack used by the engine
# feature-test suites and wait until the Iceberg REST catalog is usable from
# the host.
#
# Usage:
#   tests/docker/start-polaris.sh
#
# Environment overrides (all optional; defaults match the suites' defaults):
#   POLARIS_VERSION        apache/polaris image tag               (default: 1.7.0)
#   POLARIS_CATALOG        catalog (warehouse) name to create      (default: demo)
#   POLARIS_BUCKET         MinIO bucket backing the catalog        (default: warehouse)
#   POLARIS_S3_KEY_ID      MinIO access key                        (default: minio)
#   POLARIS_S3_SECRET      MinIO secret key                        (default: minio12345)
#   POLARIS_S3_REGION      region reported to clients              (default: us-east-1)
#   POLARIS_REALM          Polaris realm                           (default: POLARIS)
#   POLARIS_CLIENT_ID      root principal client id                (default: root)
#   POLARIS_CLIENT_SECRET  root principal client secret            (default: s3cr3t)
#   POLARIS_URI            host-visible Polaris base URI           (default: http://127.0.0.1:8181)
#   POLARIS_S3_HOST        host address the catalog advertises for MinIO
#                          (default: this host's detected IP)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.polaris.yml"
# shellcheck source=tests/docker/host-ip.sh
source "${SCRIPT_DIR}/host-ip.sh"

export POLARIS_VERSION="${POLARIS_VERSION:-1.7.0}"
export POLARIS_CATALOG="${POLARIS_CATALOG:-demo}"
export POLARIS_BUCKET="${POLARIS_BUCKET:-warehouse}"
export POLARIS_S3_KEY_ID="${POLARIS_S3_KEY_ID:-minio}"
export POLARIS_S3_SECRET="${POLARIS_S3_SECRET:-minio12345}"
export POLARIS_S3_REGION="${POLARIS_S3_REGION:-us-east-1}"
export POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
export POLARIS_CLIENT_ID="${POLARIS_CLIENT_ID:-root}"
export POLARIS_CLIENT_SECRET="${POLARIS_CLIENT_SECRET:-s3cr3t}"
POLARIS_URI="${POLARIS_URI:-http://127.0.0.1:8181}"

# detect_host_ip comes from host-ip.sh, which explains why the host's own IP is
# the only address that works for both containers and host-side engines. Polaris
# advertises it as the S3 endpoint in every loadTable response.

wait_for_job() {
  local service="$1" cid status exit_code
  for _ in $(seq 1 120); do
    cid="$(docker compose -f "${COMPOSE_FILE}" ps -aq "${service}" 2>/dev/null || true)"
    if [[ -n "${cid}" ]]; then
      status="$(docker inspect -f '{{.State.Status}}' "${cid}")"
      if [[ "${status}" == "exited" ]]; then
        exit_code="$(docker inspect -f '{{.State.ExitCode}}' "${cid}")"
        if [[ "${exit_code}" == "0" ]]; then
          echo "[polaris] ${service} completed"
          return 0
        fi
        echo "[polaris] ${service} failed (exit ${exit_code}):" >&2
        docker logs "${cid}" >&2 || true
        return 1
      fi
    fi
    sleep 2
  done
  echo "[polaris] timed out waiting for ${service}" >&2
  docker compose -f "${COMPOSE_FILE}" logs "${service}" >&2 || true
  return 1
}

wait_for_catalog() {
  local token url
  url="${POLARIS_URI}/api/catalog/v1/config?warehouse=${POLARIS_CATALOG}"
  for _ in $(seq 1 45); do
    token="$(curl -fsS "${POLARIS_URI}/api/catalog/v1/oauth/tokens" \
      --user "${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}" \
      -H "Polaris-Realm: ${POLARIS_REALM}" \
      -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL 2>/dev/null \
      | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p' || true)"
    if [[ -n "${token}" ]] && curl -fsS -H "Authorization: Bearer ${token}" "${url}" >/dev/null 2>&1; then
      echo "[polaris] REST catalog ready at ${POLARIS_URI}/api/catalog (catalog '${POLARIS_CATALOG}')"
      curl -fsS -H "Authorization: Bearer ${token}" "${url}" || true
      echo
      return 0
    fi
    sleep 2
  done
  echo "[polaris] timed out waiting for ${url}" >&2
  docker compose -f "${COMPOSE_FILE}" logs polaris >&2 || true
  return 1
}

export POLARIS_S3_HOST="${POLARIS_S3_HOST:-$(detect_host_ip)}"
if [[ -z "${POLARIS_S3_HOST}" ]]; then
  echo "[polaris] could not determine this host's IP address; set POLARIS_S3_HOST" >&2
  exit 1
fi
echo "[polaris] catalog will advertise S3 endpoint http://${POLARIS_S3_HOST}:9000"

echo "[polaris] starting stack (catalog '${POLARIS_CATALOG}' on bucket '${POLARIS_BUCKET}', image apache/polaris:${POLARIS_VERSION})"
docker compose -f "${COMPOSE_FILE}" up -d

wait_for_job polaris-setup
wait_for_catalog
