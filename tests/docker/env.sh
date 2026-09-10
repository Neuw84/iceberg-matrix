# Source this to point a local suite run at the Polaris + MinIO stack started by
# tests/docker/start-polaris.sh:   source tests/docker/env.sh
#
# Every suite already defaults to these values; the file exists so a shell
# with a stale ICEBERG_* environment from another catalog can be reset quickly,
# and to document the one set of settings the whole tests/ directory shares.
export ICEBERG_REST_URI="${ICEBERG_REST_URI:-http://127.0.0.1:8181/api/catalog}"
export ICEBERG_REST_WAREHOUSE="${ICEBERG_REST_WAREHOUSE:-demo}"
export ICEBERG_REST_CREDENTIAL="${ICEBERG_REST_CREDENTIAL:-root:s3cr3t}"
export ICEBERG_REST_SCOPE="${ICEBERG_REST_SCOPE:-PRINCIPAL_ROLE:ALL}"
export ICEBERG_S3_KEY_ID="${ICEBERG_S3_KEY_ID:-minio}"
export ICEBERG_S3_SECRET="${ICEBERG_S3_SECRET:-minio12345}"
export ICEBERG_S3_REGION="${ICEBERG_S3_REGION:-us-east-1}"
# ICEBERG_S3_ENDPOINT is deliberately not set here: the Spark-based suites
# expect a scheme ("http://127.0.0.1:9000") while DuckDB's secret takes a bare
# host:port; each suite's default is right for itself.
