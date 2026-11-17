# Source before running the GCP suites: `source tests/gcp/env.sh`.
# Resource names match infra/gcp/setup.sh defaults; override any of them first.
export GCP_PROJECT="${GCP_PROJECT:-icebergmatrix}"
export GCP_REGION="${GCP_REGION:-us-central1}"
export GCP_DATA_BUCKET="${GCP_DATA_BUCKET:-${GCP_PROJECT}-iceberg-tests}"
export GCP_DATASET="${GCP_DATASET:-icebergmatrix_tests}"
export GCP_CONNECTION="${GCP_CONNECTION:-${GCP_PROJECT}.${GCP_REGION}.icebergmatrix-conn}"
export GCP_LAKEHOUSE_CATALOG="${GCP_LAKEHOUSE_CATALOG:-icebergmatrix_lakehouse}"

# Python deps the BigQuery suite needs on top of tests/requirements.txt.
# Usage: uv run --python 3.11 $GCP_UV_WITH python tests/bigquery_feature_tests.py
export GCP_UV_WITH="--with pyspark==4.1.2 --with google-cloud-bigquery --with google-cloud-storage --with fastavro"
