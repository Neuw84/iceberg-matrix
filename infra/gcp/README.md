# GCP platform test infrastructure

One-time setup for running the BigQuery and Managed Service for Apache Spark
feature suites, locally or from GitHub Actions
(`.github/workflows/gcp-platform-tests.yml`).

## 1. Test resources

```bash
GCP_PROJECT=<project> GCP_REGION=us-central1 infra/gcp/setup.sh
```

Idempotent. Enables the APIs and creates, all in one region:

- a private bucket `<project>-iceberg-tests` (uniform access, public access
  prevention, lifecycle rules in `gcs-lifecycle.json`)
- the dataset `icebergmatrix_tests` and the Cloud resource connection
  `icebergmatrix-conn` for BigQuery managed Iceberg tables
- the Lakehouse runtime catalog `icebergmatrix_lakehouse` in credential-vending
  mode
- the service account `icebergmatrix-spark` that Managed Spark batches run as
  (`dataproc.serverlessNode`, `dataprocrm.nodeServiceAgent`, `biglake.editor`,
  `serviceUsageConsumer`; object access on the test bucket only)

The connection's and the catalog's Google-managed service accounts get object
access on that one bucket. Serverless runtime 3.0+ also needs the Cloud
Resource Manager API (it tags the subnet for its firewall policy); the default
subnet is used and Google enables Private Google Access on it.

## 2. Run locally

```bash
source tests/gcp/env.sh
MATRIX_STORAGE_MODE=managed   uv run --python 3.11 $GCP_UV_WITH python tests/bigquery_feature_tests.py
MATRIX_STORAGE_MODE=lakehouse uv run --python 3.11 $GCP_UV_WITH python tests/bigquery_feature_tests.py
uv run --python 3.11 $GCP_UV_WITH python -u tests/gcp/run_managed_spark.py
```

Application-default credentials (`gcloud auth application-default login`). Each
BigQuery mode takes about 30 minutes; the Managed Spark batch about 35.
`tests/gcp/teardown.py` removes what an interrupted run left behind; never run
it while a run is active.

## 3. GitHub Actions

```bash
GCP_PROJECT=<project> GITHUB_REPO=<owner>/<repo> infra/gcp/ci-setup.sh
```

Creates a Workload Identity pool and GitHub OIDC provider whose attribute
condition accepts only tokens from that repository in the `gcp-live`
environment, and the CI service account `icebergmatrix-ci` it may impersonate.
No service-account key exists. Then create the `gcp-live` environment with a
required reviewer and a `main` branch policy, and set the three secrets the
script prints (`GCP_WORKLOAD_IDENTITY_PROVIDER`, `GCP_SERVICE_ACCOUNT`,
`GCP_PROJECT`).

The workflow is dispatch-only. Start with `dry-run: true` to prove the
federation without spending anything.
