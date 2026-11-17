#!/usr/bin/env bash
# One-time, idempotent setup of the Google Cloud resources the GCP feature
# suites need. Safe to re-run: every step checks before it creates.
#
# Creates (all in one region, which BigQuery requires):
#   - a private Cloud Storage bucket for table data, with a lifecycle rule
#   - a BigQuery dataset for Apache Iceberg managed tables
#   - a BigQuery Cloud resource connection; its service account gets object
#     access on that one bucket only
#   - a Lakehouse runtime catalog (Iceberg REST) in credential-vending mode;
#     its service account gets object access on that one bucket only
#
# Usage: GCP_PROJECT=icebergmatrix GCP_REGION=us-central1 infra/gcp/setup.sh
set -euo pipefail

PROJECT="${GCP_PROJECT:?set GCP_PROJECT}"
REGION="${GCP_REGION:-us-central1}"
PREFIX="${RESOURCE_PREFIX:-icebergmatrix}"
BUCKET="${GCP_DATA_BUCKET:-${PROJECT}-iceberg-tests}"
DATASET="${GCP_DATASET:-${PREFIX}_tests}"
CONNECTION="${GCP_CONNECTION:-${PREFIX}-conn}"
CATALOG="${GCP_LAKEHOUSE_CATALOG:-${PREFIX}_lakehouse}"
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "project=$PROJECT region=$REGION bucket=$BUCKET dataset=$DATASET connection=$CONNECTION catalog=$CATALOG"

gcloud services enable --project="$PROJECT" \
  bigquery.googleapis.com bigqueryconnection.googleapis.com \
  biglake.googleapis.com dataproc.googleapis.com storage.googleapis.com \
  cloudresourcemanager.googleapis.com

# --- Bucket: private, uniform access, short-lived test data ---------------
if ! gcloud storage buckets describe "gs://$BUCKET" --project="$PROJECT" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://$BUCKET" --project="$PROJECT" \
    --location="$REGION" --uniform-bucket-level-access --public-access-prevention
fi
gcloud storage buckets update "gs://$BUCKET" --lifecycle-file="$HERE/gcs-lifecycle.json"

# --- BigQuery dataset ------------------------------------------------------
if ! bq --project_id="$PROJECT" show --dataset "$PROJECT:$DATASET" >/dev/null 2>&1; then
  bq --project_id="$PROJECT" --location="$REGION" mk --dataset \
    --description="Iceberg compatibility matrix feature tests" "$PROJECT:$DATASET"
fi

# --- Cloud resource connection for managed Iceberg tables -----------------
if ! bq --project_id="$PROJECT" --location="$REGION" show --connection \
    "$PROJECT.$REGION.$CONNECTION" >/dev/null 2>&1; then
  bq --project_id="$PROJECT" --location="$REGION" mk --connection \
    --connection_type=CLOUD_RESOURCE "$CONNECTION"
fi
CONN_SA="$(bq --project_id="$PROJECT" --location="$REGION" show --format=json \
  --connection "$PROJECT.$REGION.$CONNECTION" | python3 -c \
  'import json,sys; print(json.load(sys.stdin)["cloudResource"]["serviceAccountId"])')"
for ROLE in roles/storage.objectUser roles/storage.legacyBucketReader; do
  gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
    --member="serviceAccount:$CONN_SA" --role="$ROLE" >/dev/null
done

# --- Lakehouse runtime catalog (Iceberg REST, vended credentials) ----------
if ! gcloud biglake iceberg catalogs describe "$CATALOG" --project="$PROJECT" >/dev/null 2>&1; then
  gcloud biglake iceberg catalogs create "$CATALOG" --project="$PROJECT" \
    --catalog-type=biglake --default-location="gs://$BUCKET/lakehouse" \
    --credential-mode=vended-credentials
fi
CATALOG_SA="$(gcloud biglake iceberg catalogs describe "$CATALOG" --project="$PROJECT" \
  --format='value(biglake-service-account)')"
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$CATALOG_SA" --role=roles/storage.objectUser >/dev/null

# --- Workload identity for Managed Spark batches ------------------------------
# Serverless runtime 3.0+ otherwise runs a batch with the submitter's own
# credentials, which needs an interactive consent and does not work from CI.
# Roles are the ones Google lists for runtime 3.0+ with service account
# credentials, plus object access on the test bucket only.
SPARK_SA_NAME="${PREFIX}-spark"
SPARK_SA="${SPARK_SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
if ! gcloud iam service-accounts describe "$SPARK_SA" --project="$PROJECT" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SPARK_SA_NAME" --project="$PROJECT" \
    --display-name="Iceberg matrix Managed Spark batches"
  # A new account takes a few seconds to become visible to IAM policy calls.
  sleep 15
fi
for ROLE in roles/dataproc.serverlessNode roles/dataprocrm.nodeServiceAgent \
            roles/biglake.editor roles/serviceusage.serviceUsageConsumer; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$SPARK_SA" \
    --role="$ROLE" --condition=None >/dev/null
done
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$SPARK_SA" --role=roles/storage.objectUser >/dev/null

cat <<EOF

Done. Export these for the test suites:
  export GCP_PROJECT=$PROJECT
  export GCP_REGION=$REGION
  export GCP_DATA_BUCKET=$BUCKET
  export GCP_DATASET=$DATASET
  export GCP_CONNECTION=$PROJECT.$REGION.$CONNECTION
  export GCP_LAKEHOUSE_CATALOG=$CATALOG
  export MANAGED_SPARK_SA=$SPARK_SA
EOF
