#!/usr/bin/env bash
# One-time, idempotent setup of the identity GitHub Actions uses for the GCP
# live tests (.github/workflows/gcp-platform-tests.yml). No service-account key
# is created: the workflow exchanges its GitHub OIDC token through Workload
# Identity Federation, the GCP counterpart of the AWS OIDC role.
#
# Trust is pinned twice, like the AWS role: the provider only accepts tokens
# from GITHUB_REPO, and only for jobs running in the GITHUB_ENVIRONMENT
# environment (which has a required reviewer), so a token minted on a fork, a
# pull request or any other workflow cannot be exchanged.
#
# Grants the CI service account only what the suites use:
#   project: bigquery.jobUser, bigquery.connectionUser, biglake.editor,
#            dataproc.editor, serviceusage.serviceUsageConsumer
#   dataset: bigquery.dataEditor (GRANT ... ON SCHEMA)
#   (bigquery.connectionUser is project-wide because per-connection IAM through
#   bq needs allowlisting; the project holds only the suite's connection)
#   bucket:  storage.objectAdmin      spark SA:   iam.serviceAccountUser
#
# Run infra/gcp/setup.sh first. Usage:
#   GCP_PROJECT=icebergmatrix GITHUB_REPO=Neuw84/iceberg-matrix infra/gcp/ci-setup.sh
set -euo pipefail

PROJECT="${GCP_PROJECT:?set GCP_PROJECT}"
REGION="${GCP_REGION:-us-central1}"
REPO="${GITHUB_REPO:?set GITHUB_REPO (owner/name)}"
ENVIRONMENT="${GITHUB_ENVIRONMENT:-gcp-live}"
PREFIX="${RESOURCE_PREFIX:-icebergmatrix}"
BUCKET="${GCP_DATA_BUCKET:-${PROJECT}-iceberg-tests}"
DATASET="${GCP_DATASET:-${PREFIX}_tests}"
CONNECTION="${GCP_CONNECTION_ID:-${PREFIX}-conn}"
POOL="${PREFIX}-github"
PROVIDER="github-actions"
CI_SA_NAME="${PREFIX}-ci"
CI_SA="${CI_SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
SPARK_SA="${PREFIX}-spark@${PROJECT}.iam.gserviceaccount.com"
NUMBER="$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')"

gcloud services enable --project="$PROJECT" \
  iam.googleapis.com iamcredentials.googleapis.com sts.googleapis.com

# --- Workload Identity pool and GitHub provider -------------------------------
if ! gcloud iam workload-identity-pools describe "$POOL" --project="$PROJECT" \
    --location=global >/dev/null 2>&1; then
  gcloud iam workload-identity-pools create "$POOL" --project="$PROJECT" --location=global \
    --display-name="Iceberg matrix GitHub Actions"
fi
CONDITION="assertion.repository == '${REPO}' && assertion.environment == '${ENVIRONMENT}'"
MAPPING="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.environment=assertion.environment"
if ! gcloud iam workload-identity-pools providers describe "$PROVIDER" --project="$PROJECT" \
    --location=global --workload-identity-pool="$POOL" >/dev/null 2>&1; then
  gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" --project="$PROJECT" \
    --location=global --workload-identity-pool="$POOL" \
    --issuer-uri="https://token.actions.githubusercontent.com" \
    --attribute-mapping="$MAPPING" --attribute-condition="$CONDITION"
else
  gcloud iam workload-identity-pools providers update-oidc "$PROVIDER" --project="$PROJECT" \
    --location=global --workload-identity-pool="$POOL" \
    --attribute-mapping="$MAPPING" --attribute-condition="$CONDITION" >/dev/null
fi

# --- CI service account -------------------------------------------------------
if ! gcloud iam service-accounts describe "$CI_SA" --project="$PROJECT" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$CI_SA_NAME" --project="$PROJECT" \
    --display-name="Iceberg matrix GitHub Actions (gcp-live)"
  sleep 15  # a new account takes a few seconds to become visible to IAM
fi
for ROLE in roles/bigquery.jobUser roles/bigquery.connectionUser roles/biglake.editor \
            roles/dataproc.editor roles/serviceusage.serviceUsageConsumer; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$CI_SA" \
    --role="$ROLE" --condition=None >/dev/null
done
# Dataset-scoped grant through BigQuery DCL (bq add-iam-policy-binding on a
# dataset needs allowlisting).
bq --project_id="$PROJECT" --location="$REGION" query --use_legacy_sql=false --quiet \
  "GRANT \`roles/bigquery.dataEditor\` ON SCHEMA \`$PROJECT.$DATASET\` TO \"serviceAccount:$CI_SA\"" >/dev/null
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$CI_SA" --role=roles/storage.objectAdmin >/dev/null
# Submitting a batch that runs as the Spark account requires acting as it.
gcloud iam service-accounts add-iam-policy-binding "$SPARK_SA" --project="$PROJECT" \
  --member="serviceAccount:$CI_SA" --role=roles/iam.serviceAccountUser >/dev/null

# The federated identity may impersonate the CI account; the provider condition
# above already restricts which tokens reach this principal set.
gcloud iam service-accounts add-iam-policy-binding "$CI_SA" --project="$PROJECT" \
  --role=roles/iam.workloadIdentityUser \
  --member="principalSet://iam.googleapis.com/projects/${NUMBER}/locations/global/workloadIdentityPools/${POOL}/attribute.repository/${REPO}" \
  >/dev/null

cat <<EOF

Done. Create the GitHub environment "${ENVIRONMENT}" (with a required reviewer)
and set these environment secrets on it:
  GCP_WORKLOAD_IDENTITY_PROVIDER = projects/${NUMBER}/locations/global/workloadIdentityPools/${POOL}/providers/${PROVIDER}
  GCP_SERVICE_ACCOUNT            = ${CI_SA}
  GCP_PROJECT                    = ${PROJECT}
EOF
