#!/usr/bin/env python3
"""Run the Spark feature suite on Managed Service for Apache Spark (serverless).

The GCP counterpart of tests/aws/run_emr_serverless.py: bundle the suite and
the matrix data, upload them, submit one serverless PySpark batch that runs
tests/gcp/managed_spark_entrypoint.py against the Lakehouse runtime catalog,
wait, then download the report and print a summary. No cluster is created and
nothing stays running after the batch ends.

Environment (see tests/gcp/env.sh):
    GCP_PROJECT, GCP_REGION, GCP_DATA_BUCKET, GCP_LAKEHOUSE_CATALOG  (required)
    MANAGED_SPARK_RUNTIME   serverless runtime version (default 4.0)
    MANAGED_SPARK_SUBNET    subnetwork (default "default"; 4.0 requires one)
    MANAGED_SPARK_SA        service account for the batch (optional; runtime
                            3.0+ otherwise runs with the submitter's credentials)
    RUN_TAG                 unique per run (default random)
    JOB_TIMEOUT_MINUTES     default 60

Exit code: 1 if the batch failed, produced no report, or the report has errors
or discrepancies.
"""
import json
import os
import sys
import time
import uuid
import zipfile
from pathlib import Path

from google.api_core.client_options import ClientOptions
from google.cloud import dataproc_v1, storage

PROJECT = os.environ["GCP_PROJECT"]
REGION = os.environ.get("GCP_REGION", "us-central1")
BUCKET = os.environ["GCP_DATA_BUCKET"]
CATALOG = os.environ["GCP_LAKEHOUSE_CATALOG"]
RUNTIME = os.environ.get("MANAGED_SPARK_RUNTIME", "4.0")
SUBNET = os.environ.get("MANAGED_SPARK_SUBNET", "default")
SERVICE_ACCOUNT = os.environ.get("MANAGED_SPARK_SA", "")
RUN_TAG = os.environ.get("RUN_TAG", uuid.uuid4().hex[:8]).lower()
TIMEOUT_MIN = int(os.environ.get("JOB_TIMEOUT_MINUTES", "60"))
# Lakehouse namespaces allow letters, digits and underscores.
NS_PREFIX = "ms_" + "".join(c if c.isalnum() else "_" for c in RUN_TAG)[-12:] + "_"

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
LOCAL_REPORT_DIR = REPO_ROOT / "test-reports"
PREFIX = f"spark/{RUN_TAG}"
gcs = storage.Client(project=PROJECT)


def build_bundle(dest: Path) -> Path:
    """Zip the suite, its fixture helper and the matrix data it compares with."""
    includes = [
        REPO_ROOT / "tests" / "iceberg_feature_tests.py",
        REPO_ROOT / "tests" / "spark_fixture.py",
        REPO_ROOT / "src" / "data" / "features.json",
        REPO_ROOT / "src" / "data" / "platforms" / "gcp" / "dataproc" / "dataproc.json",
    ]
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in includes:
            zf.write(path, path.relative_to(REPO_ROOT).as_posix())
    print(f"[driver] bundle: {dest} ({dest.stat().st_size // 1024} KiB)")
    return dest


def upload(local: Path, name: str) -> str:
    blob = gcs.bucket(BUCKET).blob(f"{PREFIX}/{name}")
    blob.upload_from_filename(str(local))
    uri = f"gs://{BUCKET}/{blob.name}"
    print(f"[driver] uploaded {uri}")
    return uri


def submit(entry_uri: str, bundle_uri: str) -> dataproc_v1.Batch:
    client = dataproc_v1.BatchControllerClient(
        client_options=ClientOptions(api_endpoint=f"{REGION}-dataproc.googleapis.com:443"))
    batch_id = f"icebergmatrix-{RUN_TAG}"[:63].rstrip("-")
    execution = dataproc_v1.ExecutionConfig(subnetwork_uri=SUBNET, ttl={"seconds": TIMEOUT_MIN * 60})
    if SERVICE_ACCOUNT:
        execution.service_account = SERVICE_ACCOUNT
    batch = dataproc_v1.Batch(
        pyspark_batch=dataproc_v1.PySparkBatch(
            main_python_file_uri=entry_uri,
            args=["--bundle", bundle_uri, "--report-uri", f"gs://{BUCKET}/{PREFIX}/reports/",
                  "--project", PROJECT, "--catalog", CATALOG, "--ns-prefix", NS_PREFIX,
                  "--platform-label", f"Managed Spark runtime {RUNTIME}"],
        ),
        runtime_config=dataproc_v1.RuntimeConfig(
            version=RUNTIME,
            properties={
                # Setting the default catalog keeps the runtime from creating
                # its regional starter catalog in the project.
                "spark.sql.defaultCatalog": "local",
                # The suite runs one small table at a time; the minimum shape
                # keeps the run cheap.
                "spark.executor.instances": "2",
                "spark.dynamicAllocation.maxExecutors": "2",
            },
        ),
        environment_config=dataproc_v1.EnvironmentConfig(execution_config=execution),
        labels={"app": "icebergmatrix", "run": RUN_TAG[:63]},
    )
    print(f"[driver] submitting batch {batch_id} (runtime {RUNTIME}, subnet {SUBNET})")
    op = client.create_batch(parent=f"projects/{PROJECT}/locations/{REGION}",
                             batch=batch, batch_id=batch_id)
    name = f"projects/{PROJECT}/locations/{REGION}/batches/{batch_id}"
    deadline = time.time() + TIMEOUT_MIN * 60 + 600
    last = None
    while time.time() < deadline:
        got = client.get_batch(name=name)
        state = dataproc_v1.Batch.State(got.state).name
        if state != last:
            print(f"[driver] batch {batch_id}: {state}")
            last = state
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return got
        time.sleep(20)
    del op
    raise TimeoutError(f"batch {batch_id} still {last} after {TIMEOUT_MIN} minutes")


def download_reports() -> tuple:
    LOCAL_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = md_path = None
    for blob in gcs.bucket(BUCKET).list_blobs(prefix=f"{PREFIX}/reports/"):
        local = LOCAL_REPORT_DIR / f"managed-spark-{blob.name.rsplit('/', 1)[-1]}"
        blob.download_to_filename(str(local))
        print(f"[driver] report: {local.relative_to(REPO_ROOT)}")
        if local.suffix == ".json":
            json_path = local
        elif local.suffix == ".md":
            md_path = local
    return json_path, md_path


def main() -> int:
    work = REPO_ROOT / "test-reports" / f".managed-spark-{RUN_TAG}"
    bundle = upload(build_bundle(work / "bundle.zip"), "bundle.zip")
    entry = upload(REPO_ROOT / "tests" / "gcp" / "managed_spark_entrypoint.py", "entrypoint.py")
    batch = submit(entry, bundle)
    state = dataproc_v1.Batch.State(batch.state).name
    json_path, md_path = download_reports()
    lines = ["# Managed Service for Apache Spark Iceberg Feature Tests", "",
             f"- Runtime: {RUNTIME} (batch `{batch.name.rsplit('/', 1)[-1]}`, state {state})",
             f"- Catalog: Lakehouse runtime catalog `{CATALOG}` ({REGION})", ""]
    worst = 0
    if state != "SUCCEEDED":
        lines.append(f"Batch {state}: {batch.state_message}")
        worst = 1
    if not json_path:
        lines.append("No report was produced.")
        worst = 1
    else:
        s = json.loads(json_path.read_text())["summary"]
        lines.append(f"{s['passed']} passed, {s['failed']} failed, {s['skipped']} skipped, "
                     f"{s['errors']} errors, {s['discrepancies']} discrepancies")
        if s["errors"] or s["discrepancies"]:
            worst = 1
    if md_path:
        lines += ["", md_path.read_text()]
    text = "\n".join(lines)
    print("\n" + text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(text + "\n")
    return worst


if __name__ == "__main__":
    sys.exit(main())
