"""Batch-side entry point: run the Spark feature suite on Managed Service for Apache Spark.

Submitted by tests/gcp/run_managed_spark.py as the PySpark main file of a
serverless batch. It downloads the repo bundle (suite, fixture helper, matrix
data) from Cloud Storage itself: runtime 4.0 was measured not to stage
--archives (the batch failed with NoSuchFileException on the archive). The suite runs unmodified against the
Lakehouse runtime catalog over Iceberg REST, the same 70-odd checks that run
against OSS Spark, and the reports are uploaded back to Cloud Storage.

Arguments:
    --bundle gs://bucket/spark/<run>/bundle.zip
    --report-uri gs://bucket/spark/reports/<run>/
    --project <project id>
    --catalog <Lakehouse runtime catalog id>
    --ns-prefix <namespace prefix>
    --platform-label "<runtime> / <engine>"
"""
import argparse
import os
import runpy
import sys
import zipfile
WORK_DIR = "/tmp/icebergmatrix"
REPO = os.path.join(WORK_DIR, "repo")
REPORT_DIR = os.path.join(WORK_DIR, "reports")


def fetch_bundle(uri: str) -> None:
    from google.cloud import storage

    bucket_name, _, key = uri[len("gs://"):].partition("/")
    os.makedirs(WORK_DIR, exist_ok=True)
    local = os.path.join(WORK_DIR, "bundle.zip")
    storage.Client().bucket(bucket_name).blob(key).download_to_filename(local)
    with zipfile.ZipFile(local) as zf:
        zf.extractall(REPO)
    print(f"[entrypoint] bundle extracted to {REPO}")
def upload_reports(report_uri: str) -> None:
    from google.cloud import storage

    bucket_name, _, prefix = report_uri[len("gs://"):].partition("/")
    bucket = storage.Client().bucket(bucket_name)
    if not os.path.isdir(REPORT_DIR):
        print(f"[entrypoint] no reports at {REPORT_DIR}")
        return
    for name in sorted(os.listdir(REPORT_DIR)):
        local = os.path.join(REPORT_DIR, name)
        if os.path.isfile(local):
            blob = bucket.blob(f"{prefix.rstrip('/')}/{name}")
            blob.upload_from_filename(local)
            print(f"[entrypoint] uploaded gs://{bucket_name}/{blob.name}")


def detect_versions() -> tuple:
    """(iceberg version, spark version) as the runtime actually ships them.

    Asked of the JVM rather than read from jar names: the runtime documents no
    Iceberg version, and IcebergBuild reports whatever is on the classpath.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    try:
        iceberg = str(spark._jvm.org.apache.iceberg.IcebergBuild.version())
    except Exception as e:  # noqa: BLE001
        print(f"[entrypoint] Iceberg version not detectable: {e}")
        iceberg = "in-image"
    return iceberg, spark.version


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bundle", required=True)
    p.add_argument("--report-uri", required=True)
    p.add_argument("--project", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--ns-prefix", default="ms_")
    p.add_argument("--platform-label", default="")
    args, extra = p.parse_known_args()
    if extra:
        print(f"[entrypoint] ignoring arguments this script does not own: {extra}")

    fetch_bundle(args.bundle)
    iceberg, spark_version = detect_versions()
    print(f"[entrypoint] Spark {spark_version}, Iceberg {iceberg}")
    os.makedirs(REPORT_DIR, exist_ok=True)
    rest_props = ",".join([
        "uri=https://biglake.googleapis.com/iceberg/v1/restcatalog",
        "rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager",
        "io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO",
        f"header.x-goog-user-project={args.project}",
        "header.X-Iceberg-Access-Delegation=vended-credentials",
    ])
    os.environ.update({
        "REPO_ROOT": REPO,
        "REPORT_DIR": REPORT_DIR,
        "ICEBERG_VERSION": iceberg,
        "ICEBERG_WAREHOUSE": os.path.join(WORK_DIR, "hadoop-warehouse"),
        "MATRIX_PLATFORM_ID": "google-dataproc",
        "MATRIX_DATA_PATH": "src/data/platforms/gcp/dataproc/dataproc.json",
        "MATRIX_NS_PREFIX": args.ns_prefix,
        "MATRIX_STORAGE_MODE": "lakehouse",
        "PLATFORM_LABEL": args.platform_label or f"Spark {spark_version} / Iceberg {iceberg}",
        # Platform catalog: the service owns the jars and the master.
        "ICEBERG_CATALOG_TYPE": "rest",
        "ICEBERG_CATALOG_WAREHOUSE": f"bl://projects/{args.project}/catalogs/{args.catalog}",
        "ICEBERG_CATALOG_PROPS": rest_props,
        "ICEBERG_REST_URI": "",
        # The suite's equality-delete helper builds its own session config from
        # these when it needs one; point it at the same catalog.
        "ICEBERG_CATALOG_FLAVOR": "gcp-lakehouse",
        "GCP_PROJECT": args.project,
        "GCP_LAKEHOUSE_CATALOG": args.catalog,
    })
    sys.path.insert(0, os.path.join(REPO, "tests"))
    suite = os.path.join(REPO, "tests", "iceberg_feature_tests.py")
    print(f"[entrypoint] running {suite}")
    exit_code = 0
    try:
        # The suite exits non-zero on discrepancies; that is a result, not a
        # failure of the batch, so capture it and still upload the report.
        runpy.run_path(suite, run_name="__main__")
    except SystemExit as e:
        exit_code = int(e.code or 0)
    finally:
        upload_reports(args.report_uri)
    print(f"[entrypoint] suite exit code: {exit_code}")
    return 0


if __name__ == "__main__":
    code = main()
    if code:
        sys.exit(code)
