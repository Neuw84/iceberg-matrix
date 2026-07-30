"""EMR Serverless entry point: run the Spark feature suite against an AWS catalog.

Submitted by tests/aws/run_emr_serverless.py as the spark-submit entry point.
This runs *inside* the EMR Serverless job, so it must not assume anything about
the repo being present: it pulls a bundle (the suite plus the matrix data) from
S3, points the suite at the right catalog and matrix cells, then uploads the
resulting report back to S3.

The suite itself is unmodified -- the same 70 checks that run against OSS Spark
run here, which is the whole reason for choosing EMR as the first platform.

Arguments (all required, passed as entryPointArguments):
    --bundle s3://bucket/emr/scripts/<run>/bundle.zip
    --report-uri s3://bucket/emr/reports/<run>/<mode>/
    --mode s3buckets|s3tables
    --catalog-impl <iceberg catalog-impl class>
    --warehouse <warehouse location or table bucket ARN>
    --ns-prefix <namespace prefix the job role is allowed to create>
    --platform-label "<release label> / <mode>"
"""

import argparse
import os
import runpy
import sys
import zipfile
from urllib.parse import urlparse

WORK_DIR = "/tmp/icebergmatrix"
REPORT_DIR = os.path.join(WORK_DIR, "reports")


def _split_s3(uri: str):
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


def fetch_bundle(bundle_uri: str, dest_dir: str) -> str:
    """Download and unpack the repo bundle. Returns the extracted repo root."""
    import boto3

    bucket, key = _split_s3(bundle_uri)
    local_zip = os.path.join(WORK_DIR, "bundle.zip")
    os.makedirs(WORK_DIR, exist_ok=True)
    boto3.client("s3").download_file(bucket, key, local_zip)
    with zipfile.ZipFile(local_zip) as zf:
        zf.extractall(dest_dir)
    print(f"[entrypoint] bundle extracted to {dest_dir}")
    return dest_dir


def upload_reports(report_uri: str) -> None:
    import boto3

    bucket, prefix = _split_s3(report_uri)
    s3 = boto3.client("s3")
    if not os.path.isdir(REPORT_DIR):
        print(f"[entrypoint] no reports at {REPORT_DIR}")
        return
    for name in sorted(os.listdir(REPORT_DIR)):
        local = os.path.join(REPORT_DIR, name)
        if os.path.isfile(local):
            key = f"{prefix.rstrip('/')}/{name}"
            s3.upload_file(local, bucket, key)
            print(f"[entrypoint] uploaded s3://{bucket}/{key}")


def detect_iceberg_version() -> str:
    """Read the Iceberg version out of the runtime jar shipped in the image.

    The jar is named iceberg-spark-runtime-<spark>_<scala>-<version>.jar, e.g.
    iceberg-spark-runtime-4.0_2.13-1.10.1-amzn-0.jar on emr-spark-8.0.0. Falls
    back to "in-image" rather than guessing.
    """
    import glob
    import re

    for path in sorted(glob.glob("/usr/share/aws/iceberg/lib/iceberg-spark-runtime-*.jar")):
        m = re.search(r"iceberg-spark-runtime-[\d.]+_[\d.]+-(.+)\.jar$", os.path.basename(path))
        if m:
            print(f"[entrypoint] iceberg runtime: {os.path.basename(path)}")
            return m.group(1)
    return "in-image"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bundle", required=True)
    p.add_argument("--report-uri", required=True)
    p.add_argument("--mode", required=True, choices=["s3buckets", "s3tables"])
    # Exactly one of these: a catalog-impl class name, or a built-in type such as
    # "rest" for the Glue Iceberg REST endpoint used by the s3tables mode.
    p.add_argument("--catalog-impl", default="")
    p.add_argument("--catalog-type", default="")
    p.add_argument("--warehouse", default="")
    # Extra catalog properties as "key=value,key=value", e.g. glue.id and
    # client.region for the S3 Tables federation.
    p.add_argument("--catalog-props", default="")
    p.add_argument("--ns-prefix", default="icebergmatrix_")
    p.add_argument("--platform-label", default="")
    args = p.parse_args()

    repo_root = fetch_bundle(args.bundle, os.path.join(WORK_DIR, "repo"))
    os.makedirs(REPORT_DIR, exist_ok=True)

    os.environ.update({
        "REPO_ROOT": repo_root,
        "REPORT_DIR": REPORT_DIR,
        # Report the Iceberg version actually in the image rather than the
        # suite's default, which only describes the OSS Maven coordinates.
        "ICEBERG_VERSION": detect_iceberg_version(),
        "ICEBERG_WAREHOUSE": os.path.join(WORK_DIR, "hadoop-warehouse"),
        # Compare against the AWS EMR cells for this storage mode.
        "MATRIX_PLATFORM_ID": "aws-emr",
        "MATRIX_DATA_PATH": f"src/data/platforms/aws/{args.mode}/emr/emr.json",
        "MATRIX_NS_PREFIX": args.ns_prefix,
        "MATRIX_STORAGE_MODE": args.mode,
        "PLATFORM_LABEL": args.platform_label,
        # Platform catalog: no REST, no jar wiring, no master override.
        "ICEBERG_CATALOG_IMPL": args.catalog_impl,
        "ICEBERG_CATALOG_TYPE": args.catalog_type,
        "ICEBERG_CATALOG_WAREHOUSE": args.warehouse,
        "ICEBERG_CATALOG_PROPS": args.catalog_props,
        # S3 Tables owns the storage and rejects a metadata-only DROP TABLE with
        # "S3 managed Iceberg table must be purged when dropped".
        "ICEBERG_DROP_PURGE": "1" if args.mode == "s3tables" else "",
        "ICEBERG_REST_URI": "",
    })

    suite = os.path.join(repo_root, "tests", "iceberg_feature_tests.py")
    print(f"[entrypoint] running {suite} (mode={args.mode})")

    exit_code = 0
    try:
        # The suite calls sys.exit() to signal discrepancies; that is a result,
        # not a failure of this job, so capture it and keep going to the upload.
        runpy.run_path(suite, run_name="__main__")
    except SystemExit as e:
        exit_code = int(e.code or 0)
    finally:
        upload_reports(args.report_uri)

    print(f"[entrypoint] suite exit code: {exit_code}")
    # Always exit 0: a discrepancy is data, and failing the job run would only
    # obscure the report the driver is about to read.
    return 0


if __name__ == "__main__":
    sys.exit(main())
