"""Run the Iceberg feature suite on AWS EMR Serverless, both storage modes.

Creates an ephemeral EMR Serverless application, submits one Spark job per
storage mode (S3 buckets via the Glue catalog, and S3 Tables), downloads the
reports and prints them in the same shape the OSS engine suites produce.

Everything lives under a per-engine prefix in the data bucket so the other
platforms can be added alongside without collisions:

    s3://<bucket>/emr/scripts/<run>/     entry point + repo bundle
    s3://<bucket>/emr/warehouse/<run>/   table data (s3buckets mode)
    s3://<bucket>/emr/logs/<run>/        EMR Serverless job logs
    s3://<bucket>/emr/reports/<run>/     JSON + markdown reports

The application has no pre-initialized capacity, so it costs nothing while
idle; billing is per job vCPU/memory-hour. tests/aws/teardown.py removes it
along with everything else billable.

Environment:
    AWS_REGION            (required)
    AWS_DATA_BUCKET       (required) S3 bucket for scripts/warehouse/logs/reports
    AWS_EMR_JOB_ROLE_ARN  (required) execution role passed to EMR Serverless
    AWS_TABLE_BUCKET_ARN  (required for s3tables mode)
    RUN_TAG               (required) unique per run, e.g. icebergmatrix-<run_id>
    MODES                 both|s3buckets|s3tables      (default: both)
    EMR_RELEASE_LABEL     (default: emr-spark-8.0.0) EMR 8 labels are
                          engine-specific, unlike the emr-7.x.y form
    RESOURCE_PREFIX       (default: icebergmatrix) must match the IAM scope
    JOB_TIMEOUT_MINUTES   (default: 30)
"""

import json
import os
import sys
import time
import zipfile
from pathlib import Path

import boto3

REGION = os.environ["AWS_REGION"]
DATA_BUCKET = os.environ["AWS_DATA_BUCKET"]
JOB_ROLE_ARN = os.environ["AWS_EMR_JOB_ROLE_ARN"]
TABLE_BUCKET_ARN = os.environ.get("AWS_TABLE_BUCKET_ARN", "")
RUN_TAG = os.environ["RUN_TAG"]
MODES = os.environ.get("MODES", "both")
RELEASE_LABEL = os.environ.get("EMR_RELEASE_LABEL", "emr-spark-8.0.0")
RESOURCE_PREFIX = os.environ.get("RESOURCE_PREFIX", "icebergmatrix")
JOB_TIMEOUT_MINUTES = int(os.environ.get("JOB_TIMEOUT_MINUTES", "30"))

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
LOCAL_REPORT_DIR = REPO_ROOT / "test-reports"
ENGINE = "emr"

# Iceberg is on the default classpath in the EMR Serverless image, so no
# spark.jars is passed: the extension and catalog configuration below is enough.
#
# Do not reinstate the path from the EMR Serverless "Using Apache Iceberg" docs
# (/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar). That page describes
# the 7.x images -- the jar name carries "spark3" while EMR 8 runs Spark 4 -- and
# on emr-spark-8.0.0 it fails the job with NoSuchFileException.
#
# ICEBERG_JAR_PATH stays overridable for releases that do need an explicit jar;
# empty (the default) omits spark.jars. PROBE=1 makes the image report its own
# layout if this ever needs revisiting.
ICEBERG_JAR = os.environ.get("ICEBERG_JAR_PATH", "")
PROBE = os.environ.get("PROBE", "").lower() in ("1", "true", "yes")

# Catalog implementations per storage mode.
GLUE_CATALOG_IMPL = "org.apache.iceberg.aws.glue.GlueCatalog"
# NOTE: unverified against EMR 8.0. If the s3tables job fails to resolve this
# class, override with S3TABLES_CATALOG_IMPL and add the client jar via
# S3TABLES_EXTRA_JARS.
S3TABLES_CATALOG_IMPL = os.environ.get(
    "S3TABLES_CATALOG_IMPL", "software.amazon.s3tables.iceberg.S3TablesCatalog"
)
S3TABLES_EXTRA_JARS = os.environ.get("S3TABLES_EXTRA_JARS", "")

emr = boto3.client("emr-serverless", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def s3_uri(*parts: str) -> str:
    return f"s3://{DATA_BUCKET}/" + "/".join(p.strip("/") for p in parts)


def build_bundle(dest: Path) -> Path:
    """Zip the suite and the matrix data the suite needs to compare against."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    includes = [REPO_ROOT / "tests" / "iceberg_feature_tests.py",
                REPO_ROOT / "src" / "data" / "features.json"]
    for mode in ("s3buckets", "s3tables"):
        includes.append(REPO_ROOT / "src" / "data" / "platforms" / "aws" / mode / "emr" / "emr.json")

    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in includes:
            if not path.is_file():
                raise FileNotFoundError(f"bundle input missing: {path}")
            zf.write(path, path.relative_to(REPO_ROOT).as_posix())
    print(f"[driver] bundle: {dest} ({dest.stat().st_size // 1024} KiB)")
    return dest


def upload(local: Path, key: str) -> str:
    s3.upload_file(str(local), DATA_BUCKET, key)
    uri = f"s3://{DATA_BUCKET}/{key}"
    print(f"[driver] uploaded {uri}")
    return uri


def create_application() -> str:
    # Reuse an existing application when asked. Useful when iterating locally:
    # an idle application costs nothing, and skipping creation saves a minute
    # per attempt.
    existing = os.environ.get("EMR_APPLICATION_ID", "")
    if existing:
        state = emr.get_application(applicationId=existing)["application"]["state"]
        print(f"[driver] reusing application {existing} ({state})")
        return existing
    return _create_application()


def _create_application() -> str:
    # RUN_TAG already carries the prefix in CI (icebergmatrix-<run_id>); don't
    # repeat it, and leave room for the 64-char limit.
    name = RUN_TAG if RUN_TAG.startswith(RESOURCE_PREFIX) else f"{RESOURCE_PREFIX}-{RUN_TAG}"
    name = name[:64]
    print(f"[driver] creating application {name} ({RELEASE_LABEL})")
    resp = emr.create_application(
        name=name,
        releaseLabel=RELEASE_LABEL,
        type="SPARK",
        clientToken=RUN_TAG[:64],
        # No initialCapacity: nothing is pre-warmed, so idle cost is zero.
        autoStartConfiguration={"enabled": True},
        autoStopConfiguration={"enabled": True, "idleTimeoutMinutes": 5},
        maximumCapacity={"cpu": "8 vCPU", "memory": "32 GB", "disk": "60 GB"},
        tags={"project": "iceberg-matrix", "run": RUN_TAG},
    )
    app_id = resp["applicationId"]
    _wait_for(lambda: emr.get_application(applicationId=app_id)["application"]["state"],
              want={"CREATED", "STARTED"}, bad={"TERMINATED"}, what=f"application {app_id}")
    print(f"[driver] application ready: {app_id}")
    return app_id


def _wait_for(get_state, want: set, bad: set, what: str, timeout_s: int = 600) -> str:
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        state = get_state()
        if state != last:
            print(f"[driver] {what}: {state}")
            last = state
        if state in want:
            return state
        if state in bad:
            raise RuntimeError(f"{what} entered {state}")
        time.sleep(10)
    raise TimeoutError(f"{what} still {last} after {timeout_s}s")


def spark_submit_params(mode: str, catalog_impl: str, warehouse: str) -> str:
    jars = ICEBERG_JAR
    if mode == "s3tables" and S3TABLES_EXTRA_JARS:
        jars = ",".join(j for j in (jars, S3TABLES_EXTRA_JARS) if j)

    params = []
    if jars:
        params.append(f"--conf spark.jars={jars}")
    params += [
        "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog",
        f"--conf spark.sql.catalog.local.catalog-impl={catalog_impl}",
        f"--conf spark.sql.catalog.local.warehouse={warehouse}",
        "--conf spark.sql.defaultCatalog=local",
    ]
    if mode == "s3buckets":
        params.append(
            "--conf spark.hadoop.hive.metastore.client.factory.class="
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        )
    return " ".join(params)


def run_probe(app_id: str, probe_uri: str) -> int:
    """Submit the diagnostic job and print where its output landed.

    Cheap way to settle release-dependent questions (jar path, whether Iceberg is
    already on the classpath, whether the S3 Tables catalog exists) instead of
    guessing across job runs. Output goes to the driver stdout log in S3.
    """
    print("\n[driver] === probe: reporting the image layout ===")
    resp = emr.start_job_run(
        applicationId=app_id,
        executionRoleArn=JOB_ROLE_ARN,
        name=f"{RESOURCE_PREFIX}-probe"[:64],
        executionTimeoutMinutes=15,
        # sparkSubmitParameters must be non-empty if present, so omit it: the
        # probe deliberately runs with the image defaults and no Iceberg config.
        jobDriver={"sparkSubmit": {"entryPoint": probe_uri, "entryPointArguments": []}},
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": s3_uri(ENGINE, "logs", RUN_TAG) + "/"}
            }
        },
        tags={"project": "iceberg-matrix", "run": RUN_TAG, "mode": "probe"},
    )
    job_id = resp["jobRunId"]
    state = _wait_for(
        lambda: emr.get_job_run(applicationId=app_id, jobRunId=job_id)["jobRun"]["state"],
        want={"SUCCESS", "FAILED", "CANCELLED"}, bad=set(),
        what=f"probe job {job_id}", timeout_s=1200,
    )
    log_prefix = f"{ENGINE}/logs/{RUN_TAG}/applications/{app_id}/jobs/{job_id}/"
    print(f"[driver] probe {state}")
    print(f"[driver] read the PROBE lines from the driver stdout under:")
    print(f"[driver]   s3://{DATA_BUCKET}/{log_prefix}SPARK_DRIVER/stdout.gz")
    print(f"[driver] e.g. aws s3 cp s3://{DATA_BUCKET}/{log_prefix}SPARK_DRIVER/stdout.gz - "
          "| gunzip | grep '^PROBE'")
    return 0 if state == "SUCCESS" else 1


def run_mode(app_id: str, mode: str, bundle_uri: str, entry_uri: str) -> dict:
    if mode == "s3buckets":
        catalog_impl = GLUE_CATALOG_IMPL
        warehouse = s3_uri(ENGINE, "warehouse", RUN_TAG) + "/"
    else:
        if not TABLE_BUCKET_ARN:
            raise SystemExit("AWS_TABLE_BUCKET_ARN is required for the s3tables mode")
        catalog_impl = S3TABLES_CATALOG_IMPL
        warehouse = TABLE_BUCKET_ARN

    report_uri = s3_uri(ENGINE, "reports", RUN_TAG, mode) + "/"
    print(f"\n[driver] === {mode}: {catalog_impl} -> {warehouse} ===")

    resp = emr.start_job_run(
        applicationId=app_id,
        executionRoleArn=JOB_ROLE_ARN,
        name=f"{RESOURCE_PREFIX}-{mode}"[:64],
        executionTimeoutMinutes=JOB_TIMEOUT_MINUTES,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": entry_uri,
                "entryPointArguments": [
                    "--bundle", bundle_uri,
                    "--report-uri", report_uri,
                    "--mode", mode,
                    "--catalog-impl", catalog_impl,
                    "--warehouse", warehouse,
                    "--ns-prefix", f"{RESOURCE_PREFIX}_",
                    "--platform-label", f"{RELEASE_LABEL} / {mode}",
                ],
                "sparkSubmitParameters": spark_submit_params(mode, catalog_impl, warehouse),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": s3_uri(ENGINE, "logs", RUN_TAG) + "/"}
            }
        },
        tags={"project": "iceberg-matrix", "run": RUN_TAG, "mode": mode},
    )
    job_id = resp["jobRunId"]

    state = _wait_for(
        lambda: emr.get_job_run(applicationId=app_id, jobRunId=job_id)["jobRun"]["state"],
        want={"SUCCESS", "FAILED", "CANCELLED"}, bad=set(),
        what=f"job {job_id} ({mode})", timeout_s=JOB_TIMEOUT_MINUTES * 60 + 300,
    )
    details = emr.get_job_run(applicationId=app_id, jobRunId=job_id)["jobRun"]
    if state != "SUCCESS":
        print(f"[driver] job {job_id} {state}: {details.get('stateDetails', '')}")
        print(f"[driver] logs: {s3_uri(ENGINE, 'logs', RUN_TAG)}/applications/{app_id}/jobs/{job_id}/")
    return {"mode": mode, "job_run_id": job_id, "state": state,
            "state_details": details.get("stateDetails", ""), "report_uri": report_uri}


def download_reports(mode: str) -> Path | None:
    """Pull the report this mode produced into test-reports/, OSS-style names."""
    prefix = f"{ENGINE}/reports/{RUN_TAG}/{mode}/"
    listing = s3.list_objects_v2(Bucket=DATA_BUCKET, Prefix=prefix).get("Contents", [])
    if not listing:
        print(f"[driver] no report objects under s3://{DATA_BUCKET}/{prefix}")
        return None

    LOCAL_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = None
    for obj in listing:
        name = obj["Key"].rsplit("/", 1)[-1]
        # emr-s3buckets-iceberg-test-report.json / .md
        local = LOCAL_REPORT_DIR / f"{ENGINE}-{mode}-{name}"
        s3.download_file(DATA_BUCKET, obj["Key"], str(local))
        print(f"[driver] report: {local.relative_to(REPO_ROOT)}")
        if local.suffix == ".json":
            json_path = local
    return json_path


def summarise(results: list, reports: dict) -> int:
    """Print the same summary the OSS suites print, and mirror it to the job summary."""
    lines = ["# AWS EMR Serverless Iceberg Feature Test Report", "",
             f"- **Release:** {RELEASE_LABEL}", f"- **Run:** {RUN_TAG}", ""]
    worst = 0

    for r in results:
        rep = reports.get(r["mode"])
        lines.append(f"## {r['mode']}")
        lines.append("")
        if r["state"] != "SUCCESS":
            lines.append(f"Job run {r['state']}: {r['state_details']}")
            lines.append("")
            worst = max(worst, 1)
            continue
        if not rep:
            lines.append("Job succeeded but produced no report.")
            lines.append("")
            worst = max(worst, 1)
            continue

        s = rep["summary"]
        lines += [
            f"- **Catalog:** {rep.get('catalog_mode', '')}",
            f"- **Spark:** {rep.get('spark_version', '')} | **Iceberg:** {rep.get('iceberg_version', '')}",
            "",
            "| Metric | Count |", "|--------|-------|",
            f"| Total | {s['total']} |",
            f"| Passed | {s['passed']} |",
            f"| Failed | {s['failed']} |",
            f"| Skipped | {s['skipped']} |",
            f"| Errors | {s['errors']} |",
            f"| Discrepancies | {s['discrepancies']} |",
            "",
        ]
        discs = [t for t in rep["tests"] if not t["match"]]
        if discs:
            lines.append("### Discrepancies")
            lines.append("")
            for t in discs:
                lines.append(f"- **{t['feature_name']}** ({t['version']}): "
                             f"test={t['result']}, json={t['json_level']} — {t['details'][:160]}")
            lines.append("")
        if s["discrepancies"] or s["errors"]:
            worst = max(worst, 1)

    text = "\n".join(lines)
    print("\n" + text)
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(text + "\n")
    return worst


def main() -> int:
    modes = ["s3buckets", "s3tables"] if MODES == "both" else [MODES]
    print(f"[driver] region={REGION} bucket={DATA_BUCKET} modes={modes}")

    if PROBE:
        probe_uri = upload(Path(__file__).with_name("emr_probe.py"),
                           f"{ENGINE}/scripts/{RUN_TAG}/emr_probe.py")
        app_id = create_application()
        Path("/tmp/emr-application-id").write_text(app_id)
        if os.environ.get("GITHUB_ENV"):
            with open(os.environ["GITHUB_ENV"], "a") as f:
                f.write(f"EMR_APPLICATION_ID={app_id}\n")
        return run_probe(app_id, probe_uri)

    bundle = build_bundle(Path("/tmp") / f"{RUN_TAG}-bundle.zip")
    bundle_uri = upload(bundle, f"{ENGINE}/scripts/{RUN_TAG}/bundle.zip")
    entry_uri = upload(Path(__file__).with_name("emr_entrypoint.py"),
                       f"{ENGINE}/scripts/{RUN_TAG}/emr_entrypoint.py")

    app_id = create_application()
    # Record the application id so teardown can find it even if we crash.
    Path("/tmp/emr-application-id").write_text(app_id)
    if os.environ.get("GITHUB_ENV"):
        with open(os.environ["GITHUB_ENV"], "a") as f:
            f.write(f"EMR_APPLICATION_ID={app_id}\n")

    results, reports = [], {}
    for mode in modes:
        try:
            r = run_mode(app_id, mode, bundle_uri, entry_uri)
        except Exception as e:  # noqa: BLE001 - one mode failing must not hide the other
            print(f"[driver] {mode} raised: {type(e).__name__}: {e}")
            results.append({"mode": mode, "job_run_id": "", "state": "DRIVER_ERROR",
                            "state_details": str(e)[:300], "report_uri": ""})
            continue
        results.append(r)
        path = download_reports(mode)
        if path:
            reports[mode] = json.loads(path.read_text())

    return summarise(results, reports)


if __name__ == "__main__":
    sys.exit(main())
