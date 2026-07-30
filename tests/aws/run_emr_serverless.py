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
from pathlib import Path

import boto3

from platform_common import (  # noqa: E402 - sibling module, not a package
    DATA_BUCKET,
    GLUE_CATALOG_IMPL,
    JOB_TIMEOUT_MINUTES,
    REGION,
    RESOURCE_PREFIX,
    RUN_TAG,
    TABLE_BUCKET_ARN,
    build_bundle,
    download_reports,
    modes_to_run,
    s3_uri,
    s3tables_catalog_props,
    summarise,
    upload,
    wait_for,
)

JOB_ROLE_ARN = os.environ["AWS_EMR_JOB_ROLE_ARN"]
RELEASE_LABEL = os.environ.get("EMR_RELEASE_LABEL", "emr-spark-8.0.0")
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

# S3 Tables goes through the Glue Data Catalog federation, not the native
# software.amazon.s3tables.iceberg.S3TablesCatalog client.
#
# The client catalog talks straight to s3tables.<region>.amazonaws.com, and an
# EMR Serverless application with no networkConfiguration has no route there:
# every call dies with "Connect timed out" after about 57 seconds. S3 Tables is
# not among the services EMR Serverless reaches by default. The federation keeps
# all metadata traffic on Glue, which it does reach, so no VPC is needed.
# See infra/aws/README.MD section 7 for the full diagnosis.
#
# Requires the table bucket to be integrated with AWS analytics services, which
# creates the s3tablescatalog federated catalog.
S3TABLES_EXTRA_JARS = os.environ.get("S3TABLES_EXTRA_JARS", "")

emr = boto3.client("emr-serverless", region_name=REGION)


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
    wait_for(lambda: emr.get_application(applicationId=app_id)["application"]["state"],
              want={"CREATED", "STARTED"}, bad={"TERMINATED"}, what=f"application {app_id}")
    print(f"[driver] application ready: {app_id}")
    return app_id


def spark_submit_params(mode: str, catalog_impl: str, warehouse: str,
                        catalog_props: str = "", catalog_type: str = "") -> str:
    jars = ICEBERG_JAR
    if mode == "s3tables" and S3TABLES_EXTRA_JARS:
        jars = ",".join(j for j in (jars, S3TABLES_EXTRA_JARS) if j)

    params = []
    if jars:
        params.append(f"--conf spark.jars={jars}")
    params += [
        "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog",
        # type and catalog-impl are mutually exclusive; Iceberg rejects both.
        (f"--conf spark.sql.catalog.local.type={catalog_type}" if catalog_type
         else f"--conf spark.sql.catalog.local.catalog-impl={catalog_impl}"),
        "--conf spark.sql.defaultCatalog=local",
    ]
    # A catalog that owns its storage takes no warehouse. S3 Tables through the
    # federation is one: the table bucket decides table locations and the
    # documented configuration sets glue.id in place of a warehouse.
    if warehouse:
        params.append(f"--conf spark.sql.catalog.local.warehouse={warehouse}")
    for prop in (p.strip() for p in catalog_props.split(",") if p.strip()):
        params.append(f"--conf spark.sql.catalog.local.{prop}")
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
    state = wait_for(
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
    catalog_type = ""
    catalog_impl = GLUE_CATALOG_IMPL
    # Both modes talk to Glue; they differ in which catalog and where the data
    # lands. s3buckets writes to the warehouse path below. For s3tables the
    # warehouse is never written to -- S3 Tables assigns a service-managed
    # location -- but GlueCatalog still derives one client-side before calling
    # Glue and fails on an empty value, so a real, permitted path must be given.
    warehouse = s3_uri(ENGINE, "warehouse", RUN_TAG) + "/"
    if mode == "s3buckets":
        catalog_props = ""
    else:
        if not TABLE_BUCKET_ARN:
            raise SystemExit("AWS_TABLE_BUCKET_ARN is required for the s3tables mode")
        catalog_props = s3tables_catalog_props()

    report_uri = s3_uri(ENGINE, "reports", RUN_TAG, mode) + "/"
    target = catalog_props or warehouse
    print(f"\n[driver] === {mode}: {catalog_impl} -> {target} ===")

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
                    "--engine", ENGINE,
                    "--catalog-impl", catalog_impl,
                    "--catalog-type", catalog_type,
                    "--warehouse", warehouse,
                    "--catalog-props", catalog_props,
                    "--ns-prefix", f"{RESOURCE_PREFIX}_",
                    "--platform-label", f"{RELEASE_LABEL} / {mode}",
                ],
                "sparkSubmitParameters": spark_submit_params(
                    mode, catalog_impl, warehouse, catalog_props, catalog_type),
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

    state = wait_for(
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


def main() -> int:
    modes = modes_to_run()
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
    entry_uri = upload(Path(__file__).with_name("platform_entrypoint.py"),
                       f"{ENGINE}/scripts/{RUN_TAG}/platform_entrypoint.py")

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
        path = download_reports(ENGINE, mode)
        if path:
            reports[mode] = json.loads(path.read_text())

    return summarise(
        ENGINE,
        "AWS EMR Serverless Iceberg Feature Test Report",
        [f"- **Release:** {RELEASE_LABEL}", f"- **Run:** {RUN_TAG}"],
        results, reports,
    )


if __name__ == "__main__":
    sys.exit(main())
