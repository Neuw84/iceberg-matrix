"""Run the Iceberg feature suite on AWS Glue for Spark, both storage modes.

Creates an ephemeral Glue ETL job definition, runs it once per storage mode
(S3 buckets via the Glue catalog, and S3 Tables via the federated
s3tablescatalog), downloads the reports and prints them in the shape every other
engine in this repo produces.

The catalog configuration is identical to EMR Serverless -- both engines reach
Iceberg through GlueCatalog, and S3 Tables through glue.id -- so the interesting
differences are the runtime versions, not the wiring. See infra/aws/README.MD
section 7 for why S3 Tables is addressed this way.

Glue version matters more than it looks. Iceberg is pinned by the Glue release,
not chosen by us:

    Glue 5.1  Spark 3.5.6  Iceberg 1.10.0  Iceberg format version 3 supported
    Glue 5.0  Spark 3.5.4  Iceberg 1.7.1   predates most V3 features
    Glue 4.0  Spark 3.3    Iceberg 1.0.0

So a V3 run on anything below 5.1 mostly measures the absence of V3, which is
why 5.1 is the default.

What bills: Glue charges per DPU-hour while a job run is active, with a one
minute minimum. The job *definition* is free, but it is deleted anyway so nothing
accumulates. tests/aws/teardown.py removes it along with the tables and objects.

Environment (plus the shared set documented in platform_common):
    AWS_GLUE_JOB_ROLE_ARN  execution role for the Glue job; falls back to
                           AWS_EMR_JOB_ROLE_ARN, which is the same role
    GLUE_VERSION           (default: 5.1)
    GLUE_WORKER_TYPE       (default: G.1X)
    GLUE_NUMBER_OF_WORKERS (default: 2)
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
    STORAGE_MODES,
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

JOB_ROLE_ARN = os.environ.get("AWS_GLUE_JOB_ROLE_ARN") or os.environ["AWS_EMR_JOB_ROLE_ARN"]
GLUE_VERSION = os.environ.get("GLUE_VERSION", "5.1")
WORKER_TYPE = os.environ.get("GLUE_WORKER_TYPE", "G.1X")
NUMBER_OF_WORKERS = int(os.environ.get("GLUE_NUMBER_OF_WORKERS", "2"))
ENGINE = "glue"

glue = boto3.client("glue", region_name=REGION)

# Glue job run states. Unlike EMR Serverless there is no separate application to
# start and stop, so the only lifecycle to wait on is the run itself.
TERMINAL = {"SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED", "ERROR"}


def job_name() -> str:
    # Must start with the resource prefix: the CI role is scoped to job/<prefix>*.
    name = RUN_TAG if RUN_TAG.startswith(RESOURCE_PREFIX) else f"{RESOURCE_PREFIX}-{RUN_TAG}"
    return name[:255]


def create_job(entry_uri: str) -> str:
    """Create the ETL job definition the runs will use.

    One definition serves both modes; the per-mode catalog configuration is
    passed as job-run arguments rather than baked in, so nothing has to be
    recreated between modes.
    """
    name = job_name()
    # Only job-level settings here. Nothing per-run and nothing empty: Glue
    # forwards an argument declared with an empty value to the script as a bare
    # flag with no value, and argparse then dies with "expected one argument".
    # Per-run values are supplied by start_job_run instead.
    args = {
        # Turns on the in-image Iceberg libraries. Without this the Iceberg
        # classes are absent and every test fails to resolve the catalog.
        "--datalake-formats": "iceberg",
        "--job-language": "python",
        "--TempDir": s3_uri(ENGINE, "tmp", RUN_TAG) + "/",
        "--enable-continuous-cloudwatch-log": "true",
    }
    print(f"[driver] creating glue job {name} (Glue {GLUE_VERSION}, "
          f"{NUMBER_OF_WORKERS}x{WORKER_TYPE})")
    glue.create_job(
        Name=name,
        Role=JOB_ROLE_ARN,
        GlueVersion=GLUE_VERSION,
        Command={"Name": "glueetl", "PythonVersion": "3", "ScriptLocation": entry_uri},
        DefaultArguments=args,
        WorkerType=WORKER_TYPE,
        NumberOfWorkers=NUMBER_OF_WORKERS,
        Timeout=JOB_TIMEOUT_MINUTES,
        # The modes run strictly one after another, but 1 is still too tight:
        # Glue keeps counting a run as active for a moment after it reports
        # SUCCEEDED, so starting the next mode immediately failed with
        # ConcurrentRunsExceededException. Allowing a little headroom removes the
        # race without ever running two modes at once.
        ExecutionProperty={"MaxConcurrentRuns": len(STORAGE_MODES) + 1},
        Tags={"project": "iceberg-matrix", "run": RUN_TAG},
    )
    # Record it so teardown can remove the definition even if this crashes.
    Path("/tmp/glue-job-name").write_text(name)
    if os.environ.get("GITHUB_ENV"):
        with open(os.environ["GITHUB_ENV"], "a") as f:
            f.write(f"GLUE_JOB_NAME={name}\n")
    return name


def spark_conf(mode: str, warehouse: str, catalog_props: str) -> str:
    """The --conf value for a Glue run.

    Glue takes Spark configuration as a single --conf argument with entries
    separated by "--conf", which is not the same shape as spark-submit takes on
    EMR. The catalog settings themselves are identical.
    """
    settings = [
        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.local.catalog-impl={GLUE_CATALOG_IMPL}",
        "spark.sql.defaultCatalog=local",
        f"spark.sql.catalog.local.warehouse={warehouse}",
    ]
    for prop in (p.strip() for p in catalog_props.split(",") if p.strip()):
        settings.append(f"spark.sql.catalog.local.{prop}")
    # Secondary local-filesystem catalog the hadoop-catalog test uses. The suite
    # configures this itself too; setting it here keeps the two consistent.
    settings.append("spark.sql.catalog.hadoop_local=org.apache.iceberg.spark.SparkCatalog")
    settings.append("spark.sql.catalog.hadoop_local.type=hadoop")
    settings.append("spark.sql.catalog.hadoop_local.warehouse=/tmp/icebergmatrix/hadoop-warehouse")
    return " --conf ".join(settings)


def run_mode(name: str, mode: str, bundle_uri: str) -> dict:
    warehouse = s3_uri(ENGINE, "warehouse", RUN_TAG) + "/"
    if mode == "s3buckets":
        catalog_props = ""
    else:
        if not TABLE_BUCKET_ARN:
            raise SystemExit("AWS_TABLE_BUCKET_ARN is required for the s3tables mode")
        # Same federation the EMR driver uses. The warehouse above is never
        # written to in this mode -- S3 Tables assigns its own location -- but
        # GlueCatalog derives one client-side and rejects an empty value.
        catalog_props = s3tables_catalog_props()

    report_uri = s3_uri(ENGINE, "reports", RUN_TAG, mode) + "/"
    target = catalog_props or warehouse
    print(f"\n[driver] === {mode}: {GLUE_CATALOG_IMPL} -> {target} ===")

    run_args = {
        "--bundle": bundle_uri,
        "--report-uri": report_uri,
        "--engine": ENGINE,
        "--mode": mode,
        "--catalog-impl": GLUE_CATALOG_IMPL,
        "--warehouse": warehouse,
        "--catalog-props": catalog_props,
        "--ns-prefix": f"{RESOURCE_PREFIX}_",
        "--platform-label": f"Glue {GLUE_VERSION} / {mode}",
        "--conf": spark_conf(mode, warehouse, catalog_props),
    }
    # Drop empties for the same reason as in create_job: an empty value arrives
    # as a valueless flag and argparse rejects it. Omitted means "use the
    # entrypoint default", which is what an empty value meant anyway.
    run_args = {k: v for k, v in run_args.items() if v != ""}

    resp = glue.start_job_run(
        JobName=name,
        Arguments=run_args,
        WorkerType=WORKER_TYPE,
        NumberOfWorkers=NUMBER_OF_WORKERS,
        Timeout=JOB_TIMEOUT_MINUTES,
    )
    run_id = resp["JobRunId"]

    state = wait_for(
        lambda: glue.get_job_run(JobName=name, RunId=run_id)["JobRun"]["JobRunState"],
        want=TERMINAL, bad=set(),
        what=f"run {run_id} ({mode})", timeout_s=JOB_TIMEOUT_MINUTES * 60 + 300,
    )
    run = glue.get_job_run(JobName=name, RunId=run_id)["JobRun"]
    if state != "SUCCEEDED":
        print(f"[driver] run {run_id} {state}: {run.get('ErrorMessage', '')}")
        print(f"[driver] logs: CloudWatch /aws-glue/jobs/output, stream {run_id}")

    # summarise() speaks EMR's vocabulary, where success is "SUCCESS".
    return {"mode": mode, "job_run_id": run_id,
            "state": "SUCCESS" if state == "SUCCEEDED" else state,
            "state_details": run.get("ErrorMessage", ""), "report_uri": report_uri}


def delete_job(name: str) -> None:
    try:
        glue.delete_job(JobName=name)
        print(f"[driver] deleted glue job {name}")
    except Exception as e:  # noqa: BLE001 - teardown also removes it; never mask results
        print(f"[driver] could not delete glue job {name}: {type(e).__name__}: {e}")


def main() -> int:
    modes = modes_to_run()
    print(f"[driver] region={REGION} bucket={DATA_BUCKET} "
          f"glue={GLUE_VERSION} modes={modes}")

    bundle = build_bundle(Path("/tmp") / f"{RUN_TAG}-bundle.zip")
    bundle_uri = upload(bundle, f"{ENGINE}/scripts/{RUN_TAG}/bundle.zip")
    entry_uri = upload(Path(__file__).with_name("platform_entrypoint.py"),
                       f"{ENGINE}/scripts/{RUN_TAG}/platform_entrypoint.py")

    name = create_job(entry_uri)
    results, reports = [], {}
    try:
        for mode in modes:
            try:
                r = run_mode(name, mode, bundle_uri)
            except Exception as e:  # noqa: BLE001 - one mode must not hide the other
                print(f"[driver] {mode} raised: {type(e).__name__}: {e}")
                results.append({"mode": mode, "job_run_id": "", "state": "DRIVER_ERROR",
                                "state_details": str(e)[:300], "report_uri": ""})
                continue
            results.append(r)
            path = download_reports(ENGINE, mode)
            if path:
                reports[mode] = json.loads(path.read_text())
    finally:
        delete_job(name)

    return summarise(
        ENGINE,
        "AWS Glue Iceberg Feature Test Report",
        [f"- **Glue version:** {GLUE_VERSION}", f"- **Run:** {RUN_TAG}"],
        results, reports,
    )


if __name__ == "__main__":
    sys.exit(main())
