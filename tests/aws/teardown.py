"""Delete everything billable that a platform test run created.

Runs with `if: always()` so it must never raise: every step is best-effort and
reports what it did. Anything it cannot remove is printed loudly, because the
alternative is silent spend.

Order matters. Job runs are cancelled before the application is stopped, and the
application is stopped before it is deleted, otherwise the delete is rejected.

What is billable, and therefore what this removes:
  - EMR Serverless job runs      billed per vCPU/memory-hour while RUNNING
  - EMR Serverless application   free while idle (no pre-init capacity), removed anyway
  - Glue job runs                billed per DPU-hour while active, one minute minimum
  - Glue job definitions         free to keep, removed so they do not accumulate
  - S3 objects                   warehouse data, logs, scripts
  - Glue databases and tables    catalog storage and requests
  - S3 Tables namespaces/tables  storage plus automatic maintenance, which bills
                                 even when nobody queries -- so tables must be
                                 deleted, not just emptied

Reports under <engine>/reports/<run>/ are kept deliberately: they are tiny and
they are the output of the run. The S3 lifecycle rules expire them later.

Environment: AWS_REGION, AWS_DATA_BUCKET, RUN_TAG,
             AWS_TABLE_BUCKET_ARN (optional), EMR_APPLICATION_ID (optional),
             RESOURCE_PREFIX (default: icebergmatrix), ENGINE (default: emr)
"""

import os
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION", "us-east-1")
DATA_BUCKET = os.environ.get("AWS_DATA_BUCKET", "")
TABLE_BUCKET_ARN = os.environ.get("AWS_TABLE_BUCKET_ARN", "")
RUN_TAG = os.environ.get("RUN_TAG", "")
RESOURCE_PREFIX = os.environ.get("RESOURCE_PREFIX", "icebergmatrix")
ENGINE = os.environ.get("ENGINE", "emr")

problems: list[str] = []


def note(msg: str) -> None:
    print(f"[teardown] {msg}")


def failed(what: str, e: Exception) -> None:
    msg = f"{what}: {type(e).__name__}: {e}"
    problems.append(msg)
    print(f"[teardown] COULD NOT CLEAN {msg}")


def application_id() -> str:
    app_id = os.environ.get("EMR_APPLICATION_ID", "")
    if app_id:
        return app_id
    marker = Path("/tmp/emr-application-id")
    return marker.read_text().strip() if marker.is_file() else ""


def kill_emr(app_id: str) -> None:
    if not app_id:
        note("no EMR application id recorded; skipping (check the console if a run was started)")
        return
    emr = boto3.client("emr-serverless", region_name=REGION)

    live = {"SUBMITTED", "PENDING", "SCHEDULED", "RUNNING"}
    try:
        runs = emr.list_job_runs(applicationId=app_id, states=list(live)).get("jobRuns", [])
        for run in runs:
            note(f"cancelling job run {run['id']} ({run['state']})")
            emr.cancel_job_run(applicationId=app_id, jobRunId=run["id"])
        for run in runs:  # wait so the stop below is not rejected
            for _ in range(30):
                state = emr.get_job_run(applicationId=app_id, jobRunId=run["id"])["jobRun"]["state"]
                if state not in live and state != "CANCELLING":
                    break
                time.sleep(5)
    except ClientError as e:
        failed(f"cancel job runs on {app_id}", e)

    try:
        state = emr.get_application(applicationId=app_id)["application"]["state"]
        if state in {"STARTED", "STARTING"}:
            note(f"stopping application {app_id}")
            emr.stop_application(applicationId=app_id)
            for _ in range(30):
                state = emr.get_application(applicationId=app_id)["application"]["state"]
                if state in {"STOPPED", "CREATED", "TERMINATED"}:
                    break
                time.sleep(5)
    except ClientError as e:
        failed(f"stop application {app_id}", e)

    try:
        note(f"deleting application {app_id}")
        emr.delete_application(applicationId=app_id)
    except ClientError as e:
        failed(f"delete application {app_id}", e)


def kill_glue_jobs() -> None:
    """Stop and delete the ETL job definitions a Glue run created.

    A job definition costs nothing to keep, but an *active run* bills per
    DPU-hour, so running ones are stopped first. Prefix-scoped and run-agnostic,
    so this also sweeps up definitions left behind by a crashed earlier run.
    """
    glue = boto3.client("glue", region_name=REGION)
    try:
        names = [
            n for page in glue.get_paginator("list_jobs").paginate()
            for n in page.get("JobNames", [])
            if n.startswith(RESOURCE_PREFIX)
        ]
    except ClientError as e:
        failed("list glue jobs", e)
        return

    live = {"STARTING", "RUNNING", "STOPPING", "WAITING"}
    for name in names:
        try:
            running = [
                r["Id"] for page in glue.get_paginator("get_job_runs").paginate(JobName=name)
                for r in page.get("JobRuns", [])
                if r.get("JobRunState") in live
            ]
            if running:
                note(f"stopping {len(running)} run(s) of glue job {name}")
                glue.batch_stop_job_run(JobName=name, JobRunIds=running)
        except ClientError as e:
            failed(f"stop runs of glue job {name}", e)
        try:
            glue.delete_job(JobName=name)
            note(f"deleted glue job {name}")
        except ClientError as e:
            failed(f"delete glue job {name}", e)


def drop_glue_databases() -> None:
    """Delete Glue databases this run created (prefix-scoped, run-agnostic)."""
    glue = boto3.client("glue", region_name=REGION)
    try:
        paginator = glue.get_paginator("get_databases")
        names = [
            db["Name"]
            for page in paginator.paginate()
            for db in page.get("DatabaseList", [])
            if db["Name"].startswith(f"{RESOURCE_PREFIX}_")
        ]
    except ClientError as e:
        failed("list glue databases", e)
        return

    for name in names:
        try:
            # Deleting the database drops its tables; the metadata in S3 is
            # removed by the prefix delete below.
            glue.delete_database(Name=name)
            note(f"deleted glue database {name}")
        except ClientError as e:
            failed(f"delete glue database {name}", e)


def drop_s3tables_namespaces() -> None:
    if not TABLE_BUCKET_ARN:
        note("no table bucket configured; skipping S3 Tables cleanup")
        return
    s3t = boto3.client("s3tables", region_name=REGION)
    try:
        namespaces = [
            ns
            for page in s3t.get_paginator("list_namespaces").paginate(tableBucketARN=TABLE_BUCKET_ARN)
            for ns in page.get("namespaces", [])
        ]
    except ClientError as e:
        failed("list s3tables namespaces", e)
        return

    for ns in namespaces:
        names = ns.get("namespace", [])
        name = names[0] if names else ""
        if not name.startswith(f"{RESOURCE_PREFIX}_"):
            continue
        try:
            tables = [
                t["name"]
                for page in s3t.get_paginator("list_tables").paginate(
                    tableBucketARN=TABLE_BUCKET_ARN, namespace=name)
                for t in page.get("tables", [])
            ]
            for table in tables:
                s3t.delete_table(tableBucketARN=TABLE_BUCKET_ARN, namespace=name, name=table)
                note(f"deleted s3 table {name}.{table}")
            s3t.delete_namespace(tableBucketARN=TABLE_BUCKET_ARN, namespace=name)
            note(f"deleted s3tables namespace {name}")
        except ClientError as e:
            failed(f"delete s3tables namespace {name}", e)


def delete_s3_prefixes() -> None:
    if not (DATA_BUCKET and RUN_TAG):
        note("no data bucket or run tag; skipping S3 cleanup")
        return
    s3 = boto3.client("s3", region_name=REGION)
    # reports/ is intentionally left behind; lifecycle expires it.
    #
    # fixtures-warehouse holds real Parquet for the Spark-built Redshift fixtures
    # and bills like any other warehouse, so it has to be swept even when the
    # engine under test was not EMR. The Redshift suite cleans its own
    # redshift/<run>/ prefix on the way out, but a crashed run would not, so it is
    # listed here too: deleting an absent prefix is free.
    prefixes = [f"{ENGINE}/{kind}/{RUN_TAG}/"
                for kind in ("warehouse", "logs", "scripts",
                             "fixtures", "fixtures-warehouse")]
    # Deliberately not "/"-terminated. The Redshift driver derives one tag per
    # storage mode by sanitising the run tag for use in SQL identifiers
    # (icebergmatrix-123 -> icebergmatrix_123_s3buckets), so this has to match a
    # tag prefix rather than an exact directory.
    prefixes.append(f"redshift/{RUN_TAG.replace('-', '_').lower()}")
    for prefix in prefixes:
        deleted = 0
        try:
            pages = s3.get_paginator("list_objects_v2").paginate(Bucket=DATA_BUCKET, Prefix=prefix)
            batch: list[dict] = []
            for page in pages:
                for obj in page.get("Contents", []):
                    batch.append({"Key": obj["Key"]})
                    if len(batch) == 1000:
                        s3.delete_objects(Bucket=DATA_BUCKET, Delete={"Objects": batch})
                        deleted += len(batch)
                        batch = []
            if batch:
                s3.delete_objects(Bucket=DATA_BUCKET, Delete={"Objects": batch})
                deleted += len(batch)
            note(f"deleted {deleted} objects under s3://{DATA_BUCKET}/{prefix}")
        except ClientError as e:
            failed(f"delete s3://{DATA_BUCKET}/{prefix}", e)


def main() -> int:
    note(f"run={RUN_TAG or '(unset)'} engine={ENGINE} region={REGION}")
    kill_emr(application_id())
    kill_glue_jobs()
    drop_glue_databases()
    drop_s3tables_namespaces()
    delete_s3_prefixes()

    if problems:
        print("\n[teardown] the following could not be cleaned and may still bill:")
        for p in problems:
            print(f"  - {p}")
        print("[teardown] check the EMR Serverless console and the S3 Tables bucket by hand")
    else:
        note("all billable resources removed")
    # Never fail the workflow: a teardown error must not mask the test report.
    return 0


if __name__ == "__main__":
    sys.exit(main())
