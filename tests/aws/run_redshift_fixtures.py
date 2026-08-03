"""Submit the Redshift fixture job to EMR Serverless and report the manifest.

Creates, with Spark, the Iceberg tables Redshift refuses to create, so the
Redshift suite can tell "cannot create" from "cannot read". See
redshift_fixtures.py for what is built and why.

Reuses the EMR plumbing in platform_common.py and run_emr_serverless.py rather
than duplicating it: the same application, the same S3 layout, the same catalog
wiring for both storage modes. The only difference is the entry point and that
this job writes a manifest instead of a report.

Environment:
    AWS_REGION            (required)
    AWS_DATA_BUCKET       (required)
    AWS_EMR_JOB_ROLE_ARN  (required)
    AWS_TABLE_BUCKET_ARN  (required for the s3tables mode)
    RUN_TAG               (required)
    MODES                 both|s3buckets|s3tables      (default: both)
    EMR_APPLICATION_ID    reuse an existing application instead of creating one
    EMR_RELEASE_LABEL     (default: emr-spark-8.0.0)
"""

import json
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
    modes_to_run,
    s3_uri,
    s3tables_catalog_props,
    upload,
    wait_for,
)
from run_emr_serverless import (  # noqa: E402
    JOB_ROLE_ARN,
    RELEASE_LABEL,
    create_application,
    spark_submit_params,
)

ENGINE = "emr"
emr = boto3.client("emr-serverless", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# The namespace the fixtures live in. Deliberately stable rather than per-run:
# the Redshift suite has to find it, and a fixture set is cheap to leave in place
# between the two runs of a single CI job. tests/aws/teardown.py removes it.
FIXTURE_NAMESPACE = f"{RESOURCE_PREFIX}_rsfix"


def manifest_key(mode: str) -> str:
    return f"{ENGINE}/fixtures/{RUN_TAG}/{mode}/manifest.json"


def run_mode(app_id: str, mode: str, entry_uri: str) -> dict:
    warehouse = s3_uri(ENGINE, "fixtures-warehouse", RUN_TAG) + "/"
    if mode == "s3tables":
        if not TABLE_BUCKET_ARN:
            raise SystemExit("AWS_TABLE_BUCKET_ARN is required for the s3tables mode")
        catalog_props = s3tables_catalog_props()
    else:
        catalog_props = ""

    manifest_uri = f"s3://{DATA_BUCKET}/{manifest_key(mode)}"
    print(f"\n[fixtures] === {mode}: namespace {FIXTURE_NAMESPACE} ===")

    resp = emr.start_job_run(
        applicationId=app_id,
        executionRoleArn=JOB_ROLE_ARN,
        name=f"{RESOURCE_PREFIX}-rsfix-{mode}"[:64],
        executionTimeoutMinutes=JOB_TIMEOUT_MINUTES,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": entry_uri,
                "entryPointArguments": [
                    "--namespace", FIXTURE_NAMESPACE,
                    "--manifest-uri", manifest_uri,
                    "--mode", mode,
                ],
                "sparkSubmitParameters": spark_submit_params(
                    mode, GLUE_CATALOG_IMPL, warehouse, catalog_props, ""),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": s3_uri(ENGINE, "logs", RUN_TAG) + "/"
                }
            }
        },
        tags={"project": "iceberg-matrix", "run": RUN_TAG, "mode": f"rsfix-{mode}"},
    )
    job_id = resp["jobRunId"]
    state = wait_for(
        lambda: emr.get_job_run(applicationId=app_id, jobRunId=job_id)["jobRun"]["state"],
        want={"SUCCESS", "FAILED", "CANCELLED"}, bad=set(),
        what=f"fixture job {job_id} ({mode})",
        timeout_s=JOB_TIMEOUT_MINUTES * 60 + 300,
    )
    details = emr.get_job_run(applicationId=app_id, jobRunId=job_id)["jobRun"]
    if state != "SUCCESS":
        print(f"[fixtures] job {job_id} {state}: {details.get('stateDetails', '')}")
        print(f"[fixtures] logs: {s3_uri(ENGINE, 'logs', RUN_TAG)}"
              f"/applications/{app_id}/jobs/{job_id}/")

    manifest = None
    try:
        body = s3.get_object(Bucket=DATA_BUCKET, Key=manifest_key(mode))["Body"].read()
        manifest = json.loads(body)
    except Exception as e:  # noqa: BLE001
        print(f"[fixtures] no manifest for {mode}: {type(e).__name__}: {e}")

    return {"mode": mode, "job_run_id": job_id, "state": state,
            "manifest_uri": manifest_uri, "manifest": manifest}


def main() -> int:
    modes = modes_to_run()
    print(f"[fixtures] region={REGION} bucket={DATA_BUCKET} modes={modes}")

    entry_uri = upload(Path(__file__).with_name("redshift_fixtures.py"),
                       f"{ENGINE}/scripts/{RUN_TAG}/redshift_fixtures.py")
    app_id = create_application()
    Path("/tmp/emr-application-id").write_text(app_id)

    worst = 0
    for mode in modes:
        result = run_mode(app_id, mode, entry_uri)
        if result["state"] != "SUCCESS" or not result["manifest"]:
            worst = 1
            continue
        fixtures = result["manifest"]["fixtures"]
        made = [n for n, e in fixtures.items() if e.get("created")]
        missed = {n: e.get("error", "") for n, e in fixtures.items()
                  if not e.get("created")}
        print(f"\n[fixtures] {mode}: created {len(made)}/{len(fixtures)}")
        for name in made:
            print(f"  OK   {name}: {fixtures[name].get('stored')}")
        for name, err in missed.items():
            print(f"  MISS {name}: {err[:200]}")
        print(f"[fixtures] manifest: {result['manifest_uri']}")
        print(f"[fixtures] point the suite at it with "
              f"REDSHIFT_FIXTURE_MANIFEST={result['manifest_uri']}")

    return worst


if __name__ == "__main__":
    sys.exit(main())
