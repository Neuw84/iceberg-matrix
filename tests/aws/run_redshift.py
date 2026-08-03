"""Run the Redshift Iceberg feature suite against Redshift Serverless, both modes.

Unlike the EMR and Glue drivers, this one does not ship code to a cluster. Redshift
is driven entirely through the Data API, so the suite runs here in the runner and
every statement is a redshift-data call. That removes the bundle, the entry point
and the report round-trip through S3, which is why this driver is much shorter
despite covering the same 70 checks.

What it still shares with the other drivers is the output: the reports are renamed
into the <engine>-<mode>-iceberg-test-report.* convention and handed to
platform_common.summarise(), so a Redshift run publishes the same job summary
shape as every other engine. That is the whole point of driving managed engines
this way and it must not drift.

Environment:
    AWS_REGION              (required)
    REDSHIFT_WORKGROUP      (default: <prefix>-wg)
    REDSHIFT_DATABASE       (default: dev)
    REDSHIFT_NAMESPACE      (default: <prefix>-ns) used to look the secret up
    REDSHIFT_SECRET_ARN     admin secret; looked up from the namespace if unset
    REDSHIFT_ROLE_ARN       (required) role the external schema names
    AWS_DATA_BUCKET         (required for the s3buckets mode)
    AWS_TABLE_BUCKET_ARN    (required for the s3tables mode)
    REDSHIFT_FIXTURE_DB     optional Spark-created fixtures (see redshift_fixtures.py)
    RUN_TAG                 (required) unique per run
    MODES                   both|s3buckets|s3tables      (default: both)
"""

import json
import os
import runpy
import shutil
import sys
from pathlib import Path

import boto3

from platform_common import (  # noqa: E402 - sibling module, not a package
    DATA_BUCKET,
    LOCAL_REPORT_DIR,
    REGION,
    RESOURCE_PREFIX,
    REPO_ROOT,
    RUN_TAG,
    TABLE_BUCKET_ARN,
    modes_to_run,
    summarise,
)

ENGINE = "redshift"
WORKGROUP = os.environ.get("REDSHIFT_WORKGROUP", f"{RESOURCE_PREFIX}-wg")
NAMESPACE = os.environ.get("REDSHIFT_NAMESPACE", f"{RESOURCE_PREFIX}-ns")
DATABASE = os.environ.get("REDSHIFT_DATABASE", "dev")
ROLE_ARN = os.environ.get("REDSHIFT_ROLE_ARN", "")
FIXTURE_DB = os.environ.get("REDSHIFT_FIXTURE_DB", "")

SUITE = REPO_ROOT / "tests" / "redshift_feature_tests.py"


def admin_secret_arn() -> str:
    """The namespace's managed admin secret, looked up rather than configured.

    CloudFormation cannot return it: AWS::RedshiftServerless::Namespace rejects
    !GetAtt Namespace.AdminPasswordSecretArn as "must be a readonly property in
    schema", so the stack has no output to wire into a GitHub secret. The API does
    expose it, and with ManageAdminPassword the ARN is stable for the namespace, so
    resolving it here keeps the credential out of the template, the workflow inputs
    and the repository.
    """
    configured = os.environ.get("REDSHIFT_SECRET_ARN", "")
    if configured:
        return configured
    ns = boto3.client("redshift-serverless", region_name=REGION).get_namespace(
        namespaceName=NAMESPACE
    )["namespace"]
    arn = ns.get("adminPasswordSecretArn", "")
    if not arn:
        raise SystemExit(
            f"namespace {NAMESPACE} has no adminPasswordSecretArn; either it was "
            "not created with ManageAdminPassword or REDSHIFT_SECRET_ARN must be set"
        )
    print(f"[driver] resolved admin secret for namespace {NAMESPACE}")
    return arn


def engine_version() -> str:
    """Redshift's own version string, for the report.

    Best-effort: the suite detects it too, and a driver that died here would be
    failing the run over a cosmetic field.
    """
    try:
        wg = boto3.client("redshift-serverless", region_name=REGION).get_workgroup(
            workgroupName=WORKGROUP
        )["workgroup"]
        return f"{wg.get('baseCapacity', '?')} RPU"
    except Exception as e:  # noqa: BLE001
        print(f"[driver] could not describe workgroup: {type(e).__name__}: {e}")
        return "unknown"


def run_mode(mode: str, secret_arn: str) -> dict:
    """Run the suite in-process for one storage mode.

    runpy rather than a subprocess so a crash surfaces here with a real traceback
    instead of an exit code, and so the report paths need no plumbing. The suite
    calls sys.exit() to signal discrepancies, which is a result and not a driver
    failure, so SystemExit is caught.
    """
    data_path = f"src/data/platforms/aws/{mode}/redshift-s3/redshift-s3.json"
    print(f"\n[driver] === {mode}: workgroup {WORKGROUP}, database {DATABASE} ===")

    os.environ.update({
        "REPO_ROOT": str(REPO_ROOT),
        "REPORT_DIR": str(LOCAL_REPORT_DIR),
        "AWS_REGION": REGION,
        "REDSHIFT_WORKGROUP": WORKGROUP,
        "REDSHIFT_DATABASE": DATABASE,
        "REDSHIFT_SECRET_ARN": secret_arn,
        "REDSHIFT_ROLE_ARN": ROLE_ARN,
        "AWS_DATA_BUCKET": DATA_BUCKET,
        "AWS_TABLE_BUCKET_ARN": TABLE_BUCKET_ARN,
        "MATRIX_STORAGE_MODE": mode,
        "MATRIX_DATA_PATH": data_path,
        "MATRIX_NS_PREFIX": RESOURCE_PREFIX,
        "PLATFORM_LABEL": f"Redshift Serverless {engine_version()} / {mode}",
        "REDSHIFT_FIXTURE_DB": FIXTURE_DB,
        # One tag per mode. The two runs own separate external schemas, Glue
        # databases and S3 Tables namespaces, so a shared tag would have the
        # second mode's teardown delete the first mode's objects. Hyphens become
        # underscores because the tag ends up inside SQL identifiers.
        "RUN_TAG": f"{RUN_TAG}_{mode}".replace("-", "_").lower(),
    })

    exit_code = 0
    try:
        runpy.run_path(str(SUITE), run_name="__main__")
    except SystemExit as e:
        exit_code = int(e.code or 0)

    # The suite names its own artefacts; rename them into the shared convention so
    # summarise() and the other engines' reports stay interchangeable.
    produced = LOCAL_REPORT_DIR / f"redshift-iceberg-test-report-{mode}.json"
    report = None
    if produced.is_file():
        for suffix in (".json", ".md"):
            src = produced.with_suffix(suffix)
            if src.is_file():
                shutil.move(str(src),
                            str(LOCAL_REPORT_DIR / f"{ENGINE}-{mode}-iceberg-test-report{suffix}"))
        report = json.loads(
            (LOCAL_REPORT_DIR / f"{ENGINE}-{mode}-iceberg-test-report.json").read_text()
        )
        print(f"[driver] {mode}: {json.dumps(report['summary'])}")
    else:
        print(f"[driver] {mode} produced no report at {produced}")

    return {"mode": mode, "job_run_id": "", "report": report,
            "state": "SUCCESS" if report else "NO_REPORT",
            "state_details": f"suite exit {exit_code}", "report_uri": ""}


def main() -> int:
    if not ROLE_ARN:
        raise SystemExit("REDSHIFT_ROLE_ARN is required: the external schema must "
                         "name a role, because the auto-mounted catalog cannot write")
    modes = modes_to_run()
    print(f"[driver] region={REGION} workgroup={WORKGROUP} modes={modes}")
    print(f"[driver] fixtures: {FIXTURE_DB or 'none configured'}")

    secret_arn = admin_secret_arn()
    LOCAL_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    results, reports = [], {}
    for mode in modes:
        try:
            r = run_mode(mode, secret_arn)
        except Exception as e:  # noqa: BLE001 - one mode must not hide the other
            print(f"[driver] {mode} raised: {type(e).__name__}: {e}")
            results.append({"mode": mode, "job_run_id": "", "state": "DRIVER_ERROR",
                            "state_details": str(e)[:300], "report_uri": ""})
            continue
        results.append(r)
        if r["report"]:
            reports[mode] = r["report"]

    return summarise(
        ENGINE,
        "AWS Redshift Serverless Iceberg Feature Test Report",
        [f"- **Workgroup:** {WORKGROUP} ({engine_version()})",
         f"- **Catalog:** Glue Data Catalog external schema with IAM_ROLE",
         f"- **Fixtures:** {FIXTURE_DB or 'none (features Redshift cannot create report as unmeasured)'}",
         f"- **Run:** {RUN_TAG}"],
        results, reports,
    )


if __name__ == "__main__":
    sys.exit(main())
