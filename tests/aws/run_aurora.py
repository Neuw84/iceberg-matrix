#!/usr/bin/env python3
"""
Driver that runs SQL against a PRIVATE Aurora PostgreSQL cluster through an
SSM-managed bastion, for tests/aurora_feature_tests.py.

The Aurora cluster has no public access, so there is no local network path to
it. Instead an EC2 bastion inside the VPC runs psql against the cluster
endpoint; this module drives that bastion with ``ssm:SendCommand`` +
``GetCommandInvocation`` (boto3), so no local psql, tunnel, or
session-manager-plugin is required. The bastion fetches the managed master-user
password from Secrets Manager itself (its instance role is granted
``secretsmanager:GetSecretValue`` on exactly the cluster secret), so no password
ever travels through this process.

Environment variables:
    AURORA_BASTION    SSM-managed bastion instance id (required)
    AURORA_ENDPOINT   Aurora cluster endpoint hostname (required)
    AURORA_SECRET     managed master-user secret ARN (required)
    AURORA_PORT       cluster port (default: 5432)
    AURORA_DATABASE   default database to connect to (default: postgres)
    AURORA_REGION     region (default: AWS_REGION or us-east-1)
    AURORA_SSM_TIMEOUT per-statement-batch timeout seconds (default: 120)

Public API:
    run_sql(sql, database=None) -> (ok, output)
        ok is False when psql printed any 'ERROR'. output is the combined
        psql stdout/stderr. SQL is run with ON_ERROR_STOP=0 so every statement
        in a batch runs and all diagnostics are captured.
"""

import base64
import json
import os
import time

REGION = os.environ.get("AURORA_REGION", os.environ.get("AWS_REGION", "us-east-1"))
BASTION = os.environ.get("AURORA_BASTION", "")
ENDPOINT = os.environ.get("AURORA_ENDPOINT", "")
SECRET = os.environ.get("AURORA_SECRET", "")
PORT = os.environ.get("AURORA_PORT", "5432")
DEFAULT_DB = os.environ.get("AURORA_DATABASE", "postgres")
SSM_TIMEOUT = int(os.environ.get("AURORA_SSM_TIMEOUT", "120"))

_SSM = None


def _ssm():
    global _SSM
    if _SSM is None:
        import boto3
        _SSM = boto3.client("ssm", region_name=REGION)
    return _SSM


def _remote_script(sql: str, database: str) -> str:
    """A bash script the bastion runs: fetch creds, run the SQL via psql."""
    sql_b64 = base64.b64encode(sql.encode()).decode()
    return "\n".join([
        "#!/bin/bash",
        "set -uo pipefail",
        f"echo {sql_b64} | base64 -d > /tmp/aurora_probe.sql",
        f"SJSON=$(aws secretsmanager get-secret-value --secret-id '{SECRET}' "
        f"--query SecretString --output text --region {REGION})",
        "export PGPASSWORD=$(echo \"$SJSON\" | jq -r .password)",
        "PGUSER=$(echo \"$SJSON\" | jq -r .username)",
        f"psql -h '{ENDPOINT}' -p {PORT} -d '{database}' -U \"$PGUSER\" "
        "-v ON_ERROR_STOP=0 -A -t -f /tmp/aurora_probe.sql 2>&1",
        "rm -f /tmp/aurora_probe.sql",
    ])


def run_sql(sql: str, database: str = None) -> tuple:
    """Run a SQL batch on the bastion. Returns (ok, combined_output)."""
    missing = [n for n, v in (("AURORA_BASTION", BASTION),
                              ("AURORA_ENDPOINT", ENDPOINT),
                              ("AURORA_SECRET", SECRET)) if not v]
    if missing:
        return False, f"missing required env: {', '.join(missing)}"

    database = database or DEFAULT_DB
    script = _remote_script(sql, database)
    try:
        cid = _ssm().send_command(
            InstanceIds=[BASTION],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [script]},
            TimeoutSeconds=max(SSM_TIMEOUT, 60),
        )["Command"]["CommandId"]
    except Exception as e:  # noqa: BLE001
        return False, f"ssm send-command failed: {type(e).__name__}: {e}"

    deadline = time.time() + SSM_TIMEOUT
    inv = None
    while time.time() < deadline:
        time.sleep(3)
        try:
            inv = _ssm().get_command_invocation(CommandId=cid, InstanceId=BASTION)
        except _ssm().exceptions.InvocationDoesNotExist:
            continue
        except Exception as e:  # noqa: BLE001
            return False, f"ssm get-command-invocation failed: {type(e).__name__}: {e}"
        if inv["Status"] in ("Success", "Failed", "Cancelled", "TimedOut"):
            break

    if inv is None:
        return False, "ssm invocation never registered"
    out = (inv.get("StandardOutputContent", "") or "")
    err = (inv.get("StandardErrorContent", "") or "")
    combined = (out + ("\n" + err if err.strip() else "")).strip()
    # psql ran with ON_ERROR_STOP=0, so SSM status is Success even on SQL errors;
    # detect SQL failure by the ERROR marker psql prints.
    ok = ("ERROR:" not in combined) and inv.get("Status") == "Success"
    return ok, combined


def detect_version(database: str = None) -> str:
    ok, out = run_sql("SELECT version();", database=database)
    if ok and out.strip():
        # e.g. "PostgreSQL 17.11 on aarch64-linux, ..."
        first = out.strip().splitlines()[0]
        parts = first.split()
        return parts[1] if len(parts) > 1 else first[:40]
    return "unknown"


if __name__ == "__main__":
    # Smoke test: print the server version.
    print("version:", detect_version())
    print(run_sql("SELECT 1;"))
