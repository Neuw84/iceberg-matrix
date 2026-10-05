#!/usr/bin/env python3
"""
Build the Glue/Iceberg fixtures the Aurora feature suite reads.

Aurora writes nothing to the lake, so the tables it reads are produced here with
Amazon Athena (engine v3), which writes Iceberg V2 tables with row-level deletes
into the AWS Glue Data Catalog. These cover the read-capable cells the suite
measures by read: position deletes / merge-on-read (``posdel``), hidden
partitioning (``hidpart``), and partition evolution (``partevo``).

V3 fixtures (deletion vectors, nanosecond timestamps, geometry, lineage) are NOT
built here: Athena engine v3 rejects ``format_version=3`` and pyiceberg 0.11.1
cannot write V3, so those cells are left as honest "skip" in the suite rather
than measured against a fabricated fixture. Producing them would need Spark with
Iceberg 1.11 writing into Glue (future work).

Usage:
    export AURORA_FIXTURE_DB=aurora_probe_db
    export AURORA_FIXTURE_LOCATION=s3://<bucket>/aurora-probe-fixtures
    export AURORA_ATHENA_WORKGROUP=version3           # an Athena engine v3 workgroup
    export AURORA_ATHENA_OUTPUT=s3://<results-bucket>/aurora-probe/
    python tests/aws/aurora_fixtures.py create
    python tests/aws/aurora_fixtures.py drop
"""

import os
import sys
import time

REGION = os.environ.get("AURORA_REGION", os.environ.get("AWS_REGION", "us-east-1"))
DB = os.environ.get("AURORA_FIXTURE_DB", "aurora_probe_db")
LOCATION = os.environ.get("AURORA_FIXTURE_LOCATION", "").rstrip("/")
WORKGROUP = os.environ.get("AURORA_ATHENA_WORKGROUP", "primary")
OUTPUT = os.environ.get("AURORA_ATHENA_OUTPUT", "")

FIXTURES = ("posdel", "hidpart", "partevo")


def _athena():
    import boto3
    return boto3.client("athena", region_name=REGION)


def _run(sql: str) -> tuple:
    """Run one Athena statement, wait, return (ok, state_or_reason)."""
    kwargs = {"QueryString": sql, "WorkGroup": WORKGROUP}
    if OUTPUT:
        kwargs["ResultConfiguration"] = {"OutputLocation": OUTPUT}
    qid = _athena().start_query_execution(**kwargs)["QueryExecutionId"]
    for _ in range(80):
        time.sleep(3)
        q = _athena().get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        st = q["Status"]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            reason = q["Status"].get("StateChangeReason", "")
            return st == "SUCCEEDED", (reason or st)
    return False, "timeout"


def create() -> int:
    if not LOCATION:
        print("AURORA_FIXTURE_LOCATION is required", file=sys.stderr)
        return 2
    steps = [
        ("database", f"CREATE DATABASE IF NOT EXISTS {DB}"),

        # posdel: V2 Iceberg, 5 rows, delete 2 -> 3 survive (positional deletes).
        ("posdel.create", f"CREATE TABLE {DB}.posdel (id int, name string) "
         f"LOCATION '{LOCATION}/posdel' "
         "TBLPROPERTIES ('table_type'='ICEBERG','format'='parquet')"),
        ("posdel.insert", f"INSERT INTO {DB}.posdel VALUES "
         "(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')"),
        ("posdel.delete", f"DELETE FROM {DB}.posdel WHERE id IN (2,4)"),

        # hidpart: hidden partitioning day(ts), bucket(4, cat); 3 rows, 2 on Jan 1.
        ("hidpart.create", f"CREATE TABLE {DB}.hidpart (id int, ts timestamp, cat string) "
         "PARTITIONED BY (day(ts), bucket(4, cat)) "
         f"LOCATION '{LOCATION}/hidpart' "
         "TBLPROPERTIES ('table_type'='ICEBERG','format'='parquet')"),
        ("hidpart.insert", f"INSERT INTO {DB}.hidpart VALUES "
         "(1, timestamp '2026-01-01 10:00', 'a'),"
         "(2, timestamp '2026-01-02 10:00', 'b'),"
         "(3, timestamp '2026-01-01 20:00', 'c')"),

        # partevo: a partitioned table carrying rows across the spec.
        ("partevo.create", f"CREATE TABLE {DB}.partevo (id int, ts timestamp) "
         "PARTITIONED BY (day(ts)) "
         f"LOCATION '{LOCATION}/partevo' "
         "TBLPROPERTIES ('table_type'='ICEBERG','format'='parquet')"),
        ("partevo.insert", f"INSERT INTO {DB}.partevo VALUES "
         "(1, timestamp '2026-01-01 10:00'),(2, timestamp '2026-02-01 10:00')"),
    ]
    for label, sql in steps:
        ok, reason = _run(sql)
        print(f"{'ok ' if ok else 'ERR'} {label}: {reason}")
        if not ok and ".create" in label:
            # a create failing is fatal for that fixture's downstream steps
            return 1
    return 0


def drop() -> int:
    for t in FIXTURES:
        ok, reason = _run(f"DROP TABLE IF EXISTS {DB}.{t}")
        print(f"{'ok ' if ok else 'ERR'} drop {t}: {reason}")
    return 0


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "create"
    sys.exit(create() if cmd == "create" else drop())
