"""Create, with Spark on EMR, the Iceberg tables Redshift cannot create itself.

Redshift gained format-version 3 support on 2026-08-31, so it now creates most v3
tables itself. What it still has no DDL for is branches, tags, equality deletes,
and the v3 types it documents as unsupported (variant, geometry, timestamp_ns,
unknown and the other complex types). On its own a refusal only tells us Redshift
cannot *write* those things, which is not the same question the matrix asks. A
cell should distinguish

    "this engine cannot produce the feature"     -> write gap
    "this engine cannot even read the feature"   -> no support at all

so the features Redshift refuses to create are created here by Spark instead, and
the Redshift suite then tries to read and write them. A table it can read but not
create is partial support, not absent support.

Runs as an EMR Serverless spark-submit entry point, driven by
run_redshift_fixtures.py. It writes a manifest next to the tables describing what
it actually managed to create, because that is release-dependent: the fixtures a
given Iceberg build refuses are exactly the ones the Redshift side must not claim
to have tested.

Arguments (all passed as "--name value" by the driver):
    --namespace <namespace/glue database to create the tables in>
    --manifest-uri s3://bucket/key.json   where to write the manifest
    --mode s3buckets|s3tables
"""

import argparse
import json
import sys
import traceback


# Each fixture is (name, format_version, what it demonstrates, builder).
# A builder gets (spark, fqn) and raises if the runtime cannot express it; the
# failure is recorded in the manifest rather than aborting the job, so one
# unsupported fixture does not cost us the others.

def _v3_basic(spark, fqn):
    """A plain v3 table. The single most important fixture.

    If Redshift can read this, every v3 cell becomes a write gap rather than a
    total absence, which is a different matrix answer.
    """
    spark.sql(f"""CREATE TABLE {fqn} (id BIGINT, name STRING)
                  USING iceberg TBLPROPERTIES ('format-version'='3')""")
    spark.sql(f"INSERT INTO {fqn} VALUES (1,'alpha'),(2,'beta'),(3,'gamma')")


def _v3_deletion_vectors(spark, fqn):
    """A v3 table carrying deletion vectors.

    v3 replaces position-delete files with deletion vectors, so a DELETE on a v3
    merge-on-read table is enough to produce them.
    """
    spark.sql(f"""CREATE TABLE {fqn} (id BIGINT, name STRING)
                  USING iceberg TBLPROPERTIES (
                      'format-version'='3',
                      'write.delete.mode'='merge-on-read')""")
    spark.sql(f"INSERT INTO {fqn} VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
    spark.sql(f"DELETE FROM {fqn} WHERE id = 2")


def _v3_variant(spark, fqn):
    """A v3 table with a VARIANT column."""
    spark.sql(f"""CREATE TABLE {fqn} (id BIGINT, payload VARIANT)
                  USING iceberg TBLPROPERTIES ('format-version'='3')""")
    spark.sql(f"""INSERT INTO {fqn}
                  SELECT 1, parse_json('{{"a":1,"b":"two"}}')""")


def _v3_geometry(spark, fqn):
    """A v3 table with a GEOMETRY column."""
    spark.sql(f"""CREATE TABLE {fqn} (id BIGINT, shape GEOMETRY)
                  USING iceberg TBLPROPERTIES ('format-version'='3')""")
    spark.sql(f"INSERT INTO {fqn} SELECT 1, ST_Point(1.0, 2.0)")


def _v3_nanosecond(spark, fqn):
    """A v3 table with a nanosecond-precision timestamp."""
    spark.sql(f"""CREATE TABLE {fqn} (id BIGINT, ts TIMESTAMP_NS)
                  USING iceberg TBLPROPERTIES ('format-version'='3')""")
    spark.sql(f"INSERT INTO {fqn} VALUES (1, TIMESTAMP_NS '2026-01-01 00:00:00.123456789')")


def _v2_branch_tag(spark, fqn):
    """A v2 table with a branch and a tag.

    Redshift has no branch or tag DDL. What matters for the cell is whether it can
    still read the table, and whether it can address a branch at all.
    """
    spark.sql(f"CREATE TABLE {fqn} (id BIGINT, name STRING) USING iceberg")
    spark.sql(f"INSERT INTO {fqn} VALUES (1,'main-one'),(2,'main-two')")
    spark.sql(f"ALTER TABLE {fqn} CREATE BRANCH audit_branch")
    spark.sql(f"ALTER TABLE {fqn} CREATE TAG audit_tag")
    # Diverge the branch so reading it gives a different answer from main, which
    # is the only way to prove a branch read really happened.
    spark.sql(f"INSERT INTO {fqn}.branch_audit_branch VALUES (3,'branch-only')")


def _v2_equality_deletes(spark, fqn):
    """A v2 table carrying equality deletes.

    Spark writes position deletes, not equality deletes, so this is expected to
    be unavailable here; it is attempted anyway rather than assumed, and the
    manifest records the outcome so the Redshift side reports "not tested"
    instead of inventing a result.
    """
    spark.sql(f"""CREATE TABLE {fqn} (id BIGINT, name STRING)
                  USING iceberg TBLPROPERTIES (
                      'format-version'='2',
                      'write.delete.mode'='merge-on-read',
                      'write.update.mode'='merge-on-read',
                      'write.merge.mode'='merge-on-read',
                      'write.delete.granularity'='file')""")
    spark.sql(f"INSERT INTO {fqn} VALUES (1,'a'),(2,'b'),(3,'c')")
    spark.sql(f"DELETE FROM {fqn} WHERE id = 2")
    # Confirm a delete file exists at all; whether it is an equality delete is
    # read off the metadata by the checker below.
    spark.sql(f"SELECT * FROM {fqn}.delete_files").collect()


FIXTURES = [
    ("fx_v3_basic", 3, "plain v3 table", _v3_basic),
    ("fx_v3_dv", 3, "v3 deletion vectors", _v3_deletion_vectors),
    ("fx_v3_variant", 3, "v3 VARIANT column", _v3_variant),
    ("fx_v3_geometry", 3, "v3 GEOMETRY column", _v3_geometry),
    ("fx_v3_ts_ns", 3, "v3 nanosecond timestamp", _v3_nanosecond),
    ("fx_v2_branch", 2, "v2 branch and tag", _v2_branch_tag),
    ("fx_v2_eqdel", 2, "v2 equality deletes", _v2_equality_deletes),
]


def describe(spark, fqn: str) -> dict:
    """Read back what Iceberg actually stored, rather than what was asked for.

    A CREATE that succeeds does not prove the property took effect -- Redshift
    itself silently discards PARTITIONED BY on S3 Tables -- so the fixture's real
    format version and delete-file content are read from the metadata and shipped
    in the manifest. That way the Redshift side can say "read a v3 table" only
    when the table on disk is genuinely v3.
    """
    facts = {}
    try:
        rows = spark.sql(f"SELECT * FROM {fqn}.metadata_log_entries").collect()
        facts["snapshots"] = len(rows)
    except Exception:  # noqa: BLE001
        pass
    try:
        props = {r["key"]: r["value"]
                 for r in spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()}
        facts["format_version"] = props.get("format-version", "?")
    except Exception:  # noqa: BLE001
        facts["format_version"] = "?"
    try:
        files = spark.sql(
            f"SELECT content, count(*) AS n FROM {fqn}.delete_files GROUP BY content"
        ).collect()
        # Iceberg content codes: 1 = position deletes, 2 = equality deletes.
        facts["delete_file_content"] = {str(r["content"]): r["n"] for r in files}
    except Exception:  # noqa: BLE001
        facts["delete_file_content"] = {}
    try:
        refs = spark.sql(f"SELECT name, type FROM {fqn}.refs").collect()
        facts["refs"] = {r["name"]: r["type"] for r in refs}
    except Exception:  # noqa: BLE001
        facts["refs"] = {}
    try:
        facts["row_count"] = spark.sql(f"SELECT count(*) AS n FROM {fqn}").collect()[0]["n"]
    except Exception:  # noqa: BLE001
        pass
    return facts


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--namespace", required=True)
    p.add_argument("--manifest-uri", required=True)
    p.add_argument("--mode", required=True, choices=["s3buckets", "s3tables"])
    args, extra = p.parse_known_args()
    if extra:
        print(f"[fixtures] ignoring: {extra}")

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("redshift-fixtures").getOrCreate()

    print(f"[fixtures] creating namespace {args.namespace}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.namespace}")

    try:
        version = spark.sql("SELECT 1").sparkSession.version
    except Exception:  # noqa: BLE001
        version = "?"

    manifest = {
        "mode": args.mode,
        "namespace": args.namespace,
        "spark_version": version,
        "fixtures": {},
    }

    for name, fmt, what, build in FIXTURES:
        fqn = f"{args.namespace}.{name}"
        entry = {"format_version_requested": fmt, "describes": what}
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fqn} PURGE")
        except Exception:  # noqa: BLE001
            pass
        try:
            build(spark, fqn)
            entry["created"] = True
            entry["stored"] = describe(spark, fqn)
            print(f"[fixtures] OK   {name}: {entry['stored']}")
        except Exception as e:  # noqa: BLE001 - a refused fixture is a result
            entry["created"] = False
            entry["error"] = f"{type(e).__name__}: {e}"[:400]
            print(f"[fixtures] FAIL {name}: {entry['error']}")
            print(traceback.format_exc()[:1500])
        manifest["fixtures"][name] = entry

    created = [n for n, e in manifest["fixtures"].items() if e.get("created")]
    print(f"[fixtures] created {len(created)}/{len(FIXTURES)}: {', '.join(created)}")

    import boto3
    from urllib.parse import urlparse

    parsed = urlparse(args.manifest_uri)
    boto3.client("s3").put_object(
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/"),
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json",
    )
    print(f"[fixtures] manifest written to {args.manifest_uri}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
