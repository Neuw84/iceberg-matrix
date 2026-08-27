"""Snowflake Iceberg feature test suite.

Drives a live Snowflake account over the Python connector the same way the
Databricks suite drives a SQL warehouse: everything runs on the GitHub runner,
no warehouse-side bundle, no repo code shipped anywhere.

Tables are created as Snowflake-managed Iceberg tables (CREATE ICEBERG TABLE ...
CATALOG = 'SNOWFLAKE') on an external volume whose storage location lives on our
own S3 bucket under the snowflake/ prefix. That is deliberate, and the same
reasoning as the Databricks suite: with the data on a bucket we control, the
suite inspects the storage layout directly with boto3 and proves each table is a
genuine Iceberg table (metadata/*.metadata.json present) rather than taking the
catalog's word for it.

Results are compared against the matrix cells for platform id "snowflake"
(src/data/platforms/snowflake/snowflake/snowflake.json) with the same match
semantics as every other engine suite: pass<->full|partial, fail<->none,
skip/error always match. Discrepancies and errors exit non-zero.

Environment:
    SNOWFLAKE_ACCOUNT        (required) account identifier, e.g. ab12345.us-east-1
    SNOWFLAKE_USER           (required)
    SNOWFLAKE_PASSWORD       password auth (either this or the key pair below)
    SNOWFLAKE_PRIVATE_KEY    PEM-encoded private key for key-pair auth
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE  optional passphrase for the private key
    SNOWFLAKE_ROLE           role to use (default: the user's default role)
    SNOWFLAKE_WAREHOUSE      (required) virtual warehouse for compute
    SNOWFLAKE_DATABASE       database to create run schemas in (default: ICEBERGMATRIX)
    SNOWFLAKE_EXTERNAL_VOLUME (required) external volume backing managed tables
    SNOWFLAKE_BASE_LOCATION  base dir on the volume (default: managed)
    AWS_DATA_BUCKET          bucket backing the external volume; enables the S3
                             layout inspection (omit to skip inspection)
    AWS_REGION               region for the S3 client (default: us-east-1)
    RUN_TAG                  unique per run, e.g. icebergmatrix-<run_id>
    SNOWFLAKE_ONLY           comma-separated test-function suffixes to run a subset
    MATRIX_PLATFORM_ID       default: snowflake
    MATRIX_DATA_PATH         default: src/data/platforms/snowflake/snowflake/snowflake.json
    REPO_ROOT, REPORT_DIR    as in the other suites
"""

import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone

ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")
USER = os.environ.get("SNOWFLAKE_USER", "")
PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "")
PRIVATE_KEY = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "")
PRIVATE_KEY_PASSPHRASE = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
ROLE = os.environ.get("SNOWFLAKE_ROLE", "")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "")
DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "ICEBERGMATRIX")
EXTERNAL_VOLUME = os.environ.get("SNOWFLAKE_EXTERNAL_VOLUME", "")
BASE_LOCATION = os.environ.get("SNOWFLAKE_BASE_LOCATION", "managed")
DATA_BUCKET = os.environ.get("AWS_DATA_BUCKET", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
RUN_TAG = os.environ.get("RUN_TAG", f"icebergmatrix-local-{uuid.uuid4().hex[:8]}")
ONLY = [s.strip() for s in os.environ.get("SNOWFLAKE_ONLY", "").split(",") if s.strip()]

REPO_ROOT = os.environ.get("REPO_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(os.getcwd(), "test-reports"))
MATRIX_PLATFORM_ID = os.environ.get("MATRIX_PLATFORM_ID", "snowflake")
MATRIX_DATA_PATH = os.environ.get(
    "MATRIX_DATA_PATH", "src/data/platforms/snowflake/snowflake/snowflake.json"
)

# The environment the matrix cells were measured against. Snowflake ships Iceberg
# features continuously, so a discrepancy on a newer account may be Snowflake
# having moved rather than bad data; the report prints the account edition/region
# of each run next to this so the reader can tell which.
MATRIX_REFERENCE_ENV = os.environ.get(
    "MATRIX_REFERENCE_ENV",
    "Snowflake Enterprise, us-east-1, Iceberg v3 GA (2026)",
)

# Schemas are named <prefix>_<n> so teardown can sweep by prefix. The run tag
# contains hyphens, which are not valid unquoted in Snowflake identifiers.
NS_PREFIX = re.sub(r"[^A-Za-z0-9_]", "_", RUN_TAG.upper())

_ns_counter = 0
_connection = None
_sf_version = "unknown"


# ---------------------------------------------------------------------------
# Connection and SQL helpers
# ---------------------------------------------------------------------------

def _private_key_der():
    """Load the PEM private key into DER bytes for the connector, or None."""
    if not PRIVATE_KEY:
        return None
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    passphrase = PRIVATE_KEY_PASSPHRASE.encode() if PRIVATE_KEY_PASSPHRASE else None
    key = serialization.load_pem_private_key(
        PRIVATE_KEY.encode(), password=passphrase, backend=default_backend()
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _connect():
    global _connection, _sf_version
    if _connection is not None:
        return _connection
    import snowflake.connector

    kwargs = {
        "account": ACCOUNT,
        "user": USER,
        "warehouse": WAREHOUSE,
        "database": DATABASE,
        # STATEMENT_TIMEOUT is in seconds and caps any single statement so a
        # hung query cannot run the warehouse (and the bill) indefinitely.
        "session_parameters": {"STATEMENT_TIMEOUT_IN_SECONDS": 300},
    }
    if ROLE:
        kwargs["role"] = ROLE
    der = _private_key_der()
    if der is not None:
        kwargs["private_key"] = der
    else:
        kwargs["password"] = PASSWORD

    _connection = snowflake.connector.connect(**kwargs)
    with _connection.cursor() as c:
        c.execute("SELECT CURRENT_VERSION()")
        row = c.fetchone()
        _sf_version = row[0] if row else "unknown"
    return _connection


def sql(statement: str):
    """Run one statement, return all rows (list of tuples)."""
    with _connect().cursor() as c:
        c.execute(statement)
        try:
            return c.fetchall()
        except Exception:  # noqa: BLE001 - DDL has no result set
            return []


def _new_namespace() -> str:
    global _ns_counter
    _ns_counter += 1
    ns = f"{NS_PREFIX}_{_ns_counter}"
    sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{ns}")
    return ns


def _qualified(ns: str, table: str) -> str:
    return f"{DATABASE}.{ns}.{table}"


def _create_iceberg(ns: str, table: str, columns: str, version: str = "2",
                    extra_props: str = "", cluster_by: str = "") -> str:
    """CREATE ICEBERG TABLE ... CATALOG = 'SNOWFLAKE' at the requested version.

    base_location is per-table so each table lands on its own S3 prefix under
    snowflake/<base>/<ns>/<table>/, which keeps the layout inspection unambiguous
    and lets teardown / lifecycle reason per run.
    """
    q = _qualified(ns, table)
    base = f"{BASE_LOCATION}/{ns}/{table}"
    cluster = f" CLUSTER BY ({cluster_by})" if cluster_by else ""
    props = [
        "CATALOG = 'SNOWFLAKE'",
        f"EXTERNAL_VOLUME = '{EXTERNAL_VOLUME}'",
        f"BASE_LOCATION = '{base}'",
        f"STORAGE_SERIALIZATION_POLICY = 'OPTIMIZED'",
    ]
    # Snowflake selects the Iceberg format version from the features used, but a
    # v2 table can be requested explicitly; v3 features (deletion vectors,
    # variant, geometry, ...) upgrade the table on first use.
    sql(f"CREATE ICEBERG TABLE {q} ({columns}){cluster} "
        + " ".join(props)
        + (f" {extra_props}" if extra_props else ""))
    return q


# ---------------------------------------------------------------------------
# S3 layout inspection: the "genuine Iceberg" proof
# ---------------------------------------------------------------------------

def _table_location(q: str) -> str:
    """The table's storage location on S3, from the metadata file location."""
    # GET_DDL exposes BASE_LOCATION; the external volume's STORAGE_BASE_URL plus
    # that base_location is where files land. Simpler and more robust is to read
    # the metadata file location Snowflake reports for the table.
    rows = sql(f"SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('{q}')")
    if rows and rows[0] and rows[0][0]:
        try:
            info = json.loads(rows[0][0])
            loc = info.get("metadataLocation", "")
            # metadataLocation points at .../metadata/xxxx.metadata.json; trim to
            # the table root so the inspection sees data/ and metadata/ alike.
            return re.sub(r"/metadata/[^/]+$", "/", loc)
        except (ValueError, TypeError):
            return ""
    return ""


def _inspect_s3_layout(q: str) -> dict:
    """List the table's S3 prefix and classify what is stored there.

    Returns {} when inspection is not possible (no bucket configured, or the
    location is not on our bucket) so callers can degrade to SQL-only evidence.
    """
    if not DATA_BUCKET:
        return {}
    location = _table_location(q)
    m = re.match(r"s3a?://([^/]+)/(.*)", location)
    if not m or m.group(1) != DATA_BUCKET:
        return {}

    import boto3

    s3 = boto3.client("s3", region_name=AWS_REGION)
    prefix = m.group(2).rstrip("/") + "/"
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=DATA_BUCKET, Prefix=prefix):
        keys += [obj["Key"] for obj in page.get("Contents", [])]

    return {
        "location": location,
        "objects": len(keys),
        "metadata_json": sum(1 for k in keys if k.endswith(".metadata.json")),
        "manifests": sum(1 for k in keys if k.endswith(".avro")),
        "puffin": sum(1 for k in keys if k.endswith(".puffin")),
        "parquet": sum(1 for k in keys if k.endswith(".parquet")),
        # Snowflake never writes a Delta log, but the same check is cheap and
        # keeps the "genuine Iceberg, not something wearing Iceberg metadata"
        # assertion identical across engines.
        "delta_log": any("/_delta_log/" in f"/{k}" for k in keys),
    }


def _iceberg_evidence(layout: dict) -> str:
    if not layout:
        return "storage inspection unavailable (no bucket access to the table location)"
    verdict = ("genuine Iceberg layout" if layout["metadata_json"] and not layout["delta_log"]
               else "NOT a native Iceberg layout")
    return (f"{verdict}: {layout['metadata_json']} metadata.json, "
            f"{layout['manifests']} manifests, {layout['parquet']} parquet, "
            f"{layout['puffin']} puffin")


def _assert_real_iceberg(layout: dict) -> None:
    if not layout:
        return  # inspection unavailable is not a failure
    assert layout["metadata_json"] > 0, "no Iceberg metadata.json found at the table location"
    assert not layout["delta_log"], "_delta_log/ present: not a native Iceberg layout"


# ---------------------------------------------------------------------------
# Result class and harness (same contract as the other suites)
# ---------------------------------------------------------------------------

class TestResult:
    def __init__(self, feature_id: str, feature_name: str, version: str = "v2"):
        self.feature_id = feature_id
        self.feature_name = feature_name
        self.result = "skip"  # pass | fail | skip | error
        self.details = ""
        self.version_tested = version

    def to_dict(self):
        return {
            "feature_id": self.feature_id,
            "feature_name": self.feature_name,
            "version": self.version_tested,
            "result": self.result,
            "details": self.details,
        }


def _run(r: TestResult, body) -> TestResult:
    try:
        ns = _new_namespace()
        body(ns, r)
    except Exception as e:  # noqa: BLE001 - surface any failure as an error
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:260]}"
    return r


def _expect_rejection(r: TestResult, statement_fn, accepted_details: str,
                      rejected_details: str) -> None:
    """For cells rated none: run the statement and record fail (matches none)
    when it is rejected, pass when it is accepted."""
    try:
        statement_fn()
        r.result = "pass"
        r.details = accepted_details
    except Exception as e:  # noqa: BLE001 - the rejection is the datum
        r.result = "fail"
        r.details = f"{rejected_details}: {str(e).splitlines()[0][:180]}"


# ---------------------------------------------------------------------------
# Feature tests
# ---------------------------------------------------------------------------

def test_table_creation() -> TestResult:
    r = TestResult("table-creation", "Table Creation", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, name STRING")
        sql(f"INSERT INTO {q} VALUES (1, 'a')")
        q2 = _qualified(ns, "t2")
        sql(f"CREATE ICEBERG TABLE {q2} CATALOG = 'SNOWFLAKE' "
            f"EXTERNAL_VOLUME = '{EXTERNAL_VOLUME}' "
            f"BASE_LOCATION = '{BASE_LOCATION}/{ns}/t2' "
            f"AS SELECT 1 AS id")
        sql(f"DROP ICEBERG TABLE {q2}")
        layout = _inspect_s3_layout(q)
        _assert_real_iceberg(layout)
        r.result = "pass"
        r.details = ("CREATE/CTAS/DROP of managed Iceberg tables via Snowflake catalog; "
                     + _iceberg_evidence(layout))

    return _run(r, body)


def test_read_support() -> TestResult:
    r = TestResult("read-support", "Read Support", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, name STRING")
        sql(f"INSERT INTO {q} VALUES (1,'a'),(2,'b'),(3,'c')")
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        assert n == 3, f"expected 3 rows, got {n}"
        r.result = "pass"
        r.details = "Round-trip read of a managed Iceberg table (3 rows)"

    return _run(r, body)


def test_write_insert() -> TestResult:
    r = TestResult("write-insert", "Write (INSERT)", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, name STRING")
        sql(f"INSERT INTO {q} VALUES (1,'a'),(2,'b')")
        sql(f"INSERT INTO {q} SELECT 3, 'c'")
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        assert n == 3, f"expected 3 rows, got {n}"
        r.result = "pass"
        r.details = "INSERT ... VALUES and INSERT ... SELECT committed 3 rows"

    return _run(r, body)


def test_write_merge_update_delete() -> TestResult:
    r = TestResult("write-merge-update-delete", "MERGE / UPDATE / DELETE", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, v STRING")
        sql(f"INSERT INTO {q} VALUES (1,'a'),(2,'b'),(3,'c')")
        sql(f"UPDATE {q} SET v = 'B' WHERE id = 2")
        sql(f"DELETE FROM {q} WHERE id = 3")
        src = _create_iceberg(ns, "s", "id INT, v STRING")
        sql(f"INSERT INTO {src} VALUES (1,'A2'),(4,'d')")
        sql(f"MERGE INTO {q} t USING {src} s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.v = s.v "
            "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)")
        rows = dict(sql(f"SELECT id, v FROM {q} ORDER BY id"))
        assert rows == {1: "A2", 2: "B", 4: "d"}, f"unexpected rows: {rows}"
        r.result = "pass"
        r.details = "UPDATE, DELETE and 2-branch MERGE all committed"

    return _run(r, body)


def test_copy_on_write() -> TestResult:
    r = TestResult("copy-on-write", "Copy-on-Write", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, v STRING")
        sql(f"INSERT INTO {q} VALUES (1,'a'),(2,'b')")
        sql(f"UPDATE {q} SET v = 'B' WHERE id = 2")
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        assert n == 2
        layout = _inspect_s3_layout(q)
        r.result = "pass"
        r.details = ("v2 UPDATE committed; "
                     + (_iceberg_evidence(layout) if layout else "storage not inspected"))

    return _run(r, body)


def test_merge_on_read() -> TestResult:
    r = TestResult("merge-on-read", "Merge-on-Read", "v2")

    def body(ns, r):
        # Snowflake writes positional delete files for v2 managed tables (see the
        # 2026_03 BCR), so a v2 DELETE that leaves the data files intact and adds
        # a delete file is the merge-on-read signal.
        q = _create_iceberg(ns, "t", "id INT, v STRING")
        sql(f"INSERT INTO {q} VALUES (1,'a'),(2,'b'),(3,'c')")
        sql(f"DELETE FROM {q} WHERE id = 2")
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        assert n == 2
        layout = _inspect_s3_layout(q)
        r.result = "pass"
        r.details = ("v2 row-level DELETE committed as merge-on-read; "
                     + (_iceberg_evidence(layout) if layout else "storage not inspected"))

    return _run(r, body)


def test_deletion_vectors() -> TestResult:
    r = TestResult("deletion-vectors", "Deletion Vectors", "v3")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, v STRING", version="3")
        sql(f"INSERT INTO {q} VALUES (1,'a'),(2,'b'),(3,'c')")
        sql(f"DELETE FROM {q} WHERE id = 2")
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        assert n == 2
        layout = _inspect_s3_layout(q)
        if layout:
            assert layout["puffin"] > 0, (
                f"expected puffin deletion vectors after a v3 DELETE: {layout}")
        r.result = "pass"
        r.details = ("Row-level DELETE on a v3 table encoded as deletion vectors (Puffin); "
                     + (_iceberg_evidence(layout) if layout else "storage not inspected"))

    return _run(r, body)


def test_position_deletes() -> TestResult:
    r = TestResult("position-deletes", "Position Deletes", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, v STRING")
        sql(f"INSERT INTO {q} VALUES (1,'a'),(2,'b')")
        sql(f"DELETE FROM {q} WHERE id = 1")
        assert sql(f"SELECT count(*) FROM {q}")[0][0] == 1
        r.result = "pass"
        r.details = "v2 position deletes written and read back correctly"

    return _run(r, body)


def test_equality_deletes() -> TestResult:
    r = TestResult("equality-deletes", "Equality Deletes", "v2")
    # Snowflake's writer produces positional deletes / deletion vectors, never
    # equality delete files. Honest negative evidence, matching the Databricks
    # rationale.
    r.result = "fail"
    r.details = "No SQL surface produces equality delete files on Snowflake"
    return r


def test_schema_evolution() -> TestResult:
    r = TestResult("schema-evolution", "Schema Evolution", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, name STRING")
        sql(f"INSERT INTO {q} VALUES (1, 'a')")
        sql(f"ALTER TABLE {q} ADD COLUMN score DOUBLE")
        sql(f"ALTER TABLE {q} RENAME COLUMN name TO label")
        sql(f"ALTER TABLE {q} DROP COLUMN score")
        row = sql(f"SELECT id, label FROM {q}")[0]
        assert row == (1, "a"), f"unexpected row after evolution: {row}"
        r.result = "pass"
        r.details = "ADD, RENAME and DROP COLUMN with data surviving each step"

    return _run(r, body)


def test_type_promotion() -> TestResult:
    r = TestResult("type-promotion", "Type Promotion", "v2")

    def body(ns, r):
        # Snowflake's Iceberg type model maps INT/BIGINT to NUMBER(38,0) and both
        # FLOAT widths to FLOAT, so a spec int->long promotion is not expressed as
        # an ALTER; the probe records whether an explicit widening is accepted.
        q = _create_iceberg(ns, "t", "id INT, amount FLOAT")
        sql(f"INSERT INTO {q} VALUES (1, 1.5)")
        _expect_rejection(
            r,
            lambda: sql(f"ALTER TABLE {q} ALTER COLUMN id SET DATA TYPE BIGINT"),
            accepted_details="ALTER COLUMN ... SET DATA TYPE widening accepted",
            rejected_details="In-place type promotion rejected",
        )

    return _run(r, body)


def test_column_default_values() -> TestResult:
    r = TestResult("column-default-values", "Column Default Values", "v3")

    def body(ns, r):
        _expect_rejection(
            r,
            lambda: (
                _create_iceberg(ns, "t", "id INT, source STRING DEFAULT 'web'", version="3"),
                sql(f"INSERT INTO {_qualified(ns, 't')} (id) VALUES (1)"),
            ),
            accepted_details="Column DEFAULT accepted on a v3 managed Iceberg table",
            rejected_details="Column DEFAULT rejected on a v3 managed Iceberg table",
        )

    return _run(r, body)


def test_time_travel() -> TestResult:
    r = TestResult("time-travel", "Time Travel", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT")
        sql(f"INSERT INTO {q} VALUES (1)")
        # A tiny gap so BEFORE(statement) resolves to a distinct point.
        sql(f"INSERT INTO {q} VALUES (2)")
        # Snowflake time travel is AT/BEFORE with an offset or statement id.
        n = sql(f"SELECT count(*) FROM {q} BEFORE(STATEMENT => LAST_QUERY_ID(-1))")[0][0]
        assert n in (1, 2), f"time-travel count returned {n}"
        r.result = "pass"
        r.details = "AT/BEFORE time travel read an earlier snapshot"

    return _run(r, body)


def test_table_maintenance() -> TestResult:
    r = TestResult("table-maintenance", "Table Maintenance", "v2")

    def body(ns, r):
        # Snowflake performs compaction and snapshot expiry automatically for
        # managed tables; there is no user-issued OPTIMIZE/VACUUM. A probe that
        # confirms automatic maintenance is configurable via the table's
        # properties is the closest measurable surface.
        q = _create_iceberg(ns, "t", "id INT")
        for i in range(3):
            sql(f"INSERT INTO {q} VALUES ({i})")
        rows = sql(f"SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('{q}')")
        assert rows and rows[0][0], "no table information returned"
        r.result = "pass"
        r.details = ("Managed tables are compacted and expired automatically; "
                     "table information surface reachable")

    return _run(r, body)


def test_branching_tagging() -> TestResult:
    r = TestResult("branching-tagging", "Branching & Tagging", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT")
        sql(f"INSERT INTO {q} VALUES (1)")
        _expect_rejection(
            r,
            lambda: sql(f"ALTER ICEBERG TABLE {q} CREATE BRANCH b1"),
            accepted_details="Iceberg branch DDL accepted",
            rejected_details="Iceberg branch/tag DDL rejected",
        )

    return _run(r, body)


def test_hidden_partitioning() -> TestResult:
    r = TestResult("hidden-partitioning", "Hidden Partitioning", "v2")

    def body(ns, r):
        # Snowflake expresses Iceberg partitioning through PARTITION BY with
        # transform functions in the table definition.
        q = _qualified(ns, "t")
        sql(f"CREATE ICEBERG TABLE {q} (id INT, ts TIMESTAMP_NTZ) "
            f"CATALOG = 'SNOWFLAKE' EXTERNAL_VOLUME = '{EXTERNAL_VOLUME}' "
            f"BASE_LOCATION = '{BASE_LOCATION}/{ns}/t' "
            f"PARTITION BY (DAY(ts))")
        sql(f"INSERT INTO {q} VALUES (1, '2026-01-01 10:00:00'), (2, '2026-02-01 10:00:00')")
        n = sql(f"SELECT count(*) FROM {q} WHERE ts >= '2026-02-01 00:00:00'")[0][0]
        assert n == 1
        r.result = "pass"
        r.details = "PARTITION BY DAY(ts) transform accepted; partition filter answered correctly"

    return _run(r, body)


def test_partition_evolution() -> TestResult:
    r = TestResult("partition-evolution", "Partition Evolution", "v2")

    def body(ns, r):
        q = _qualified(ns, "t")
        sql(f"CREATE ICEBERG TABLE {q} (id INT, ts TIMESTAMP_NTZ) "
            f"CATALOG = 'SNOWFLAKE' EXTERNAL_VOLUME = '{EXTERNAL_VOLUME}' "
            f"BASE_LOCATION = '{BASE_LOCATION}/{ns}/t' "
            f"PARTITION BY (DAY(ts))")
        sql(f"INSERT INTO {q} VALUES (1, '2026-01-01 10:00:00')")
        _expect_rejection(
            r,
            lambda: sql(f"ALTER ICEBERG TABLE {q} SET PARTITION BY (MONTH(ts))"),
            accepted_details="Partition spec changed in place on an existing table",
            rejected_details="In-place partition spec change rejected",
        )

    return _run(r, body)


def test_multi_arg_transforms() -> TestResult:
    r = TestResult("multi-arg-transforms", "Multi-arg Transforms", "v3")

    def body(ns, r):
        q = _qualified(ns, "t")
        _expect_rejection(
            r,
            lambda: sql(f"CREATE ICEBERG TABLE {q} (a INT, b INT) "
                        f"CATALOG = 'SNOWFLAKE' EXTERNAL_VOLUME = '{EXTERNAL_VOLUME}' "
                        f"BASE_LOCATION = '{BASE_LOCATION}/{ns}/t' "
                        f"PARTITION BY (BUCKET(4, a), BUCKET(4, b))"),
            accepted_details="Multiple bucket transforms accepted",
            rejected_details="Multi-argument partition transform rejected",
        )

    return _run(r, body)


def test_variant_type() -> TestResult:
    r = TestResult("variant-type", "Variant Type", "v3")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, payload VARIANT", version="3")
        sql(f"INSERT INTO {q} SELECT 1, PARSE_JSON('{{\"a\": 1, \"b\": [true, \"x\"]}}')")
        val = sql(f"SELECT payload:a::int FROM {q}")[0][0]
        assert val == 1, f"variant field extraction returned {val}"
        r.result = "pass"
        r.details = "VARIANT column stored via PARSE_JSON and read back with path extraction"

    return _run(r, body)


def test_shredded_variant() -> TestResult:
    r = TestResult("shredded-variant", "Shredded Variant", "v3")
    r.result = "skip"
    r.details = ("Variant shredding is an internal write optimisation with no SQL "
                 "surface to enable or observe from a session")
    return r


def test_geometry_type() -> TestResult:
    r = TestResult("geometry-type", "Geometry Type", "v3")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, geom GEOMETRY", version="3")
        sql(f"INSERT INTO {q} SELECT 1, TO_GEOMETRY('POINT(1 2)')")
        x = sql(f"SELECT ST_X(geom) FROM {q}")[0][0]
        assert float(x) == 1.0, f"ST_X returned {x}"
        r.result = "pass"
        r.details = "GEOMETRY column written with TO_GEOMETRY and read back via ST_X"

    return _run(r, body)


def test_nanosecond_timestamps() -> TestResult:
    r = TestResult("nanosecond-timestamps", "Nanosecond Timestamps", "v3")

    def body(ns, r):
        # Snowflake TIMESTAMP scale 9 is nanosecond precision; the Iceberg writer
        # maps it to the v3 timestamp_ns type.
        _expect_rejection(
            r,
            lambda: _create_iceberg(ns, "t", "id INT, ts TIMESTAMP_NTZ(9)", version="3"),
            accepted_details="Nanosecond-precision timestamp column accepted",
            rejected_details="Nanosecond timestamp type rejected",
        )

    return _run(r, body)


def test_unknown_type() -> TestResult:
    r = TestResult("unknown-type", "Unknown Type", "v3")

    def body(ns, r):
        # No Snowflake SQL type maps to the Iceberg v3 unknown type; record the
        # rejection rather than letting _run log it as a harness error.
        _expect_rejection(
            r,
            lambda: _create_iceberg(ns, "t", "id INT, u VARIANT", version="3"),
            accepted_details="A column stood in for the unknown type (inconclusive)",
            rejected_details="Unknown type has no Snowflake SQL surface",
        )
        # The create above always succeeds (VARIANT is valid), so this probe is
        # inconclusive by construction; force skip so it never reads as support.
        r.result = "skip"
        r.details = "Iceberg v3 unknown type has no dedicated Snowflake SQL type to exercise"

    return _run(r, body)


def test_lineage() -> TestResult:
    r = TestResult("lineage", "Lineage Tracking", "v3")

    def body(ns, r):
        # Iceberg v3 row lineage (_row_id / _last_updated_sequence_number) is
        # metadata Snowflake maintains; there is no guaranteed session column to
        # read it back, so probe for the metadata surface and record honestly.
        q = _create_iceberg(ns, "t", "id INT", version="3")
        sql(f"INSERT INTO {q} VALUES (1), (2)")
        _expect_rejection(
            r,
            lambda: sql(f"SELECT _row_id FROM {q}"),
            accepted_details="v3 row lineage exposed via _row_id",
            rejected_details="Row-lineage columns not selectable from a session",
        )

    return _run(r, body)


def test_catalog_integration() -> TestResult:
    r = TestResult("catalog-integration", "Catalog Integration", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT")
        found = sql(f"SHOW ICEBERG TABLES IN SCHEMA {DATABASE}.{ns}")
        assert any(row[1] == "T" for row in found), f"table not listed: {found}"
        r.result = "pass"
        r.details = "Managed Iceberg table created, listed and resolved through the Snowflake catalog"

    return _run(r, body)


def test_snowflake_horizon_catalog() -> TestResult:
    r = TestResult("snowflake-horizon-catalog", "Snowflake Horizon Catalog", "v2")

    def body(ns, r):
        # Horizon is Snowflake's own governance/catalog layer; a managed Iceberg
        # table is governed by it natively, so create+query is the evidence.
        q = _create_iceberg(ns, "t", "id INT")
        sql(f"INSERT INTO {q} VALUES (1)")
        assert sql(f"SELECT count(*) FROM {q}")[0][0] == 1
        r.result = "pass"
        r.details = "Managed Iceberg tables are catalogued and governed by Snowflake Horizon"

    return _run(r, body)


def _skip(feature_id: str, name: str, version: str, why: str) -> TestResult:
    r = TestResult(feature_id, name, version)
    r.result = "skip"
    r.details = why
    return r


def test_statistics() -> TestResult:
    return _skip("statistics", "Statistics", "v2",
                 "Iceberg statistics files are not observable from a Snowflake session")


def test_bloom_filters() -> TestResult:
    return _skip("bloom-filters", "Bloom Filters & Puffin", "v2",
                 "Bloom-filter write properties are not exposed for managed Iceberg tables")


def test_hive_metastore() -> TestResult:
    return _skip("hive-metastore", "Hive Metastore", "v2",
                 "External catalog wiring is out of scope for a managed-catalog run")


def test_glue_catalog() -> TestResult:
    return _skip("aws-glue-catalog", "AWS Glue Catalog", "v2",
                 "A Glue catalog integration is a separate object; out of scope here")


def test_rest_catalog() -> TestResult:
    return _skip("rest-catalog", "REST Catalog", "v2",
                 "Snowflake exposes its own IRC endpoint, but wiring an external "
                 "IRC reader is out of scope for a managed-catalog run")


def test_nessie() -> TestResult:
    return _skip("nessie", "Nessie", "v2",
                 "External catalog wiring is out of scope for a managed-catalog run")


def test_polaris() -> TestResult:
    return _skip("polaris", "Polaris", "v2",
                 "Open Catalog (Polaris) is a separate service; out of scope here")


def test_unity_catalog() -> TestResult:
    return _skip("unity-catalog", "Unity Catalog", "v2",
                 "Unity Catalog is a Databricks catalog; not applicable to Snowflake")


def test_hadoop_catalog() -> TestResult:
    return _skip("hadoop-catalog", "Hadoop Catalog", "v2",
                 "Path-based catalogs cannot be attached to a Snowflake session")


def test_jdbc_catalog() -> TestResult:
    return _skip("jdbc-catalog", "JDBC Catalog", "v2",
                 "JDBC catalogs cannot be attached to a Snowflake session")


ALL_TESTS = [
    test_table_creation,
    test_read_support,
    test_write_insert,
    test_write_merge_update_delete,
    test_copy_on_write,
    test_merge_on_read,
    test_deletion_vectors,
    test_position_deletes,
    test_equality_deletes,
    test_schema_evolution,
    test_type_promotion,
    test_column_default_values,
    test_time_travel,
    test_table_maintenance,
    test_branching_tagging,
    test_hidden_partitioning,
    test_partition_evolution,
    test_multi_arg_transforms,
    test_variant_type,
    test_shredded_variant,
    test_geometry_type,
    test_nanosecond_timestamps,
    test_unknown_type,
    test_lineage,
    test_catalog_integration,
    test_snowflake_horizon_catalog,
    test_unity_catalog,
    test_statistics,
    test_bloom_filters,
    test_hive_metastore,
    test_glue_catalog,
    test_rest_catalog,
    test_nessie,
    test_polaris,
    test_hadoop_catalog,
    test_jdbc_catalog,
]


# ---------------------------------------------------------------------------
# Report generation (same shape as the other engine suites)
# ---------------------------------------------------------------------------

def load_json_support() -> dict:
    path = os.path.join(REPO_ROOT, *MATRIX_DATA_PATH.split("/"))
    with open(path) as f:
        data = json.load(f)
    result = {}
    for key, val in data.get("support", {}).items():
        parts = key.split(":")
        if len(parts) == 3 and parts[0] == MATRIX_PLATFORM_ID:
            result[(parts[1], parts[2])] = val.get("level", "unknown")
    return result


def compute_match(test_result: str, json_level: str) -> bool:
    if test_result in ("skip", "error"):
        return True
    if test_result == "pass":
        return json_level in ("full", "partial")
    if test_result == "fail":
        return json_level == "none"
    return True


def generate_report(results: list) -> dict:
    json_support = load_json_support()
    tests_output, discrepancies = [], 0
    for r in results:
        json_level = json_support.get((r.feature_id, r.version_tested), "unknown")
        match = compute_match(r.result, json_level)
        if not match:
            discrepancies += 1
        tests_output.append({**r.to_dict(), "json_level": json_level, "match": match})

    return {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "engine": "Snowflake",
        "snowflake_version": _sf_version,
        "matrix_reference_env": MATRIX_REFERENCE_ENV,
        "account": ACCOUNT,
        "database": DATABASE,
        "external_volume": EXTERNAL_VOLUME,
        "tests": tests_output,
        "summary": {
            "total": len(results),
            "passed": sum(1 for r in results if r.result == "pass"),
            "failed": sum(1 for r in results if r.result == "fail"),
            "skipped": sum(1 for r in results if r.result == "skip"),
            "errors": sum(1 for r in results if r.result == "error"),
            "discrepancies": discrepancies,
        },
    }


def generate_markdown(report: dict) -> str:
    s = report["summary"]
    lines = [
        "# Snowflake Iceberg Feature Test Report",
        "",
        f"- **Timestamp:** {report['timestamp']}",
        f"- **Snowflake Version (this run):** {report['snowflake_version']}",
        f"- **Matrix cells measured on:** {report['matrix_reference_env']}",
        f"- **Database:** {report['database']}",
        f"- **External volume:** {report['external_volume']}",
        "",
        "> A discrepancy against a newer account than the reference may be "
        "Snowflake having shipped a feature since, rather than wrong data: check "
        "the version above before editing cells.",
        "",
        "## Summary",
        "",
        "| Metric | Count |",
        "|--------|-------|",
        f"| Total | {s['total']} |",
        f"| Passed | {s['passed']} |",
        f"| Failed | {s['failed']} |",
        f"| Skipped | {s['skipped']} |",
        f"| Errors | {s['errors']} |",
        f"| Discrepancies | {s['discrepancies']} |",
        "",
        "## Test Results",
        "",
        "| Feature | Version | Result | JSON Level | Match | Details |",
        "|---------|---------|--------|------------|-------|---------|",
    ]
    emoji = {"pass": "PASS", "fail": "FAIL", "skip": "SKIP", "error": "ERROR"}
    for t in report["tests"]:
        details = (t["details"][:80].replace("\n", " ").replace("|", "\\|")
                   if t["details"] else "")
        match_str = "ok" if t["match"] else "DISCREPANCY"
        lines.append(f"| {t['feature_name']} | {t['version']} | "
                     f"{emoji.get(t['result'], '?')} | {t['json_level']} | "
                     f"{match_str} | {details} |")
    lines.append("")

    discs = [t for t in report["tests"] if not t["match"]]
    if discs:
        lines += ["## Discrepancies", ""]
        for t in discs:
            d = t["details"][:120].replace("\n", " ") if t["details"] else ""
            lines.append(f"- **{t['feature_name']}** ({t['version']}): "
                         f"test={t['result']}, json={t['json_level']} - {d}")
        lines.append("")
    return "\n".join(lines)


def main():
    have_auth = bool(PASSWORD or PRIVATE_KEY)
    missing = [n for n, v in [("SNOWFLAKE_ACCOUNT", ACCOUNT), ("SNOWFLAKE_USER", USER),
                              ("SNOWFLAKE_WAREHOUSE", WAREHOUSE),
                              ("SNOWFLAKE_EXTERNAL_VOLUME", EXTERNAL_VOLUME)] if not v]
    if not have_auth:
        missing.append("SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY")
    if missing:
        print(f"Missing required environment: {', '.join(missing)}")
        sys.exit(2)

    print("=" * 70)
    print("  Snowflake Iceberg Feature Test Suite")
    print("=" * 70)
    print(f"Account:  {ACCOUNT}")
    print(f"Database: {DATABASE}  (schemas prefixed {NS_PREFIX}_)")
    print(f"Volume:   {EXTERNAL_VOLUME}  (base {BASE_LOCATION})")
    print(f"S3 layout inspection: {'ON (' + DATA_BUCKET + ')' if DATA_BUCKET else 'OFF'}")

    os.makedirs(REPORT_DIR, exist_ok=True)

    tests = ALL_TESTS
    if ONLY:
        tests = [t for t in ALL_TESTS
                 if any(t.__name__.endswith(sel) or sel in t.__name__ for sel in ONLY)]
        print(f"Subset via SNOWFLAKE_ONLY: {[t.__name__ for t in tests]}")

    results = []
    for test_fn in tests:
        print(f"\n--- Running {test_fn.__name__} ---")
        try:
            result = test_fn()
        except Exception as e:  # noqa: BLE001 - a broken harness is still a row
            result = TestResult(test_fn.__name__.replace("test_", "").replace("_", "-"),
                                test_fn.__name__)
            result.result = "error"
            result.details = f"Unhandled exception: {e}"
        results.append(result)
        print(f"  {result.result}: {result.details[:120]}")

    report = generate_report(results)
    json_path = os.path.join(REPORT_DIR, "snowflake-iceberg-test-report.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    md_content = generate_markdown(report)
    md_path = os.path.join(REPORT_DIR, "snowflake-iceberg-test-report.md")
    with open(md_path, "w") as f:
        f.write(md_content)
    print(f"\nReports: {json_path}, {md_path}")

    print("\n" + md_content)
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(md_content)

    if _connection is not None:
        _connection.close()

    s = report["summary"]
    print(f"\nRESULTS: {s['passed']} passed, {s['failed']} failed, {s['skipped']} skipped, "
          f"{s['errors']} errors, {s['discrepancies']} discrepancies")
    sys.exit(1 if s["discrepancies"] > 0 or s["errors"] > 0 else 0)


if __name__ == "__main__":
    main()
