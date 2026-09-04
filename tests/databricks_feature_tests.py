"""Databricks Iceberg feature test suite.

Drives a live Databricks SQL warehouse over the SQL Statement API (via
databricks-sql-connector) the same way the Redshift suite drives the Data API:
everything runs on the GitHub runner, no cluster-side code, no repo bundle.

Tables are created as Unity Catalog *managed Iceberg* tables (USING ICEBERG)
inside a pre-provisioned catalog whose managed location lives on our own S3
bucket. That is deliberate: with the data on a bucket we control, the suite
inspects the storage layout directly with boto3 and proves each table is a
genuine Iceberg table (metadata/*.metadata.json present) and not a Delta table
with UniForm-generated Iceberg metadata (_delta_log/ present).

Results are compared against the matrix cells for platform id "databricks"
(src/data/platforms/databricks/databricks/databricks.json) with the same
match semantics as every other engine suite: pass↔full|partial, fail↔none,
skip/error always match. Discrepancies and errors exit non-zero.

Environment:
    DATABRICKS_HOST          (required) e.g. https://dbc-xxxx.cloud.databricks.com
    DATABRICKS_TOKEN         (required) PAT or service-principal OAuth token
    DATABRICKS_WAREHOUSE_ID  (required) SQL warehouse id (Connection details tab)
    DATABRICKS_CATALOG       UC catalog to create run schemas in (default: icebergmatrix)
    AWS_DATA_BUCKET          bucket backing the catalog's managed location; enables
                             the S3 layout inspection AND the Iceberg manifest
                             column-statistics inspection (omit to skip both)
    AWS_REGION               region for the S3 client (default: us-east-1)
    RUN_TAG                  unique per run, e.g. icebergmatrix-<run_id>
    DATABRICKS_ONLY          comma-separated test-function suffixes to run a subset
    MATRIX_PLATFORM_ID       default: databricks
    MATRIX_DATA_PATH         default: src/data/platforms/databricks/databricks/databricks.json
    REPO_ROOT, REPORT_DIR    as in the other suites
"""

import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone

HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "icebergmatrix")
DATA_BUCKET = os.environ.get("AWS_DATA_BUCKET", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
RUN_TAG = os.environ.get("RUN_TAG", f"icebergmatrix-local-{uuid.uuid4().hex[:8]}")
ONLY = [s.strip() for s in os.environ.get("DATABRICKS_ONLY", "").split(",") if s.strip()]

REPO_ROOT = os.environ.get("REPO_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(os.getcwd(), "test-reports"))
MATRIX_PLATFORM_ID = os.environ.get("MATRIX_PLATFORM_ID", "databricks")
MATRIX_DATA_PATH = os.environ.get(
    "MATRIX_DATA_PATH", "src/data/platforms/databricks/databricks/databricks.json"
)

# The environment the matrix cells were originally measured on. A discrepancy
# against a warehouse newer than this may be version drift (Databricks moved),
# not bad data; the report shows both so the reader can tell which.
MATRIX_REFERENCE_ENV = os.environ.get(
    "MATRIX_REFERENCE_ENV",
    "DBSQL 2026.15 warehouse, serverless Spark 4.1.0, DBR 18.2 cluster",
)

# Schemas are named <prefix>_<n> so teardown can sweep by prefix. The run tag
# contains hyphens, which are not valid in schema names.
NS_PREFIX = re.sub(r"[^a-z0-9_]", "_", RUN_TAG.lower())

_ns_counter = 0
_connection = None
_dbr_version = "unknown"


# ---------------------------------------------------------------------------
# Connection and SQL helpers
# ---------------------------------------------------------------------------

def _connect():
    global _connection, _dbr_version
    if _connection is not None:
        return _connection
    from databricks import sql as dbsql

    _connection = dbsql.connect(
        server_hostname=HOST.replace("https://", ""),
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        access_token=TOKEN,
        session_configuration={"STATEMENT_TIMEOUT": "300"},
    )
    with _connection.cursor() as c:
        c.execute("SELECT current_version().dbsql_version")
        row = c.fetchone()
        _dbr_version = row[0] if row else "unknown"
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
    sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{ns}")
    return ns


def _qualified(ns: str, table: str) -> str:
    return f"{CATALOG}.{ns}.{table}"


def _create_iceberg(ns: str, table: str, columns: str, version: str = "2",
                    extra_props: str = "", partitioned_by: str = "") -> str:
    """CREATE TABLE ... USING ICEBERG at the requested format version."""
    q = _qualified(ns, table)
    props = [f"'format-version'='{version}'"]
    if extra_props:
        props.append(extra_props)
    part = f" PARTITIONED BY ({partitioned_by})" if partitioned_by else ""
    sql(f"CREATE TABLE {q} ({columns}) USING ICEBERG{part} "
        f"TBLPROPERTIES ({', '.join(props)})")
    return q


# ---------------------------------------------------------------------------
# S3 layout inspection: the "genuine Iceberg, not Delta+UniForm" proof
# ---------------------------------------------------------------------------

def _table_location(q: str) -> str:
    """The table's storage location, from DESCRIBE EXTENDED."""
    for row in sql(f"DESCRIBE TABLE EXTENDED {q}"):
        if str(row[0]).strip().lower() == "location":
            return str(row[1]).strip()
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
        "delta_log": any("/_delta_log/" in f"/{k}" for k in keys),
    }


def _iceberg_evidence(layout: dict) -> str:
    """Human-readable storage evidence for the report details."""
    if not layout:
        return "storage inspection unavailable (no bucket access to the table location)"
    verdict = ("genuine Iceberg layout" if layout["metadata_json"] and not layout["delta_log"]
               else "NOT a native Iceberg layout")
    return (f"{verdict}: {layout['metadata_json']} metadata.json, "
            f"{layout['manifests']} manifests, {layout['parquet']} parquet, "
            f"_delta_log={'present' if layout['delta_log'] else 'absent'}")


# ---------------------------------------------------------------------------
# Iceberg manifest column-statistics inspection (on S3)
# ---------------------------------------------------------------------------
# The statistics cell asks whether the Iceberg metadata Databricks writes for a
# managed Iceberg table carries per-column statistics (min/max bounds, value
# and null counts, column sizes). Those live in the data_file entries of the
# Avro manifests, which a SQL warehouse session cannot show -- but we own the
# S3 bucket, so we read the manifest bytes directly and check the five stats
# maps. This mirrors tests/delta-uniform (the OSS Delta UniForm reproduction of
# delta-io/delta#5469): the same measurement, applied here to the Databricks
# native Iceberg write path where stats are expected to be populated.

_STATS_FIELDS = [
    "column_sizes",
    "value_counts",
    "null_value_counts",
    "nan_value_counts",
    "lower_bounds",
    "upper_bounds",
]


def _newest_manifest_key(keys: list) -> str:
    """Pick the newest Iceberg data-file manifest key from a listing.

    Iceberg writes two kinds of .avro under metadata/: the manifest *list*
    (snap-*.avro) and the data-file manifests (which hold the stats maps). We
    want the latter. Keys already sort lexically close to write order; we take
    the last non-snap .avro, and fall back to mtime via a HEAD if needed.
    """
    manifests = [k for k in keys
                 if k.endswith(".avro") and not os.path.basename(k).startswith("snap-")]
    if not manifests:
        return ""
    manifests.sort()
    return manifests[-1]


def _inspect_manifest_stats(q: str) -> dict:
    """Download the newest Iceberg manifest for table q and summarise stats.

    Returns {} when inspection is not possible (no bucket, location off-bucket,
    no manifest found) so the caller degrades to skip rather than a false
    verdict. On success returns:
        {
          "manifest": <key>,
          "entries": [ {field: {"populated": bool, "count": int}, ...}, ... ],
          "populated": {field: bool, ...},   # any data file has it
        }
    """
    if not DATA_BUCKET:
        return {}
    location = _table_location(q)
    m = re.match(r"s3a?://([^/]+)/(.*)", location)
    if not m or m.group(1) != DATA_BUCKET:
        return {}

    import boto3
    import fastavro

    s3 = boto3.client("s3", region_name=AWS_REGION)
    prefix = m.group(2).rstrip("/") + "/"
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=DATA_BUCKET, Prefix=prefix):
        keys += [obj["Key"] for obj in page.get("Contents", [])]

    manifest_key = _newest_manifest_key(keys)
    if not manifest_key:
        return {}

    body = s3.get_object(Bucket=DATA_BUCKET, Key=manifest_key)["Body"].read()
    import io

    entries = []
    reader = fastavro.reader(io.BytesIO(body))
    for record in reader:
        data_file = record.get("data_file") or {}
        per_file = {"status": record.get("status")}
        for field in _STATS_FIELDS:
            val = data_file.get(field)
            if val is None:
                populated, count = False, 0
            elif isinstance(val, (dict, list, tuple)):
                populated, count = len(val) > 0, len(val)
            else:
                populated, count = True, 1
            per_file[field] = {"populated": populated, "count": count}
        entries.append(per_file)

    populated = {
        field: any(e[field]["populated"] for e in entries) for field in _STATS_FIELDS
    }
    return {"manifest": manifest_key, "entries": entries, "populated": populated}


def _assert_real_iceberg(layout: dict) -> None:
    if not layout:
        return  # inspection unavailable is not a failure
    assert layout["metadata_json"] > 0, "no Iceberg metadata.json found at the table location"
    assert not layout["delta_log"], "_delta_log/ present: this is a Delta table (UniForm), not Iceberg"


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
    """Run a test body; any exception becomes an error with a compact message."""
    try:
        ns = _new_namespace()
        body(ns, r)
    except Exception as e:  # noqa: BLE001 - surface any failure as an error
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:260]}"
    return r


def _expect_rejection(r: TestResult, statement_fn, accepted_details: str,
                      rejected_details: str) -> None:
    """For cells rated none: pass/fail is inverted evidence, so run the
    statement and record fail (matches none) when it is rejected."""
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
        sql(f"CREATE TABLE {q2} USING ICEBERG AS SELECT 1 AS id")
        sql(f"DROP TABLE {q2}")
        layout = _inspect_s3_layout(q)
        _assert_real_iceberg(layout)
        r.result = "pass"
        r.details = ("CREATE/CTAS/DROP of managed Iceberg tables in Unity Catalog; "
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
        r.details = "INSERT INTO ... VALUES and INSERT INTO ... SELECT committed 3 rows"

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
        layout = _inspect_s3_layout(q)
        if layout:
            # COW rewrites data files; no deletion-vector puffins should appear
            # on a v2 table.
            assert layout["puffin"] == 0, f"unexpected puffin delete files: {layout}"
        r.result = "pass"
        r.details = ("v2 UPDATE executed as copy-on-write; "
                     + (_iceberg_evidence(layout) if layout else "storage not inspected"))

    return _run(r, body)


def test_merge_on_read() -> TestResult:
    r = TestResult("merge-on-read", "Merge-on-Read", "v3")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, v STRING", version="3")
        sql(f"INSERT INTO {q} VALUES (1,'a'),(2,'b'),(3,'c')")
        sql(f"UPDATE {q} SET v = 'B' WHERE id = 2")
        layout = _inspect_s3_layout(q)
        if layout:
            assert layout["puffin"] > 0, (
                f"expected deletion-vector puffin files after a v3 UPDATE: {layout}")
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        assert n == 3
        r.result = "pass"
        r.details = ("v3 UPDATE produced merge-on-read deletion vectors; "
                     + (_iceberg_evidence(layout) if layout else "storage not inspected"))

    return _run(r, body)


def test_deletion_vectors() -> TestResult:
    r = TestResult("deletion-vectors", "Deletion Vectors", "v3")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, v STRING", version="3")
        sql(f"INSERT INTO {q} VALUES (1,'a'),(2,'b'),(3,'c')")
        sql(f"DELETE FROM {q} WHERE id = 2")
        layout = _inspect_s3_layout(q)
        if layout:
            assert layout["puffin"] > 0, f"expected puffin deletion vectors: {layout}"
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        assert n == 2
        r.result = "pass"
        r.details = ("Row-level DELETE on a v3 table encoded as binary deletion "
                     "vectors (Puffin); "
                     + (_iceberg_evidence(layout) if layout else "storage not inspected"))

    return _run(r, body)


def test_position_deletes() -> TestResult:
    r = TestResult("position-deletes", "Position Deletes", "v3")

    def body(ns, r):
        # On v3 the position-delete representation is the deletion vector.
        q = _create_iceberg(ns, "t", "id INT, v STRING", version="3")
        sql(f"INSERT INTO {q} VALUES (1,'a'),(2,'b')")
        sql(f"DELETE FROM {q} WHERE id = 1")
        layout = _inspect_s3_layout(q)
        if layout:
            assert layout["puffin"] > 0, f"expected puffin deletion vectors: {layout}"
        assert sql(f"SELECT count(*) FROM {q}")[0][0] == 1
        r.result = "pass"
        r.details = "v3 position deletes written as deletion vectors and read back correctly"

    return _run(r, body)


def test_equality_deletes() -> TestResult:
    r = TestResult("equality-deletes", "Equality Deletes", "v2")
    # No SQL surface on Databricks produces equality delete files; deletes are
    # DVs (v3) or copy-on-write (v2). Honest negative evidence.
    r.result = "fail"
    r.details = "No SQL surface produces equality delete files on Databricks"
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
        q = _create_iceberg(ns, "t", "id INT, amount FLOAT")
        sql(f"INSERT INTO {q} VALUES (1, 1.5)")
        sql(f"ALTER TABLE {q} ALTER COLUMN id TYPE BIGINT")
        sql(f"ALTER TABLE {q} ALTER COLUMN amount TYPE DOUBLE")
        sql(f"INSERT INTO {q} VALUES (2147483648, 2.5)")
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        assert n == 2
        r.result = "pass"
        r.details = "Spec promotions int->bigint and float->double applied to a populated table"

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
        sql(f"INSERT INTO {q} VALUES (2)")
        history = sql(f"SELECT version FROM (DESCRIBE HISTORY {q}) ORDER BY version")
        first = history[0][0]
        n = sql(f"SELECT count(*) FROM {q} VERSION AS OF {first + 1}")[0][0]
        assert n == 1, f"expected 1 row at the first insert version, got {n}"
        r.result = "pass"
        r.details = "VERSION AS OF read an earlier snapshot with the expected row count"

    return _run(r, body)


def test_table_maintenance() -> TestResult:
    r = TestResult("table-maintenance", "Table Maintenance", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT")
        for i in range(3):
            sql(f"INSERT INTO {q} VALUES ({i})")
        sql(f"OPTIMIZE {q}")
        sql(f"VACUUM {q}")
        r.result = "pass"
        r.details = "OPTIMIZE (compaction) and VACUUM ran on a managed Iceberg table"

    return _run(r, body)


def test_branching_tagging() -> TestResult:
    r = TestResult("branching-tagging", "Branching & Tagging", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT")
        sql(f"INSERT INTO {q} VALUES (1)")
        _expect_rejection(
            r,
            lambda: sql(f"ALTER TABLE {q} CREATE BRANCH b1"),
            accepted_details="CREATE BRANCH accepted",
            rejected_details="Iceberg branch/tag DDL rejected",
        )

    return _run(r, body)


def test_hidden_partitioning() -> TestResult:
    # Rated none: the docs rule out expression-based partition transforms on
    # managed Iceberg tables, so the rejection is the datum. Asserting the
    # transform works would have logged a bare error on rejection, and an
    # error matches any level -- it would never have contradicted the cell.
    r = TestResult("hidden-partitioning", "Hidden Partitioning", "v2")

    def body(ns, r):
        _expect_rejection(
            r,
            lambda: _create_iceberg(ns, "t", "id INT, ts TIMESTAMP",
                                    partitioned_by="days(ts)"),
            accepted_details="PARTITIONED BY days(ts) transform accepted",
            rejected_details="Expression partition transform rejected",
        )

    return _run(r, body)


def test_partition_evolution() -> TestResult:
    r = TestResult("partition-evolution", "Partition Evolution", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, ts TIMESTAMP",
                            partitioned_by="days(ts)")
        sql(f"INSERT INTO {q} VALUES (1, TIMESTAMP'2026-01-01 10:00:00')")
        _expect_rejection(
            r,
            lambda: sql(f"ALTER TABLE {q} PARTITIONED BY (months(ts))"),
            accepted_details="Partition spec changed in place on an existing table",
            rejected_details="In-place partition spec change rejected",
        )

    return _run(r, body)


def test_multi_arg_transforms() -> TestResult:
    r = TestResult("multi-arg-transforms", "Multi-arg Transforms", "v3")

    def body(ns, r):
        _expect_rejection(
            r,
            lambda: _create_iceberg(ns, "t", "a INT, b INT", version="3",
                                    partitioned_by="bucket(4, a, b)"),
            accepted_details="Multi-argument bucket transform accepted",
            rejected_details="Multi-argument partition transform rejected",
        )

    return _run(r, body)


def test_variant_type() -> TestResult:
    r = TestResult("variant-type", "Variant Type", "v3")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, payload VARIANT", version="3")
        sql(f"INSERT INTO {q} SELECT 1, parse_json('{{\"a\": 1, \"b\": [true, \"x\"]}}')")
        val = sql(f"SELECT payload:a::int FROM {q}")[0][0]
        assert val == 1, f"variant field extraction returned {val}"
        r.result = "pass"
        r.details = "VARIANT column stored via parse_json and read back with path extraction"

    return _run(r, body)


def test_shredded_variant() -> TestResult:
    r = TestResult("shredded-variant", "Shredded Variant", "v3")
    r.result = "skip"
    r.details = ("Shredding is an internal write optimisation with no SQL surface "
                 "to enable or observe from a warehouse session")
    return r


def test_geometry_type() -> TestResult:
    r = TestResult("geometry-type", "Geometry Type", "v3")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT, geom GEOMETRY", version="3")
        sql(f"INSERT INTO {q} SELECT 1, st_geomfromtext('POINT(1 2)')")
        x = sql(f"SELECT st_x(geom) FROM {q}")[0][0]
        assert float(x) == 1.0, f"st_x returned {x}"
        r.result = "pass"
        r.details = "GEOMETRY column written with st_geomfromtext and read back via st_x"

    return _run(r, body)


def test_nanosecond_timestamps() -> TestResult:
    r = TestResult("nanosecond-timestamps", "Nanosecond Timestamps", "v3")

    def body(ns, r):
        _expect_rejection(
            r,
            lambda: _create_iceberg(ns, "t", "id INT, ts TIMESTAMP_NS", version="3"),
            accepted_details="TIMESTAMP_NS column accepted",
            rejected_details="Nanosecond timestamp type rejected",
        )

    return _run(r, body)


def test_unknown_type() -> TestResult:
    r = TestResult("unknown-type", "Unknown Type", "v3")

    def body(ns, r):
        # VOID is Spark's spelling of the Iceberg V3 unknown type. Recorded as a
        # rejection probe because a warehouse that refuses the column is the
        # measured answer, and _run would otherwise log it as a harness error.
        _expect_rejection(
            r,
            lambda: _create_iceberg(ns, "t", "id INT, u VOID", version="3"),
            accepted_details="VOID column accepted as the V3 unknown type",
            rejected_details="Unknown type rejected",
        )

    return _run(r, body)


def test_lineage() -> TestResult:
    r = TestResult("lineage", "Lineage Tracking", "v3")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT", version="3")
        sql(f"INSERT INTO {q} VALUES (1), (2)")
        rows = sql(f"SELECT _metadata.row_id FROM {q}")
        ids = [row[0] for row in rows]
        assert len(ids) == 2 and all(i is not None for i in ids), f"row ids: {ids}"
        r.result = "pass"
        r.details = "v3 row lineage exposed through _metadata.row_id on a managed Iceberg table"

    return _run(r, body)


def test_catalog_integration() -> TestResult:
    r = TestResult("catalog-integration", "Catalog Integration", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT")
        found = sql(f"SHOW TABLES IN {CATALOG}.{ns}")
        assert any(row[1] == "t" for row in found), f"table not listed: {found}"
        r.result = "pass"
        r.details = "Managed Iceberg table created, listed and resolved through Unity Catalog"

    return _run(r, body)


def test_unity_catalog() -> TestResult:
    r = TestResult("unity-catalog", "Unity Catalog", "v2")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id INT")
        sql(f"INSERT INTO {q} VALUES (1)")
        assert sql(f"SELECT count(*) FROM {q}")[0][0] == 1
        r.result = "pass"
        r.details = "The whole suite runs against Unity Catalog as the Iceberg catalog"

    return _run(r, body)


def _skip(feature_id: str, name: str, version: str, why: str) -> TestResult:
    r = TestResult(feature_id, name, version)
    r.result = "skip"
    r.details = why
    return r


def _statistics_probe(version: str) -> TestResult:
    """Create a managed Iceberg table, insert rows with known min/max, then
    read the Iceberg manifest bytes off S3 and check whether the per-column
    statistics maps are populated.

    pass  -> the manifest carries bounds + counts (stats present)
    fail  -> a genuine Iceberg manifest was read but its stats maps are empty
    skip  -> stats could not be inspected (no bucket access / no manifest);
             a SQL session alone cannot see them, so we do not guess
    """
    r = TestResult("statistics", "Statistics", f"v{version}")

    def body(ns, r):
        q = _create_iceberg(ns, "t", "id BIGINT, category STRING, amount DOUBLE",
                            version=version)
        # Known min/max so populated stats are unmistakable:
        #   id [1,5], amount [10.5,500.25], category ['a','e']
        sql(f"INSERT INTO {q} VALUES "
            "(1,'a',10.5),(2,'b',42.0),(3,'c',100.0),(4,'d',250.75),(5,'e',500.25)")

        stats = _inspect_manifest_stats(q)
        if not stats or not stats.get("entries"):
            r.result = "skip"
            r.details = ("Iceberg manifest column statistics could not be inspected "
                         "(no bucket access to the table location, or no data-file "
                         "manifest found); not observable from a SQL session alone")
            return

        pop = stats["populated"]
        core_present = pop["lower_bounds"] and pop["upper_bounds"] and pop["value_counts"]
        present_names = [f for f in _STATS_FIELDS if pop[f]]
        empty_names = [f for f in _STATS_FIELDS if not pop[f]]
        n_files = len(stats["entries"])

        if core_present:
            r.result = "pass"
            r.details = (
                f"Iceberg manifest ({os.path.basename(stats['manifest'])}, {n_files} "
                f"data file(s)) carries column statistics: populated="
                f"{', '.join(present_names)}"
                + (f"; empty={', '.join(empty_names)}" if empty_names else "")
            )
        else:
            r.result = "fail"
            r.details = (
                f"Iceberg manifest ({os.path.basename(stats['manifest'])}, {n_files} "
                f"data file(s)) has empty column statistics: missing="
                f"{', '.join(empty_names)}"
                + (f"; populated={', '.join(present_names)}" if present_names else "")
            )

    return _run(r, body)


def test_statistics() -> TestResult:
    return _statistics_probe("2")


def test_statistics_v3() -> TestResult:
    return _statistics_probe("3")


def test_bloom_filters() -> TestResult:
    return _skip("bloom-filters", "Bloom Filters & Puffin", "v2",
                 "Bloom-filter write properties are not exposed for managed Iceberg tables")


def test_hive_metastore() -> TestResult:
    return _skip("hive-metastore", "Hive Metastore", "v2",
                 "External catalog wiring is out of scope for a single-workspace run")


def test_glue_catalog() -> TestResult:
    return _skip("aws-glue-catalog", "AWS Glue Catalog", "v2",
                 "External catalog wiring is out of scope for a single-workspace run")


def test_rest_catalog() -> TestResult:
    return _skip("rest-catalog", "REST Catalog", "v2",
                 "Requires Lakehouse Federation setup against an external IRC endpoint")


def test_nessie() -> TestResult:
    return _skip("nessie", "Nessie", "v2",
                 "External catalog wiring is out of scope for a single-workspace run")


def test_polaris() -> TestResult:
    return _skip("polaris", "Polaris", "v2",
                 "External catalog wiring is out of scope for a single-workspace run")


def test_snowflake_horizon_catalog() -> TestResult:
    return _skip("snowflake-horizon-catalog", "Snowflake Horizon Catalog", "v2",
                 "External catalog wiring is out of scope for a single-workspace run")


def test_hadoop_catalog() -> TestResult:
    return _skip("hadoop-catalog", "Hadoop Catalog", "v2",
                 "Path-based catalogs cannot be attached to a UC SQL warehouse")


def test_jdbc_catalog() -> TestResult:
    return _skip("jdbc-catalog", "JDBC Catalog", "v2",
                 "JDBC catalogs cannot be attached to a UC SQL warehouse")


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
    test_unity_catalog,
    test_statistics,
    test_statistics_v3,
    test_bloom_filters,
    test_hive_metastore,
    test_glue_catalog,
    test_rest_catalog,
    test_nessie,
    test_polaris,
    test_snowflake_horizon_catalog,
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
        "engine": "Databricks",
        "databricks_version": _dbr_version,
        "matrix_reference_env": MATRIX_REFERENCE_ENV,
        "warehouse": f"{HOST}/sql/1.0/warehouses/{WAREHOUSE_ID}",
        "catalog": CATALOG,
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
        "# Databricks Iceberg Feature Test Report",
        "",
        f"- **Timestamp:** {report['timestamp']}",
        f"- **DBSQL Version (this run):** {report['databricks_version']}",
        f"- **Matrix cells measured on:** {report['matrix_reference_env']}",
        f"- **Catalog:** {report['catalog']}",
        "",
        "> A discrepancy against a newer warehouse than the reference may be "
        "version drift rather than wrong data: check the DBSQL version above "
        "before editing cells.",
        "",
        "## Summary",
        "",
        "| Metric | Count |",
        "|--------|-------|",
        f"| Total | {s['total']} |",
        f"| ✅ Passed | {s['passed']} |",
        f"| ❌ Failed | {s['failed']} |",
        f"| ⏭️ Skipped | {s['skipped']} |",
        f"| ⚠️ Errors | {s['errors']} |",
        f"| 🔍 Discrepancies | {s['discrepancies']} |",
        "",
        "## Test Results",
        "",
        "| Feature | Version | Result | JSON Level | Match | Details |",
        "|---------|---------|--------|------------|-------|---------|",
    ]
    emoji = {"pass": "✅", "fail": "❌", "skip": "⏭️", "error": "⚠️"}
    for t in report["tests"]:
        details = (t["details"][:80].replace("\n", " ").replace("|", "\\|")
                   if t["details"] else "")
        match_str = "✅" if t["match"] else "❌ DISCREPANCY"
        lines.append(f"| {t['feature_name']} | {t['version']} | "
                     f"{emoji.get(t['result'], '?')} {t['result']} | {t['json_level']} | "
                     f"{match_str} | {details} |")
    lines.append("")

    discs = [t for t in report["tests"] if not t["match"]]
    if discs:
        lines += ["## ⚠️ Discrepancies", ""]
        for t in discs:
            d = t["details"][:120].replace("\n", " ") if t["details"] else ""
            lines.append(f"- **{t['feature_name']}** ({t['version']}): "
                         f"test={t['result']}, json={t['json_level']} — {d}")
        lines.append("")
    return "\n".join(lines)


def main():
    missing = [n for n, v in [("DATABRICKS_HOST", HOST), ("DATABRICKS_TOKEN", TOKEN),
                              ("DATABRICKS_WAREHOUSE_ID", WAREHOUSE_ID)] if not v]
    if missing:
        print(f"Missing required environment: {', '.join(missing)}")
        sys.exit(2)

    print("=" * 70)
    print("  Databricks Iceberg Feature Test Suite")
    print("=" * 70)
    print(f"Workspace: {HOST}")
    print(f"Catalog:   {CATALOG}  (schemas prefixed {NS_PREFIX}_)")
    print(f"S3 layout inspection: {'ON (' + DATA_BUCKET + ')' if DATA_BUCKET else 'OFF'}")

    os.makedirs(REPORT_DIR, exist_ok=True)

    tests = ALL_TESTS
    if ONLY:
        tests = [t for t in ALL_TESTS
                 if any(t.__name__.endswith(sel) or sel in t.__name__ for sel in ONLY)]
        print(f"Subset via DATABRICKS_ONLY: {[t.__name__ for t in tests]}")

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
        icon = {"pass": "✅", "fail": "❌", "skip": "⏭️", "error": "⚠️"}.get(result.result, "?")
        print(f"  {icon} {result.result}: {result.details[:120]}")

    report = generate_report(results)
    json_path = os.path.join(REPORT_DIR, "databricks-iceberg-test-report.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    md_content = generate_markdown(report)
    md_path = os.path.join(REPORT_DIR, "databricks-iceberg-test-report.md")
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
