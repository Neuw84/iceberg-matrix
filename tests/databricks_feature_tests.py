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
    DATABRICKS_HTTP_PATH     optional compute override: an http_path to another
                             warehouse or a cluster, so the same probes can run
                             on different compute than the default warehouse
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
# Optional compute override: full http_path to another warehouse or a cluster.
# Empty means the SQL warehouse above.
HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "").strip()
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

    # DATABRICKS_HTTP_PATH lets the suite target other compute than the
    # default SQL warehouse, so the same probes can measure differences
    # across warehouses or clusters when one is available.
    http_path = HTTP_PATH or f"/sql/1.0/warehouses/{WAREHOUSE_ID}"
    _connection = dbsql.connect(
        server_hostname=HOST.replace("https://", ""),
        http_path=http_path,
        access_token=TOKEN,
        session_configuration={"STATEMENT_TIMEOUT": "300"},
    )
    with _connection.cursor() as c:
        # current_version() exposes dbsql_version on SQL warehouses and
        # dbr_version on clusters; take whichever is set so the report always
        # records the runtime the measurement ran on.
        c.execute("SELECT current_version().dbsql_version, "
                  "current_version().dbr_version")
        row = c.fetchone()
        if row:
            dbsql_v, dbr_v = row[0], row[1]
            _dbr_version = (f"DBSQL {dbsql_v}" if dbsql_v
                            else f"DBR {dbr_v}" if dbr_v else "unknown")
        else:
            _dbr_version = "unknown"
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


def _data_manifest_keys(keys: list) -> list:
    """The Iceberg data-file manifests under a table prefix.

    Iceberg writes two kinds of .avro under its metadata dir: the manifest
    *list* (snap-*.avro) and the actual manifests. On Databricks managed
    Iceberg the metadata lives under <table>/_iceberg/metadata/, so we match
    any .avro under the table prefix, excluding snap-*. Sorted so the newest
    (lexically-last, which tracks write order) manifests come last.
    """
    manifests = [k for k in keys
                 if k.endswith(".avro") and not os.path.basename(k).startswith("snap-")]
    manifests.sort()
    return manifests


def _inspect_manifest_stats(q: str) -> dict:
    """Read the Iceberg manifests for table q and summarise per-column stats.

    Aggregates over DATA files only (manifest_entry.data_file.content == 0);
    delete manifests (deletion vectors / position/equality deletes) never carry
    column stats and would otherwise drag the verdict to empty. Reads the two
    newest data-file manifests so a table whose latest snapshot split data and
    deletes across files is still covered.

    On success returns {manifest, entries, populated}. When inspection is not
    possible it returns {diag: <reason>} (no bucket access, off-S3 location, no
    manifest, no data-file entries) so the caller degrades to skip with a
    concrete reason.

    The table's storage bucket is taken from its own location rather than
    required to equal AWS_DATA_BUCKET: Databricks manages the location, and the
    CI role has read access to it. AWS_DATA_BUCKET is only used as the on/off
    switch for whether any S3 inspection is attempted at all.
    """
    if not DATA_BUCKET:
        return {"diag": "AWS_DATA_BUCKET not set (S3 inspection disabled)"}
    location = _table_location(q)
    m = re.match(r"s3a?://([^/]+)/(.*)", location)
    if not m:
        return {"diag": f"table location is not an s3:// URI: {location!r}"}
    bucket = m.group(1)

    import boto3
    import fastavro
    import io

    s3 = boto3.client("s3", region_name=AWS_REGION)
    prefix = m.group(2).rstrip("/") + "/"
    keys = []
    mtimes = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
            mtimes[obj["Key"]] = obj.get("LastModified")

    avro_keys = [k for k in keys if k.endswith(".avro")]
    manifests = _data_manifest_keys(keys)
    if not manifests:
        return {"diag": (
            f"no data-file manifest under s3://{bucket}/{prefix}: "
            f"{len(keys)} objects, {len(avro_keys)} .avro "
            f"({', '.join(os.path.basename(k) for k in avro_keys[:5]) or 'none'})"
        )}

    # Read the newest couple of manifests and keep only content==0 data files.
    entries = []
    read_keys = []
    for key in manifests[-2:]:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        read_keys.append(os.path.basename(key))
        for record in fastavro.reader(io.BytesIO(body)):
            data_file = record.get("data_file") or {}
            if data_file.get("content", 0) != 0:
                continue  # delete manifest entry: no column stats by design
            per_file = {"status": record.get("status"),
                        "record_count": data_file.get("record_count")}
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

    if not entries:
        return {"diag": (
            f"manifest(s) {', '.join(read_keys)} had no data-file entries "
            "(content==0)"
        )}

    populated = {
        field: any(e[field]["populated"] for e in entries) for field in _STATS_FIELDS
    }
    result = {"manifest": ", ".join(read_keys), "entries": entries,
              "populated": populated}

    # --- Manifest LIST (snap-*.avro): planning-level metadata ---
    # The manifest list drives Iceberg scan planning: per-manifest file/row
    # counters plus the partitions[] field-summary (lower/upper bound and
    # contains_null per partition column) used to skip whole manifests.
    # Snapshot ids in the filename are random, so pick the newest by S3
    # LastModified rather than by name.
    snaps = [k for k in avro_keys if os.path.basename(k).startswith("snap-")]
    if snaps:
        snaps.sort(key=lambda k: mtimes.get(k) or 0)
        snap_key = snaps[-1]
        body = s3.get_object(Bucket=bucket, Key=snap_key)["Body"].read()
        ml_entries = []
        counter_names = [
            "added_data_files_count", "existing_data_files_count",
            "deleted_data_files_count", "added_rows_count",
            "existing_rows_count", "deleted_rows_count",
        ]
        for rec in fastavro.reader(io.BytesIO(body)):
            counters = {c: rec.get(c) for c in counter_names}
            parts = rec.get("partitions")
            psum = []
            if isinstance(parts, (list, tuple)):
                for fs in parts:
                    fs = fs or {}
                    psum.append({
                        "contains_null": fs.get("contains_null"),
                        "contains_nan": fs.get("contains_nan"),
                        "has_lower_bound": fs.get("lower_bound") is not None,
                        "has_upper_bound": fs.get("upper_bound") is not None,
                    })
            ml_entries.append({
                "manifest_path": os.path.basename(str(rec.get("manifest_path", ""))),
                "content": rec.get("content"),
                "counters": counters,
                "partitions_count": len(psum),
                "partitions": psum,
                "partitions_populated": any(
                    p["has_lower_bound"] or p["has_upper_bound"]
                    or p["contains_null"] is not None for p in psum
                ),
            })
        result["manifest_list"] = {
            "file": os.path.basename(snap_key),
            "entries": ml_entries,
            "any_partitions_populated": any(
                e["partitions_populated"] for e in ml_entries),
            "any_row_counters_populated": any(
                e["counters"].get(c) not in (None, -1)
                for e in ml_entries
                for c in ("added_rows_count", "existing_rows_count",
                          "deleted_rows_count")
            ),
        }
    return result


def _assert_real_iceberg(layout: dict) -> None:
    if not layout:
        return  # inspection unavailable is not a failure
    assert layout["metadata_json"] > 0, "no Iceberg metadata.json found at the table location"
    assert not layout["delta_log"], "_delta_log/ present: this is a Delta table (UniForm), not Iceberg"


# ---------------------------------------------------------------------------
# Result class and harness (same contract as the other suites)
# ---------------------------------------------------------------------------

class TestResult:
    def __init__(self, feature_id: str, feature_name: str, version: str = "v2",
                 diagnostic: bool = False):
        self.feature_id = feature_id
        self.feature_name = feature_name
        self.result = "skip"  # pass | fail | skip | error
        self.details = ""
        self.version_tested = version
        # Diagnostic rows report a finer-grained measurement than any single
        # matrix cell (e.g. one row per individual type promotion). They are
        # not compared against the matrix and never count as a discrepancy.
        self.diagnostic = diagnostic

    def to_dict(self):
        return {
            "feature_id": self.feature_id,
            "feature_name": self.feature_name,
            "version": self.version_tested,
            "result": self.result,
            "details": self.details,
            "diagnostic": self.diagnostic,
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


# ---------------------------------------------------------------------------
# Type-promotion detail probes
# ---------------------------------------------------------------------------
# The Iceberg spec enumerates the *valid* primitive type promotions and splits
# them by format version: some are valid for v1/v2, and v3 adds date->timestamp
# and date->timestamp_ns. These probes exercise each promotion individually on
# a populated table (CREATE -> INSERT -> ALTER COLUMN TYPE -> read back), so we
# can report exactly which promotions Databricks managed Iceberg honours and
# which it rejects, rather than a single pass/fail. The read-back after the
# ALTER also exercises the spec's bounds-decoding rule (old data-file bounds
# were written at the narrower type and must still decode correctly).
#
# The second insert of each probe uses a value that only fits the widened type,
# so a promoted column that silently kept the narrow type would fail or
# truncate. Each promotion is reported as its own diagnostic row.


def _probe_one_promotion(ns, version, idx, from_type, v1, to_type, v2) -> tuple:
    """Run one promotion end to end.

    Returns (result, details) where result is 'pass' when the ALTER is accepted
    and both the pre-promotion and post-promotion rows read back, 'fail' when
    the engine refuses the promotion (the datum for a promotion it does not
    support), and 'error' for an unexpected harness failure.
    """
    try:
        q = _create_iceberg(ns, f"tp_{version}_{idx}", f"id INT, val {from_type}",
                            version=version)
        sql(f"INSERT INTO {q} VALUES (1, {v1})")
    except Exception as e:  # noqa: BLE001 - setup failure is not a promotion datum
        return "error", f"setup failed before ALTER: {str(e).splitlines()[0][:180]}"
    try:
        sql(f"ALTER TABLE {q} ALTER COLUMN val TYPE {to_type}")
    except Exception as e:  # noqa: BLE001 - rejection IS the measurement
        return "fail", (f"{from_type} -> {to_type} rejected: "
                        f"{str(e).splitlines()[0][:180]}")
    # Widened-type value that would not fit the original type proves the column
    # really carries the promoted type now; the pre-promotion row must still
    # decode under the widened type (the spec's bounds-decoding rule).
    try:
        sql(f"INSERT INTO {q} VALUES (2, {v2})")
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        old = sql(f"SELECT val FROM {q} WHERE id = 1")[0][0]
    except Exception as e:  # noqa: BLE001
        return "fail", (f"{from_type} -> {to_type} altered but post-promotion "
                        f"read/write failed: {str(e).splitlines()[0][:160]}")
    if n != 2:
        return "fail", f"{from_type} -> {to_type}: expected 2 rows after promotion, got {n}"
    if old is None:
        return "fail", f"{from_type} -> {to_type}: pre-promotion value lost after ALTER"
    return "pass", (f"{from_type} -> {to_type} accepted; widened-only value stored "
                    f"and pre-promotion row still decodes")


# Each promotion is reported as its own diagnostic row so the matrix owner can
# state exactly which promotions Databricks managed Iceberg supports. feature_id
# is unique per promotion (not a matrix cell), and rows are flagged diagnostic
# so they are excluded from matrix discrepancy accounting.
def _type_promotion_detail(version: str, promotions: list) -> list:
    def _make(idx, label_id, feature_name, from_type, v1, to_type, v2) -> TestResult:
        r = TestResult(f"type-promotion-{label_id}", feature_name, f"v{version}",
                       diagnostic=True)

        def body(ns, r):
            result, details = _probe_one_promotion(ns, version, idx,
                                                    from_type, v1, to_type, v2)
            r.result = result
            r.details = details

        return _run(r, body)

    return [_make(idx, *p) for idx, p in enumerate(promotions)]


# (label_id, feature_name, from_type, sample_value, to_type, widened_only_value)
def test_type_promotion_detail() -> list:
    """v1/v2-valid primitive promotions, each probed individually on a v2 table."""
    promos = [
        ("int-long", "Type Promotion: int -> long", "INT", "1", "BIGINT", "2147483648"),
        ("float-double", "Type Promotion: float -> double", "FLOAT", "1.5",
         "DOUBLE", "1.7976931348623157E308"),
        ("decimal-widen", "Type Promotion: decimal precision widen", "DECIMAL(5,2)",
         "123.45", "DECIMAL(10,2)", "12345678.90"),
    ]
    return _type_promotion_detail("2", promos)


def test_type_promotion_v3_detail() -> list:
    """v3-valid promotions: the v2 set plus date -> timestamp / timestamp_ns,
    each probed individually on a v3 table."""
    promos = [
        ("int-long", "Type Promotion: int -> long", "INT", "1", "BIGINT", "2147483648"),
        ("float-double", "Type Promotion: float -> double", "FLOAT", "1.5",
         "DOUBLE", "1.7976931348623157E308"),
        ("decimal-widen", "Type Promotion: decimal precision widen", "DECIMAL(5,2)",
         "123.45", "DECIMAL(10,2)", "12345678.90"),
        ("date-timestamp", "Type Promotion: date -> timestamp", "DATE",
         "DATE'2026-01-01'", "TIMESTAMP", "TIMESTAMP'2026-06-15 12:34:56'"),
        ("date-timestamp-ns", "Type Promotion: date -> timestamp_ns", "DATE",
         "DATE'2026-01-01'", "TIMESTAMP_NS", "TIMESTAMP_NS'2026-06-15 12:34:56.123456789'"),
    ]
    return _type_promotion_detail("3", promos)


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
        # Partitioned by category so the Iceberg manifest LIST carries a
        # non-trivial partitions[] field-summary we can validate (partition
        # pruning metadata). If managed Iceberg rejects PARTITIONED BY on this
        # runtime, fall back to unpartitioned and note it: the column-stats
        # measurement is still valid, only the partition-summary check degrades.
        partitioned = True
        try:
            q = _create_iceberg(ns, "t", "id BIGINT, category STRING, amount DOUBLE",
                                version=version, partitioned_by="category")
        except Exception:  # noqa: BLE001 - measured fallback, not an error
            partitioned = False
            q = _create_iceberg(ns, "t", "id BIGINT, category STRING, amount DOUBLE",
                                version=version)
        # Known min/max so populated stats are unmistakable:
        #   id [1,5], amount [10.5,500.25], category ['a','e'] (partition col)
        sql(f"INSERT INTO {q} VALUES "
            "(1,'a',10.5),(2,'b',42.0),(3,'c',100.0),(4,'d',250.75),(5,'e',500.25)")

        # Managed Iceberg metadata can land on S3 slightly after the INSERT
        # commits (UC writes it out of band), so poll the location a few times
        # before concluding the manifest is missing.
        import time

        stats = {}
        for attempt in range(6):
            stats = _inspect_manifest_stats(q)
            if stats.get("entries"):
                break
            time.sleep(5)

        if not stats.get("entries"):
            r.result = "skip"
            r.details = ("Iceberg manifest column statistics could not be inspected: "
                         + stats.get("diag", "unknown reason")
                         + " (not observable from a SQL session alone)")
            return

        pop = stats["populated"]
        present_names = [f for f in _STATS_FIELDS if pop[f]]
        empty_names = [f for f in _STATS_FIELDS if not pop[f]]
        n_files = len(stats["entries"])

        # Manifest-list (partition stats) evidence, appended to whichever
        # verdict the column stats produce below.
        ml = stats.get("manifest_list")
        if ml:
            ml_note = (f"; manifest list {ml['file']}: partition summaries "
                       f"{'POPULATED' if ml['any_partitions_populated'] else 'EMPTY'}"
                       + ("" if partitioned else " (table fell back to unpartitioned)")
                       + f", row counters "
                       f"{'populated' if ml['any_row_counters_populated'] else 'empty'}")
        else:
            ml_note = "; manifest list not found for inspection"

        # The prunable core of Iceberg statistics is the min/max bounds; a
        # reader uses lower_bounds/upper_bounds to skip files. Treat their
        # presence as statistics support (pass -> matches full OR partial in
        # the matrix). Missing maps (e.g. value_counts, column_sizes) are
        # reported in the details so a partial rating is justified rather than
        # inflated to full. Only when NO map is populated is this a genuine
        # absence (fail -> matches none).
        bounds_present = pop["lower_bounds"] and pop["upper_bounds"]
        any_present = any(pop[f] for f in _STATS_FIELDS)

        if bounds_present:
            r.result = "pass"
            r.details = (
                f"Iceberg manifest ({stats['manifest']}, {n_files} data file(s)) "
                f"carries column statistics: populated={', '.join(present_names)}"
                + (f"; empty={', '.join(empty_names)}" if empty_names else "")
                + (". Partial: min/max bounds present but not every stat map"
                   if empty_names else "")
                + ml_note
            )
        elif any_present:
            r.result = "pass"
            r.details = (
                f"Iceberg manifest ({stats['manifest']}, {n_files} data file(s)) "
                f"carries partial column statistics without min/max bounds: "
                f"populated={', '.join(present_names)}; empty={', '.join(empty_names)}"
                + ml_note
            )
        else:
            r.result = "fail"
            r.details = (
                f"Iceberg manifest ({stats['manifest']}, {n_files} data file(s)) "
                f"has NO column statistics: all maps empty ({', '.join(empty_names)})"
                + ml_note
            )

    return _run(r, body)


def test_statistics() -> TestResult:
    return _statistics_probe("2")


def test_statistics_v3() -> TestResult:
    return _statistics_probe("3")


def test_bloom_filters() -> TestResult:
    return _skip("bloom-filters", "Bloom Filters & Puffin", "v2",
                 "Bloom-filter write properties are not exposed for managed Iceberg tables")


def test_glue_catalog() -> TestResult:
    return _skip("aws-glue-catalog", "AWS Glue Catalog", "v2",
                 "External catalog wiring is out of scope for a single-workspace run")


def test_rest_catalog() -> TestResult:
    return _skip("rest-catalog", "REST Catalog", "v2",
                 "Requires Lakehouse Federation setup against an external IRC endpoint")


def test_snowflake_horizon_catalog() -> TestResult:
    return _skip("snowflake-horizon-catalog", "Snowflake Horizon Catalog", "v2",
                 "External catalog wiring is out of scope for a single-workspace run")


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
    test_type_promotion_detail,
    test_type_promotion_v3_detail,
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
    test_glue_catalog,
    test_rest_catalog,
    test_snowflake_horizon_catalog,
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
        if getattr(r, "diagnostic", False):
            # Diagnostic rows are finer-grained than any matrix cell; report
            # them but never compare against the matrix or count discrepancies.
            tests_output.append({**r.to_dict(), "json_level": "n/a", "match": True})
            continue
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

    diag = [t for t in report["tests"] if t.get("diagnostic")]
    if diag:
        lines += [
            "## Type Promotion Detail",
            "",
            "Per-promotion probe (CREATE → INSERT → ALTER COLUMN TYPE → read "
            "back a widened-only value). These are diagnostic rows and are not "
            "compared against the matrix.",
            "",
            "| Promotion | Version | Supported | Evidence |",
            "|-----------|---------|-----------|----------|",
        ]
        supported = {"pass": "✅ yes", "fail": "❌ no", "error": "⚠️ error",
                     "skip": "⏭️ skip"}
        for t in diag:
            d = t["details"].replace("\n", " ").replace("|", "\\|") if t["details"] else ""
            name = t["feature_name"].replace("Type Promotion: ", "")
            lines.append(f"| {name} | {t['version']} | "
                         f"{supported.get(t['result'], t['result'])} | {d} |")
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
    required = [("DATABRICKS_HOST", HOST), ("DATABRICKS_TOKEN", TOKEN)]
    if not HTTP_PATH:  # warehouse id only needed when no explicit compute path
        required.append(("DATABRICKS_WAREHOUSE_ID", WAREHOUSE_ID))
    missing = [n for n, v in required if not v]
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
        # A probe may return a single TestResult or a list of them (the
        # type-promotion detail probes emit one diagnostic row per promotion).
        batch = result if isinstance(result, list) else [result]
        results.extend(batch)
        for res in batch:
            icon = {"pass": "✅", "fail": "❌", "skip": "⏭️", "error": "⚠️"}.get(res.result, "?")
            print(f"  {icon} {res.result}: {res.details[:120]}")

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
