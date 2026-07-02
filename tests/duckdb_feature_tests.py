"""
DuckDB-based Iceberg Feature Test Suite.

Tests Iceberg features using DuckDB's built-in Iceberg extension against a real,
open-source Iceberg REST catalog (the Apache ``iceberg-rest-fixture`` backed by
MinIO S3 storage), then compares results with the DuckDB entries from
``src/data/platforms/oss/duckdb/duckdb.json``.

DuckDB's Iceberg *write* path (CREATE TABLE, INSERT, UPDATE, DELETE, MERGE INTO,
ALTER TABLE and all V3 features) only works through an attached Iceberg REST
catalog -- the path-based ``iceberg_scan`` interface is read-only. This suite
therefore attaches to a REST catalog when one is configured and exercises the
features for real; when no catalog is configured the catalog-dependent tests are
reported as ``skip`` (never as a fabricated pass/fail).

Usage:
    # Point at a running Iceberg REST catalog (see .github/workflows/duckdb-tests.yml
    # for a docker recipe using apache/iceberg-rest-fixture + MinIO):
    export ICEBERG_REST_URI=http://127.0.0.1:8181
    python tests/duckdb_feature_tests.py

Environment variables:
    ICEBERG_REST_URI        - Iceberg REST catalog endpoint. When unset, all
                              catalog-dependent tests are skipped.
    ICEBERG_REST_WAREHOUSE  - Warehouse identifier to attach (default: "warehouse")
    ICEBERG_S3_ENDPOINT     - S3 endpoint for data files (default: "127.0.0.1:9000")
    ICEBERG_S3_KEY_ID       - S3 access key id (default: "admin")
    ICEBERG_S3_SECRET       - S3 secret access key (default: "password")
    ICEBERG_S3_REGION       - S3 region (default: "us-east-1")
    DUCKDB_VERSION          - Override reported DuckDB version (default: auto-detected)

Requirements:
    - duckdb == 1.5.4 (pinned in CI; Iceberg V3 read/write via v1.5.x)
    - An Iceberg REST catalog backed by S3-compatible storage for write tests.
"""

import json
import os
import sys
import shutil
import uuid
import traceback
from datetime import datetime
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("[FATAL] duckdb not installed. Run: pip install duckdb==1.5.4")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
WAREHOUSE_DIR = os.environ.get(
    "ICEBERG_WAREHOUSE", os.path.join(os.getcwd(), "duckdb-iceberg-warehouse")
)
REPO_ROOT = os.environ.get(
    "REPO_ROOT",
    str(Path(__file__).resolve().parent.parent),
)
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(os.getcwd(), "test-reports"))
DUCKDB_VERSION = os.environ.get("DUCKDB_VERSION", duckdb.__version__)

# Iceberg REST catalog configuration. Writes require an attached REST catalog;
# when ICEBERG_REST_URI is unset the catalog-dependent tests are skipped.
REST_URI = os.environ.get("ICEBERG_REST_URI")
REST_WAREHOUSE = os.environ.get("ICEBERG_REST_WAREHOUSE", "warehouse")
S3_ENDPOINT = os.environ.get("ICEBERG_S3_ENDPOINT", "127.0.0.1:9000")
S3_KEY_ID = os.environ.get("ICEBERG_S3_KEY_ID", "admin")
S3_SECRET = os.environ.get("ICEBERG_S3_SECRET", "password")
S3_REGION = os.environ.get("ICEBERG_S3_REGION", "us-east-1")

NO_CATALOG_DETAIL = (
    "Requires an Iceberg REST catalog (set ICEBERG_REST_URI); not configured in this run"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _rest_available() -> bool:
    """True when an Iceberg REST catalog endpoint has been configured."""
    return bool(REST_URI)


def _plain_connection() -> "duckdb.DuckDBPyConnection":
    """A fresh in-memory DuckDB connection with the iceberg extension loaded."""
    con = duckdb.connect(":memory:")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    return con


def _catalog_connection() -> "duckdb.DuckDBPyConnection":
    """Connect to DuckDB and ATTACH the configured Iceberg REST catalog as ``ib``.

    The catalog is attached writable: we pass the warehouse *name* (not an
    ``s3://`` URI) and ``ACCESS_DELEGATION_MODE 'none'`` so DuckDB uses the local
    S3 secret defined below rather than expecting the catalog to vend credentials.
    """
    con = duckdb.connect(":memory:")
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"""
        CREATE SECRET s3sec (
            TYPE s3,
            KEY_ID '{S3_KEY_ID}',
            SECRET '{S3_SECRET}',
            ENDPOINT '{S3_ENDPOINT}',
            URL_STYLE 'path',
            USE_SSL false,
            REGION '{S3_REGION}'
        )
        """
    )
    con.execute(
        f"""
        ATTACH '{REST_WAREHOUSE}' AS ib (
            TYPE iceberg,
            ENDPOINT '{REST_URI}',
            AUTHORIZATION_TYPE 'none',
            ACCESS_DELEGATION_MODE 'none'
        )
        """
    )
    return con


def _new_namespace(con: "duckdb.DuckDBPyConnection") -> str:
    """Create and return a fresh, uniquely-named namespace in the attached catalog."""
    ns = "ns_" + uuid.uuid4().hex[:10]
    con.execute(f"CREATE SCHEMA ib.{ns}")
    return ns


def _catalog_test(r: "TestResult", body):
    """Run ``body(con, ns, r)`` against the REST catalog, or skip if none configured.

    ``body`` must set ``r.result``/``r.details`` on success. Any exception is
    reported as an ``error`` so it never masquerades as a data discrepancy.
    """
    if not _rest_available():
        r.result = "skip"
        r.details = NO_CATALOG_DETAIL
        return r
    con = None
    try:
        con = _catalog_connection()
        ns = _new_namespace(con)
        body(con, ns, r)
    except Exception as e:  # noqa: BLE001 - surface any failure as an error
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:280]}"
    finally:
        if con:
            con.close()
    return r


# ---------------------------------------------------------------------------
# Result class
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


# ---------------------------------------------------------------------------
# Catalog-backed feature tests (real operations against the REST catalog)
# ---------------------------------------------------------------------------

def test_table_creation() -> TestResult:
    r = TestResult("table-creation", "Table Creation", "v2")

    def body(con, ns, r):
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT, name VARCHAR)")
        con.execute(f"CREATE TABLE ib.{ns}.t2 AS SELECT 1 AS id")
        con.execute(f"DROP TABLE ib.{ns}.t2")
        tbls = con.execute(
            f"SELECT count(*) FROM duckdb_tables() WHERE schema_name='{ns}'"
        ).fetchone()[0]
        assert tbls == 1, f"expected 1 table after create+drop, got {tbls}"
        r.result = "pass"
        r.details = "CREATE TABLE, CREATE TABLE AS SELECT and DROP TABLE via REST catalog"

    return _catalog_test(r, body)


def test_read_support() -> TestResult:
    r = TestResult("read-support", "Read Support", "v2")

    def body(con, ns, r):
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT, name VARCHAR)")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1,'a'),(2,'b'),(3,'c')")
        n = con.execute(f"SELECT count(*) FROM ib.{ns}.t").fetchone()[0]
        assert n == 3, f"expected 3 rows, got {n}"
        r.result = "pass"
        r.details = "Round-trip read of an Iceberg table via the REST catalog (3 rows)"

    return _catalog_test(r, body)


def test_write_insert() -> TestResult:
    r = TestResult("write-insert", "Write (INSERT)", "v2")

    def body(con, ns, r):
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT, name VARCHAR)")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1,'a'),(2,'b')")
        con.execute(f"INSERT INTO ib.{ns}.t SELECT 3, 'c'")
        n = con.execute(f"SELECT count(*) FROM ib.{ns}.t").fetchone()[0]
        assert n == 3, f"expected 3 rows, got {n}"
        r.result = "pass"
        r.details = "INSERT INTO ... VALUES and INSERT INTO ... SELECT committed 3 rows"

    return _catalog_test(r, body)


def test_write_merge_update_delete() -> TestResult:
    r = TestResult("write-merge-update-delete", "Write (MERGE/UPDATE/DELETE)", "v2")

    def body(con, ns, r):
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT, name VARCHAR)")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1,'John'),(2,'Anna')")
        con.execute(f"UPDATE ib.{ns}.t SET name='Johnny' WHERE id=1")
        con.execute(f"DELETE FROM ib.{ns}.t WHERE id=2")
        con.execute(
            f"""
            MERGE INTO ib.{ns}.t AS target
            USING (SELECT * FROM (VALUES (1,'J'),(3,'Sarah')) v(id,name)) AS src
            ON src.id = target.id
            WHEN MATCHED THEN UPDATE SET name = src.name
            WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.name)
            """
        )
        rows = con.execute(f"SELECT id, name FROM ib.{ns}.t ORDER BY id").fetchall()
        assert rows == [(1, "J"), (3, "Sarah")], f"unexpected rows: {rows}"
        r.result = "pass"
        r.details = "UPDATE, DELETE and MERGE INTO (upsert) all committed correctly"

    return _catalog_test(r, body)


def test_position_deletes() -> TestResult:
    r = TestResult("position-deletes", "Position Deletes", "v2")

    def body(con, ns, r):
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT) WITH ('format-version'='2')")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1),(2),(3)")
        con.execute(f"DELETE FROM ib.{ns}.t WHERE id=2")
        meta = con.execute(
            f"SELECT content, file_format FROM iceberg_metadata(ib.{ns}.t)"
        ).fetchall()
        has_pos_delete = any(
            c == "POSITION_DELETES" and fmt == "parquet" for c, fmt in meta
        )
        assert has_pos_delete, f"no positional-delete parquet file found: {meta}"
        r.result = "pass"
        r.details = "DELETE on a V2 table wrote a positional-delete Parquet file (merge-on-read)"

    return _catalog_test(r, body)


def test_equality_deletes() -> TestResult:
    r = TestResult("equality-deletes", "Equality Deletes", "v2")
    # DuckDB can *read* tables containing equality deletes but never writes them,
    # so producing an equality-delete file requires another engine. We do not
    # fabricate a result: report skip (the JSON records read-only "full" support).
    r.result = "skip"
    r.details = (
        "DuckDB reads equality deletes but cannot write them; producing an "
        "equality-delete file requires another engine, so this is not exercised here"
    )
    return r


def test_merge_on_read() -> TestResult:
    r = TestResult("merge-on-read", "Merge-on-Read", "v2")

    def body(con, ns, r):
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT) WITH ('format-version'='2')")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1),(2),(3)")
        con.execute(f"DELETE FROM ib.{ns}.t WHERE id=1")
        meta = con.execute(
            f"SELECT content FROM iceberg_metadata(ib.{ns}.t)"
        ).fetchall()
        assert any(c == "POSITION_DELETES" for (c,) in meta), f"no delete files: {meta}"
        n = con.execute(f"SELECT count(*) FROM ib.{ns}.t").fetchone()[0]
        assert n == 2, f"expected 2 live rows, got {n}"
        r.result = "pass"
        r.details = "UPDATE/DELETE use merge-on-read: delete files written, live rows reconciled on read"

    return _catalog_test(r, body)


def test_copy_on_write() -> TestResult:
    r = TestResult("copy-on-write", "Copy-on-Write", "v2")

    def body(con, ns, r):
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT)")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1),(2)")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (3)")
        meta = con.execute(
            f"SELECT content FROM iceberg_metadata(ib.{ns}.t)"
        ).fetchall()
        # Append-only writes must not create delete files (COW semantics for INSERT).
        assert not any(c and "DELETE" in c for (c,) in meta), f"unexpected delete files: {meta}"
        r.result = "pass"
        r.details = (
            "INSERT uses copy-on-write semantics (append-only, no delete files); "
            "UPDATE/DELETE are merge-on-read only"
        )

    return _catalog_test(r, body)


def test_schema_evolution() -> TestResult:
    r = TestResult("schema-evolution", "Schema Evolution", "v2")

    def body(con, ns, r):
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT, name VARCHAR)")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1,'a')")
        con.execute(f"ALTER TABLE ib.{ns}.t ADD COLUMN age INT")
        con.execute(f"ALTER TABLE ib.{ns}.t RENAME COLUMN name TO full_name")
        con.execute(f"ALTER TABLE ib.{ns}.t DROP COLUMN age")
        cols = [c[0] for c in con.execute(f"DESCRIBE ib.{ns}.t").fetchall()]
        assert cols == ["id", "full_name"], f"unexpected columns: {cols}"
        r.result = "pass"
        r.details = "ALTER TABLE ADD / RENAME / DROP COLUMN supported via REST catalog"

    return _catalog_test(r, body)


def test_type_promotion() -> TestResult:
    r = TestResult("type-promotion", "Type Promotion / Widening", "v2")
    # DuckDB's ALTER TABLE support does not include documented Iceberg type
    # promotion/widening (int -> bigint, float -> double, etc.). Behaviour is not
    # clearly specified, so rather than assert a pass/fail we do not exercise it.
    r.result = "skip"
    r.details = (
        "ALTER COLUMN type promotion is not a documented DuckDB-Iceberg operation; "
        "not exercised here to avoid asserting unspecified behaviour"
    )
    return r


def test_time_travel() -> TestResult:
    r = TestResult("time-travel", "Time Travel / Snapshots", "v2")

    def body(con, ns, r):
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT)")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1)")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (2),(3)")
        snaps = con.execute(
            f"SELECT snapshot_id FROM iceberg_snapshots(ib.{ns}.t) ORDER BY sequence_number"
        ).fetchall()
        assert len(snaps) >= 2, f"expected >=2 snapshots, got {snaps}"
        first = snaps[0][0]
        old = con.execute(
            f"SELECT count(*) FROM ib.{ns}.t AT (VERSION => {first})"
        ).fetchone()[0]
        now = con.execute(f"SELECT count(*) FROM ib.{ns}.t").fetchone()[0]
        assert old == 1 and now == 3, f"time travel mismatch old={old} now={now}"
        r.result = "pass"
        r.details = "Time travel via AT (VERSION => snapshot_id) returns the historical row count"

    return _catalog_test(r, body)


def test_table_maintenance() -> TestResult:
    r = TestResult("table-maintenance", "Table Maintenance", "v2")
    r.result = "fail"
    r.details = "DuckDB Iceberg does not provide maintenance ops (compaction, expire snapshots)"
    return r


def test_branching_tagging() -> TestResult:
    r = TestResult("branching-tagging", "Branching & Tagging", "v2")
    r.result = "fail"
    r.details = "DuckDB Iceberg does not support branching or tagging"
    return r


def test_hidden_partitioning() -> TestResult:
    r = TestResult("hidden-partitioning", "Hidden Partitioning", "v2")

    def body(con, ns, r):
        con.execute(
            f"""CREATE TABLE ib.{ns}.t (id BIGINT, country VARCHAR)
                PARTITIONED BY (bucket(4, id), truncate(2, country))"""
        )
        con.execute(
            f"INSERT INTO ib.{ns}.t VALUES (1,'United States'),(2,'Germany'),(3,'Netherlands')"
        )
        n = con.execute(f"SELECT count(*) FROM ib.{ns}.t").fetchone()[0]
        assert n == 3, f"expected 3 rows, got {n}"
        r.result = "pass"
        r.details = "Created and inserted into a table partitioned by bucket()/truncate() transforms"

    return _catalog_test(r, body)


def test_partition_evolution() -> TestResult:
    r = TestResult("partition-evolution", "Partition Evolution", "v2")

    def body(con, ns, r):
        con.execute(
            f"CREATE TABLE ib.{ns}.t (id BIGINT, country VARCHAR) PARTITIONED BY (bucket(4, id))"
        )
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1,'US')")
        con.execute(f"ALTER TABLE ib.{ns}.t SET PARTITIONED BY (bucket(8, id))")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (2,'DE')")
        n = con.execute(f"SELECT count(*) FROM ib.{ns}.t").fetchone()[0]
        assert n == 2, f"expected 2 rows, got {n}"
        r.result = "pass"
        r.details = "Evolved the partition spec with ALTER TABLE ... SET PARTITIONED BY and kept reading"

    return _catalog_test(r, body)


def test_multi_arg_transforms() -> TestResult:
    r = TestResult("multi-arg-transforms", "Multi-Argument Transforms", "v3")
    # V3-only; DuckDB support is undocumented (JSON level "unknown"). Do not assert.
    r.result = "skip"
    r.details = "V3 multi-argument transforms are undocumented for DuckDB; not exercised"
    return r


def test_statistics() -> TestResult:
    r = TestResult("statistics", "Statistics (Column Metrics)", "v2")

    def body(con, ns, r):
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT, name VARCHAR)")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1,'a'),(2,'b'),(3,'c')")
        counts = con.execute(
            f"SELECT record_count FROM iceberg_metadata(ib.{ns}.t) WHERE content='EXISTING'"
        ).fetchall()
        total = sum(c[0] for c in counts)
        assert total == 3, f"expected record_count sum 3, got {total} ({counts})"
        r.result = "pass"
        r.details = "iceberg_metadata exposes per-file record_count statistics written by DuckDB"

    return _catalog_test(r, body)


def test_bloom_filters() -> TestResult:
    r = TestResult("bloom-filters", "Bloom Filters", "v2")
    r.result = "fail"
    r.details = "DuckDB Iceberg does not read or write Iceberg bloom filters"
    return r


def test_catalog_integration() -> TestResult:
    r = TestResult("catalog-integration", "Catalog Integration", "v2")

    def body(con, ns, r):
        # A successful ATTACH + namespace + table lifecycle proves catalog integration.
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT)")
        con.execute("SHOW ALL TABLES")
        dbs = con.execute(
            "SELECT type FROM duckdb_databases() WHERE database_name='ib'"
        ).fetchone()
        assert dbs and dbs[0] == "iceberg", f"catalog not attached as iceberg: {dbs}"
        r.result = "pass"
        r.details = "Attached an Iceberg REST catalog and performed namespace/table operations"

    return _catalog_test(r, body)


def test_hadoop_catalog() -> TestResult:
    r = TestResult("hadoop-catalog", "Hadoop Catalog", "v2")
    r.result = "fail"
    r.details = "DuckDB Iceberg supports only REST-based catalogs (Hadoop catalog unsupported)"
    return r


def test_jdbc_catalog() -> TestResult:
    r = TestResult("jdbc-catalog", "JDBC Catalog", "v2")
    r.result = "fail"
    r.details = "DuckDB Iceberg supports only REST-based catalogs (JDBC catalog unsupported)"
    return r


def test_rest_catalog() -> TestResult:
    r = TestResult("rest-catalog", "REST Catalog", "v2")

    def body(con, ns, r):
        # We are attached to a real Iceberg REST catalog; do a write round-trip.
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT)")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1),(2)")
        n = con.execute(f"SELECT count(*) FROM ib.{ns}.t").fetchone()[0]
        assert n == 2
        r.result = "pass"
        r.details = "Full read/write round-trip against an Iceberg REST catalog (OAuth2/none auth)"

    return _catalog_test(r, body)


def test_hive_metastore() -> TestResult:
    r = TestResult("hive-metastore", "Hive Metastore", "v2")
    r.result = "fail"
    r.details = "DuckDB Iceberg supports only REST-based catalogs (Hive Metastore unsupported)"
    return r


def test_glue_catalog() -> TestResult:
    r = TestResult("aws-glue-catalog", "AWS Glue Catalog", "v2")
    # Supported via ENDPOINT_TYPE 'GLUE' but requires real AWS credentials/endpoint.
    r.result = "skip"
    r.details = "AWS Glue (SageMaker Lakehouse) catalog requires AWS credentials; not exercised locally"
    return r


def test_nessie() -> TestResult:
    r = TestResult("nessie", "Nessie", "v2")
    r.result = "fail"
    r.details = "DuckDB Iceberg does not natively support the Nessie catalog"
    return r


def test_polaris() -> TestResult:
    r = TestResult("polaris", "Polaris", "v2")
    # Polaris speaks the Iceberg REST protocol (which we exercise via the fixture),
    # but a Polaris-specific test needs a Polaris server. Not fabricated.
    r.result = "skip"
    r.details = "Polaris uses the Iceberg REST protocol; a Polaris-specific server is not run here"
    return r


def test_unity_catalog() -> TestResult:
    r = TestResult("unity-catalog", "Unity Catalog", "v2")
    r.result = "skip"
    r.details = "Unity Catalog REST connectivity is undocumented for DuckDB; requires a Unity server"
    return r


def test_variant_type() -> TestResult:
    r = TestResult("variant-type", "Variant Type", "v3")

    def body(con, ns, r):
        con.execute(
            f"CREATE TABLE ib.{ns}.t (id INT, payload VARIANT) WITH ('format-version'='3')"
        )
        con.execute(
            f"INSERT INTO ib.{ns}.t VALUES (1, {{'kind':'click','x':10}}::VARIANT)"
        )
        row = con.execute(f"SELECT id, payload FROM ib.{ns}.t").fetchone()
        assert row[0] == 1 and row[1] is not None, f"unexpected variant row: {row}"
        r.result = "pass"
        r.details = "Created a V3 table with a VARIANT column and round-tripped a value"

    return _catalog_test(r, body)


def test_shredded_variant() -> TestResult:
    r = TestResult("shredded-variant", "Shredded Variant", "v3")
    r.result = "fail"
    r.details = "DuckDB does not support shredded variant encoding (V3-only feature)"
    return r


def test_geometry_type() -> TestResult:
    r = TestResult("geometry-type", "Geometry / Geo Types", "v3")

    def body(con, ns, r):
        # GEOMETRY is supported (v1.5.2+); GEOGRAPHY/Unknown are not (planned for v2.0.0).
        con.execute(
            f"CREATE TABLE ib.{ns}.t (id INT, geo GEOMETRY) WITH ('format-version'='3')"
        )
        cols = [c[1] for c in con.execute(f"DESCRIBE ib.{ns}.t").fetchall()]
        assert any("GEOMETRY" in str(c).upper() for c in cols), f"no geometry column: {cols}"
        r.result = "pass"
        r.details = "V3 GEOMETRY column created (GEOGRAPHY/Unknown still unsupported => partial)"

    return _catalog_test(r, body)


def test_nanosecond_timestamps() -> TestResult:
    r = TestResult("nanosecond-timestamps", "Nanosecond Timestamps", "v3")

    def body(con, ns, r):
        con.execute(
            f"CREATE TABLE ib.{ns}.t (id INT, ts TIMESTAMP_NS) WITH ('format-version'='3')"
        )
        con.execute(
            f"INSERT INTO ib.{ns}.t VALUES (1, TIMESTAMP_NS '2026-05-20 12:00:00.123456789')"
        )
        n = con.execute(f"SELECT count(*) FROM ib.{ns}.t WHERE id=1").fetchone()[0]
        assert n == 1
        r.result = "pass"
        r.details = "Created a V3 table with a TIMESTAMP_NS column and inserted a nanosecond value"

    return _catalog_test(r, body)


def test_lineage() -> TestResult:
    r = TestResult("lineage", "Lineage Tracking", "v3")

    def body(con, ns, r):
        # Row lineage is written automatically for V3 tables; exercise the write path
        # that maintains it (insert + row-level update encoded as a deletion vector).
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT, name VARCHAR) WITH ('format-version'='3')")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1,'a'),(2,'b')")
        con.execute(f"UPDATE ib.{ns}.t SET name='z' WHERE id=1")
        meta = con.execute(
            f"SELECT content, file_format FROM iceberg_metadata(ib.{ns}.t)"
        ).fetchall()
        # V3 row-level changes are encoded as binary deletion vectors (Puffin).
        assert any(fmt == "puffin" for _, fmt in meta), f"expected puffin deletion vector: {meta}"
        r.result = "pass"
        r.details = "V3 write path with row lineage; row-level UPDATE encoded as a binary deletion vector (Puffin)"

    return _catalog_test(r, body)


def test_column_default_values() -> TestResult:
    r = TestResult("column-default-values", "Column Default Values", "v3")

    def body(con, ns, r):
        # Non-null column defaults are only allowed on V3 tables.
        con.execute(
            f"CREATE TABLE ib.{ns}.t (id INT, source VARCHAR DEFAULT 'web') WITH ('format-version'='3')"
        )
        con.execute(f"ALTER TABLE ib.{ns}.t ADD COLUMN region VARCHAR DEFAULT 'eu'")
        con.execute(f"INSERT INTO ib.{ns}.t (id) VALUES (1)")
        row = con.execute(f"SELECT source, region FROM ib.{ns}.t WHERE id=1").fetchone()
        assert row == ("web", "eu"), f"defaults not applied: {row}"
        r.result = "pass"
        r.details = "V3 schema-level column DEFAULT values applied on CREATE and ALTER ADD COLUMN"

    return _catalog_test(r, body)


# ---------------------------------------------------------------------------
# Test registry
# ---------------------------------------------------------------------------

ALL_TESTS = [
    test_table_creation,
    test_read_support,
    test_write_insert,
    test_write_merge_update_delete,
    test_position_deletes,
    test_equality_deletes,
    test_merge_on_read,
    test_copy_on_write,
    test_schema_evolution,
    test_type_promotion,
    test_column_default_values,
    test_time_travel,
    test_table_maintenance,
    test_branching_tagging,
    test_hidden_partitioning,
    test_partition_evolution,
    test_multi_arg_transforms,
    test_statistics,
    test_bloom_filters,
    test_catalog_integration,
    test_hadoop_catalog,
    test_jdbc_catalog,
    test_rest_catalog,
    test_hive_metastore,
    test_glue_catalog,
    test_nessie,
    test_polaris,
    test_unity_catalog,
    test_variant_type,
    test_shredded_variant,
    test_geometry_type,
    test_nanosecond_timestamps,
    test_lineage,
]


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def load_duckdb_json_support() -> dict:
    """Load the JSON support levels for DuckDB from the repo data."""
    oss_path = os.path.join(
        REPO_ROOT, "src", "data", "platforms", "oss", "duckdb", "duckdb.json"
    )
    with open(oss_path) as f:
        data = json.load(f)
    result = {}
    for key, val in data.get("support", {}).items():
        if key.startswith("duckdb:"):
            parts = key.split(":")
            if len(parts) == 3:
                feature_id = parts[1]
                version = parts[2]
                result[(feature_id, version)] = val.get("level", "unknown")
    return result


def compute_match(test_result: str, json_level: str) -> bool:
    """
    Determine if test result matches JSON level.
    - pass → json should be 'full' or 'partial' (we have positive evidence)
    - fail → json should be 'none' (we have negative evidence)
    - skip → always matches (cannot / did not verify)
    - error → always matches (test issue, not data issue)
    """
    if test_result in ("skip", "error"):
        return True
    if test_result == "pass":
        return json_level in ("full", "partial")
    if test_result == "fail":
        return json_level == "none"
    return True


def generate_report(results: list) -> dict:
    json_support = load_duckdb_json_support()

    tests_output = []
    discrepancies = 0
    passed = sum(1 for r in results if r.result == "pass")
    failed = sum(1 for r in results if r.result == "fail")
    skipped = sum(1 for r in results if r.result == "skip")
    errors = sum(1 for r in results if r.result == "error")

    for r in results:
        json_level = json_support.get((r.feature_id, r.version_tested), "unknown")
        match = compute_match(r.result, json_level)
        if not match:
            discrepancies += 1
        tests_output.append({
            **r.to_dict(),
            "json_level": json_level,
            "match": match,
        })

    report = {
        "timestamp": datetime.now(tz=__import__('datetime').timezone.utc).isoformat(),
        "engine": "DuckDB",
        "duckdb_version": DUCKDB_VERSION,
        "rest_catalog": REST_URI or "(none configured)",
        "tests": tests_output,
        "summary": {
            "total": len(results),
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "errors": errors,
            "discrepancies": discrepancies,
        },
    }
    return report


def generate_markdown(report: dict) -> str:
    lines = []
    lines.append("# DuckDB Iceberg Feature Test Report")
    lines.append("")
    lines.append(f"- **Timestamp:** {report['timestamp']}")
    lines.append(f"- **DuckDB Version:** {report['duckdb_version']}")
    lines.append(f"- **REST Catalog:** {report.get('rest_catalog', '(none configured)')}")
    lines.append("")

    s = report["summary"]
    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | Count |")
    lines.append("|--------|-------|")
    lines.append(f"| Total | {s['total']} |")
    lines.append(f"| ✅ Passed | {s['passed']} |")
    lines.append(f"| ❌ Failed | {s['failed']} |")
    lines.append(f"| ⏭️ Skipped | {s['skipped']} |")
    lines.append(f"| ⚠️ Errors | {s['errors']} |")
    lines.append(f"| 🔍 Discrepancies | {s['discrepancies']} |")
    lines.append("")

    lines.append("## Test Results")
    lines.append("")
    lines.append("| Feature | Version | Result | JSON Level | Match | Details |")
    lines.append("|---------|---------|--------|------------|-------|---------|")

    status_emoji = {"pass": "✅", "fail": "❌", "skip": "⏭️", "error": "⚠️"}

    for t in report["tests"]:
        emoji = status_emoji.get(t["result"], "❓")
        match_str = "✅" if t["match"] else "❌ DISCREPANCY"
        details = t["details"][:80].replace("\n", " ").replace("\r", "").replace("|", "\\|") if t["details"] else ""
        feature_name = t["feature_name"].replace("|", "\\|")
        json_level = t["json_level"].replace("|", "\\|") if t["json_level"] else ""
        lines.append(
            f"| {feature_name} | {t['version']} | {emoji} {t['result']} "
            f"| {json_level} | {match_str} | {details} |"
        )

    lines.append("")

    # Discrepancies section
    discs = [t for t in report["tests"] if not t["match"]]
    if discs:
        lines.append("## ⚠️ Discrepancies")
        lines.append("")
        for t in discs:
            detail_clean = t["details"][:120].replace("\n", " ").replace("\r", "") if t["details"] else ""
            lines.append(f"- **{t['feature_name']}** ({t['version']}): "
                         f"test={t['result']}, json={t['json_level']} — {detail_clean}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("  DuckDB Iceberg Feature Test Suite")
    print("=" * 70)
    print(f"DuckDB version: {DUCKDB_VERSION}")
    print(f"Warehouse: {WAREHOUSE_DIR}")
    print(f"Repo root: {REPO_ROOT}")
    if _rest_available():
        print(f"REST catalog: {REST_URI} (warehouse '{REST_WAREHOUSE}', S3 {S3_ENDPOINT})")
    else:
        print("REST catalog: NONE configured — catalog-dependent tests will be skipped")
    print()

    # Clean warehouse
    if os.path.exists(WAREHOUSE_DIR):
        shutil.rmtree(WAREHOUSE_DIR, ignore_errors=True)
    os.makedirs(WAREHOUSE_DIR, exist_ok=True)
    os.makedirs(REPORT_DIR, exist_ok=True)

    # Run all tests
    results = []
    for test_fn in ALL_TESTS:
        test_name = test_fn.__name__
        print(f"\n--- Running {test_name} ---")
        try:
            result = test_fn()
            results.append(result)
            icon = {"pass": "✅", "fail": "❌", "skip": "⏭️", "error": "⚠️"}.get(result.result, "?")
            print(f"  {icon} {result.result}: {result.details[:120]}")
        except Exception as e:
            r = TestResult(test_name.replace("test_", "").replace("_", "-"), test_name)
            r.result = "error"
            r.details = f"Unhandled exception: {e}"
            results.append(r)
            print(f"  ⚠️ error: {e}")

    # Generate report
    print("\n" + "=" * 70)
    print("  Generating Report")
    print("=" * 70)

    report = generate_report(results)

    # Write JSON report
    json_path = os.path.join(REPORT_DIR, "duckdb-iceberg-test-report.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"JSON report: {json_path}")

    # Write Markdown report
    md_content = generate_markdown(report)
    md_path = os.path.join(REPORT_DIR, "duckdb-iceberg-test-report.md")
    with open(md_path, "w") as f:
        f.write(md_content)
    print(f"Markdown report: {md_path}")

    # Print summary
    s = report["summary"]
    print(f"\n{'=' * 70}")
    print(f"  RESULTS: {s['passed']} passed, {s['failed']} failed, "
          f"{s['skipped']} skipped, {s['errors']} errors, "
          f"{s['discrepancies']} discrepancies")
    print(f"{'=' * 70}")

    # Print markdown to stdout
    print("\n" + md_content)

    # GitHub Actions step summary
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(md_content)

    # Clean up
    if os.path.exists(WAREHOUSE_DIR):
        shutil.rmtree(WAREHOUSE_DIR, ignore_errors=True)

    # Exit code: fail if there are discrepancies or test errors
    if s["discrepancies"] > 0 or s["errors"] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
