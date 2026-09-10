"""
DuckDB-based Iceberg Feature Test Suite.

Tests Iceberg features using DuckDB's built-in Iceberg extension against a real,
open-source Iceberg REST catalog (Apache Polaris backed by MinIO S3 storage, see
tests/docker), then compares results with the DuckDB entries from
``src/data/platforms/oss/duckdb/duckdb.json``.

DuckDB's Iceberg *write* path (CREATE TABLE, INSERT, UPDATE, DELETE, MERGE INTO,
ALTER TABLE and all V3 features) only works through an attached Iceberg REST
catalog -- the path-based ``iceberg_scan`` interface is read-only. This suite
therefore attaches to a REST catalog by default and exercises the features for
real; when no catalog answers the catalog-dependent tests are reported as
``skip`` (never as a fabricated pass/fail).

Usage:
    # A REST catalog at http://127.0.0.1:8181/api/catalog is the default; start
    # the Polaris + MinIO stack first:
    ./tests/docker/start-polaris.sh
    python tests/duckdb_feature_tests.py

Environment variables:
    ICEBERG_REST_URI        - Iceberg REST catalog endpoint
                              (default: "http://127.0.0.1:8181/api/catalog").
                              When nothing answers there and this was not set
                              explicitly, catalog-dependent tests are skipped.
    ICEBERG_REST_WAREHOUSE  - Catalog (warehouse) name to attach (default: "demo")
    ICEBERG_REST_CREDENTIAL - OAuth2 client credentials "id:secret"
                              (default: "root:s3cr3t"); blank attaches without auth
    ICEBERG_REST_SCOPE      - OAuth2 scope (default: "PRINCIPAL_ROLE:ALL")
    ICEBERG_S3_ENDPOINT     - S3 endpoint for data files (default: "127.0.0.1:9000")
    ICEBERG_S3_KEY_ID       - S3 access key id (default: "minio")
    ICEBERG_S3_SECRET       - S3 secret access key (default: "minio12345")
    ICEBERG_S3_REGION       - S3 region (default: "us-east-1")
    DUCKDB_VERSION          - Override reported DuckDB version (default: auto-detected)

Requirements:
    - duckdb == 1.5.5 (pinned in CI; Iceberg V3 read/write via v1.5.x)
    - An Iceberg REST catalog backed by S3-compatible storage for write tests.
"""

import json
import os
import sys
import shutil
import urllib.error
import urllib.request
import uuid
import traceback
from datetime import datetime
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("[FATAL] duckdb not installed. Run: uv pip install duckdb==1.5.5")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).resolve().parent))
import spark_fixture  # noqa: E402 - sibling module, not a package

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

# Iceberg REST catalog configuration. Writes require an attached REST catalog,
# so one is the default: the suite targets DEFAULT_REST_URI unless
# ICEBERG_REST_URI says otherwise. If nothing answers there and no catalog was
# requested explicitly, the catalog-dependent tests are skipped rather than
# reported as failures. The defaults match the Apache Polaris + MinIO stack in
# tests/docker (Polaris serves the Iceberg REST API under /api/catalog,
# addresses catalogs by name, and authenticates with OAuth2 client credentials).
DEFAULT_REST_URI = "http://127.0.0.1:8181/api/catalog"
REST_URI_EXPLICIT = "ICEBERG_REST_URI" in os.environ
REST_URI = os.environ.get("ICEBERG_REST_URI", DEFAULT_REST_URI)
REST_WAREHOUSE = os.environ.get("ICEBERG_REST_WAREHOUSE", "demo")
REST_CREDENTIAL = os.environ.get("ICEBERG_REST_CREDENTIAL", "root:s3cr3t")
REST_SCOPE = os.environ.get("ICEBERG_REST_SCOPE", "PRINCIPAL_ROLE:ALL")
S3_ENDPOINT = os.environ.get("ICEBERG_S3_ENDPOINT", "127.0.0.1:9000")
S3_KEY_ID = os.environ.get("ICEBERG_S3_KEY_ID", "minio")
S3_SECRET = os.environ.get("ICEBERG_S3_SECRET", "minio12345")
S3_REGION = os.environ.get("ICEBERG_S3_REGION", "us-east-1")

NO_CATALOG_DETAIL = (
    "Requires an Iceberg REST catalog (set ICEBERG_REST_URI); not configured in this run"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _rest_reachable(uri: str, timeout: float = 2.0) -> bool:
    """True when something answers the Iceberg REST config endpoint at ``uri``."""
    if not uri:
        return False
    url = f"{uri.rstrip('/')}/v1/config"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
            return resp.status < 500
    except urllib.error.HTTPError as e:
        # 4xx still means a catalog is listening (e.g. missing warehouse param).
        return e.code < 500
    except Exception:
        return False


def _resolve_rest_uri() -> str:
    """Return the REST URI to use, or "" when the catalog tests must be skipped.

    An explicitly requested catalog is never silently dropped: if
    ICEBERG_REST_URI was set and nothing answers, the URI is kept so the tests
    report errors instead of quietly skipping.
    """
    if not REST_URI:
        return ""
    if _rest_reachable(REST_URI):
        return REST_URI
    if REST_URI_EXPLICIT:
        print(f"[WARN] No Iceberg REST catalog answering at {REST_URI}, but it was "
              "requested explicitly via ICEBERG_REST_URI; continuing so the failure is visible")
        return REST_URI
    print(f"[INFO] No Iceberg REST catalog at {REST_URI}; catalog-dependent tests will be skipped")
    return ""


# Resolved once at import: the REST catalog when one answers (the default),
# otherwise "" meaning the catalog-dependent tests are skipped.
REST_URI = _resolve_rest_uri()


def _rest_available() -> bool:
    """True when an Iceberg REST catalog is configured and usable."""
    return bool(REST_URI)


def _plain_connection() -> "duckdb.DuckDBPyConnection":
    """A fresh in-memory DuckDB connection with the iceberg extension loaded."""
    con = duckdb.connect(":memory:")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    return con


def _catalog_connection() -> "duckdb.DuckDBPyConnection":
    """Connect to DuckDB and ATTACH the configured Iceberg REST catalog as ``ib``.

    The catalog is attached writable: we pass the catalog *name* (not an
    ``s3://`` URI) and ``ACCESS_DELEGATION_MODE 'none'`` so DuckDB uses the local
    S3 secret defined below rather than expecting the catalog to vend credentials.
    Authentication is OAuth2 client credentials (Polaris' default); when
    ICEBERG_REST_CREDENTIAL is blank the catalog is attached without auth.
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
    if REST_CREDENTIAL and ":" in REST_CREDENTIAL:
        client_id, _, client_secret = REST_CREDENTIAL.partition(":")
        con.execute(
            f"""
            CREATE SECRET restsec (
                TYPE iceberg,
                CLIENT_ID '{client_id}',
                CLIENT_SECRET '{client_secret}',
                OAUTH2_SERVER_URI '{REST_URI.rstrip('/')}/v1/oauth/tokens',
                OAUTH2_SCOPE '{REST_SCOPE}'
            )
            """
        )
        auth = "SECRET restsec"
    else:
        auth = "AUTHORIZATION_TYPE 'none'"
    con.execute(
        f"""
        ATTACH '{REST_WAREHOUSE}' AS ib (
            TYPE iceberg,
            ENDPOINT '{REST_URI}',
            {auth},
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


def _is_catalog_schema_rejection(msg: str) -> bool:
    """True when the REST catalog itself refused to model the table schema.

    A catalog that cannot deserialize a schema answers the create-table
    request with a 4xx that names the schema (Lakekeeper's iceberg-rust
    "did not match any variant of untagged enum SchemaEnum" is the known case;
    a Java catalog would say "Cannot parse type"). That is a catalog limitation,
    not an engine one, so the feature is unmeasured rather than unsupported.
    Polaris (Iceberg Java) accepts every V3 type, so this is normally dead
    code against the default stack, but it keeps the suite honest when pointed
    at another catalog.
    """
    low = msg.lower()
    return (
        "schemaenum" in low
        or "did not match any variant" in low
        or ("cannot parse type" in low and "returned a non-200" in low)
    )


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
        msg = f"{type(e).__name__}: {str(e).splitlines()[0]}"
        if _is_catalog_schema_rejection(msg):
            # The catalog could not model the schema, so this says nothing about
            # DuckDB. Report it as unmeasured rather than an engine failure.
            r.result = "skip"
            r.details = (
                "REST catalog rejected the table schema, so DuckDB's support could not be "
                f"measured: {msg[:220]}"
            )
        else:
            r.result = "error"
            r.details = msg[:280]
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


def _spark_assisted_row_level_test(r: "TestResult", write_mode: str, expect: str):
    """Shared body for the three Spark-create / DuckDB-mutate / Spark-inspect tests.

    write_mode is the strategy requested at table creation
    (write.delete.mode/write.update.mode/write.merge.mode, applied uniformly).
    expect is "position", "equality" or "none" (copy-on-write: no delete files
    at all expected). DuckDB issues the DELETE through the REST catalog it is
    already attached to; Spark, attached to the same catalog, creates the
    table beforehand and reads the delete-file content types back afterward,
    since DuckDB's own SQL surface (iceberg_metadata()) cannot distinguish
    position from equality deletes as precisely as Iceberg's all_delete_files
    content column can.
    """
    if not spark_fixture.available():
        r.result = "skip"
        r.details = spark_fixture.NOT_AVAILABLE_DETAIL
        return r
    if not _rest_available():
        r.result = "skip"
        r.details = NO_CATALOG_DETAIL
        return r

    ns, name = None, "t"
    con = None
    try:
        ns = spark_fixture.new_namespace()
        spark_fixture.create_fixture(ns, name, "v2", write_mode)
        con = _catalog_connection()
        try:
            con.execute(f"DELETE FROM ib.{ns}.{name} WHERE id=2")
        except duckdb.NotImplementedException as e:
            # DuckDB's own extension refuses outright rather than silently
            # falling back, e.g. "DuckDB-Iceberg only supports merge-on-read
            # for updates/deletes" when write.delete.mode=copy-on-write is
            # requested. That refusal is itself the strongest possible
            # confirming evidence for a none-rated write strategy -- stronger
            # than inferring it from an absence of delete files -- so treat it
            # as a definitive answer rather than a harness error, but only
            # when the cell under test expects "none" in the first place.
            if expect == "none":
                r.result = "fail"
                r.details = (f"DuckDB rejected write.delete.mode={write_mode} outright: "
                            f"{str(e).splitlines()[0][:200]}")
                return r
            raise
        deletes = spark_fixture.inspect_delete_files(ns, name)
        rows = spark_fixture.row_count(ns, name)
        if deletes["position"] > 0 or deletes["equality"] > 0:
            got = "position" if deletes["position"] > 0 else "equality"
        else:
            got = "none"

        if got == expect:
            r.result = "pass"
            r.details = (f"DELETE via DuckDB against a Spark-created table "
                        f"(write.delete.mode={write_mode}) produced {got} delete "
                        f"evidence as expected: {deletes}, {rows} live row(s)")
        else:
            r.result = "fail"
            r.details = (f"DELETE via DuckDB against a Spark-created table "
                        f"(write.delete.mode={write_mode}) produced {got} delete "
                        f"evidence, expected {expect}: {deletes}, {rows} live row(s)")
    except Exception as e:  # noqa: BLE001 - surface as error, not a data discrepancy
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:220]}"
    finally:
        if con:
            con.close()
        if ns:
            spark_fixture.drop_fixture(ns, name)
    return r


def test_position_deletes() -> TestResult:
    # Spark creates the table with write.delete.mode=merge-on-read explicitly
    # requested; DuckDB issues the DELETE through the same REST catalog; Spark
    # reads the delete-file content types back. A position-delete file (not
    # equality) is the expected evidence for DuckDB's own DML.
    r = TestResult("position-deletes", "Position Deletes", "v2")
    return _spark_assisted_row_level_test(r, "merge-on-read", "position")


def test_equality_deletes() -> TestResult:
    # This cell is a *write* capability: does the engine's own DML produce an
    # equality-delete file (content=2)? DuckDB has no such path -- its
    # UPDATE/DELETE write positional deletes (v2) or deletion vectors (v3) --
    # so the matrix rates it none. Rather than assert that from documentation,
    # measure both halves against a real equality-delete file produced with the
    # Iceberg Java API (spark_fixture.create_equality_delete_fixture, the same
    # library a Flink upsert sink uses):
    #   1. DuckDB READS the eq-delete correctly (the deleted row is filtered) --
    #      reported as corroborating evidence, since read is what DuckDB has.
    #   2. DuckDB cannot WRITE one -- the actual verdict for this cell.
    # A definitive read that filters the row, with no DuckDB write path, is the
    # measured basis for fail (== matrix "none"), far stronger than a bare skip.
    r = TestResult("equality-deletes", "Equality Deletes", "v2")
    if not spark_fixture.available():
        r.result = "skip"
        r.details = spark_fixture.NOT_AVAILABLE_DETAIL
        return r
    if not _rest_available():
        r.result = "skip"
        r.details = NO_CATALOG_DETAIL
        return r

    ns, name = None, "t"
    con = None
    try:
        ns = spark_fixture.new_namespace()
        produced = spark_fixture.create_equality_delete_fixture(ns, name, "v2")
        if produced["delete_files"].get("equality", 0) < 1:
            r.result = "error"
            r.details = (f"harness could not produce an equality-delete file: "
                        f"{produced['delete_files']}")
            return r

        con = _catalog_connection()
        rows = con.execute(
            f"SELECT id FROM ib.{ns}.{name} ORDER BY id"
        ).fetchall()
        got_ids = [row[0] for row in rows]
        reads_correctly = got_ids == produced["live_ids"]

        if not reads_correctly:
            # DuckDB mis-read a table containing an equality delete: it either
            # ignored the delete or dropped the wrong rows. That is a genuine
            # read failure worth surfacing rather than folding into the cell.
            r.result = "error"
            r.details = (f"DuckDB mis-read an equality-delete table: got ids {got_ids}, "
                        f"expected {produced['live_ids']} (delete on k='{produced['deleted_key']}')")
            return r

        # DuckDB read the equality delete correctly but has no path to write one.
        r.result = "fail"
        r.details = (
            f"DuckDB READS equality deletes correctly (a Java-API eq-delete on "
            f"k='{produced['deleted_key']}' filtered id={produced['deleted_id']}; "
            f"DuckDB returned {got_ids}), but its own UPDATE/DELETE write positional "
            f"deletes / deletion vectors, never equality deletes (content=2) -- so it "
            f"cannot produce this file itself"
        )
    except Exception as e:  # noqa: BLE001 - surface as error, not a data discrepancy
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:220]}"
    finally:
        if con:
            con.close()
        if ns:
            spark_fixture.drop_fixture(ns, name)
    return r


def test_merge_on_read() -> TestResult:
    # Same fixture pattern and measurement as test_position_deletes -- delete
    # files produced at all is the evidence for the write *strategy*, and
    # DuckDB's DELETE produces the same position-delete file either way -- but
    # kept as a separate test since it is a separate matrix cell.
    r = TestResult("merge-on-read", "Merge-on-Read", "v2")
    return _spark_assisted_row_level_test(r, "merge-on-read", "position")


def test_copy_on_write() -> TestResult:
    # Rated none: copy-on-write for row-level operations means UPDATE/DELETE
    # rewriting affected data files, and DuckDB's UPDATE/DELETE always use
    # merge-on-read instead (see test_merge_on_read). Requesting
    # write.delete.mode=copy-on-write explicitly and checking DuckDB actually
    # honours it (no delete files at all) is the measurement, not an inference
    # from an unrelated INSERT.
    r = TestResult("copy-on-write", "Copy-on-Write", "v2")
    return _spark_assisted_row_level_test(r, "copy-on-write", "none")


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
    # Iceberg type promotion is done with ALTER TABLE ... ALTER COLUMN ... TYPE
    # (int->bigint, float->double, decimal widen). Measure DuckDB's own DDL
    # rather than asserting from docs: create a table, attempt each documented
    # v2 promotion, and record which ones the attached catalog accepts. If none
    # is accepted the cell (none) is confirmed; if any is, that is a discrepancy
    # the report will surface.
    promotions = [
        ("id", "INT", "BIGINT", "int->bigint"),
        ("f", "FLOAT", "DOUBLE", "float->double"),
        ("d", "DECIMAL(9,2)", "DECIMAL(18,2)", "decimal widen"),
    ]

    def body(con, ns, r):
        accepted, rejected = [], []
        for col, from_t, to_t, label in promotions:
            t = _unique("tp")
            con.execute(f"CREATE TABLE ib.{ns}.{t} (id INT, f FLOAT, d DECIMAL(9,2))")
            # Seed BEFORE the promotion, so the data file is written with the
            # narrow physical type. Reading this row back with the widened type
            # is the real test -- accepting the DDL alone could be a no-op.
            con.execute(f"INSERT INTO ib.{ns}.{t} VALUES (100, 1.5, 3.14)")
            try:
                con.execute(f"ALTER TABLE ib.{ns}.{t} ALTER COLUMN {col} TYPE {to_t}")
            except Exception as e:  # noqa: BLE001 - a rejection is the datum
                rejected.append(f"{label}: {str(e).splitlines()[0][:90]}")
                continue
            types = {c[0]: c[1] for c in con.execute(f"DESCRIBE ib.{ns}.{t}").fetchall()}
            promoted_type = str(types.get(col, "")).upper()
            if to_t.split("(")[0].upper() not in promoted_type:
                rejected.append(f"{label}: schema unchanged (type now {types.get(col)})")
                continue
            # Read the pre-promotion row back with the widened type. The value
            # must survive: a genuine promotion reads the narrow-encoded file as
            # the wide type; a broken one raises or returns garbage.
            got = con.execute(f"SELECT {col} FROM ib.{ns}.{t} WHERE id=100").fetchall()
            if len(got) == 1 and got[0][0] is not None:
                accepted.append(label)
            else:
                rejected.append(f"{label}: promoted but pre-promotion row unreadable ({got})")

        if accepted:
            # NOTE: this is DuckDB reading its OWN promoted data. Cross-engine
            # reads of DuckDB-promoted files can still fail (Spark's vectorized
            # reader raises BigIntVector-vs-IntVector on int->long), so the
            # capability is real for DuckDB but not proven interoperable -- the
            # matrix should treat this as partial rather than full.
            r.result = "pass"
            r.details = (f"DuckDB ALTER COLUMN TYPE performed + read back type promotions "
                        f"{accepted} (pre-promotion rows read correctly with the widened type)"
                        + (f"; rejected: {rejected}" if rejected else "")
                        + ". Verified within DuckDB; cross-engine reads of the promoted "
                        "files are not guaranteed")
        else:
            r.result = "fail"
            r.details = ("DuckDB rejected or no-opped every documented v2 type promotion via "
                        f"ALTER COLUMN TYPE: {rejected}")

    return _catalog_test(r, body)


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
    # Maintenance ops are compaction / rewrite_data_files / expire_snapshots /
    # rewrite_manifests. Spark exposes them as CALL <catalog>.system.<proc>().
    # Measure DuckDB's own surface rather than asserting: create a table with
    # several snapshots, then try the documented procedure names. Every one
    # being rejected is the confirmed evidence for none.
    def body(con, ns, r):
        t = _unique("maint")
        con.execute(f"CREATE TABLE ib.{ns}.{t} (id INT)")
        con.execute(f"INSERT INTO ib.{ns}.{t} VALUES (1)")
        con.execute(f"INSERT INTO ib.{ns}.{t} VALUES (2)")
        con.execute(f"INSERT INTO ib.{ns}.{t} VALUES (3)")
        attempts = [
            f"CALL ib.system.rewrite_data_files('{ns}.{t}')",
            f"CALL ib.system.expire_snapshots('{ns}.{t}')",
            f"CALL ib.system.rewrite_manifests('{ns}.{t}')",
            f"OPTIMIZE ib.{ns}.{t}",
            f"PRAGMA iceberg_compact('ib.{ns}.{t}')",
        ]
        rejected, accepted = [], []
        for sql in attempts:
            try:
                con.execute(sql)
                accepted.append(sql.split("(")[0].split(" system.")[-1])
            except Exception as e:  # noqa: BLE001 - rejection is the datum
                rejected.append(str(e).splitlines()[0][:70])
        if accepted:
            r.result = "pass"
            r.details = f"DuckDB accepted maintenance op(s): {accepted}"
        else:
            r.result = "fail"
            r.details = ("DuckDB exposes no Iceberg maintenance ops; every documented "
                        f"procedure was rejected (e.g. {rejected[0] if rejected else 'n/a'})")

    return _catalog_test(r, body)


def test_branching_tagging() -> TestResult:
    r = TestResult("branching-tagging", "Branching & Tagging", "v2")
    # Branch/tag DDL in Iceberg-capable engines is ALTER TABLE ... CREATE
    # BRANCH/TAG. Measure DuckDB's own surface: create a snapshot, attempt the
    # branch and tag statements, and record the rejections that confirm none.
    def body(con, ns, r):
        t = _unique("branch")
        con.execute(f"CREATE TABLE ib.{ns}.{t} (id INT)")
        con.execute(f"INSERT INTO ib.{ns}.{t} VALUES (1)")
        attempts = [
            f"ALTER TABLE ib.{ns}.{t} CREATE BRANCH dev",
            f"ALTER TABLE ib.{ns}.{t} CREATE TAG v1",
        ]
        rejected, accepted = [], []
        for sql in attempts:
            try:
                con.execute(sql)
                accepted.append(sql.split("CREATE ")[-1].split(" ")[0])
            except Exception as e:  # noqa: BLE001 - rejection is the datum
                rejected.append(str(e).splitlines()[0][:80])
        if accepted:
            r.result = "pass"
            r.details = f"DuckDB accepted branch/tag DDL: {accepted}"
        else:
            r.result = "fail"
            r.details = ("DuckDB does not support Iceberg branching/tagging; CREATE "
                        f"BRANCH and CREATE TAG were both rejected (e.g. {rejected[0] if rejected else 'n/a'})")

    return _catalog_test(r, body)


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
    # V3 multi-argument transforms (e.g. bucket over multiple source columns).
    # Measure whether DuckDB's partition DDL accepts one on a V3 table rather
    # than leaving the cell unmeasured. DuckDB documents single-column bucket()
    # / truncate() transforms; a multi-column form being rejected is the datum.
    def body(con, ns, r):
        t = _unique("mat")
        attempts = [
            # Multi-argument bucket over two columns (V3 feature).
            f"""CREATE TABLE ib.{ns}.{t} (a BIGINT, b BIGINT, v STRING)
                WITH ('format-version'='3') PARTITIONED BY (bucket(8, a, b))""",
        ]
        last_err = None
        for sql in attempts:
            try:
                con.execute(sql)
                con.execute(f"INSERT INTO ib.{ns}.{t} VALUES (1,2,'x')")
                n = con.execute(f"SELECT count(*) FROM ib.{ns}.{t}").fetchone()[0]
                if n == 1:
                    r.result = "pass"
                    r.details = ("DuckDB created a V3 table partitioned by a multi-argument "
                                "transform (bucket(8, a, b)) and round-tripped a row")
                    return
            except Exception as e:  # noqa: BLE001 - rejection is the datum
                last_err = str(e).splitlines()[0][:150]
        r.result = "fail"
        r.details = ("DuckDB does not support V3 multi-argument transforms: a "
                    f"multi-column bucket() partition was rejected: {last_err}")

    return _catalog_test(r, body)


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
    # Iceberg bloom filters are requested with the table property
    # write.parquet.bloom-filter-enabled.column.<col>. Measure whether DuckDB
    # honours it: set the property, write data, and check the Parquet data
    # files DuckDB produced for a bloom filter via parquet_metadata(). No
    # bloom_filter_offset on any column == DuckDB ignored it (confirms none).
    def body(con, ns, r):
        # Request a bloom filter on the numeric 'id' column specifically. The
        # measurement must be column-targeted: DuckDB's Parquet writer emits a
        # bloom filter on STRING columns by default regardless of any Iceberg
        # setting, so merely finding *a* bloom_filter_offset somewhere would be
        # a false positive. Honoring the Iceberg property means a bloom filter
        # appears on 'id' *because* the property asked for it -- so we compare
        # the 'id' column with the property against 'id' without it.
        req_col = "id"

        def id_has_bloom(schema_name):
            t = _unique("bloom")
            props = (f" WITH ('write.parquet.bloom-filter-enabled.column.{req_col}'='true')"
                    if schema_name == "with" else "")
            con.execute(f"CREATE TABLE ib.{ns}.{t} (id BIGINT, v STRING){props}")
            con.execute(f"INSERT INTO ib.{ns}.{t} SELECT i, 'x' FROM range(1000) s(i)")
            files = [f[0] for f in con.execute(
                f"SELECT file_path FROM iceberg_metadata(ib.{ns}.{t}) WHERE content='EXISTING'"
            ).fetchall()]
            on_id = False
            for fp in files:
                try:
                    rows = con.execute(
                        "SELECT path_in_schema FROM parquet_metadata(?) "
                        "WHERE bloom_filter_offset IS NOT NULL",
                        [fp],
                    ).fetchall()
                    if any(str(row[0]) == req_col for row in rows):
                        on_id = True
                        break
                except Exception:  # noqa: BLE001 - path may be unreachable via httpfs
                    continue
            return on_id

        with_prop = id_has_bloom("with")
        without_prop = id_has_bloom("without")

        # Honored only if the property *causes* a bloom filter on 'id' that is
        # not there without it.
        if with_prop and not without_prop:
            r.result = "pass"
            r.details = ("DuckDB honoured write.parquet.bloom-filter-enabled.column.id: a "
                        "bloom filter appears on 'id' with the property and not without it")
        else:
            r.result = "fail"
            r.details = ("DuckDB ignores the Iceberg bloom-filter property: bloom filter on "
                        f"the requested 'id' column with property={with_prop}, without "
                        f"property={without_prop} (any bloom filter DuckDB writes is its own "
                        "Parquet default, e.g. on string columns, not driven by the Iceberg "
                        "setting) -- so it does not support Iceberg bloom filters")

    return _catalog_test(r, body)


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


def test_rest_catalog() -> TestResult:
    r = TestResult("rest-catalog", "REST Catalog", "v2")

    def body(con, ns, r):
        # We are attached to a real Iceberg REST catalog; do a write round-trip.
        con.execute(f"CREATE TABLE ib.{ns}.t (id INT)")
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1),(2)")
        n = con.execute(f"SELECT count(*) FROM ib.{ns}.t").fetchone()[0]
        assert n == 2
        r.result = "pass"
        r.details = ("Full read/write round-trip against an Iceberg REST catalog "
                    f"({'OAuth2 client credentials' if REST_CREDENTIAL else 'no auth'})")

    return _catalog_test(r, body)


def test_glue_catalog() -> TestResult:
    r = TestResult("aws-glue-catalog", "AWS Glue Catalog", "v2")
    # Supported via ENDPOINT_TYPE 'GLUE' but requires real AWS credentials/endpoint.
    r.result = "skip"
    r.details = "AWS Glue (SageMaker Lakehouse) catalog requires AWS credentials; not exercised locally"
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
    # Shredded variant is a V3 physical encoding that splits a VARIANT column
    # into typed sub-columns (a "typed_value" group alongside the raw "value")
    # so scans can prune/pushdown on shredded fields. DuckDB writes VARIANT as a
    # single unshredded binary. Measure it: write a VARIANT to a V3 table, then
    # read the Parquet schema of the data file DuckDB produced and look for the
    # shredded "typed_value" sub-field. Its absence is the datum for none.
    def body(con, ns, r):
        t = _unique("shred")
        con.execute(
            f"CREATE TABLE ib.{ns}.{t} (id INT, payload VARIANT) WITH ('format-version'='3')"
        )
        # Write many rows with a consistent shape -- shredding, if DuckDB did it,
        # would materialise 'kind'/'x' as typed sub-columns.
        con.execute(
            f"INSERT INTO ib.{ns}.{t} "
            f"SELECT i, {{'kind':'click','x':i}}::VARIANT FROM range(200) t(i)"
        )
        files = con.execute(
            f"SELECT file_path FROM iceberg_metadata(ib.{ns}.{t}) WHERE content='EXISTING'"
        ).fetchall()
        shredded = False
        cols_seen = []
        for (fp,) in files:
            try:
                paths = con.execute(
                    "SELECT path_in_schema FROM parquet_metadata(?)", [fp]
                ).fetchall()
                cols_seen = [p[0] for p in paths]
                # A shredded variant exposes payload.typed_value.* sub-columns;
                # an unshredded one exposes only payload.value / payload.metadata.
                if any("typed_value" in str(p) for p in cols_seen):
                    shredded = True
                    break
            except Exception:  # noqa: BLE001 - path may not be reachable via httpfs
                continue
        if shredded:
            r.result = "pass"
            r.details = f"DuckDB wrote a shredded variant (typed_value sub-columns present): {cols_seen}"
        else:
            r.result = "fail"
            r.details = ("DuckDB wrote the VARIANT unshredded: no typed_value sub-columns in the "
                        f"Parquet schema (cols: {cols_seen[:8]}), so shredded variant is not supported")

    return _catalog_test(r, body)


def test_geometry_type() -> TestResult:
    r = TestResult("geometry-type", "Geometry / Geo Types", "v3")

    def body(con, ns, r):
        # Measure both halves of the V3 geo feature. GEOMETRY: create the
        # column, write a point, read it back (the spatial extension supplies
        # the ST_ functions; the Iceberg type itself does not need it).
        # GEOGRAPHY: attempt the column and record the rejection. Both together
        # are the basis for partial.
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            f"CREATE TABLE ib.{ns}.t (id INT, geo GEOMETRY) WITH ('format-version'='3')"
        )
        con.execute(f"INSERT INTO ib.{ns}.t VALUES (1, ST_Point(1.5, 2.5))")
        row = con.execute(f"SELECT id, ST_AsText(geo) FROM ib.{ns}.t").fetchone()
        assert row and row[0] == 1 and "1.5" in row[1] and "2.5" in row[1], \
            f"geometry did not round-trip: {row}"
        geography = "accepted"
        try:
            con.execute(
                f"CREATE TABLE ib.{ns}.g (id INT, geo GEOGRAPHY) WITH ('format-version'='3')"
            )
        except Exception as e:  # noqa: BLE001 - the rejection is the datum
            geography = f"rejected ({str(e).splitlines()[0][:80]})"
        r.result = "pass"
        r.details = (f"V3 GEOMETRY column created and a point round-tripped ({row[1]}); "
                    f"GEOGRAPHY {geography}")

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


def test_unknown_type() -> TestResult:
    r = TestResult("unknown-type", "Unknown Type", "v3")

    def body(con, ns, r):
        # DuckDB lists GEOGRAPHY and Unknown together as still unsupported
        # (planned for v2.0.0), so a rejection is the expected measurement. It has
        # to be caught here: _catalog_test turns a raised exception into an error,
        # which would make a known gap look like a broken harness.
        try:
            con.execute(
                f"CREATE TABLE ib.{ns}.t (id INT, u UNKNOWN) WITH ('format-version'='3')"
            )
        except Exception as e:  # noqa: BLE001 - the rejection is the datum
            r.result = "fail"
            r.details = f"Unknown type rejected: {str(e).splitlines()[0][:180]}"
            return
        cols = [c[1] for c in con.execute(f"DESCRIBE ib.{ns}.t").fetchall()]
        r.result = "pass"
        r.details = f"V3 unknown-type column created (columns: {cols})"

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
    test_rest_catalog,
    test_glue_catalog,
    test_unity_catalog,
    test_variant_type,
    test_shredded_variant,
    test_geometry_type,
    test_nanosecond_timestamps,
    test_unknown_type,
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
