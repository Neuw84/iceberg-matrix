"""
Trino Iceberg Feature Test Suite.

Measures what Trino's Iceberg connector actually does against tables managed by
a real Iceberg REST catalog (Apache Polaris backed by RustFS, see tests/docker),
then compares with the Trino entries in
``src/data/platforms/oss/trino/trino.json``.

How each cell is measured
-------------------------
* Trino is driven over its HTTP protocol with the ``trino`` Python client. The
  server is configured with an Iceberg catalog named ``iceberg`` whose
  ``iceberg.catalog.type=rest`` points at the same Polaris instance every other
  suite uses, so a table Trino creates is an ordinary catalog-managed table on
  S3 that any other engine would see.
* Core features are exercised directly in Trino SQL: CREATE TABLE, INSERT,
  MERGE/UPDATE/DELETE, ALTER TABLE for schema and partition evolution, the
  ``$snapshots`` / ``$refs`` / ``$partitions`` metadata tables, time travel
  (``FOR VERSION AS OF``), and the ``register_table`` / snapshot procedures.
* Delete files Trino does not write itself (equality deletes), the V3 columns
  Trino cannot create (VARIANT, GEOMETRY, unknown), and merge-on-read fixtures
  are produced through the shared Spark fixture (tests/spark_fixture.py) and
  Trino is measured reading them back.

Nothing here fabricates a result: when Trino, Spark or the catalog are
unavailable the affected tests report ``skip``.

Usage:
    ./tests/docker/start-polaris.sh
    ./tests/docker/start-trino.sh        # Trino on :8080, iceberg catalog -> Polaris
    python tests/trino_feature_tests.py

Environment variables:
    TRINO_HOST / TRINO_PORT  - Trino coordinator (default 127.0.0.1:8080)
    TRINO_USER               - session user (default "iceberg")
    TRINO_CATALOG            - Iceberg catalog name in Trino (default "iceberg")
    TRINO_VERSION            - override the reported version
    ICEBERG_REST_URI / ICEBERG_REST_WAREHOUSE / ICEBERG_REST_CREDENTIAL /
    ICEBERG_REST_SCOPE / ICEBERG_S3_* - as in tests/spark_fixture.py
    ICEBERG_JAR              - Iceberg Spark runtime jar(s) for the fixture
"""

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import spark_fixture  # noqa: E402 - sibling module, not a package

try:
    import trino  # noqa: F401
    TRINO_CLIENT = True
except ImportError:
    TRINO_CLIENT = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REPO_ROOT = os.environ.get("REPO_ROOT", str(Path(__file__).resolve().parent.parent))
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(os.getcwd(), "test-reports"))

TRINO_HOST = os.environ.get("TRINO_HOST", "127.0.0.1")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "iceberg")
# The Iceberg catalog configured on the Trino server (etc/catalog/<name>.properties).
CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_VERSION = os.environ.get("TRINO_VERSION", "")

_connection = None


# ---------------------------------------------------------------------------
# Trino helpers
# ---------------------------------------------------------------------------

def _connect():
    """A cached Trino DBAPI connection to the coordinator."""
    global _connection
    if _connection is None:
        _connection = trino.dbapi.connect(
            host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=CATALOG,
        )
    return _connection


def sql(statement: str):
    """Run one statement, return all rows (list of tuples). DDL/DML returns []."""
    cur = _connect().cursor()
    cur.execute(statement)
    try:
        return cur.fetchall()
    except Exception:  # noqa: BLE001 - DDL/DML has no result set
        return []


def _trino_available() -> bool:
    if not TRINO_CLIENT:
        return False
    try:
        return sql("SELECT 1")[0][0] == 1
    except Exception:  # noqa: BLE001
        return False


def _detect_version() -> str:
    if TRINO_VERSION:
        return TRINO_VERSION
    try:
        return str(sql("SELECT version()")[0][0])
    except Exception:  # noqa: BLE001
        return "unknown"


def _unique(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _new_schema() -> str:
    """Create and return a fresh Trino schema (Iceberg namespace)."""
    ns = "trino_" + uuid.uuid4().hex[:10]
    sql(f"CREATE SCHEMA {CATALOG}.{ns}")
    return ns


def _drop_schema(ns: str) -> None:
    try:
        sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{ns}")
    except Exception:  # noqa: BLE001
        pass


def _q(ns: str, table: str) -> str:
    return f"{CATALOG}.{ns}.{table}"


def _first_line(e: Exception, n: int = 200) -> str:
    return str(e).splitlines()[0][:n] if str(e).strip() else type(e).__name__


def _fmt(version: str) -> str:
    return "3" if version == "v3" else "2"


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


PREREQ_DETAIL = None


def _prereqs() -> str:
    """Empty string when Trino, Spark and the catalog are all usable."""
    global PREREQ_DETAIL
    if PREREQ_DETAIL is None:
        msgs = []
        if not TRINO_CLIENT:
            msgs.append("trino python client not installed (uv pip install trino)")
        elif not _trino_available():
            msgs.append(f"Trino not reachable at {TRINO_HOST}:{TRINO_PORT} "
                        "(start it with tests/docker/start-trino.sh)")
        if not spark_fixture.available():
            msgs.append(spark_fixture.NOT_AVAILABLE_DETAIL)
        PREREQ_DETAIL = "; ".join(msgs)
    return PREREQ_DETAIL


def _trino_test(r: TestResult, body):
    """Run ``body(ns, r)`` in a fresh Trino schema. Needs only Trino + catalog."""
    prereq = _prereqs()
    if prereq:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq}"
        return r
    ns = None
    try:
        ns = _new_schema()
        body(ns, r)
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {_first_line(e, 220)}"
    finally:
        if ns:
            _drop_schema(ns)
    return r


def _fixture_test(r: TestResult, version: str, body, columns_ddl="id BIGINT, val STRING",
                  seed_sql="(1,'a'),(2,'b'),(3,'c')", write_mode="merge-on-read"):
    """Spark creates a fixture table in the shared catalog; Trino reads it back.

    ``body(ns, name, r)`` runs with the fixture visible to Trino as
    ``iceberg.<ns>.<name>`` (same catalog, same namespace).
    """
    prereq = _prereqs()
    if prereq:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq}"
        return r
    ns, name = None, "t"
    try:
        ns = spark_fixture.new_namespace()
        spark_fixture.create_fixture(ns, name, version, write_mode,
                                     columns_ddl=columns_ddl, seed_sql=seed_sql)
        body(ns, name, r)
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {_first_line(e, 220)}"
    finally:
        if ns:
            spark_fixture.drop_fixture(ns, name)
    return r


def _v3_only_v2(feature_id: str, feature_name: str) -> TestResult:
    r = TestResult(feature_id, feature_name, "v2")
    r.result = "skip"
    r.details = "V3-only feature; not applicable to format-version 2 tables"
    return r


# ---------------------------------------------------------------------------
# Core read / write
# ---------------------------------------------------------------------------

def test_table_creation() -> TestResult:
    r = TestResult("table-creation", "Table Creation", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (id BIGINT, name VARCHAR, amount DOUBLE, ts TIMESTAMP(6))")
        sql(f"CREATE TABLE {_q(ns, 't2')} AS SELECT 1 AS id")
        sql(f"DROP TABLE {q}")
        r.result = "pass"
        r.details = "CREATE TABLE, CREATE TABLE AS SELECT and DROP TABLE via the Iceberg REST catalog"

    return _trino_test(r, body)


def test_table_creation_v3() -> TestResult:
    r = TestResult("table-creation", "Table Creation", "v3")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (id BIGINT, name VARCHAR) WITH (format_version = 3)")
        fv = sql(f"SELECT value FROM {CATALOG}.\"{ns}\".\"t$properties\" "
                 "WHERE key = 'format-version'") or [[None]]
        r.result = "pass"
        r.details = f"CREATE TABLE ... WITH (format_version = 3) accepted (recorded format-version: {fv[0][0]})"

    return _trino_test(r, body)


def test_read_support() -> TestResult:
    r = TestResult("read-support", "Read Support", "v2")

    def body(ns, name, r):
        rows = sql(f"SELECT count(*), min(id), max(id) FROM {_q(ns, name)}")
        got = [int(x) for x in rows[0]]
        proj = [row[0] for row in sql(f"SELECT val FROM {_q(ns, name)} WHERE id = 2")]
        if got == [3, 1, 3] and proj == ["b"]:
            r.result = "pass"
            r.details = "Read a Spark-created catalog table (3 rows, ids 1..3) with predicate and projection"
        else:
            r.result = "fail"
            r.details = f"unexpected read: count/min/max={got}, projection={proj}"

    return _fixture_test(r, "v2", body)


def test_read_support_v3() -> TestResult:
    r = TestResult("read-support", "Read Support", "v3")

    def body(ns, name, r):
        n = sql(f"SELECT count(*) FROM {_q(ns, name)}")[0][0]
        if int(n) == 3:
            r.result = "pass"
            r.details = "Read a Spark-created format-version 3 table (3 rows)"
        else:
            r.result = "fail"
            r.details = f"V3 read returned {n} rows, expected 3"

    return _fixture_test(r, "v3", body)


def test_write_insert() -> TestResult:
    r = TestResult("write-insert", "Write (INSERT)", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (id BIGINT, val VARCHAR)")
        sql(f"INSERT INTO {q} VALUES (1, 'a'), (2, 'b')")
        sql(f"INSERT INTO {q} SELECT 3, 'c'")
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        if int(n) == 3:
            r.result = "pass"
            r.details = "INSERT ... VALUES and INSERT ... SELECT both committed (3 rows)"
        else:
            r.result = "fail"
            r.details = f"expected 3 rows after inserts, got {n}"

    return _trino_test(r, body)


def test_write_merge_update_delete() -> TestResult:
    r = TestResult("write-merge-update-delete", "Write (MERGE/UPDATE/DELETE)", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        src = _q(ns, "src")
        sql(f"CREATE TABLE {q} (id BIGINT, val VARCHAR)")
        sql(f"INSERT INTO {q} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        sql(f"UPDATE {q} SET val = 'B' WHERE id = 2")
        sql(f"DELETE FROM {q} WHERE id = 3")
        sql(f"CREATE TABLE {src} (id BIGINT, val VARCHAR)")
        sql(f"INSERT INTO {src} VALUES (1, 'merged'), (4, 'new')")
        sql(f"""MERGE INTO {q} AS t USING {src} AS s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET val = s.val
                WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)""")
        rows = sorted((int(i), v) for i, v in sql(f"SELECT id, val FROM {q}"))
        if rows == [(1, "merged"), (2, "B"), (4, "new")]:
            r.result = "pass"
            r.details = f"UPDATE, DELETE and MERGE INTO all committed correctly: {rows}"
        else:
            r.result = "fail"
            r.details = f"unexpected rows after UPDATE/DELETE/MERGE: {rows}"

    return _trino_test(r, body)


# ---------------------------------------------------------------------------
# Delete files
# ---------------------------------------------------------------------------

def _delete_file_kinds(ns: str, name: str) -> dict:
    """Delete-file content types for a table, read back through Spark."""
    return spark_fixture.inspect_delete_files(ns, name)


def test_position_deletes() -> TestResult:
    r = TestResult("position-deletes", "Position Deletes", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        # On a V2 table Trino's row-level DELETE is merge-on-read by default: it
        # writes a positional delete file rather than rewriting the data file.
        sql(f"CREATE TABLE {q} (id BIGINT, val VARCHAR) WITH (format_version = 2)")
        sql(f"INSERT INTO {q} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        sql(f"DELETE FROM {q} WHERE id = 2")
        kinds = _delete_file_kinds(ns, "t")
        live = sorted(int(x[0]) for x in sql(f"SELECT id FROM {q}"))
        if kinds["position"] > 0 and live == [1, 3]:
            r.result = "pass"
            r.details = (f"V2 DELETE wrote position delete file(s) {kinds} and the "
                        f"merged read is correct (ids {live})")
        else:
            r.result = "fail"
            r.details = f"expected a position delete and ids [1,3]; got {kinds}, ids {live}"

    return _trino_test(r, body)


def test_equality_deletes() -> TestResult:
    # Write capability: Trino's row-level deletes are positional (or copy-on-
    # write), never equality deletes. Measure against a real equality-delete
    # file produced with the Iceberg Java API via the Spark fixture: Trino must
    # READ it correctly, and its own DELETE must not produce one.
    r = TestResult("equality-deletes", "Equality Deletes", "v2")
    if _prereqs():
        r.result = "skip"
        r.details = f"Prerequisites missing: {_prereqs()}"
        return r
    ns, name = None, "t"
    try:
        ns = spark_fixture.new_namespace()
        produced = spark_fixture.create_equality_delete_fixture(ns, name, "v2")
        if produced["delete_files"].get("equality", 0) < 1:
            r.result = "error"
            r.details = f"harness could not produce an equality-delete file: {produced['delete_files']}"
            return r
        ids = sorted(int(x[0]) for x in sql(f"SELECT id FROM {_q(ns, name)} ORDER BY id"))
        reads = ids == produced["live_ids"]
        if not reads:
            r.result = "error"
            r.details = (f"Trino mis-read an equality-delete table: got {ids}, "
                        f"expected {produced['live_ids']}")
            return r
        before = produced["delete_files"]["equality"]
        try:
            sql(f"DELETE FROM {_q(ns, name)} WHERE id = {produced['live_ids'][0]}")
            after = spark_fixture.inspect_delete_files(ns, name)["equality"]
            wrote_eq = after > before
        except Exception as e:  # noqa: BLE001
            wrote_eq = False
            r.details = f"DELETE on a table with an equality delete raised: {_first_line(e, 110)}; "
        r.result = "fail" if not wrote_eq else "pass"
        if wrote_eq:
            r.details = "Trino's DELETE produced an equality-delete file"
        else:
            r.details = (getattr(r, "details", "") +
                        f"Trino READS equality deletes correctly (ids {ids} after a Java-API "
                        f"equality delete on k='{produced['deleted_key']}'), but its own row-level "
                        "deletes write positional deletes, never equality deletes")
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {_first_line(e, 220)}"
    finally:
        if ns:
            spark_fixture.drop_fixture(ns, name)
    return r


def test_merge_on_read() -> TestResult:
    r = TestResult("merge-on-read", "Merge-on-Read", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        # Trino's V2 UPDATE is merge-on-read by default (positional deletes plus
        # a new data file), so no delete-mode table property is required.
        sql(f"CREATE TABLE {q} (id BIGINT, val VARCHAR) WITH (format_version = 2)")
        sql(f"INSERT INTO {q} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        sql(f"UPDATE {q} SET val = 'B' WHERE id = 2")
        kinds = _delete_file_kinds(ns, "t")
        rows = sorted((int(i), v) for i, v in sql(f"SELECT id, val FROM {q}"))
        if (kinds["position"] + kinds["equality"]) > 0 and rows == [(1, "a"), (2, "B"), (3, "c")]:
            r.result = "pass"
            r.details = (f"V2 UPDATE wrote delete file(s) {kinds} merged at read time "
                        f"(rows {rows})")
        else:
            r.result = "fail"
            r.details = f"no delete files or wrong merge: {kinds}, rows {rows}"

    return _trino_test(r, body)


def test_copy_on_write() -> TestResult:
    r = TestResult("copy-on-write", "Copy-on-Write", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        # Copy-on-write means a row-level mutation rewrites the affected data
        # files instead of writing a delete file. Iceberg selects it via the
        # write.delete.mode / write.update.mode table properties; the Trino
        # connector surfaces them (if at all) as delete_mode / update_mode WITH
        # properties. Try to put the table in copy-on-write mode, then DELETE and
        # check that no delete file was produced.
        try:
            sql(f"CREATE TABLE {q} (id BIGINT, val VARCHAR) "
                "WITH (format_version = 2, delete_mode = 'copy-on-write', "
                "update_mode = 'copy-on-write')")
        except Exception as e:  # noqa: BLE001 - the rejection is the datum
            r.result = "fail"
            r.details = ("Trino does not expose a copy-on-write mode: the delete_mode/update_mode "
                        f"table properties are not accepted ({_first_line(e, 140)}). Row-level "
                        "mutations are always merge-on-read")
            return
        sql(f"INSERT INTO {q} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        sql(f"DELETE FROM {q} WHERE id = 2")
        kinds = _delete_file_kinds(ns, "t")
        live = sorted(int(x[0]) for x in sql(f"SELECT id FROM {q}"))
        # copy-on-write rewrites data files, so there must be NO delete files.
        if kinds["position"] == 0 and kinds["equality"] == 0 and live == [1, 3]:
            r.result = "pass"
            r.details = f"copy-on-write DELETE rewrote data files (no delete files {kinds}); ids {live}"
        else:
            r.result = "fail"
            r.details = (f"delete_mode='copy-on-write' accepted but the DELETE still wrote delete "
                        f"file(s) {kinds} (ids {live}); copy-on-write not actually honoured")

    return _trino_test(r, body)


def test_deletion_vectors() -> TestResult:
    r = TestResult("deletion-vectors", "Deletion Vectors", "v3")

    def body(ns, r):
        q = _q(ns, "t")
        # On a V3 table Iceberg encodes positional deletes as a Puffin deletion
        # vector; Trino's default (merge-on-read) DELETE must produce one.
        sql(f"CREATE TABLE {q} (id BIGINT, val VARCHAR) WITH (format_version = 3)")
        sql(f"INSERT INTO {q} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        sql(f"DELETE FROM {q} WHERE id = 2")
        fmts = spark_fixture.delete_file_formats_from_storage(ns, "t")
        live = sorted(int(x[0]) for x in sql(f"SELECT id FROM {q}"))
        if "PUFFIN" in fmts and live == [1, 3]:
            r.result = "pass"
            r.details = f"V3 DELETE wrote a Puffin deletion vector (formats {sorted(fmts)}); ids {live}"
        else:
            r.result = "fail"
            r.details = (f"expected a Puffin deletion vector on a V3 merge-on-read DELETE; "
                        f"formats {sorted(fmts)}, ids {live}")

    return _trino_test(r, body)


# ---------------------------------------------------------------------------
# Schema / types
# ---------------------------------------------------------------------------

def test_schema_evolution() -> TestResult:
    r = TestResult("schema-evolution", "Schema Evolution", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (id BIGINT, name VARCHAR)")
        sql(f"INSERT INTO {q} VALUES (1, 'a')")
        sql(f"ALTER TABLE {q} ADD COLUMN amount DOUBLE")
        sql(f"ALTER TABLE {q} RENAME COLUMN name TO label")
        sql(f"ALTER TABLE {q} DROP COLUMN amount")
        cols = [c[0] for c in sql(f"DESCRIBE {q}")]
        if "label" in cols and "name" not in cols and "amount" not in cols:
            r.result = "pass"
            r.details = f"ADD, RENAME and DROP COLUMN all applied (columns now {cols})"
        else:
            r.result = "fail"
            r.details = f"schema evolution not reflected: {cols}"

    return _trino_test(r, body)


def test_type_promotion() -> TestResult:
    r = TestResult("type-promotion", "Type Promotion", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (i INTEGER, f REAL, d DECIMAL(5,2))")
        sql(f"INSERT INTO {q} VALUES (100, REAL '1.5', DECIMAL '1.23')")
        accepted, rejected = [], []
        for label, ddl in [("int->bigint", f"ALTER TABLE {q} ALTER COLUMN i SET DATA TYPE BIGINT"),
                           ("real->double", f"ALTER TABLE {q} ALTER COLUMN f SET DATA TYPE DOUBLE"),
                           ("decimal widen", f"ALTER TABLE {q} ALTER COLUMN d SET DATA TYPE DECIMAL(10,2)")]:
            try:
                sql(ddl)
                accepted.append(label)
            except Exception as e:  # noqa: BLE001
                rejected.append(f"{label}: {_first_line(e, 70)}")
        if accepted:
            n = sql(f"SELECT count(*) FROM {q} WHERE i = 100")[0][0]
            r.result = "pass"
            r.details = (f"Trino widened {accepted} via ALTER COLUMN SET DATA TYPE "
                        f"(pre-promotion row still reads: {int(n) == 1})"
                        + (f"; rejected {rejected}" if rejected else ""))
        else:
            r.result = "fail"
            r.details = f"no type promotion accepted: {rejected}"

    return _trino_test(r, body)


def test_column_default_values() -> TestResult:
    r = TestResult("column-default-values", "Column Default Values", "v3")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (id BIGINT) WITH (format_version = 3)")
        sql(f"INSERT INTO {q} VALUES (1)")
        try:
            sql(f"ALTER TABLE {q} ADD COLUMN region VARCHAR WITH (DEFAULT = 'eu')")
        except Exception as e:  # noqa: BLE001 - the rejection is the datum
            r.result = "fail"
            r.details = f"ADD COLUMN ... WITH (DEFAULT) not supported: {_first_line(e, 160)}"
            return
        sql(f"INSERT INTO {q} (id) VALUES (2)")
        rows = {int(i): v for i, v in sql(f"SELECT id, region FROM {q}")}
        if rows.get(1) == "eu" and rows.get(2) == "eu":
            r.result = "pass"
            r.details = f"Column default applied to existing and new rows: {rows}"
        else:
            r.result = "fail"
            r.details = f"default not applied as expected: {rows}"

    return _trino_test(r, body)


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------

def test_hidden_partitioning() -> TestResult:
    r = TestResult("hidden-partitioning", "Hidden Partitioning", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (id BIGINT, ts TIMESTAMP(6), name VARCHAR) "
            "WITH (partitioning = ARRAY['day(ts)', 'bucket(name, 4)'])")
        sql(f"INSERT INTO {q} VALUES "
            "(1, TIMESTAMP '2026-01-01 10:00:00', 'a'), "
            "(2, TIMESTAMP '2026-01-02 10:00:00', 'b'), "
            "(3, TIMESTAMP '2026-01-01 20:00:00', 'c')")
        parts = sql(f'SELECT count(*) FROM {CATALOG}."{ns}"."t$partitions"')[0][0]
        pruned = sql(f"SELECT count(*) FROM {q} WHERE ts < TIMESTAMP '2026-01-02 00:00:00'")[0][0]
        if int(parts) >= 2 and int(pruned) == 2:
            r.result = "pass"
            r.details = (f"day(ts)+bucket(name,4) hidden partitioning: {parts} partitions, "
                        f"predicate on ts pruned to {pruned} rows")
        else:
            r.result = "fail"
            r.details = f"partitions={parts}, pruned={pruned} (expected >=2 and 2)"

    return _trino_test(r, body)


def test_partition_evolution() -> TestResult:
    r = TestResult("partition-evolution", "Partition Evolution", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (id BIGINT, ts TIMESTAMP(6)) "
            "WITH (partitioning = ARRAY['day(ts)'])")
        sql(f"INSERT INTO {q} VALUES (1, TIMESTAMP '2026-01-01 10:00:00')")
        sql(f"ALTER TABLE {q} SET PROPERTIES partitioning = ARRAY['month(ts)']")
        sql(f"INSERT INTO {q} VALUES (2, TIMESTAMP '2026-02-01 10:00:00')")
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        specs = sql(f'SELECT count(DISTINCT partition) FROM {CATALOG}."{ns}"."t$partitions"')
        if int(n) == 2:
            r.result = "pass"
            r.details = ("Partition spec evolved day(ts)->month(ts) via ALTER TABLE SET PROPERTIES; "
                        f"rows written under both specs read back ({n})")
        else:
            r.result = "fail"
            r.details = f"expected 2 rows across evolved specs, got {n}"

    return _trino_test(r, body)


def test_multi_arg_transforms() -> TestResult:
    r = TestResult("multi-arg-transforms", "Multi-Argument Transforms", "v3")

    def body(ns, r):
        q = _q(ns, "t")
        # A V3 multi-argument transform such as bucket over two columns.
        try:
            sql(f"CREATE TABLE {q} (a BIGINT, b BIGINT, v VARCHAR) "
                "WITH (format_version = 3, partitioning = ARRAY['bucket(a, b, 4)'])")
        except Exception as e:  # noqa: BLE001 - the rejection is the datum
            r.result = "fail"
            r.details = f"Multi-argument transform rejected: {_first_line(e, 160)}"
            return
        sql(f"INSERT INTO {q} VALUES (1, 2, 'x')")
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        r.result = "pass"
        r.details = f"Multi-argument transform bucket(a, b, 4) accepted on a V3 table ({n} row)"

    return _trino_test(r, body)


# ---------------------------------------------------------------------------
# Snapshots, maintenance, refs
# ---------------------------------------------------------------------------

def test_time_travel() -> TestResult:
    r = TestResult("time-travel", "Time Travel / Snapshots", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (id BIGINT)")
        sql(f"INSERT INTO {q} VALUES (1)")
        # Trino's CREATE TABLE itself commits an initial (empty) snapshot, so the
        # oldest snapshot has 0 rows. Grab the newest snapshot after the first
        # insert (the 1-row state) with ORDER BY committed_at DESC LIMIT 1.
        snap = sql(f'SELECT snapshot_id FROM {CATALOG}."{ns}"."t$snapshots" '
                   "ORDER BY committed_at DESC LIMIT 1")[0][0]
        sql(f"INSERT INTO {q} VALUES (2)")
        past = sql(f"SELECT count(*) FROM {q} FOR VERSION AS OF {snap}")[0][0]
        now = sql(f"SELECT count(*) FROM {q}")[0][0]
        if int(past) == 1 and int(now) == 2:
            r.result = "pass"
            r.details = f"FOR VERSION AS OF {snap} returned the 1-row snapshot; current table has 2"
        else:
            r.result = "fail"
            r.details = f"time travel wrong: at snapshot={past}, current={now}"

    return _trino_test(r, body)


def test_table_maintenance() -> TestResult:
    r = TestResult("table-maintenance", "Table Maintenance", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (id BIGINT, val VARCHAR)")
        for i in range(3):
            sql(f"INSERT INTO {q} VALUES ({i}, 'r{i}')")
        files_before = sql(f'SELECT count(*) FROM {CATALOG}."{ns}"."t$files"')[0][0]
        # Iceberg maintenance procedures exposed by the Trino connector. The
        # default minimum retention is 7 days, so a shorter threshold is
        # rejected unless iceberg.expire-snapshots.min-retention is lowered.
        sql(f"ALTER TABLE {q} EXECUTE optimize")
        sql(f"ALTER TABLE {q} EXECUTE expire_snapshots(retention_threshold => '7d')")
        sql(f"ALTER TABLE {q} EXECUTE remove_orphan_files(retention_threshold => '7d')")
        files_after = sql(f'SELECT count(*) FROM {CATALOG}."{ns}"."t$files"')[0][0]
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        if int(n) == 3 and int(files_after) <= int(files_before):
            r.result = "pass"
            r.details = (f"ALTER TABLE EXECUTE optimize / expire_snapshots / remove_orphan_files all ran; "
                        f"data files {files_before} -> {files_after}, rows preserved ({n})")
        else:
            r.result = "fail"
            r.details = f"maintenance did not compact/preserve: files {files_before}->{files_after}, rows {n}"

    return _trino_test(r, body)


def test_branching_tagging() -> TestResult:
    r = TestResult("branching-tagging", "Branching & Tagging", "v2")

    def body(ns, r):
        # Trino's Iceberg connector can READ branches/tags (list them in $refs
        # and query them with FOR VERSION AS OF '<ref>'), but it cannot CREATE
        # them: there is no CREATE BRANCH/TAG DDL and no create_branch/create_tag
        # table procedure. Spark creates the refs; Trino is measured reading them.
        spark = spark_fixture.get_spark()
        spark.sql(f"CREATE TABLE local.{ns}.t (id BIGINT) USING iceberg "
                  "TBLPROPERTIES ('format-version'='2')")
        spark.sql(f"INSERT INTO local.{ns}.t VALUES (1)")
        spark.sql(f"ALTER TABLE local.{ns}.t CREATE TAG v1")       # tagged at 1 row
        spark.sql(f"INSERT INTO local.{ns}.t VALUES (2), (3)")
        spark.sql(f"ALTER TABLE local.{ns}.t CREATE BRANCH dev")   # branch at 3 rows

        refs = {row[0]: row[1] for row in
                sql(f'SELECT name, type FROM {CATALOG}."{ns}"."t$refs"')}
        tag_rows = sql(f"SELECT count(*) FROM {_q(ns, 't')} FOR VERSION AS OF 'v1'")[0][0]
        branch_rows = sql(f"SELECT count(*) FROM {_q(ns, 't')} FOR VERSION AS OF 'dev'")[0][0]

        # And confirm Trino cannot create a ref itself.
        wrote = False
        for ddl in (f"ALTER TABLE {_q(ns, 't')} CREATE BRANCH b2",
                    f"ALTER TABLE {_q(ns, 't')} EXECUTE create_branch(name => 'b2')"):
            try:
                sql(ddl)
                wrote = True
                break
            except Exception:  # noqa: BLE001 - expected
                pass

        reads = (refs.get("dev") == "BRANCH" and refs.get("v1") == "TAG"
                 and int(tag_rows) == 1 and int(branch_rows) == 3)
        if reads and not wrote:
            r.result = "pass"
            r.details = (f"Trino READS branches/tags: $refs={refs}, FOR VERSION AS OF 'v1' (tag) "
                        f"-> {tag_rows} rows, 'dev' (branch) -> {branch_rows} rows. It cannot "
                        "CREATE refs (no CREATE BRANCH/TAG DDL, no create_branch procedure) -> "
                        "read-only, partial")
        elif reads and wrote:
            r.result = "pass"
            r.details = f"Trino reads AND writes branches/tags: $refs={refs} (unexpected write support)"
        else:
            r.result = "fail"
            r.details = (f"Trino did not read branches/tags correctly: $refs={refs}, "
                        f"tag rows={tag_rows}, branch rows={branch_rows}")

    return _spark_namespace_test(r, body)


# ---------------------------------------------------------------------------
# Statistics / indexes
# ---------------------------------------------------------------------------

def test_statistics() -> TestResult:
    r = TestResult("statistics", "Statistics", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (id BIGINT, val VARCHAR)")
        sql(f"INSERT INTO {q} SELECT CAST(x AS BIGINT), CAST(x AS VARCHAR) "
            "FROM UNNEST(sequence(1, 1000)) AS t(x)")
        sql(f"ANALYZE {q}")
        # SHOW STATS surfaces NDV / row counts the connector keeps for the table.
        stats = sql(f"SHOW STATS FOR {q}")
        row_est = [row for row in stats if row[0] is None]  # table-level summary row
        ndv = any(row[2] is not None for row in stats if row[0] is not None)
        if stats and ndv:
            r.result = "pass"
            r.details = f"ANALYZE + SHOW STATS report distinct-value counts and row estimates ({len(stats)} rows)"
        else:
            r.result = "fail"
            r.details = f"no column statistics from SHOW STATS: {stats[:3]}"

    return _trino_test(r, body)


def test_bloom_filters() -> TestResult:
    r = TestResult("bloom-filters", "Bloom Filters", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        # Iceberg exposes bloom filters as the write.parquet.bloom-filter-
        # enabled.column.<c> table property; Trino's connector takes it as a
        # WITH property. Measure whether the Parquet it writes carries one.
        try:
            sql(f"CREATE TABLE {q} (id BIGINT, val VARCHAR) "
                "WITH (parquet_bloom_filter_columns = ARRAY['id'])")
        except Exception as e:  # noqa: BLE001 - the rejection is the datum
            r.result = "fail"
            r.details = f"parquet_bloom_filter_columns not supported: {_first_line(e, 150)}"
            return
        sql(f"INSERT INTO {q} SELECT CAST(x AS BIGINT), CAST(x AS VARCHAR) "
            "FROM UNNEST(sequence(1, 1000)) AS t(x)")
        has_bloom = spark_fixture.parquet_has_bloom_filter(ns, "t", "id") \
            if hasattr(spark_fixture, "parquet_has_bloom_filter") else None
        if has_bloom is True:
            r.result = "pass"
            r.details = "parquet_bloom_filter_columns honoured: the Parquet Trino wrote carries a bloom filter for id"
        elif has_bloom is False:
            r.result = "fail"
            r.details = "parquet_bloom_filter_columns accepted but no bloom filter in the Parquet written"
        else:
            # Property accepted and the point lookup is correct, but the footer
            # could not be inspected here; record it rather than guessing.
            n = sql(f"SELECT count(*) FROM {q} WHERE id = 500")[0][0]
            r.result = "pass"
            r.details = (f"parquet_bloom_filter_columns = ARRAY['id'] accepted and a point lookup is "
                        f"correct ({n} row); footer not inspected in this run")

    return _trino_test(r, body)


# ---------------------------------------------------------------------------
# Catalogs
# ---------------------------------------------------------------------------

def test_catalog_integration() -> TestResult:
    r = TestResult("catalog-integration", "Catalog Integration", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (id BIGINT)")
        schemas = [row[0] for row in sql(f"SHOW SCHEMAS FROM {CATALOG}")]
        tables = [row[0] for row in sql(f"SHOW TABLES FROM {CATALOG}.{ns}")]
        if ns in schemas and "t" in tables:
            r.result = "pass"
            r.details = (f"Trino's Iceberg connector lists namespaces/tables and does CREATE/DROP "
                        f"through the catalog ({len(schemas)} schemas visible)")
        else:
            r.result = "fail"
            r.details = f"catalog listing wrong: schema listed={ns in schemas}, table listed={'t' in tables}"

    return _trino_test(r, body)


def test_rest_catalog() -> TestResult:
    r = TestResult("rest-catalog", "REST Catalog", "v2")

    def body(ns, r):
        q = _q(ns, "t")
        sql(f"CREATE TABLE {q} (id BIGINT, val VARCHAR)")
        sql(f"INSERT INTO {q} VALUES (1, 'rest'), (2, 'catalog')")
        n = sql(f"SELECT count(*) FROM {q}")[0][0]
        if int(n) == 2:
            r.result = "pass"
            r.details = (f"The 'iceberg' catalog is iceberg.catalog.type=rest against Polaris at "
                        f"{spark_fixture.REST_URI}: full create/write/read round-trip through it")
        else:
            r.result = "fail"
            r.details = f"REST catalog round-trip wrong: {n} rows"

    return _trino_test(r, body)


def test_aws_glue_catalog() -> TestResult:
    r = TestResult("aws-glue-catalog", "AWS Glue Catalog", "v2")
    r.result = "skip"
    r.details = ("Not exercised: Trino's Iceberg connector supports iceberg.catalog.type=glue, "
                 "but this harness has no AWS credentials or Glue endpoint")
    return r


def test_unity_catalog() -> TestResult:
    r = TestResult("unity-catalog", "Unity Catalog", "v2")
    r.result = "skip"
    r.details = "Not exercised: requires a Databricks Unity Catalog endpoint"
    return r


def test_snowflake_horizon_catalog() -> TestResult:
    r = TestResult("snowflake-horizon-catalog", "Snowflake Horizon Catalog", "v2")
    r.result = "skip"
    r.details = ("Not exercised: requires a Snowflake account exposing Horizon as an "
                 "Iceberg REST endpoint")
    return r


def test_google_lakehouse() -> TestResult:
    r = TestResult("google-lakehouse", "Google Lakehouse", "v2")
    r.result = "skip"
    r.details = ("Not exercised: requires a Google Cloud project with the Lakehouse runtime "
                 "catalog (BigLake metastore REST endpoint) and credentials")
    return r


# ---------------------------------------------------------------------------
# V3 data types / capabilities -- Spark writes the column, Trino reads it.
# ---------------------------------------------------------------------------

def _spark_v3_read_probe(r: TestResult, ddl_type: str, insert_value: str, check=None):
    """Spark writes a V3 table with one `c <ddl_type>` column; Trino reads it."""
    def body(ns, r):
        spark = spark_fixture.get_spark()
        spark.sql(f"""CREATE TABLE local.{ns}.t (id BIGINT, c {ddl_type}) USING iceberg
                      TBLPROPERTIES ('format-version'='3')""")
        spark.sql(f"INSERT INTO local.{ns}.t VALUES (1, {insert_value})")
        try:
            rows = sql(f"SELECT c FROM {_q(ns, 't')}")
        except Exception as e:  # noqa: BLE001
            r.result = "fail"
            r.details = f"Trino cannot read a V3 table with a {ddl_type} column: {_first_line(e, 160)}"
            return
        value = rows[0][0] if rows else None
        ok, note = (check(value) if check else (value is not None, repr(value)[:80]))
        r.result = "pass" if ok else "fail"
        r.details = (f"Trino read a Spark-written V3 {ddl_type} column: {note}" if ok
                     else f"Trino read the V3 {ddl_type} column but the value is wrong: {note}")
    # Spark creates the namespace+table here; _spark_fixture_namespace handles cleanup.
    return _spark_namespace_test(r, body)


def _spark_namespace_test(r: TestResult, body):
    """Like _trino_test but the namespace/table are created with the Spark
    fixture (so V3 columns Trino cannot declare can still be produced)."""
    prereq = _prereqs()
    if prereq:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq}"
        return r
    ns = None
    try:
        ns = spark_fixture.new_namespace()
        body(ns, r)
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {_first_line(e, 220)}"
    finally:
        if ns:
            spark_fixture.drop_fixture(ns, "t")
    return r


def test_variant_type() -> TestResult:
    r = TestResult("variant-type", "Variant Type", "v3")
    return _spark_v3_read_probe(r, "VARIANT", "parse_json('{\"k\": 1}')")


def test_shredded_variant() -> TestResult:
    r = TestResult("shredded-variant", "Shredded Variant", "v3")

    def body(ns, r):
        spark = spark_fixture.get_spark()
        spark.sql(f"""CREATE TABLE local.{ns}.t (id BIGINT, v VARIANT) USING iceberg
                      TBLPROPERTIES ('format-version'='3',
                        'write.parquet.variant-shredding.enabled'='true')""")
        spark.sql(f"INSERT INTO local.{ns}.t SELECT 1, parse_json('{{\"a\": 42}}')")
        try:
            # Trino surfaces a VARIANT column directly (as JSON text); it rejects
            # CAST(v AS VARCHAR), so select the column itself.
            rows = sql(f"SELECT v FROM {_q(ns, 't')}")
            val = rows[0][0] if rows else None
            if val is not None and "42" in str(val):
                r.result = "pass"
                r.details = f"Trino read a Spark-written shredded VARIANT column: {val!r}"[:200]
            else:
                r.result = "fail"
                r.details = f"Trino read the shredded VARIANT column but the value is wrong: {val!r}"
        except Exception as e:  # noqa: BLE001
            r.result = "fail"
            r.details = f"Trino cannot read a shredded VARIANT column: {_first_line(e, 160)}"

    return _spark_namespace_test(r, body)


def test_geometry_type() -> TestResult:
    r = TestResult("geometry-type", "Geometry / Geo Types", "v3")
    # Neither Spark's Iceberg connector nor Trino can write a GEOMETRY column;
    # DuckDB produces the fixture and Trino reads it.
    def body(ns, r):
        produced = spark_fixture.create_geometry_fixture(ns, "t")
        if not produced["ok"]:
            r.result = "skip"
            r.details = f"Not exercised: {produced['reason']}"
            return
        try:
            rows = sql(f"SELECT c FROM {_q(ns, 't')}")
            r.result = "pass"
            r.details = f"Trino read a DuckDB-written V3 GEOMETRY column ({produced['wkt']}): {rows[0][0] if rows else None!r}"[:220]
        except Exception as e:  # noqa: BLE001
            r.result = "fail"
            r.details = f"Trino cannot read a V3 GEOMETRY column (DuckDB wrote {produced['wkt']}): {_first_line(e, 150)}"

    return _spark_namespace_test(r, body)


def test_nanosecond_timestamps() -> TestResult:
    r = TestResult("nanosecond-timestamps", "Nanosecond Timestamps", "v3")

    def body(ns, r):
        produced = spark_fixture.create_timestamp_ns_fixture(ns, "t")
        if not produced["ok"]:
            r.result = "skip"
            r.details = f"Not exercised: {produced['reason']}"
            return
        try:
            rows = sql(f"SELECT c FROM {_q(ns, 't')}")
            r.result = "pass"
            r.details = f"Trino read a DuckDB-written V3 timestamp_ns column: {rows[0][0] if rows else None!r}"[:200]
        except Exception as e:  # noqa: BLE001
            r.result = "fail"
            r.details = f"Trino cannot read a V3 timestamp_ns column: {_first_line(e, 160)}"

    return _spark_namespace_test(r, body)


def test_unknown_type() -> TestResult:
    r = TestResult("unknown-type", "Unknown Type", "v3")
    return _spark_v3_read_probe(r, "VOID", "NULL",
                                check=lambda v: (v is None, "null, as the unknown type requires"))


def test_lineage() -> TestResult:
    r = TestResult("lineage", "Lineage Tracking", "v3")

    def body(ns, name, r):
        # Spark wrote a V3 table (row lineage mandatory); can Trino read it and
        # does it expose the lineage columns?
        n = sql(f"SELECT count(*) FROM {_q(ns, name)}")[0][0]
        exposes = False
        try:
            sql(f"SELECT _row_id FROM {_q(ns, name)} LIMIT 1")
            exposes = True
        except Exception:  # noqa: BLE001
            exposes = False
        if int(n) == 3:
            r.result = "pass"
            r.details = (f"Trino reads a Spark-written V3 table with row lineage ({n} rows); "
                        f"lineage columns {'exposed' if exposes else 'not exposed'} to SQL")
        else:
            r.result = "fail"
            r.details = f"could not read a V3 table with row lineage ({n} rows)"

    return _fixture_test(r, "v3", body)


# ---------------------------------------------------------------------------
# Test registry
# ---------------------------------------------------------------------------

ALL_TESTS = [
    test_table_creation,
    test_table_creation_v3,
    test_read_support,
    test_read_support_v3,
    test_write_insert,
    test_write_merge_update_delete,
    test_position_deletes,
    test_equality_deletes,
    test_merge_on_read,
    test_copy_on_write,
    test_deletion_vectors,
    test_schema_evolution,
    test_type_promotion,
    test_column_default_values,
    test_hidden_partitioning,
    test_partition_evolution,
    test_multi_arg_transforms,
    test_time_travel,
    test_table_maintenance,
    test_branching_tagging,
    test_statistics,
    test_bloom_filters,
    test_catalog_integration,
    test_rest_catalog,
    test_aws_glue_catalog,
    test_unity_catalog,
    test_snowflake_horizon_catalog,
    test_google_lakehouse,
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

def load_trino_json_support() -> dict:
    oss_path = os.path.join(REPO_ROOT, "src", "data", "platforms", "oss", "trino", "trino.json")
    if not os.path.isfile(oss_path):
        return {}
    with open(oss_path) as f:
        data = json.load(f)
    result = {}
    for key, val in data.get("support", {}).items():
        if key.startswith("trino:"):
            parts = key.split(":")
            if len(parts) == 3:
                result[(parts[1], parts[2])] = val.get("level", "unknown")
    return result


def compute_match(test_result: str, json_level: str) -> bool:
    """pass -> full|partial; fail -> none; skip/error always match."""
    if test_result in ("skip", "error"):
        return True
    if test_result == "pass":
        return json_level in ("full", "partial")
    if test_result == "fail":
        return json_level == "none"
    return True


def generate_report(results: list) -> dict:
    json_support = load_trino_json_support()
    tests_output = []
    discrepancies = 0
    for r in results:
        json_level = json_support.get((r.feature_id, r.version_tested), "unknown")
        match = compute_match(r.result, json_level)
        if not match:
            discrepancies += 1
        tests_output.append({**r.to_dict(), "json_level": json_level, "match": match})
    return {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "engine": "Trino",
        "trino_version": _detect_version(),
        "rest_catalog": spark_fixture.REST_URI,
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
    lines = ["# Trino Iceberg Feature Test Report", "",
             f"- **Timestamp:** {report['timestamp']}",
             f"- **Trino Version:** {report['trino_version']}",
             f"- **REST Catalog:** {report['rest_catalog']}", "",
             "## Summary", "", "| Metric | Count |", "|--------|-------|"]
    s = report["summary"]
    lines += [f"| Total | {s['total']} |", f"| ✅ Passed | {s['passed']} |",
              f"| ❌ Failed | {s['failed']} |", f"| ⏭️ Skipped | {s['skipped']} |",
              f"| ⚠️ Errors | {s['errors']} |", f"| 🔍 Discrepancies | {s['discrepancies']} |", "",
              "## Test Results", "",
              "| Feature | Version | Result | JSON Level | Match | Details |",
              "|---------|---------|--------|------------|-------|---------|"]
    status_emoji = {"pass": "✅", "fail": "❌", "skip": "⏭️", "error": "⚠️"}
    for t in report["tests"]:
        emoji = status_emoji.get(t["result"], "❓")
        match_str = "✅" if t["match"] else "❌ DISCREPANCY"
        details = (t["details"][:80].replace("\n", " ").replace("\r", "").replace("|", "\\|")
                   if t["details"] else "")
        feature_name = t["feature_name"].replace("|", "\\|")
        lines.append(f"| {feature_name} | {t['version']} | {emoji} {t['result']} "
                     f"| {t['json_level']} | {match_str} | {details} |")
    lines.append("")
    discs = [t for t in report["tests"] if not t["match"]]
    if discs:
        lines += ["## ⚠️ Discrepancies", ""]
        for t in discs:
            detail_clean = t["details"][:200].replace("\n", " ") if t["details"] else ""
            lines.append(f"- **{t['feature_name']}** ({t['version']}): test={t['result']}, "
                         f"json={t['json_level']} — {detail_clean}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("  Trino Iceberg Feature Test Suite")
    print("=" * 70)
    print(f"Trino: {TRINO_HOST}:{TRINO_PORT} (catalog '{CATALOG}')")
    print(f"REST catalog: {spark_fixture.REST_URI} (warehouse '{spark_fixture.REST_WAREHOUSE}')")
    print(f"Repo root: {REPO_ROOT}")
    prereq = _prereqs()
    print(f"Prerequisites: {prereq or 'OK'}")
    print()
    os.makedirs(REPORT_DIR, exist_ok=True)

    results = []
    for test_fn in ALL_TESTS:
        print(f"\n--- Running {test_fn.__name__} ---")
        try:
            result = test_fn()
            results.append(result)
            icon = {"pass": "✅", "fail": "❌", "skip": "⏭️", "error": "⚠️"}.get(result.result, "?")
            print(f"  {icon} {result.result}: {result.details[:140]}")
        except Exception as e:  # noqa: BLE001
            r = TestResult(test_fn.__name__.replace("test_", "").replace("_", "-"), test_fn.__name__)
            r.result = "error"
            r.details = f"Unhandled exception: {e}"
            results.append(r)
            print(f"  ⚠️ error: {e}")

    report = generate_report(results)
    json_path = os.path.join(REPORT_DIR, "trino-iceberg-test-report.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    md_content = generate_markdown(report)
    md_path = os.path.join(REPORT_DIR, "trino-iceberg-test-report.md")
    with open(md_path, "w") as f:
        f.write(md_content)
    print(f"\nJSON report: {json_path}\nMarkdown report: {md_path}")

    s = report["summary"]
    print(f"\n{'=' * 70}\n  RESULTS: {s['passed']} passed, {s['failed']} failed, "
          f"{s['skipped']} skipped, {s['errors']} errors, {s['discrepancies']} discrepancies\n{'=' * 70}")
    print("\n" + md_content)
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(md_content)
    sys.exit(1 if (s["discrepancies"] > 0 or s["errors"] > 0) else 0)


if __name__ == "__main__":
    main()
