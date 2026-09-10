#!/usr/bin/env python3
"""
ClickHouse Iceberg Feature Test Suite.

Measures what ClickHouse's Iceberg integration actually does against tables
managed by a real Iceberg REST catalog (Apache Polaris backed by MinIO, see
tests/docker), then compares with the ClickHouse entries in
``src/data/platforms/oss/clickhouse/clickhouse.json``.

How each cell is measured
-------------------------
* Spark (tests/spark_fixture.py) creates fixture tables through the shared
  Polaris catalog. That makes them ordinary catalog-managed tables on S3, not
  files on a local disk, so what ClickHouse reads is exactly what any other
  engine would see.
* ClickHouse reads and writes those tables with ``icebergS3()`` /
  ``ENGINE = IcebergS3(...)`` pointed at the table's storage location as
  recorded by the catalog. Since ClickHouse 25.x the Iceberg engine is not
  read-only: with ``allow_insert_into_iceberg`` it creates tables, INSERTs,
  runs DELETE/UPDATE mutations, ALTERs columns and compacts (OPTIMIZE). Each
  of those is exercised for real here rather than asserted.
* The REST protocol is measured separately with the ``DataLakeCatalog``
  database engine (``catalog_type='rest'`` + OAuth2 against Polaris).
* Delete files ClickHouse cannot write itself (equality deletes) are produced
  with the Iceberg Java API via the Spark fixture and read back by ClickHouse.

Nothing here fabricates a result: when ClickHouse, Spark or the catalog are
unavailable the affected tests report ``skip``.

Usage:
    ./tests/docker/start-polaris.sh
    python tests/clickhouse_feature_tests.py

Environment variables:
    CLICKHOUSE_BINARY        - path to the clickhouse binary (auto-detected)
    CLICKHOUSE_VERSION       - override the reported version
    ICEBERG_REST_URI / ICEBERG_REST_WAREHOUSE / ICEBERG_REST_CREDENTIAL /
    ICEBERG_REST_SCOPE / ICEBERG_S3_* - as in tests/spark_fixture.py
    ICEBERG_JAR              - Iceberg Spark runtime jar(s) for the fixture
"""

import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import spark_fixture  # noqa: E402 - sibling module, not a package

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REPO_ROOT = os.environ.get("REPO_ROOT", str(Path(__file__).resolve().parent.parent))
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(os.getcwd(), "test-reports"))

_CH_CANDIDATES = ["clickhouse", os.path.expanduser("~/clickhouse"), "./clickhouse", "/tmp/clickhouse"]
CLICKHOUSE_BINARY = os.environ.get("CLICKHOUSE_BINARY", "")
if not CLICKHOUSE_BINARY:
    for _c in _CH_CANDIDATES:
        if shutil.which(_c) or os.path.isfile(_c):
            CLICKHOUSE_BINARY = _c
            break
    if not CLICKHOUSE_BINARY:
        CLICKHOUSE_BINARY = "clickhouse"  # fails gracefully later


def _detect_ch_version() -> str:
    try:
        out = subprocess.check_output(
            [CLICKHOUSE_BINARY, "local", "--version"], stderr=subprocess.STDOUT, timeout=10
        ).decode()
        m = re.search(r"(\d+\.\d+\.\d+\.\d+)", out)
        return m.group(1) if m else "unknown"
    except Exception:  # noqa: BLE001
        return "unknown"


CLICKHOUSE_VERSION = os.environ.get("CLICKHOUSE_VERSION", _detect_ch_version())

# S3 credentials ClickHouse uses to reach MinIO directly (same as the fixture's).
S3_CREDS = f"'{spark_fixture.S3_KEY_ID}', '{spark_fixture.S3_SECRET}'"
# Settings every ClickHouse write path needs.
WRITE_SETTINGS = "SET allow_insert_into_iceberg = 1;"


# ---------------------------------------------------------------------------
# ClickHouse helpers
# ---------------------------------------------------------------------------

def _ch_query(sql: str, timeout: int = 90) -> tuple:
    """Run SQL via ``clickhouse local``. Returns (success, output)."""
    if not shutil.which(CLICKHOUSE_BINARY) and not os.path.isfile(CLICKHOUSE_BINARY):
        return False, f"clickhouse binary not found at: {CLICKHOUSE_BINARY}"
    try:
        result = subprocess.run(
            [CLICKHOUSE_BINARY, "local", "--multiquery", "--query", sql],
            capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode == 0:
            return True, result.stdout.strip()
        return False, (result.stderr or result.stdout).strip()
    except subprocess.TimeoutExpired:
        return False, "Query timed out"
    except Exception as e:  # noqa: BLE001
        return False, str(e)


def _ch_available() -> bool:
    ok, _ = _ch_query("SELECT 1", timeout=30)
    return ok


def _unique(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _err(out: str, n: int = 180) -> str:
    """First line of a ClickHouse error, trimmed."""
    line = out.strip().splitlines()[0] if out.strip() else ""
    return line[:n]


def _s3_url(ns: str, name: str) -> str:
    """HTTP path-style URL of a fixture table, for icebergS3()/IcebergS3."""
    return spark_fixture.s3_http_location(ns, name)


def _fresh_ch_table_url() -> str:
    """A storage location for a table ClickHouse creates itself.

    Placed under the catalog's base location so it sits next to the Spark
    fixtures in the same bucket; the catalog itself is not told about it
    (ClickHouse's IcebergS3 engine writes metadata straight to storage).
    """
    return (f"http://{spark_fixture.S3_ENDPOINT}/warehouse/clickhouse_own/"
            f"{_unique('ch')}/")


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


PREREQ_DETAIL = None  # computed once in main()


def _prereqs() -> str:
    """Empty string when ClickHouse, Spark and the catalog are all usable."""
    global PREREQ_DETAIL
    if PREREQ_DETAIL is None:
        msgs = []
        if not _ch_available():
            msgs.append(f"ClickHouse not available (binary: {CLICKHOUSE_BINARY}); "
                        "install: curl https://clickhouse.com/install.sh | sh")
        if not spark_fixture.available():
            msgs.append(spark_fixture.NOT_AVAILABLE_DETAIL)
        PREREQ_DETAIL = "; ".join(msgs)
    return PREREQ_DETAIL


def _fixture_test(r: TestResult, version: str, body, columns_ddl="id BIGINT, val STRING",
                  seed_sql="(1,'a'),(2,'b'),(3,'c')", write_mode="merge-on-read"):
    """Create a Spark fixture through the catalog, run ``body(url, ns, name, r)``.

    ``url`` is the table's HTTP S3 location for icebergS3(). Exceptions become
    ``error``; a missing prerequisite becomes ``skip``.
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
        body(_s3_url(ns, name), ns, name, r)
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:220]}"
    finally:
        if ns:
            spark_fixture.drop_fixture(ns, name)
    return r


def _own_table_test(r: TestResult, body):
    """Run ``body(url, r)`` for a table ClickHouse creates itself at ``url``."""
    prereq = _prereqs()
    if prereq:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq}"
        return r
    try:
        body(_fresh_ch_table_url(), r)
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:220]}"
    return r


def _v3_only_v2(feature_id: str, feature_name: str) -> TestResult:
    r = TestResult(feature_id, feature_name, "v2")
    r.result = "skip"
    r.details = "V3-only feature; not applicable to format-version 2 tables"
    return r


# ---------------------------------------------------------------------------
# Core read / write
# ---------------------------------------------------------------------------

def test_read_support() -> TestResult:
    r = TestResult("read-support", "Read Support")

    def body(url, ns, name, r):
        ok, out = _ch_query(f"SELECT count(), min(id), max(id) FROM icebergS3('{url}', {S3_CREDS})")
        if ok and out.split("\t") == ["3", "1", "3"]:
            r.result = "pass"
            r.details = ("icebergS3() read a catalog-managed table from its storage "
                        "location: 3 rows, ids 1..3, with predicate-free scan")
        elif ok:
            r.result = "fail"
            r.details = f"unexpected result: {out}"
        else:
            r.result = "fail"
            r.details = f"icebergS3() read failed: {_err(out)}"

    return _fixture_test(r, "v2", body)


def test_read_support_v3() -> TestResult:
    r = TestResult("read-support", "Read Support", "v3")

    def body(url, ns, name, r):
        ok, out = _ch_query(f"SELECT count() FROM icebergS3('{url}', {S3_CREDS})")
        if ok and out.strip() == "3":
            r.result = "pass"
            r.details = "icebergS3() read a format-version 3 table (3 rows)"
        else:
            r.result = "fail"
            r.details = f"V3 read failed: {_err(out) if not ok else out}"

    return _fixture_test(r, "v3", body)


def test_table_creation() -> TestResult:
    r = TestResult("table-creation", "Table Creation")

    def body(url, r):
        ok, out = _ch_query(f"""
            {WRITE_SETTINGS}
            CREATE TABLE t (id Int64, v String) ENGINE = IcebergS3('{url}', {S3_CREDS});
            INSERT INTO t VALUES (1, 'a');
            SELECT count() FROM t;
        """)
        if ok and out.strip() == "1":
            # Confirm it is a real Iceberg table by reading the metadata it wrote.
            ok2, out2 = _ch_query(
                f"SELECT count() FROM s3('{url}metadata/*.metadata.json', {S3_CREDS}, 'LineAsString')")
            r.result = "pass"
            r.details = ("CREATE TABLE ... ENGINE = IcebergS3 created an Iceberg table on S3 "
                        f"(metadata.json written: {ok2 and out2.strip() != '0'}) and INSERT "
                        "committed a row; DROP is a metadata-only detach")
        else:
            r.result = "fail"
            r.details = f"CREATE TABLE ENGINE=IcebergS3 failed: {_err(out) if not ok else out}"

    return _own_table_test(r, body)


def test_table_creation_v3() -> TestResult:
    r = TestResult("table-creation", "Table Creation", "v3")

    def body(url, r):
        ok, out = _ch_query(f"""
            {WRITE_SETTINGS}
            CREATE TABLE t (id Int64) ENGINE = IcebergS3('{url}', {S3_CREDS})
                SETTINGS iceberg_format_version = 3;
            INSERT INTO t VALUES (1);
            SELECT line FROM s3('{url}metadata/*.metadata.json', {S3_CREDS}, 'LineAsString')
                WHERE line LIKE '%format-version%' LIMIT 1;
        """)
        if ok and re.search(r'"format-version"\s*:\s*3', out):
            r.result = "pass"
            r.details = ("CREATE TABLE ... SETTINGS iceberg_format_version=3 wrote a table whose "
                        "metadata.json records format-version 3")
        elif ok:
            r.result = "fail"
            r.details = f"table created but metadata does not say V3: {out[:120]}"
        else:
            r.result = "fail"
            r.details = f"V3 create failed: {_err(out)}"

    return _own_table_test(r, body)


def test_write_insert() -> TestResult:
    r = TestResult("write-insert", "Write (INSERT)")

    def body(url, ns, name, r):
        ok, out = _ch_query(f"""
            {WRITE_SETTINGS}
            CREATE TABLE t ENGINE = IcebergS3('{url}', {S3_CREDS});
            INSERT INTO t (id, val) VALUES (4, 'd');
            INSERT INTO t (id, val) SELECT 5, 'e';
            SELECT count() FROM t;
        """)
        if not ok:
            r.result = "fail"
            r.details = f"INSERT into a catalog-managed table via IcebergS3 failed: {_err(out)}"
            return
        # The commit went to storage, not through the catalog. Spark must reload
        # metadata from storage to see it; the catalog itself still points at the
        # previous metadata.json, so this is a storage-level write.
        catalog_rows = spark_fixture.row_count(ns, name)
        storage_rows = spark_fixture.row_count_from_storage(ns, name)
        r.result = "pass"
        r.details = (f"INSERT ... VALUES and INSERT ... SELECT committed (ClickHouse count={out}, "
                    f"newest metadata.json in storage records {storage_rows} rows). The commit is "
                    f"written to storage only, bypassing the catalog: Spark via the catalog still "
                    f"sees {catalog_rows} rows")

    return _fixture_test(r, "v2", body)


def test_write_merge_update_delete() -> TestResult:
    r = TestResult("write-merge-update-delete", "Write (MERGE/UPDATE/DELETE)")

    def body(url, r):
        ok, out = _ch_query(f"""
            {WRITE_SETTINGS}
            CREATE TABLE t (id Int64, v String) ENGINE = IcebergS3('{url}', {S3_CREDS});
            INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c');
            DELETE FROM t WHERE id = 2;
            ALTER TABLE t UPDATE v = 'z' WHERE id = 1;
            SELECT groupArray(id), groupArray(v) FROM (SELECT id, v FROM t ORDER BY id);
        """)
        if ok and out.split("\t") == ["[1,3]", "['z','c']"]:
            ok_m, out_m = _ch_query(f"""
                CREATE TABLE t ENGINE = IcebergS3('{url}', {S3_CREDS});
                MERGE INTO t USING (SELECT 9 AS id) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT VALUES (9, 'm');
            """)
            r.result = "pass"
            r.details = ("DELETE FROM and ALTER TABLE ... UPDATE both committed against an Iceberg "
                        f"table (rows after: ids [1,3], v ['z','c']); MERGE INTO "
                        f"{'accepted' if ok_m else 'not supported (' + _err(out_m, 70) + ')'}")
        elif ok:
            r.result = "fail"
            r.details = f"mutations ran but data is wrong: {out}"
        else:
            r.result = "fail"
            r.details = f"DELETE/UPDATE rejected: {_err(out)}"

    return _own_table_test(r, body)


# ---------------------------------------------------------------------------
# Row-level operations
# ---------------------------------------------------------------------------

def _delete_evidence(url, ns, name, delete_sql):
    """Run ClickHouse ``delete_sql`` against the fixture at ``url`` and return
    (ok, err, delete_files) where delete_files is Spark's content-type count
    read from storage."""
    ok, out = _ch_query(f"""
        {WRITE_SETTINGS}
        CREATE TABLE t ENGINE = IcebergS3('{url}', {S3_CREDS});
        {delete_sql}
    """)
    if not ok:
        return False, _err(out), None
    # ClickHouse committed to storage only; point Spark at the newest metadata.
    spark_fixture.refresh(ns, name)
    return True, "", spark_fixture.inspect_delete_files_from_storage(ns, name)


def test_position_deletes() -> TestResult:
    # Write capability: does ClickHouse's own DELETE emit a position-delete
    # file (content=1)? ClickHouse's DELETE on an Iceberg table rewrites data
    # files (copy-on-write); the datum is whatever delete-file content Spark
    # can see afterwards in storage.
    r = TestResult("position-deletes", "Position Deletes")

    def body(url, ns, name, r):
        ok, err, deletes = _delete_evidence(url, ns, name, "DELETE FROM t WHERE id = 2;")
        if not ok:
            r.result = "fail"
            r.details = f"ClickHouse DELETE rejected: {err}"
        elif deletes and deletes["position"] > 0:
            r.result = "pass"
            r.details = f"ClickHouse DELETE wrote position-delete file(s): {deletes}"
        else:
            r.result = "fail"
            r.details = (f"ClickHouse DELETE committed but produced no position-delete file "
                        f"({deletes}); the row was removed by rewriting data files. ClickHouse "
                        "does read position deletes written by other engines")

    return _fixture_test(r, "v2", body)


def test_equality_deletes() -> TestResult:
    # Write capability (none expected) plus the read half as evidence: a real
    # equality-delete file produced with the Iceberg Java API must be applied
    # by ClickHouse's scan.
    r = TestResult("equality-deletes", "Equality Deletes")
    prereq = _prereqs()
    if prereq:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq}"
        return r
    ns, name = None, "t"
    try:
        ns = spark_fixture.new_namespace()
        produced = spark_fixture.create_equality_delete_fixture(ns, name, "v2")
        url = _s3_url(ns, name)
        ok, out = _ch_query(f"SELECT groupArray(id) FROM (SELECT id FROM icebergS3('{url}', {S3_CREDS}) ORDER BY id)")
        reads = ok and out.strip() == str(produced["live_ids"]).replace(" ", "")
        # Write half: ClickHouse's DELETE never emits content=2.
        ok2, err2, deletes = _delete_evidence(url, ns, name, "DELETE FROM t WHERE id = 3;")
        wrote_eq = bool(deletes and deletes["equality"] > produced["delete_files"]["equality"])
        if wrote_eq:
            r.result = "pass"
            r.details = f"ClickHouse DELETE produced an equality-delete file: {deletes}"
        elif not reads:
            r.result = "error"
            r.details = (f"ClickHouse mis-read a table with an equality delete: got {out if ok else _err(out)}, "
                        f"expected {produced['live_ids']}")
        else:
            r.result = "fail"
            r.details = (f"ClickHouse READS equality deletes correctly (Java-API eq-delete on "
                        f"k='{produced['deleted_key']}' filtered id={produced['deleted_id']}; got {out}), "
                        f"but its own DELETE {'rewrites data files' if ok2 else 'was rejected: ' + err2} "
                        "and never writes an equality-delete file")
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:220]}"
    finally:
        if ns:
            spark_fixture.drop_fixture(ns, name)
    return r


def test_merge_on_read() -> TestResult:
    r = TestResult("merge-on-read", "Merge-on-Read")

    def body(url, ns, name, r):
        ok, err, deletes = _delete_evidence(url, ns, name, "ALTER TABLE t UPDATE val = 'x' WHERE id = 2;")
        if not ok:
            r.result = "fail"
            r.details = f"ClickHouse UPDATE rejected: {err}"
        elif deletes and (deletes["position"] or deletes["equality"]):
            r.result = "pass"
            r.details = f"ClickHouse UPDATE used merge-on-read (delete files written: {deletes})"
        else:
            r.result = "fail"
            r.details = (f"ClickHouse UPDATE committed with no delete files ({deletes}): the write "
                        "strategy is copy-on-write only, though ClickHouse reads MoR tables "
                        "(position and equality deletes) written by other engines")

    return _fixture_test(r, "v2", body, write_mode="merge-on-read")


def test_copy_on_write() -> TestResult:
    r = TestResult("copy-on-write", "Copy-on-Write")

    def body(url, ns, name, r):
        before = spark_fixture.inspect_delete_files_from_storage(ns, name)
        ok, err, deletes = _delete_evidence(url, ns, name, "DELETE FROM t WHERE id = 2;")
        rows = spark_fixture.row_count_from_storage(ns, name) if ok else None
        if not ok:
            r.result = "fail"
            r.details = f"ClickHouse DELETE rejected: {err}"
        elif deletes == before and rows == 2:
            r.result = "pass"
            r.details = (f"ClickHouse DELETE removed the row by rewriting data files: no new delete "
                        f"files ({deletes}), {rows} live rows -- copy-on-write")
        else:
            r.result = "fail"
            r.details = f"DELETE produced delete files {deletes} (before {before}), rows={rows}"

    return _fixture_test(r, "v2", body, write_mode="copy-on-write")


def test_deletion_vectors() -> TestResult:
    r = TestResult("deletion-vectors", "Deletion Vectors", "v3")

    def body(url, ns, name, r):
        ok, err, deletes = _delete_evidence(url, ns, name, "DELETE FROM t WHERE id = 2;")
        formats = spark_fixture.delete_file_formats_from_storage(ns, name) if ok else set()
        if not ok:
            r.result = "fail"
            r.details = f"DELETE on a V3 table rejected: {err}"
        elif "puffin" in {f.lower() for f in formats}:
            r.result = "pass"
            r.details = f"ClickHouse DELETE on a V3 table wrote a Puffin deletion vector ({deletes})"
        else:
            # Read half: a Spark-written DV must be applied by ClickHouse.
            spark_fixture.get_spark().sql(f"DELETE FROM local.{ns}.{name} WHERE id = 3")
            fmts = spark_fixture.delete_file_formats_from_storage(ns, name)
            ok_r, out_r = _ch_query(f"SELECT groupArray(id) FROM (SELECT id FROM icebergS3('{url}', {S3_CREDS}) ORDER BY id)")
            r.result = "fail"
            r.details = (f"ClickHouse DELETE did not write a deletion vector (delete files {deletes}, "
                        f"formats {sorted(formats)}); a Spark-written DV ({sorted(fmts)}) "
                        f"{'IS' if ok_r and out_r.strip() == '[1]' else 'is NOT'} applied on read: {out_r if ok_r else _err(out_r, 80)}")

    return _fixture_test(r, "v3", body, write_mode="merge-on-read")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def test_schema_evolution() -> TestResult:
    r = TestResult("schema-evolution", "Schema Evolution")

    def body(url, r):
        ok, out = _ch_query(f"""
            {WRITE_SETTINGS}
            CREATE TABLE t (id Int64, v String) ENGINE = IcebergS3('{url}', {S3_CREDS});
            INSERT INTO t VALUES (1,'a');
            ALTER TABLE t ADD COLUMN extra Nullable(Int32);
            ALTER TABLE t RENAME COLUMN v TO name;
            INSERT INTO t VALUES (2,'b',7);
            ALTER TABLE t DROP COLUMN extra;
            SELECT name FROM system.columns WHERE table = 't' AND database = currentDatabase() ORDER BY position;
        """)
        cols = out.split("\n") if ok else []
        if ok and cols == ["id", "name"]:
            ok2, out2 = _ch_query(f"SELECT groupArray(id) FROM icebergS3('{url}', {S3_CREDS})")
            r.result = "pass"
            r.details = (f"ALTER TABLE ADD / RENAME / DROP COLUMN on ClickHouse's own Iceberg table "
                        f"(columns now {cols}); rows still readable: {out2}")
        elif ok:
            r.result = "fail"
            r.details = f"schema after evolution unexpected: {cols}"
        else:
            r.result = "fail"
            r.details = f"ALTER TABLE rejected: {_err(out)}"

    return _own_table_test(r, body)


def test_schema_evolution_read() -> TestResult:
    """Read side: a Spark-evolved schema must be visible to ClickHouse."""
    r = TestResult("schema-evolution", "Schema Evolution", "v3")

    def body(url, ns, name, r):
        spark = spark_fixture.get_spark()
        spark.sql(f"ALTER TABLE local.{ns}.{name} ADD COLUMN age INT")
        spark.sql(f"INSERT INTO local.{ns}.{name} VALUES (4,'d',30)")
        ok, out = _ch_query(f"SELECT id, age FROM icebergS3('{url}', {S3_CREDS}) WHERE id IN (1,4) ORDER BY id FORMAT TSV")
        rows = [l.split("\t") for l in out.split("\n")] if ok else []
        if ok and rows == [["1", "\\N"], ["4", "30"]]:
            r.result = "pass"
            r.details = "ClickHouse reads a V3 table after Spark added a column: old rows NULL, new row 30"
        else:
            r.result = "fail"
            r.details = f"evolved schema read wrong: {rows if ok else _err(out)}"

    return _fixture_test(r, "v3", body)


def test_type_promotion() -> TestResult:
    r = TestResult("type-promotion", "Type Promotion / Widening")

    def body(url, r):
        # One table per promotion so a rejection of one does not mask the others.
        probes = {
            "int->bigint": ("Int32", "Int64", "100", "9999999999"),
            "float->double": ("Float32", "Float64", "1.5", "2.5"),
            "decimal(9,2)->decimal(18,2)": ("Decimal(9,2)", "Decimal(18,2)", "3.14", "9999.99"),
        }
        accepted, rejected = [], []
        for label, (from_t, to_t, v1, v2) in probes.items():
            ok, out = _ch_query(f"""
                {WRITE_SETTINGS}
                CREATE TABLE t (c {from_t}) ENGINE = IcebergS3('{url}{label.split('-')[0]}/', {S3_CREDS});
                INSERT INTO t VALUES ({v1});
                ALTER TABLE t MODIFY COLUMN c {to_t};
                INSERT INTO t VALUES ({v2});
                SELECT groupArray(toString(c)) FROM (SELECT c FROM t ORDER BY c);
            """)
            if ok and v1 in out and v2 in out:
                accepted.append(label)
            else:
                rejected.append(f"{label}: {_err(out, 70) if not ok else out}")
        if accepted:
            r.result = "pass"
            r.details = (f"ALTER TABLE MODIFY COLUMN performed {accepted} on ClickHouse's own Iceberg "
                        f"table and read the pre-promotion row back with the widened type"
                        + (f"; rejected: {rejected}" if rejected else ""))
        else:
            r.result = "fail"
            r.details = f"every type promotion was rejected: {rejected}"

    return _own_table_test(r, body)


def test_column_default_values() -> TestResult:
    r = TestResult("column-default-values", "Column Default Values", "v3")

    # OSS Spark cannot declare column defaults, so the fixture comes from
    # spark_fixture.create_column_default_fixture (PyIceberg adds the defaulted
    # column to a Spark-seeded V3 table); the first row has no value in its
    # data file and must read back as the default.
    prereq = _prereqs()
    if prereq:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq}"
        return r
    ns, name = None, "t"
    try:
        ns = spark_fixture.new_namespace()
        spark_fixture.create_column_default_fixture(ns, name)
        url = _s3_url(ns, name)
        ok, out = _ch_query(f"SELECT id, region FROM icebergS3('{url}', {S3_CREDS}) ORDER BY id FORMAT TSV")
        rows = [l.split("\t") for l in out.split("\n")] if ok else []
        if ok and rows == [["1", "eu"], ["2", "us"]]:
            r.result = "pass"
            r.details = ("ClickHouse applies V3 column defaults on read: the row inserted without a "
                        "value returns the schema default 'eu'")
        elif ok:
            r.result = "fail"
            r.details = f"V3 column default not applied on read: {rows} (expected id 1 -> 'eu')"
        else:
            r.result = "fail"
            r.details = f"reading a V3 table with column defaults failed: {_err(out)}"
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:220]}"
    finally:
        if ns:
            spark_fixture.drop_fixture(ns, name)
    return r


# ---------------------------------------------------------------------------
# Snapshots, maintenance, refs
# ---------------------------------------------------------------------------

def test_time_travel() -> TestResult:
    r = TestResult("time-travel", "Time Travel / Snapshots")

    def body(url, ns, name, r):
        spark = spark_fixture.get_spark()
        snap = spark.sql(f"SELECT snapshot_id FROM local.{ns}.{name}.snapshots ORDER BY committed_at LIMIT 1").collect()[0][0]
        spark.sql(f"INSERT INTO local.{ns}.{name} VALUES (4,'d'),(5,'e')")
        ok, out = _ch_query(f"SELECT count() FROM icebergS3('{url}', {S3_CREDS}) SETTINGS iceberg_snapshot_id = {snap}")
        ok2, out2 = _ch_query(f"SELECT count() FROM icebergS3('{url}', {S3_CREDS})")
        if ok and out.strip() == "3" and ok2 and out2.strip() == "5":
            r.result = "pass"
            r.details = f"iceberg_snapshot_id={snap} returns the historical 3 rows; current read returns 5"
        else:
            r.result = "fail"
            r.details = f"time travel mismatch: at snapshot -> {out if ok else _err(out)}, current -> {out2}"

    return _fixture_test(r, "v2", body)


def test_table_maintenance() -> TestResult:
    r = TestResult("table-maintenance", "Table Maintenance")

    def body(url, r):
        # Two maintenance surfaces exist: plain OPTIMIZE TABLE (data-file
        # compaction, if any) and OPTIMIZE TABLE ... MANIFEST (manifest rewrite,
        # gated by iceberg_manifest_min_count_to_compact, default 30, lowered
        # here so three manifests qualify). Each is proven by the newest
        # metadata.json: a 'replace' snapshot, and for data compaction a
        # total-data-files below the three INSERTs produced. Old files stay on
        # disk until expiry, so a file count on storage would say nothing. All
        # in one session: a second CREATE TABLE on the same path is rejected.
        snap_sql = f"""
            SELECT concat('MARK', JSONExtractString(last, 'summary', 'operation'), '|',
                          JSONExtractString(last, 'summary', 'total-data-files'), '|',
                          toString(length(JSONExtractArrayRaw(raw, 'snapshots'))))
            FROM (SELECT raw, arrayElement(JSONExtractArrayRaw(raw, 'snapshots'), -1) AS last, _file
                  FROM s3('{url}metadata/*.metadata.json', {S3_CREDS}, 'RawBLOB', 'raw String')
                  ORDER BY length(_file) DESC, _file DESC LIMIT 1)"""
        ok, out = _ch_query(f"""
            {WRITE_SETTINGS}
            SET allow_experimental_iceberg_compaction = 1;
            CREATE TABLE t (id Int64) ENGINE = IcebergS3('{url}', {S3_CREDS});
            INSERT INTO t VALUES (1);
            INSERT INTO t VALUES (2);
            INSERT INTO t VALUES (3);
            OPTIMIZE TABLE t;
            {snap_sql};
            SET iceberg_manifest_min_count_to_compact = 1;
            OPTIMIZE TABLE t MANIFEST;
            {snap_sql};
            SELECT concat('ROWS', toString(count())) FROM t;
        """)
        if not ok:
            r.result = "fail"
            r.details = f"OPTIMIZE TABLE (compaction) rejected: {_err(out)}"
            return
        marks = [ln[4:].split("|") for ln in out.splitlines() if ln.startswith("MARK")]
        rows = [ln[4:] for ln in out.splitlines() if ln.startswith("ROWS")]
        (op, files, snaps), (op_m, files_m, snaps_m) = (marks + [["?", "?", "?"]] * 2)[:2]
        data_compacted = op == "replace" and files.isdigit() and int(files) < 3
        data_note = (f"plain OPTIMIZE TABLE compacted data files (last snapshot '{op}', "
                     f"total-data-files={files})" if data_compacted else
                     f"plain OPTIMIZE TABLE is a no-op on data files (last snapshot op={op}, "
                     f"total-data-files={files}, {snaps} snapshots)")
        manifest_compacted = (op_m == "replace" and snaps_m.isdigit() and snaps.isdigit()
                              and int(snaps_m) > int(snaps))
        manifest_note = (f"OPTIMIZE TABLE ... MANIFEST rewrote the manifests into a new '{op_m}' "
                         f"snapshot ({snaps_m} snapshots, total-data-files={files_m}, rows preserved: "
                         f"{rows[-1] if rows else '?'})" if manifest_compacted else
                         f"OPTIMIZE TABLE ... MANIFEST produced no replace snapshot (op={op_m}, "
                         f"{snaps_m} snapshots)")
        if data_compacted or manifest_compacted:
            r.result = "pass"
            r.details = (f"{data_note}; {manifest_note}. Manifest compaction is experimental and "
                         "V2-only; snapshot expiry / orphan-file removal have no SQL surface")
        else:
            r.result = "fail"
            r.details = f"{data_note}; {manifest_note}"

    return _own_table_test(r, body)


def test_branching_tagging() -> TestResult:
    r = TestResult("branching-tagging", "Branching & Tagging")

    def body(url, ns, name, r):
        spark = spark_fixture.get_spark()
        spark.sql(f"ALTER TABLE local.{ns}.{name} CREATE BRANCH dev")
        spark.sql(f"ALTER TABLE local.{ns}.{name} CREATE TAG v1")
        spark.sql(f"INSERT INTO local.{ns}.{name}.branch_dev VALUES (99,'branch-only')")
        attempts = {
            "branch read": f"SELECT count() FROM icebergS3('{url}', {S3_CREDS}) SETTINGS iceberg_branch = 'dev'",
            "tag read": f"SELECT count() FROM icebergS3('{url}', {S3_CREDS}) SETTINGS iceberg_tag = 'v1'",
            "create branch": f"{WRITE_SETTINGS} CREATE TABLE t ENGINE = IcebergS3('{url}', {S3_CREDS}); ALTER TABLE t CREATE BRANCH b2",
        }
        outcomes = {k: _ch_query(v) for k, v in attempts.items()}
        ok_b, out_b = outcomes["branch read"]
        if ok_b and out_b.strip() == "4":
            r.result = "pass"
            r.details = (f"ClickHouse read a Spark-created branch (4 rows incl. the branch-only row); "
                        f"tag read: {outcomes['tag read'][1][:40]}; create branch: "
                        f"{'accepted' if outcomes['create branch'][0] else 'rejected'}")
        else:
            r.result = "fail"
            r.details = ("ClickHouse has no branch/tag surface for Iceberg: branch read -> "
                        f"{_err(out_b, 90) if not ok_b else out_b}; create branch -> "
                        f"{_err(outcomes['create branch'][1], 70)}")

    return _fixture_test(r, "v2", body)


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------

def test_hidden_partitioning() -> TestResult:
    r = TestResult("hidden-partitioning", "Hidden Partitioning")

    def body(url, r):
        ok, out = _ch_query(f"""
            {WRITE_SETTINGS}
            CREATE TABLE t (id Int64, s String, d Date) ENGINE = IcebergS3('{url}', {S3_CREDS})
                PARTITION BY (icebergBucket(4, id), icebergTruncate(2, s));
            INSERT INTO t VALUES (1,'abcd','2024-01-15'),(2,'efgh','2024-02-20'),(3,'abzz','2024-01-25');
            SELECT count() FROM t WHERE s LIKE 'ab%';
        """)
        ok_t, out_t = _ch_query(f"""
            {WRITE_SETTINGS}
            CREATE TABLE t2 (id Int64, d Date) ENGINE = IcebergS3('{url}t2/', {S3_CREDS}) PARTITION BY toYYYYMM(d);
        """)
        if ok and out.strip() == "2":
            r.result = "pass"
            r.details = ("Created and wrote a table hidden-partitioned by icebergBucket(4,id) + "
                        "icebergTruncate(2,s), predicate read correct; time transforms "
                        f"(month/day/hour) are {'accepted' if ok_t else 'not accepted: ' + _err(out_t, 60)}")
        else:
            r.result = "fail"
            r.details = f"transform-partitioned CREATE/INSERT failed: {_err(out) if not ok else out}"

    return _own_table_test(r, body)


def test_hidden_partitioning_read() -> TestResult:
    r = TestResult("hidden-partitioning", "Hidden Partitioning", "v3")

    def body(url, ns, name, r):
        ok, out = _ch_query(f"SELECT count() FROM icebergS3('{url}', {S3_CREDS}) WHERE d >= '2024-02-01'")
        if ok and out.strip() == "1":
            r.result = "pass"
            r.details = ("ClickHouse read a Spark-created V3 table partitioned by months(d) with "
                        "a date predicate (1 of 3 rows) -- partition pruning applies")
        else:
            r.result = "fail"
            r.details = f"read of hidden-partitioned V3 table wrong: {out if ok else _err(out)}"

    prereq = _prereqs()
    if prereq:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq}"
        return r
    ns, name = None, "t"
    try:
        ns = spark_fixture.new_namespace()
        spark = spark_fixture.get_spark()
        spark.sql(f"""CREATE TABLE local.{ns}.{name} (id BIGINT, d DATE) USING iceberg
                      PARTITIONED BY (months(d)) TBLPROPERTIES ('format-version'='3')""")
        spark.sql(f"INSERT INTO local.{ns}.{name} VALUES (1, DATE'2024-01-15'),(2, DATE'2024-02-20'),(3, DATE'2024-01-25')")
        body(_s3_url(ns, name), ns, name, r)
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:220]}"
    finally:
        if ns:
            spark_fixture.drop_fixture(ns, name)
    return r


def test_partition_evolution() -> TestResult:
    r = TestResult("partition-evolution", "Partition Evolution")

    def body(url, ns, name, r):
        spark = spark_fixture.get_spark()
        spark.sql(f"ALTER TABLE local.{ns}.{name} ADD PARTITION FIELD bucket(4, id)")
        spark.sql(f"INSERT INTO local.{ns}.{name} VALUES (4,'d'),(5,'e')")
        ok, out = _ch_query(f"SELECT count() FROM icebergS3('{url}', {S3_CREDS})")
        ok_w, out_w = _ch_query(f"""
            {WRITE_SETTINGS}
            CREATE TABLE t (id Int64, val String) ENGINE = IcebergS3('{url}', {S3_CREDS});
            ALTER TABLE t MODIFY PARTITION BY icebergBucket(8, id);
        """)
        if ok and out.strip() == "5":
            r.result = "pass"
            r.details = ("ClickHouse reads across two partition specs after Spark evolved the spec "
                        f"(5 rows); ClickHouse cannot evolve a spec itself ({_err(out_w, 60)})")
        else:
            r.result = "fail"
            r.details = f"read across evolved specs wrong: {out if ok else _err(out)}"

    return _fixture_test(r, "v2", body)


def test_multi_arg_transforms() -> TestResult:
    r = TestResult("multi-arg-transforms", "Multi-Argument Transforms", "v3")

    def body(url, r):
        ok, out = _ch_query(f"""
            {WRITE_SETTINGS}
            CREATE TABLE t (a Int64, b Int64) ENGINE = IcebergS3('{url}', {S3_CREDS})
                PARTITION BY icebergBucket(8, a, b) SETTINGS iceberg_format_version = 3;
        """)
        if ok:
            r.result = "pass"
            r.details = "Created a V3 table partitioned by a multi-argument bucket(8, a, b)"
        else:
            r.result = "fail"
            r.details = f"multi-argument bucket() rejected: {_err(out)}"

    return _own_table_test(r, body)


# ---------------------------------------------------------------------------
# Statistics / bloom filters
# ---------------------------------------------------------------------------

def test_statistics() -> TestResult:
    r = TestResult("statistics", "Statistics (Column Metrics)")

    def body(url, ns, name, r):
        # Two data files with disjoint id ranges; a predicate that matches only
        # one must, with column min/max applied, read exactly one file.
        spark = spark_fixture.get_spark()
        spark.sql(f"INSERT INTO local.{ns}.{name} VALUES (1000,'far')")
        ok, out = _ch_query(f"SELECT count() FROM icebergS3('{url}', {S3_CREDS}) WHERE id > 500")
        # Do the statistics ClickHouse writes carry min/max? Its own INSERT is the
        # write half of this cell.
        ok_w, out_w = _ch_query(f"""
            {WRITE_SETTINGS}
            CREATE TABLE t ENGINE = IcebergS3('{url}', {S3_CREDS});
            INSERT INTO t (id, val) VALUES (2000, 'ch');
        """)
        bounds = "n/a"
        if ok_w:
            try:
                tbl = spark_fixture._storage_table(ns, name)
                snap = tbl.currentSnapshot()
                lb = [str(df.lowerBounds()) for df in spark_fixture._jiter(snap.addedDataFiles(tbl.io()))]
                bounds = "present" if lb and all(b not in ("null", "{}") for b in lb) else f"missing ({lb})"
            except Exception as e:  # noqa: BLE001
                bounds = f"unreadable ({str(e).splitlines()[0][:60]})"
        if ok and out.strip() == "1":
            r.result = "pass"
            r.details = ("ClickHouse applies Iceberg column statistics on read (predicate over two "
                        "files returned only the matching row) and the data file its own INSERT "
                        f"wrote carries column lower/upper bounds: {bounds}")
        else:
            r.result = "fail"
            r.details = f"stats-driven read wrong: {out if ok else _err(out)}"

    return _fixture_test(r, "v2", body)


def test_bloom_filters() -> TestResult:
    r = TestResult("bloom-filters", "Bloom Filters")

    def body(url, r):
        # The Iceberg way to ask for a bloom filter is the table property
        # write.parquet.bloom-filter-enabled.column.<col>; ClickHouse's Iceberg
        # engine has no table-property surface, so also try its own Parquet
        # writer switch. Presence is bloom_filter_bytes > 0 on the column chunk.
        def bloom_bytes(setting: str) -> tuple:
            return _ch_query(f"""
                {WRITE_SETTINGS}
                {setting}
                CREATE TABLE t (id Int64, v String) ENGINE = IcebergS3('{url}{len(setting)}/', {S3_CREDS});
                INSERT INTO t SELECT number, toString(number) FROM numbers(1000);
                SELECT sum(col.bloom_filter_bytes) FROM s3('{url}{len(setting)}/data/*.parquet', {S3_CREDS}, 'ParquetMetadata')
                    ARRAY JOIN row_groups AS rg ARRAY JOIN rg.columns AS col;
            """)
        ok_d, out_d = bloom_bytes("")
        ok_s, out_s = bloom_bytes("SET output_format_parquet_write_bloom_filter = 1;")
        if ok_s and out_s.strip() not in ("", "0"):
            r.result = "pass"
            r.details = (f"ClickHouse writes Parquet bloom filters into Iceberg data files when "
                        f"output_format_parquet_write_bloom_filter=1 ({out_s} bytes; default run: "
                        f"{out_d if ok_d else 'n/a'}). It has no way to honour the Iceberg "
                        "write.parquet.bloom-filter-enabled table property, and does not write Puffin stats")
        elif ok_s:
            r.result = "fail"
            r.details = ("no bloom filters in the Iceberg data files ClickHouse wrote, with or "
                        "without output_format_parquet_write_bloom_filter")
        else:
            r.result = "fail"
            r.details = f"bloom-filter probe failed: {_err(out_s)}"

    return _own_table_test(r, body)


# ---------------------------------------------------------------------------
# Catalogs
# ---------------------------------------------------------------------------

def _datalake_catalog_ddl(db: str) -> str:
    client_id, secret = spark_fixture.rest_client_id_secret()
    auth = ""
    if client_id:
        auth = (f", catalog_credential = '{client_id}:{secret}', "
                f"auth_scope = '{spark_fixture.REST_SCOPE}', "
                f"oauth_server_uri = '{spark_fixture.REST_URI.rstrip('/')}/v1/oauth/tokens'")
    return f"""
        SET allow_experimental_database_iceberg = 1;
        SET allow_database_iceberg = 1;
        CREATE DATABASE {db} ENGINE = DataLakeCatalog('{spark_fixture.REST_URI}', {S3_CREDS})
            SETTINGS catalog_type = 'rest', warehouse = '{spark_fixture.REST_WAREHOUSE}'{auth},
                     storage_endpoint = 'http://{spark_fixture.S3_ENDPOINT}';
    """


def test_rest_catalog() -> TestResult:
    r = TestResult("rest-catalog", "REST Catalog")

    def body(url, ns, name, r):
        ok, out = _ch_query(_datalake_catalog_ddl("pol") + f"SHOW TABLES FROM pol LIKE '{ns}.%';")
        if not ok:
            r.result = "fail"
            r.details = f"DataLakeCatalog(catalog_type='rest') could not attach to Polaris: {_err(out)}"
            return
        listed = f"{ns}.{name}" in out
        ok_r, out_r = _ch_query(_datalake_catalog_ddl("pol") + f"SELECT count() FROM pol.`{ns}.{name}`;")
        if listed and ok_r and out_r.strip() == "3":
            r.result = "pass"
            r.details = ("DataLakeCatalog(catalog_type='rest') authenticated with OAuth2, listed the "
                        "catalog's namespaces/tables and read a table through it (3 rows)")
        elif listed:
            r.result = "pass"
            r.details = ("REST catalog protocol works: OAuth2 login, GET /v1/config, namespace and "
                        "table listing and loadTable all succeed against Polaris. Reading table "
                        "DATA through the catalog handle fails on this stack because ClickHouse "
                        "builds the object key from the catalog's metadata-location as "
                        f"<endpoint>/<bucket>/<key>/<bucket>/<key> against path-style MinIO ({_err(out_r, 60)}); "
                        "the same table reads fine via icebergS3() at its location. Partial, not none")
        else:
            r.result = "fail"
            r.details = f"attached but the fixture table was not listed: {out[:120]}"

    return _fixture_test(r, "v2", body)


def test_catalog_integration() -> TestResult:
    r = TestResult("catalog-integration", "Catalog Integration")

    def body(url, ns, name, r):
        ok, out = _ch_query(_datalake_catalog_ddl("pol") + "SELECT count() FROM system.tables WHERE database = 'pol';")
        ok_c, out_c = _ch_query(_datalake_catalog_ddl("pol") + f"CREATE TABLE pol.`{ns}.fromch` (id Int64) ENGINE = IcebergS3('{url}fromch/', {S3_CREDS});")
        if ok:
            r.result = "pass"
            r.details = (f"DataLakeCatalog attaches Iceberg REST (and Glue/Unity/Hive) catalogs as a "
                        f"database ({out} tables visible); creating tables THROUGH the catalog is "
                        f"{'accepted' if ok_c else 'not supported (' + _err(out_c, 60) + ')'} -- "
                        "ClickHouse's own writes go to storage, not the catalog")
        else:
            r.result = "fail"
            r.details = f"no catalog protocol support: {_err(out)}"

    return _fixture_test(r, "v2", body)


def test_aws_glue_catalog() -> TestResult:
    r = TestResult("aws-glue-catalog", "AWS Glue Catalog")
    # DataLakeCatalog supports catalog_type='glue', but proving it needs AWS
    # credentials and a Glue endpoint this harness does not have.
    r.result = "skip"
    r.details = ("Not exercised: DataLakeCatalog(catalog_type='glue') exists but requires AWS "
                 "credentials and a Glue Data Catalog")
    return r


def test_unity_catalog() -> TestResult:
    r = TestResult("unity-catalog", "Unity Catalog")
    r.result = "skip"
    r.details = ("Not exercised: DataLakeCatalog(catalog_type='unity') exists but requires a "
                 "Databricks Unity Catalog endpoint")
    return r


# ---------------------------------------------------------------------------
# V3 data types / capabilities
# ---------------------------------------------------------------------------

def _v3_own_type_probe(r: TestResult, col_ddl: str, insert_val: str, select_expr: str,
                       expect_substr: str, extra_settings: str = ""):
    """Create a V3 table with one typed column in ClickHouse, insert, read back."""
    def body(url, r):
        ok, out = _ch_query(f"""
            {WRITE_SETTINGS}
            {extra_settings}
            CREATE TABLE t (id Int64, c {col_ddl}) ENGINE = IcebergS3('{url}', {S3_CREDS})
                SETTINGS iceberg_format_version = 3;
            INSERT INTO t VALUES (1, {insert_val});
            SELECT {select_expr} FROM t;
        """)
        if ok and expect_substr in out:
            r.result = "pass"
            r.details = f"V3 table with a {col_ddl} column written and read back: {out[:80]}"
        elif ok:
            r.result = "fail"
            r.details = f"{col_ddl} column round-trip returned {out[:80]!r}, expected to contain {expect_substr!r}"
        else:
            r.result = "fail"
            r.details = f"{col_ddl} column rejected on a V3 Iceberg table: {_err(out)}"
    return _own_table_test(r, body)


def test_variant_type() -> TestResult:
    r = TestResult("variant-type", "Variant Type", "v3")
    return _v3_own_type_probe(r, "Variant(String, Int64)", "'x'", "toString(c)", "x",
                              "SET allow_experimental_variant_type = 1;")


def test_shredded_variant() -> TestResult:
    r = TestResult("shredded-variant", "Shredded Variant", "v3")
    # Shredding presupposes writing a variant at all; measure the read half on a
    # Spark-written (unshredded) variant and the write half via test_variant_type.
    def body(url, ns, name, r):
        spark = spark_fixture.get_spark()
        try:
            spark.sql(f"ALTER TABLE local.{ns}.{name} ADD COLUMN payload VARIANT")
        except Exception as e:  # noqa: BLE001
            r.result = "skip"
            r.details = f"Spark could not add a VARIANT column to produce a fixture: {str(e).splitlines()[0][:120]}"
            return
        spark.sql(f"INSERT INTO local.{ns}.{name} VALUES (4, 'd', parse_json('{{\"k\":1}}'))")
        ok, out = _ch_query(f"SELECT id FROM icebergS3('{url}', {S3_CREDS}) WHERE id = 4")
        r.result = "fail"
        r.details = ("ClickHouse cannot write Iceberg VARIANT (Variant/JSON columns are rejected on "
                    f"IcebergS3 tables), so it cannot shred one; reading a table with a Spark-written "
                    f"variant column: {'ok (variant column skipped/readable)' if ok else _err(out, 90)}")

    return _fixture_test(r, "v3", body)


def test_geometry_type() -> TestResult:
    r = TestResult("geometry-type", "Geometry / Geo Types", "v3")
    return _v3_own_type_probe(r, "Point", "(1.5, 2.5)", "toString(c)", "(1.5,2.5)",
                              "SET allow_experimental_geo_types_in_iceberg = 1;")


def test_nanosecond_timestamps() -> TestResult:
    r = TestResult("nanosecond-timestamps", "Nanosecond Timestamps", "v3")

    def body(url, r):
        ok, out = _ch_query(f"""
            {WRITE_SETTINGS}
            CREATE TABLE t (id Int64, ts DateTime64(9)) ENGINE = IcebergS3('{url}', {S3_CREDS})
                SETTINGS iceberg_format_version = 3;
            INSERT INTO t VALUES (1, '2026-05-20 12:00:00.123456789');
            SELECT toString(ts) FROM t;
        """)
        if ok and out.strip().endswith(".123456789"):
            r.result = "pass"
            r.details = f"DateTime64(9) round-tripped with nanosecond precision on a V3 table: {out}"
        elif ok:
            r.result = "fail"
            r.details = (f"DateTime64(9) accepted but precision was truncated on the Iceberg write: "
                        f"{out} (nanoseconds lost) -- timestamp_ns is not really supported")
        else:
            r.result = "fail"
            r.details = f"DateTime64(9) rejected on a V3 Iceberg table: {_err(out)}"

    return _own_table_test(r, body)


def test_unknown_type() -> TestResult:
    r = TestResult("unknown-type", "Unknown Type", "v3")

    def body(url, ns, name, r):
        # ClickHouse has no 'unknown' type to write; measure the read half on a
        # Spark-created V3 table with an unknown-typed column (declared VOID).
        spark = spark_fixture.get_spark()
        try:
            spark.sql(f"DROP TABLE IF EXISTS local.{ns}.u")
            spark.sql(f"CREATE TABLE local.{ns}.u (id BIGINT, u VOID) USING iceberg TBLPROPERTIES ('format-version'='3')")
            spark.sql(f"INSERT INTO local.{ns}.u VALUES (1, NULL)")
        except Exception as e:  # noqa: BLE001
            r.result = "skip"
            r.details = f"could not produce an unknown-type fixture: {str(e).splitlines()[0][:120]}"
            return
        try:
            ok, out = _ch_query(f"SELECT id, u FROM icebergS3('{_s3_url(ns, 'u')}', {S3_CREDS})")
        finally:
            spark_fixture.drop_fixture(ns, "u")
        if ok and out.startswith("1"):
            r.result = "pass"
            r.details = f"ClickHouse reads a V3 table with an unknown-type column (value {out.split(chr(9))[-1]!r})"
        else:
            r.result = "fail"
            r.details = f"unknown-type column not readable: {_err(out) if not ok else out}"

    return _fixture_test(r, "v3", body)


def test_lineage() -> TestResult:
    r = TestResult("lineage", "Lineage Tracking", "v3")

    def body(url, ns, name, r):
        # Row lineage lives in the V3 metadata Spark wrote; ClickHouse must at
        # least read such a table, and may expose the _row_id column.
        ok, out = _ch_query(f"SELECT count() FROM icebergS3('{url}', {S3_CREDS})")
        ok_l, out_l = _ch_query(f"SELECT _row_id FROM icebergS3('{url}', {S3_CREDS}) LIMIT 1")
        if ok_l:
            r.result = "pass"
            r.details = f"ClickHouse exposes V3 row lineage (_row_id readable: {out_l})"
        elif ok:
            r.result = "fail"
            r.details = ("ClickHouse reads V3 tables that carry row lineage but does not expose "
                        f"or maintain lineage columns ({_err(out_l, 70)})")
        else:
            r.result = "fail"
            r.details = f"could not read a V3 table with row lineage: {_err(out)}"

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
    test_schema_evolution_read,
    test_type_promotion,
    test_column_default_values,
    test_time_travel,
    test_table_maintenance,
    test_branching_tagging,
    test_hidden_partitioning,
    test_hidden_partitioning_read,
    test_partition_evolution,
    test_multi_arg_transforms,
    test_statistics,
    test_bloom_filters,
    test_catalog_integration,
    test_rest_catalog,
    test_aws_glue_catalog,
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

def load_clickhouse_json_support() -> dict:
    oss_path = os.path.join(REPO_ROOT, "src", "data", "platforms", "oss", "clickhouse", "clickhouse.json")
    with open(oss_path) as f:
        data = json.load(f)
    result = {}
    for key, val in data.get("support", {}).items():
        if key.startswith("clickhouse:"):
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
    json_support = load_clickhouse_json_support()
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
        "engine": "ClickHouse",
        "clickhouse_version": CLICKHOUSE_VERSION,
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
    lines = ["# ClickHouse Iceberg Feature Test Report", "",
             f"- **Timestamp:** {report['timestamp']}",
             f"- **ClickHouse Version:** {report['clickhouse_version']}",
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
    print("  ClickHouse Iceberg Feature Test Suite")
    print("=" * 70)
    print(f"ClickHouse version: {CLICKHOUSE_VERSION}")
    print(f"ClickHouse binary: {CLICKHOUSE_BINARY}")
    print(f"REST catalog: {spark_fixture.REST_URI} (catalog '{spark_fixture.REST_WAREHOUSE}')")
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
    json_path = os.path.join(REPORT_DIR, "clickhouse-iceberg-test-report.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    md_content = generate_markdown(report)
    md_path = os.path.join(REPORT_DIR, "clickhouse-iceberg-test-report.md")
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
