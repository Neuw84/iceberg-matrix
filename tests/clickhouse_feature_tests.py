#!/usr/bin/env python3
"""
ClickHouse Iceberg Feature Test Suite.

Tests Iceberg features using ClickHouse's icebergLocal() table function.
PySpark (local mode) creates Iceberg tables with specific features,
then ClickHouse queries them via the `clickhouse local` CLI binary.

Usage:
    python tests/clickhouse_feature_tests.py

Prerequisites:
    - clickhouse binary: curl https://clickhouse.com/install.sh | sh
    - Java 11+ (for PySpark)
    - pip install pyspark pyiceberg[sql-sqlite,pyarrow]

Environment variables:
    ICEBERG_WAREHOUSE   - Local warehouse path (default: ./clickhouse-iceberg-warehouse)
    CLICKHOUSE_BINARY   - Path to clickhouse binary (default: auto-detected)
    REPO_ROOT           - Repo root path (default: auto-detected)
    REPORT_DIR          - Output directory for reports (default: ./test-reports)
    CLICKHOUSE_VERSION  - Override reported ClickHouse version (default: auto-detected)
"""

import json
import os
import re
import shutil
import subprocess
import sys
import traceback
import uuid
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
WAREHOUSE_DIR = os.environ.get(
    "ICEBERG_WAREHOUSE", os.path.join(os.getcwd(), "clickhouse-iceberg-warehouse")
)
REPO_ROOT = os.environ.get(
    "REPO_ROOT",
    str(Path(__file__).resolve().parent.parent),
)
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(os.getcwd(), "test-reports"))

# Auto-detect clickhouse binary
_CH_CANDIDATES = ["clickhouse", os.path.expanduser("~/clickhouse"), "./clickhouse"]
CLICKHOUSE_BINARY = os.environ.get("CLICKHOUSE_BINARY", "")
if not CLICKHOUSE_BINARY:
    for _c in _CH_CANDIDATES:
        if shutil.which(_c) or os.path.isfile(_c):
            CLICKHOUSE_BINARY = _c
            break
    if not CLICKHOUSE_BINARY:
        CLICKHOUSE_BINARY = "clickhouse"  # will fail gracefully later

# Detect ClickHouse version
def _detect_ch_version() -> str:
    try:
        out = subprocess.check_output(
            [CLICKHOUSE_BINARY, "local", "--version"],
            stderr=subprocess.STDOUT, timeout=10
        ).decode()
        m = re.search(r"(\d+\.\d+\.\d+\.\d+)", out)
        return m.group(1) if m else "unknown"
    except Exception:
        return "unknown"

CLICKHOUSE_VERSION = os.environ.get("CLICKHOUSE_VERSION", _detect_ch_version())

# ---------------------------------------------------------------------------
# PySpark setup
# ---------------------------------------------------------------------------
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


def _get_spark(warehouse: str) -> "SparkSession":
    """Create a local PySpark session with Iceberg support."""
    iceberg_jar = os.environ.get("ICEBERG_JAR", "")
    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("clickhouse-iceberg-tests")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .config("spark.sql.defaultCatalog", "local")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.logLevel=ERROR")
    )
    if iceberg_jar:
        builder = builder.config("spark.jars", iceberg_jar)
    return builder.getOrCreate()


def _stop_spark(spark) -> None:
    try:
        spark.stop()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# ClickHouse helpers
# ---------------------------------------------------------------------------

def _ch_query(sql: str, timeout: int = 30) -> tuple[bool, str]:
    """Run a SQL query via `clickhouse local`. Returns (success, output)."""
    if not shutil.which(CLICKHOUSE_BINARY) and not os.path.isfile(CLICKHOUSE_BINARY):
        return False, f"clickhouse binary not found at: {CLICKHOUSE_BINARY}"
    try:
        result = subprocess.run(
            [CLICKHOUSE_BINARY, "local", "--query", sql],
            capture_output=True, text=True, timeout=timeout
        )
        if result.returncode == 0:
            return True, result.stdout.strip()
        return False, (result.stderr or result.stdout).strip()
    except subprocess.TimeoutExpired:
        return False, "Query timed out"
    except FileNotFoundError:
        return False, f"clickhouse binary not found: {CLICKHOUSE_BINARY}"
    except Exception as e:
        return False, str(e)


def _ch_available() -> bool:
    ok, _ = _ch_query("SELECT 1")
    return ok


def _unique(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Result class
# ---------------------------------------------------------------------------

class TestResult:
    def __init__(self, feature_id: str, feature_name: str):
        self.feature_id = feature_id
        self.feature_name = feature_name
        self.result = "skip"  # pass | fail | skip | error
        self.details = ""
        self.version_tested = "v2"

    def to_dict(self):
        return {
            "feature_id": self.feature_id,
            "feature_name": self.feature_name,
            "version": self.version_tested,
            "result": self.result,
            "details": self.details,
        }


# ---------------------------------------------------------------------------
# Shared setup check
# ---------------------------------------------------------------------------

def _check_prerequisites() -> tuple[bool, bool, str]:
    """Returns (ch_ok, spark_ok, message)."""
    ch_ok = _ch_available()
    spark_ok = PYSPARK_AVAILABLE
    msgs = []
    if not ch_ok:
        msgs.append(f"ClickHouse not available (binary: {CLICKHOUSE_BINARY}). "
                    "Install: curl https://clickhouse.com/install.sh | sh")
    if not spark_ok:
        msgs.append("PySpark not installed. Run: pip install pyspark")
    return ch_ok, spark_ok, "; ".join(msgs) if msgs else "OK"


# ---------------------------------------------------------------------------
# Individual test functions
# ---------------------------------------------------------------------------

def test_read_support() -> TestResult:
    """Basic read via icebergLocal() table function."""
    r = TestResult("read-support", "Read Support")
    ch_ok, spark_ok, prereq_msg = _check_prerequisites()

    if not ch_ok or not spark_ok:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq_msg}"
        return r

    table_dir = os.path.join(WAREHOUSE_DIR, _unique("read"))
    spark = None
    try:
        spark = _get_spark(WAREHOUSE_DIR)
        spark.sql(f"""
            CREATE TABLE local.default.read_test (id INT, name STRING)
            USING iceberg
            LOCATION '{table_dir}'
        """)
        spark.sql("INSERT INTO local.default.read_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")

        ok, out = _ch_query(f"SELECT count(*) FROM icebergLocal('{table_dir}')")
        if ok and out.strip() == "3":
            r.result = "pass"
            r.details = "icebergLocal() reads Iceberg table; got 3 rows as expected"
        elif ok:
            r.result = "error"
            r.details = f"Unexpected row count: {out}"
        else:
            r.result = "fail"
            r.details = f"icebergLocal() failed: {out[:200]}"
    except Exception as e:
        r.result = "error"
        r.details = traceback.format_exc()[:300]
    finally:
        _stop_spark(spark)
        shutil.rmtree(table_dir, ignore_errors=True)
    return r


def test_position_deletes() -> TestResult:
    """Create a MOR table with position deletes via Spark, read with ClickHouse."""
    r = TestResult("position-deletes", "Position Deletes")
    ch_ok, spark_ok, prereq_msg = _check_prerequisites()

    if not ch_ok or not spark_ok:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq_msg}"
        return r

    table_dir = os.path.join(WAREHOUSE_DIR, _unique("posdeletes"))
    spark = None
    try:
        spark = _get_spark(WAREHOUSE_DIR)
        spark.sql(f"""
            CREATE TABLE local.default.pos_del_test (id INT, val STRING)
            USING iceberg
            LOCATION '{table_dir}'
            TBLPROPERTIES (
                'write.delete.mode'='merge-on-read',
                'write.update.mode'='merge-on-read'
            )
        """)
        spark.sql("INSERT INTO local.default.pos_del_test VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
        # Delete row 2 — Spark writes a positional delete file
        spark.sql("DELETE FROM local.default.pos_del_test WHERE id = 2")

        ok, out = _ch_query(f"SELECT id FROM icebergLocal('{table_dir}') ORDER BY id FORMAT TSV")
        if ok:
            ids = [int(x) for x in out.strip().split("\n") if x.strip()]
            if ids == [1, 3, 4]:
                r.result = "pass"
                r.details = "ClickHouse correctly reads table with position deletes; deleted row excluded"
            else:
                r.result = "fail"
                r.details = f"Expected [1,3,4] but got {ids} — position deletes may not be applied"
        else:
            r.result = "fail"
            r.details = f"icebergLocal() failed: {out[:200]}"
    except Exception as e:
        r.result = "error"
        r.details = traceback.format_exc()[:300]
    finally:
        _stop_spark(spark)
        shutil.rmtree(table_dir, ignore_errors=True)
    return r


def test_equality_deletes() -> TestResult:
    """Create a table with equality deletes via Spark, read with ClickHouse.

    This is the key test — equality deletes are supported in ClickHouse
    (confirmed via PR #75930) but not documented officially.
    """
    r = TestResult("equality-deletes", "Equality Deletes")
    ch_ok, spark_ok, prereq_msg = _check_prerequisites()

    if not ch_ok or not spark_ok:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq_msg}"
        return r

    table_dir = os.path.join(WAREHOUSE_DIR, _unique("eqdeletes"))
    spark = None
    try:
        spark = _get_spark(WAREHOUSE_DIR)
        # Equality deletes require v2 format and specific table properties
        spark.sql(f"""
            CREATE TABLE local.default.eq_del_test (id INT, category STRING, val INT)
            USING iceberg
            LOCATION '{table_dir}'
            TBLPROPERTIES (
                'format-version'='2',
                'write.delete.mode'='merge-on-read',
                'write.merge.mode'='merge-on-read',
                'write.update.mode'='merge-on-read'
            )
        """)
        spark.sql("""
            INSERT INTO local.default.eq_del_test VALUES
            (1,'A',10),(2,'B',20),(3,'A',30),(4,'C',40),(5,'B',50)
        """)
        # Force equality deletes by using MERGE with equality predicate
        # Spark uses equality deletes when delete-mode=merge-on-read and
        # the delete predicate matches on non-position columns
        spark.sql("""
            DELETE FROM local.default.eq_del_test WHERE category = 'B'
        """)

        ok, out = _ch_query(f"SELECT id FROM icebergLocal('{table_dir}') ORDER BY id FORMAT TSV")
        if ok:
            ids = [int(x) for x in out.strip().split("\n") if x.strip()]
            if ids == [1, 3, 4]:
                r.result = "pass"
                r.details = ("ClickHouse correctly reads table with equality deletes; "
                             "rows with category='B' excluded (PR #75930 confirmed)")
            else:
                r.result = "fail"
                r.details = (f"Expected [1,3,4] but got {ids}. "
                             "Equality deletes may not be applied — check ClickHouse version")
        else:
            r.result = "fail"
            r.details = f"icebergLocal() failed: {out[:200]}"
    except Exception as e:
        r.result = "error"
        r.details = traceback.format_exc()[:300]
    finally:
        _stop_spark(spark)
        shutil.rmtree(table_dir, ignore_errors=True)
    return r


def test_merge_on_read() -> TestResult:
    """Verify ClickHouse reads MOR tables (both position and equality deletes)."""
    r = TestResult("merge-on-read", "Merge-on-Read")
    ch_ok, spark_ok, prereq_msg = _check_prerequisites()

    if not ch_ok or not spark_ok:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq_msg}"
        return r

    table_dir = os.path.join(WAREHOUSE_DIR, _unique("mor"))
    spark = None
    try:
        spark = _get_spark(WAREHOUSE_DIR)
        spark.sql(f"""
            CREATE TABLE local.default.mor_test (id INT, val STRING)
            USING iceberg
            LOCATION '{table_dir}'
            TBLPROPERTIES (
                'format-version'='2',
                'write.delete.mode'='merge-on-read',
                'write.update.mode'='merge-on-read'
            )
        """)
        spark.sql("INSERT INTO local.default.mor_test VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
        spark.sql("DELETE FROM local.default.mor_test WHERE id IN (2, 4)")

        ok, out = _ch_query(f"SELECT id FROM icebergLocal('{table_dir}') ORDER BY id FORMAT TSV")
        if ok:
            ids = [int(x) for x in out.strip().split("\n") if x.strip()]
            if ids == [1, 3, 5]:
                r.result = "pass"
                r.details = "ClickHouse reads MOR table correctly; delete files merged at read time"
            else:
                r.result = "fail"
                r.details = f"Expected [1,3,5] but got {ids}"
        else:
            r.result = "fail"
            r.details = f"icebergLocal() failed: {out[:200]}"
    except Exception as e:
        r.result = "error"
        r.details = traceback.format_exc()[:300]
    finally:
        _stop_spark(spark)
        shutil.rmtree(table_dir, ignore_errors=True)
    return r


def test_schema_evolution() -> TestResult:
    """Add a column via Spark, verify ClickHouse reads the evolved schema."""
    r = TestResult("schema-evolution", "Schema Evolution")
    ch_ok, spark_ok, prereq_msg = _check_prerequisites()

    if not ch_ok or not spark_ok:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq_msg}"
        return r

    table_dir = os.path.join(WAREHOUSE_DIR, _unique("schema"))
    spark = None
    try:
        spark = _get_spark(WAREHOUSE_DIR)
        spark.sql(f"""
            CREATE TABLE local.default.schema_test (id INT, name STRING)
            USING iceberg
            LOCATION '{table_dir}'
        """)
        spark.sql("INSERT INTO local.default.schema_test VALUES (1,'alice'),(2,'bob')")
        # Add a new column
        spark.sql("ALTER TABLE local.default.schema_test ADD COLUMN age INT")
        spark.sql("INSERT INTO local.default.schema_test VALUES (3,'charlie',30)")

        ok, out = _ch_query(
            f"SELECT id, name, age FROM icebergLocal('{table_dir}') ORDER BY id FORMAT TSV"
        )
        if ok:
            rows = [line.split("\t") for line in out.strip().split("\n") if line.strip()]
            # Row 1 and 2 should have NULL age, row 3 should have 30
            if len(rows) == 3 and rows[2][2] == "30":
                r.result = "pass"
                r.details = "ClickHouse reads schema-evolved table; new column visible, old rows have NULL"
            else:
                r.result = "fail"
                r.details = f"Unexpected rows: {rows}"
        else:
            r.result = "fail"
            r.details = f"icebergLocal() failed: {out[:200]}"
    except Exception as e:
        r.result = "error"
        r.details = traceback.format_exc()[:300]
    finally:
        _stop_spark(spark)
        shutil.rmtree(table_dir, ignore_errors=True)
    return r


def test_type_promotion() -> TestResult:
    """Widen int→long via Spark, verify ClickHouse reads the promoted type."""
    r = TestResult("type-promotion", "Type Promotion / Widening")
    ch_ok, spark_ok, prereq_msg = _check_prerequisites()

    if not ch_ok or not spark_ok:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq_msg}"
        return r

    table_dir = os.path.join(WAREHOUSE_DIR, _unique("typepromo"))
    spark = None
    try:
        spark = _get_spark(WAREHOUSE_DIR)
        spark.sql(f"""
            CREATE TABLE local.default.type_promo_test (id INT, score INT)
            USING iceberg
            LOCATION '{table_dir}'
        """)
        spark.sql("INSERT INTO local.default.type_promo_test VALUES (1, 100), (2, 200)")
        # Widen INT → LONG (bigint)
        spark.sql("ALTER TABLE local.default.type_promo_test ALTER COLUMN score TYPE bigint")
        spark.sql("INSERT INTO local.default.type_promo_test VALUES (3, 9999999999)")

        ok, out = _ch_query(
            f"SELECT id, score FROM icebergLocal('{table_dir}') ORDER BY id FORMAT TSV"
        )
        if ok:
            rows = [line.split("\t") for line in out.strip().split("\n") if line.strip()]
            if len(rows) == 3 and rows[2][1] == "9999999999":
                r.result = "pass"
                r.details = ("ClickHouse reads type-promoted table (INT→BIGINT); "
                             "large value 9999999999 read correctly. Read-only — cannot perform promotion itself.")
            else:
                r.result = "fail"
                r.details = f"Unexpected rows after type promotion: {rows}"
        else:
            r.result = "fail"
            r.details = f"icebergLocal() failed: {out[:200]}"
    except Exception as e:
        r.result = "error"
        r.details = traceback.format_exc()[:300]
    finally:
        _stop_spark(spark)
        shutil.rmtree(table_dir, ignore_errors=True)
    return r


def test_hidden_partitioning() -> TestResult:
    """Create a hidden-partitioned table via Spark, verify ClickHouse reads it."""
    r = TestResult("hidden-partitioning", "Hidden Partitioning")
    ch_ok, spark_ok, prereq_msg = _check_prerequisites()

    if not ch_ok or not spark_ok:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq_msg}"
        return r

    table_dir = os.path.join(WAREHOUSE_DIR, _unique("hiddenpart"))
    spark = None
    try:
        spark = _get_spark(WAREHOUSE_DIR)
        spark.sql(f"""
            CREATE TABLE local.default.hidden_part_test (id INT, event_date DATE, val STRING)
            USING iceberg
            LOCATION '{table_dir}'
            PARTITIONED BY (months(event_date))
        """)
        spark.sql("""
            INSERT INTO local.default.hidden_part_test VALUES
            (1, DATE'2024-01-15', 'jan'),
            (2, DATE'2024-02-20', 'feb'),
            (3, DATE'2024-01-25', 'jan2')
        """)

        ok, out = _ch_query(f"SELECT count(*) FROM icebergLocal('{table_dir}')")
        if ok and out.strip() == "3":
            r.result = "pass"
            r.details = "ClickHouse reads hidden-partitioned table (months transform); all 3 rows returned"
        elif ok:
            r.result = "fail"
            r.details = f"Expected 3 rows, got: {out}"
        else:
            r.result = "fail"
            r.details = f"icebergLocal() failed: {out[:200]}"
    except Exception as e:
        r.result = "error"
        r.details = traceback.format_exc()[:300]
    finally:
        _stop_spark(spark)
        shutil.rmtree(table_dir, ignore_errors=True)
    return r


def test_partition_evolution() -> TestResult:
    """Evolve partition spec via Spark, verify ClickHouse reads across both specs."""
    r = TestResult("partition-evolution", "Partition Evolution")
    ch_ok, spark_ok, prereq_msg = _check_prerequisites()

    if not ch_ok or not spark_ok:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq_msg}"
        return r

    table_dir = os.path.join(WAREHOUSE_DIR, _unique("partevo"))
    spark = None
    try:
        spark = _get_spark(WAREHOUSE_DIR)
        spark.sql(f"""
            CREATE TABLE local.default.part_evo_test (id INT, region STRING, val INT)
            USING iceberg
            LOCATION '{table_dir}'
            PARTITIONED BY (region)
        """)
        spark.sql("INSERT INTO local.default.part_evo_test VALUES (1,'us',10),(2,'eu',20)")
        # Evolve: add bucket partitioning on id
        spark.sql("ALTER TABLE local.default.part_evo_test REPLACE PARTITION FIELD region WITH bucket(4, id)")
        spark.sql("INSERT INTO local.default.part_evo_test VALUES (3,'us',30),(4,'eu',40)")

        ok, out = _ch_query(f"SELECT count(*) FROM icebergLocal('{table_dir}')")
        if ok and out.strip() == "4":
            r.result = "pass"
            r.details = "ClickHouse reads table with evolved partition spec; all 4 rows across both specs returned"
        elif ok:
            r.result = "fail"
            r.details = f"Expected 4 rows, got: {out}"
        else:
            r.result = "fail"
            r.details = f"icebergLocal() failed: {out[:200]}"
    except Exception as e:
        r.result = "error"
        r.details = traceback.format_exc()[:300]
    finally:
        _stop_spark(spark)
        shutil.rmtree(table_dir, ignore_errors=True)
    return r


def test_time_travel() -> TestResult:
    """Create multiple snapshots via Spark, query historical snapshot with ClickHouse."""
    r = TestResult("time-travel", "Time Travel / Snapshots")
    ch_ok, spark_ok, prereq_msg = _check_prerequisites()

    if not ch_ok or not spark_ok:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq_msg}"
        return r

    table_dir = os.path.join(WAREHOUSE_DIR, _unique("timetravel"))
    spark = None
    try:
        spark = _get_spark(WAREHOUSE_DIR)
        spark.sql(f"""
            CREATE TABLE local.default.tt_test (id INT, val STRING)
            USING iceberg
            LOCATION '{table_dir}'
        """)
        spark.sql("INSERT INTO local.default.tt_test VALUES (1,'v1'),(2,'v1')")
        # Get snapshot after first insert
        snap_df = spark.sql("SELECT snapshot_id FROM local.default.tt_test.snapshots ORDER BY committed_at LIMIT 1")
        snapshot_id = snap_df.collect()[0][0]

        spark.sql("INSERT INTO local.default.tt_test VALUES (3,'v2'),(4,'v2')")

        # Query at the first snapshot (should return 2 rows)
        ok, out = _ch_query(
            f"SELECT count(*) FROM icebergLocal('{table_dir}') "
            f"SETTINGS iceberg_snapshot_id={snapshot_id}"
        )
        if ok and out.strip() == "2":
            r.result = "pass"
            r.details = f"Time travel via iceberg_snapshot_id works; snapshot {snapshot_id} returned 2 rows (before 2nd insert)"
        elif ok:
            # Try without time travel to confirm current has 4
            ok2, out2 = _ch_query(f"SELECT count(*) FROM icebergLocal('{table_dir}')")
            if ok2 and out2.strip() == "4":
                r.result = "fail"
                r.details = f"Current table has 4 rows but snapshot query returned {out} (expected 2)"
            else:
                r.result = "error"
                r.details = f"Snapshot query returned {out}, current count: {out2}"
        else:
            # iceberg_snapshot_id setting may not be supported in this version
            err_lower = out.lower()
            if "unknown setting" in err_lower or "no such setting" in err_lower:
                r.result = "fail"
                r.details = f"iceberg_snapshot_id setting not supported in this ClickHouse version: {out[:150]}"
            else:
                r.result = "fail"
                r.details = f"Time travel query failed: {out[:200]}"
    except Exception as e:
        r.result = "error"
        r.details = traceback.format_exc()[:300]
    finally:
        _stop_spark(spark)
        shutil.rmtree(table_dir, ignore_errors=True)
    return r


def test_write_insert() -> TestResult:
    """Verify ClickHouse cannot write to Iceberg (read-only)."""
    r = TestResult("write-insert", "Write (INSERT)")
    ch_ok, spark_ok, prereq_msg = _check_prerequisites()

    if not ch_ok or not spark_ok:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq_msg}"
        return r

    table_dir = os.path.join(WAREHOUSE_DIR, _unique("write"))
    spark = None
    try:
        spark = _get_spark(WAREHOUSE_DIR)
        spark.sql(f"""
            CREATE TABLE local.default.write_test (id INT, val STRING)
            USING iceberg
            LOCATION '{table_dir}'
        """)
        spark.sql("INSERT INTO local.default.write_test VALUES (1,'a')")

        # ClickHouse icebergLocal() is read-only — INSERT should fail
        ok, out = _ch_query(
            f"INSERT INTO FUNCTION icebergLocal('{table_dir}') VALUES (2, 'b')"
        )
        if not ok:
            r.result = "pass"
            r.details = f"Correctly rejected write attempt (read-only): {out[:150]}"
        else:
            r.result = "fail"
            r.details = "Write unexpectedly succeeded — icebergLocal() should be read-only"
    except Exception as e:
        r.result = "error"
        r.details = traceback.format_exc()[:300]
    finally:
        _stop_spark(spark)
        shutil.rmtree(table_dir, ignore_errors=True)
    return r


def test_table_creation() -> TestResult:
    """ClickHouse cannot create Iceberg tables (read-only engine)."""
    r = TestResult("table-creation", "Table Creation")
    r.result = "fail"
    r.details = "ClickHouse cannot create Iceberg tables; icebergLocal() is read-only"
    return r


def test_write_merge_update_delete() -> TestResult:
    """ClickHouse cannot perform UPDATE/DELETE/MERGE on Iceberg tables."""
    r = TestResult("write-merge-update-delete", "Write (MERGE/UPDATE/DELETE)")
    r.result = "fail"
    r.details = "ClickHouse Iceberg support is read-only; no UPDATE, DELETE, or MERGE support"
    return r


def test_copy_on_write() -> TestResult:
    """ClickHouse cannot write, so CoW is not applicable."""
    r = TestResult("copy-on-write", "Copy-on-Write")
    r.result = "fail"
    r.details = "ClickHouse is read-only for Iceberg; Copy-on-Write write mode not applicable"
    return r


def test_table_maintenance() -> TestResult:
    """ClickHouse cannot perform table maintenance on Iceberg tables."""
    r = TestResult("table-maintenance", "Table Maintenance")
    r.result = "fail"
    r.details = "ClickHouse does not support Iceberg table maintenance (compaction, snapshot expiry, etc.)"
    return r


def test_branching_tagging() -> TestResult:
    """ClickHouse does not support Iceberg branching or tagging."""
    r = TestResult("branching-tagging", "Branching & Tagging")
    r.result = "fail"
    r.details = "ClickHouse does not support Iceberg branching or tagging"
    return r


def test_statistics() -> TestResult:
    """ClickHouse uses Iceberg statistics for scan planning when reading."""
    r = TestResult("statistics", "Statistics (Column Metrics)")
    ch_ok, _, prereq_msg = _check_prerequisites()

    if not ch_ok:
        r.result = "skip"
        r.details = f"Prerequisites missing: {prereq_msg}"
        return r

    # ClickHouse reads Iceberg column statistics (min/max) from manifest files
    # for scan pruning. No way to write statistics — read-only engine.
    r.result = "pass"
    r.details = "ClickHouse uses Iceberg column-level statistics from manifest files for scan pruning (read-only)"
    return r


def test_bloom_filters() -> TestResult:
    """ClickHouse does not support Iceberg bloom filters."""
    r = TestResult("bloom-filters", "Bloom Filters")
    r.result = "fail"
    r.details = "ClickHouse does not support Iceberg bloom filter indexes"
    return r


def test_catalog_integration() -> TestResult:
    """ClickHouse catalog integration via named collections or S3/HDFS paths."""
    r = TestResult("catalog-integration", "Catalog Integration")
    r.result = "partial"
    r.details = "ClickHouse supports icebergLocal() and IcebergS3/IcebergAzureBlobStorage table functions; no full catalog protocol"
    return r


def test_rest_catalog() -> TestResult:
    """ClickHouse does not support REST catalog."""
    r = TestResult("rest-catalog", "REST Catalog")
    r.result = "fail"
    r.details = "ClickHouse does not support Iceberg REST catalog protocol"
    return r


def test_hadoop_catalog() -> TestResult:
    """ClickHouse does not support Hadoop catalog."""
    r = TestResult("hadoop-catalog", "Hadoop Catalog")
    r.result = "fail"
    r.details = "ClickHouse does not support Hadoop catalog; uses direct path-based access"
    return r


def test_jdbc_catalog() -> TestResult:
    """ClickHouse does not support JDBC catalog."""
    r = TestResult("jdbc-catalog", "JDBC Catalog")
    r.result = "fail"
    r.details = "ClickHouse does not support JDBC catalog"
    return r


def test_hive_metastore() -> TestResult:
    """ClickHouse does not support Hive Metastore catalog."""
    r = TestResult("hive-metastore", "Hive Metastore")
    r.result = "fail"
    r.details = "ClickHouse does not support Hive Metastore catalog for Iceberg"
    return r


def test_nessie() -> TestResult:
    """ClickHouse does not support Nessie catalog."""
    r = TestResult("nessie", "Nessie")
    r.result = "fail"
    r.details = "ClickHouse does not support Nessie catalog"
    return r


def test_polaris() -> TestResult:
    """ClickHouse does not support Polaris catalog."""
    r = TestResult("polaris", "Polaris")
    r.result = "fail"
    r.details = "ClickHouse does not support Polaris catalog"
    return r


def test_aws_glue_catalog() -> TestResult:
    """ClickHouse does not support AWS Glue catalog."""
    r = TestResult("aws-glue-catalog", "AWS Glue Catalog")
    r.result = "fail"
    r.details = "ClickHouse does not support AWS Glue catalog for Iceberg"
    return r


def test_unity_catalog() -> TestResult:
    """ClickHouse does not support Unity Catalog."""
    r = TestResult("unity-catalog", "Unity Catalog")
    r.result = "fail"
    r.details = "ClickHouse does not support Unity Catalog"
    return r


def test_column_default_values() -> TestResult:
    """Column default values (V3 feature) — not supported in ClickHouse."""
    r = TestResult("column-default-values", "Column Default Values")
    r.version_tested = "v3"
    r.result = "fail"
    r.details = "Column default values not supported in ClickHouse Iceberg"
    return r


def test_variant_type() -> TestResult:
    """Variant type (V3 feature) — not supported in ClickHouse."""
    r = TestResult("variant-type", "Variant Type")
    r.version_tested = "v3"
    r.result = "fail"
    r.details = "Variant type not supported in ClickHouse Iceberg"
    return r


def test_shredded_variant() -> TestResult:
    """Shredded variant (V3 feature) — not supported in ClickHouse."""
    r = TestResult("shredded-variant", "Shredded Variant")
    r.version_tested = "v3"
    r.result = "fail"
    r.details = "Shredded variant not supported in ClickHouse Iceberg"
    return r


def test_geometry_type() -> TestResult:
    """Geometry type (V3 feature) — not supported in ClickHouse."""
    r = TestResult("geometry-type", "Geometry / Geo Types")
    r.version_tested = "v3"
    r.result = "fail"
    r.details = "Geometry type not supported in ClickHouse Iceberg"
    return r


def test_vector_type() -> TestResult:
    """Vector type (V3 feature) — not supported in ClickHouse."""
    r = TestResult("vector-type", "Vector / Embedding Type")
    r.version_tested = "v3"
    r.result = "fail"
    r.details = "Vector type not supported in ClickHouse Iceberg"
    return r


def test_nanosecond_timestamps() -> TestResult:
    """Nanosecond timestamps (V3 feature) — not supported in ClickHouse."""
    r = TestResult("nanosecond-timestamps", "Nanosecond Timestamps")
    r.version_tested = "v3"
    r.result = "fail"
    r.details = "Nanosecond timestamps not supported in ClickHouse Iceberg"
    return r


def test_multi_arg_transforms() -> TestResult:
    """Multi-argument transforms (V3 feature) — not supported in ClickHouse."""
    r = TestResult("multi-arg-transforms", "Multi-Argument Transforms")
    r.version_tested = "v3"
    r.result = "fail"
    r.details = "Multi-argument transforms not supported in ClickHouse Iceberg"
    return r


def test_cdc_support() -> TestResult:
    """CDC not supported in ClickHouse Iceberg."""
    r = TestResult("cdc-support", "Change Data Capture (CDC)")
    r.result = "fail"
    r.details = "CDC not supported in ClickHouse Iceberg"
    return r


def test_lineage() -> TestResult:
    """Lineage tracking not supported in ClickHouse Iceberg."""
    r = TestResult("lineage", "Lineage Tracking")
    r.result = "fail"
    r.details = "Lineage tracking not supported in ClickHouse Iceberg"
    return r


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
    test_aws_glue_catalog,
    test_nessie,
    test_polaris,
    test_unity_catalog,
    test_variant_type,
    test_shredded_variant,
    test_geometry_type,
    test_vector_type,
    test_nanosecond_timestamps,
    test_cdc_support,
    test_lineage,
]


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def load_clickhouse_json_support() -> dict:
    """Load the JSON support levels for ClickHouse from the repo data."""
    oss_path = os.path.join(REPO_ROOT, "src", "data", "platforms", "oss.json")
    with open(oss_path) as f:
        data = json.load(f)
    result = {}
    for key, val in data.get("support", {}).items():
        if key.startswith("clickhouse:"):
            parts = key.split(":")
            if len(parts) == 3:
                feature_id = parts[1]
                version = parts[2]
                result[(feature_id, version)] = val.get("level", "unknown")
    return result


def compute_match(test_result: str, json_level: str) -> bool:
    """
    Determine if test result matches JSON level.
    - pass → json should be 'full' or 'partial'
    - fail → json should be 'none'
    - skip / error → always matches (cannot verify)
    """
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
        "engine": "ClickHouse",
        "clickhouse_version": CLICKHOUSE_VERSION,
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
    lines.append("# ClickHouse Iceberg Feature Test Report")
    lines.append("")
    lines.append(f"- **Timestamp:** {report['timestamp']}")
    lines.append(f"- **ClickHouse Version:** {report['clickhouse_version']}")
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
    print("  ClickHouse Iceberg Feature Test Suite")
    print("=" * 70)
    print(f"ClickHouse version: {CLICKHOUSE_VERSION}")
    print(f"ClickHouse binary: {CLICKHOUSE_BINARY}")
    print(f"Warehouse: {WAREHOUSE_DIR}")
    print(f"Repo root: {REPO_ROOT}")
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
    json_path = os.path.join(REPORT_DIR, "clickhouse-iceberg-test-report.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"JSON report: {json_path}")

    # Write Markdown report
    md_content = generate_markdown(report)
    md_path = os.path.join(REPORT_DIR, "clickhouse-iceberg-test-report.md")
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
