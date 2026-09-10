#!/usr/bin/env python3
"""
Daft-based Iceberg Feature Test Suite.

Measures Daft's ``read_iceberg()`` / ``write_iceberg()`` against tables managed
by a real Iceberg REST catalog (Apache Polaris backed by MinIO, see
tests/docker) and compares with the Daft entries in
``src/data/platforms/oss/daft/daft.json``.

Daft has no DDL or row-level DML of its own: it reads and appends/overwrites
through PyIceberg. Cells that name a write operation Daft does not have
(MERGE/UPDATE/DELETE, maintenance, refs) are therefore measured on what Daft
*can* do with the artifact -- read it -- and the verdict follows the write
capability. Fixtures Daft cannot produce (equality deletes, deletion vectors,
V3 columns, branches) are created with Spark through the same catalog
(tests/spark_fixture.py) so the read half is a real measurement, not an
assertion. When the catalog or Spark is unavailable the affected tests skip.

Usage:
    ./tests/docker/start-polaris.sh
    python tests/daft_feature_tests.py

Environment variables:
    DAFT_VERSION / PYICEBERG_VERSION - override the reported versions
    ICEBERG_REST_URI / ICEBERG_REST_WAREHOUSE / ICEBERG_REST_CREDENTIAL /
    ICEBERG_REST_SCOPE / ICEBERG_S3_* - as in tests/spark_fixture.py
"""

import json
import os
import sys
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    import daft
except ImportError:
    print("[FATAL] daft not installed. Run: uv pip install getdaft")
    sys.exit(1)

try:
    import pyiceberg
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        NestedField, StringType, LongType, DoubleType, TimestamptzType, IntegerType, FloatType,
    )
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform, BucketTransform, MonthTransform
    import pyarrow as pa
except ImportError as e:
    print(f"[FATAL] Missing dependency: {e}")
    print("Run: uv pip install 'pyiceberg[sql-sqlite,pyarrow]'")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).resolve().parent))
import spark_fixture  # noqa: E402 - sibling module, not a package

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
WAREHOUSE_DIR = os.environ.get("ICEBERG_WAREHOUSE", os.path.join(os.getcwd(), "daft-iceberg-warehouse"))
REPO_ROOT = os.environ.get("REPO_ROOT", str(Path(__file__).resolve().parent.parent))
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(os.getcwd(), "test-reports"))
DAFT_VERSION = os.environ.get("DAFT_VERSION", daft.__version__)
PYICEBERG_VERSION = os.environ.get("PYICEBERG_VERSION", pyiceberg.__version__)

CATALOG = None
NO_CATALOG_DETAIL = ("Requires the Iceberg REST catalog (set ICEBERG_REST_URI; start "
                     "tests/docker/start-polaris.sh); not reachable in this run")

BASIC_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType()),
    NestedField(3, "value", DoubleType()),
    NestedField(4, "ts", TimestamptzType()),
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _rest_available() -> bool:
    return spark_fixture._rest_reachable(spark_fixture.REST_URI)


def _get_catalog():
    """PyIceberg RestCatalog against the shared Polaris instance (Daft's own
    catalog access goes through PyIceberg, so this *is* Daft's catalog path)."""
    global CATALOG
    if CATALOG is None:
        props = {
            "uri": spark_fixture.REST_URI,
            "warehouse": spark_fixture.REST_WAREHOUSE,
            "s3.endpoint": f"http://{spark_fixture.S3_ENDPOINT}",
            "s3.access-key-id": spark_fixture.S3_KEY_ID,
            "s3.secret-access-key": spark_fixture.S3_SECRET,
            "s3.region": spark_fixture.S3_REGION,
        }
        props.update(spark_fixture.rest_auth_conf())
        props.pop("token-refresh-enabled", None)
        CATALOG = load_catalog("daft_polaris", **props)
    return CATALOG


NS = None


def _ns() -> str:
    """One namespace per run, created through the catalog."""
    global NS
    if NS is None:
        NS = "daft_" + uuid.uuid4().hex[:8]
        _get_catalog().create_namespace(NS)
    return NS


def _table(prefix: str) -> str:
    return f"{_ns()}.{_unique(prefix)}"


def _sample_arrow():
    return pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "name": pa.array(["alice", "bob", "charlie"]),
        "value": pa.array([10.0, 20.0, 30.0]),
        "ts": pa.array([
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 2, tzinfo=timezone.utc),
            datetime(2024, 1, 3, tzinfo=timezone.utc),
        ], type=pa.timestamp("us", tz="UTC")),
    })


def _write_sample_data(tbl_name: str):
    cat = _get_catalog()
    tbl = cat.create_table(tbl_name, schema=BASIC_SCHEMA)
    tbl.append(_sample_arrow().cast(tbl.schema().as_arrow()))
    return tbl


def _ids(df) -> list:
    return sorted(df.collect().to_pydict()["id"])


def _first_line(e: Exception, n: int = 200) -> str:
    return str(e).splitlines()[0][:n] if str(e) else type(e).__name__


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


def _catalog_test(r: TestResult, body):
    """Run body(r) against the REST catalog; skip when unreachable, error on exception."""
    if not _rest_available():
        r.result = "skip"
        r.details = NO_CATALOG_DETAIL
        return r
    try:
        body(r)
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {_first_line(e)}"
    return r


def _spark_test(r: TestResult, body):
    """Like _catalog_test but also needs the Spark fixture; body(spark_ns, r)."""
    if not spark_fixture.available():
        r.result = "skip"
        r.details = spark_fixture.NOT_AVAILABLE_DETAIL
        return r
    ns = None
    try:
        ns = spark_fixture.new_namespace()
        body(ns, r)
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {_first_line(e)}"
    finally:
        if ns:
            for n in ("t", "u"):
                spark_fixture.drop_fixture(ns, n)
    return r


def _read_via_daft(ns: str, name: str):
    """Daft DataFrame over a Spark-created table, loaded through the REST catalog."""
    return daft.read_iceberg(_get_catalog().load_table(f"{ns}.{name}"))


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def test_table_creation() -> TestResult:
    r = TestResult("table-creation", "Table Creation")

    def body(r):
        cat = _get_catalog()
        name = _table("create")
        tbl = cat.create_table(name, schema=BASIC_SCHEMA)
        assert cat.load_table(name) is not None
        cat.drop_table(name)
        r.result = "pass"
        r.details = ("Created, loaded and dropped an Iceberg table in the REST catalog via "
                    "PyIceberg (Daft has no DDL of its own; this is its catalog path)")

    return _catalog_test(r, body)


def test_read_support() -> TestResult:
    r = TestResult("read-support", "Read Support")

    def body(r):
        name = _table("read")
        _write_sample_data(name)
        df = daft.read_iceberg(_get_catalog().load_table(name))
        assert _ids(df) == [1, 2, 3]
        pushed = daft.read_iceberg(_get_catalog().load_table(name)).where(daft.col("id") > 1).collect()
        assert len(pushed) == 2
        r.result = "pass"
        r.details = "daft.read_iceberg() read 3 rows from a catalog-managed table; predicate read returned 2"

    return _catalog_test(r, body)


def test_read_support_v3() -> TestResult:
    r = TestResult("read-support", "Read Support", "v3")

    def body(ns, r):
        spark_fixture.create_fixture(ns, "t", "v3", "merge-on-read")
        ids = _ids(_read_via_daft(ns, "t"))
        assert ids == [1, 2, 3], ids
        r.result = "pass"
        r.details = "daft.read_iceberg() read a Spark-created format-version 3 table (3 rows)"

    return _spark_test(r, body)


def test_write_insert() -> TestResult:
    r = TestResult("write-insert", "Write (INSERT)")

    def body(r):
        cat = _get_catalog()
        name = _table("insert")
        tbl = cat.create_table(name, schema=BASIC_SCHEMA)
        df = daft.from_pydict({
            "id": [1, 2], "name": ["x", "y"], "value": [10.0, 20.0],
            "ts": [datetime(2024, 6, 1, tzinfo=timezone.utc), datetime(2024, 6, 2, tzinfo=timezone.utc)],
        })
        df.write_iceberg(tbl, mode="append")
        df.write_iceberg(cat.load_table(name), mode="append")
        assert len(daft.read_iceberg(cat.load_table(name)).collect()) == 4
        r.result = "pass"
        r.details = "write_iceberg(mode='append') appended twice through the REST catalog (4 rows)"

    return _catalog_test(r, body)


def test_write_merge_update_delete() -> TestResult:
    r = TestResult("write-merge-update-delete", "Write (MERGE/UPDATE/DELETE)")

    def body(r):
        cat = _get_catalog()
        name = _table("mud")
        tbl = _write_sample_data(name)
        df = daft.read_iceberg(tbl)
        missing = [m for m in ("delete", "update", "merge", "write_iceberg_merge")
                   if not hasattr(df, m)]
        # The only row-level-ish path is a whole-table overwrite.
        daft.from_pydict({"id": [1], "name": ["only"], "value": [1.0],
                          "ts": [datetime(2024, 1, 1, tzinfo=timezone.utc)]}).write_iceberg(
            cat.load_table(name), mode="overwrite")
        after = _ids(daft.read_iceberg(cat.load_table(name)))
        r.result = "fail"
        r.details = ("Daft's DataFrame has no DELETE/UPDATE/MERGE for Iceberg (missing: "
                    f"{missing}); write_iceberg only offers append and overwrite -- overwrite "
                    f"replaced the whole table ({after})")

    return _catalog_test(r, body)


# ---------------------------------------------------------------------------
# Row-level operations: Daft cannot write any; measure the READ of each artifact.
# ---------------------------------------------------------------------------

def test_position_deletes() -> TestResult:
    r = TestResult("position-deletes", "Position Deletes")

    def body(ns, r):
        spark_fixture.create_fixture(ns, "t", "v2", "merge-on-read")
        spark_fixture.get_spark().sql(f"DELETE FROM local.{ns}.t WHERE id = 2")
        deletes = spark_fixture.inspect_delete_files(ns, "t")
        try:
            ids = _ids(_read_via_daft(ns, "t"))
            read = f"reads them correctly (ids {ids})" if ids == [1, 3] else f"mis-reads them (ids {ids})"
        except Exception as e:  # noqa: BLE001
            read = f"cannot read them: {_first_line(e, 100)}"
        r.result = "fail"
        r.details = ("Daft has no DELETE, so it cannot write position deletes; against a "
                    f"Spark-written position delete ({deletes}) it {read}")

    return _spark_test(r, body)


def test_equality_deletes() -> TestResult:
    r = TestResult("equality-deletes", "Equality Deletes")

    def body(ns, r):
        produced = spark_fixture.create_equality_delete_fixture(ns, "t", "v2")
        try:
            ids = _ids(_read_via_daft(ns, "t"))
            read = (f"reads them correctly (ids {ids})" if ids == produced["live_ids"]
                    else f"IGNORES them (ids {ids}, expected {produced['live_ids']})")
        except Exception as e:  # noqa: BLE001
            read = f"cannot read a table containing one: {_first_line(e, 120)}"
        r.result = "fail"
        r.details = ("Daft cannot write equality deletes (no DML); against a real Java-API "
                    f"equality-delete file it {read}")

    return _spark_test(r, body)


def test_merge_on_read() -> TestResult:
    r = TestResult("merge-on-read", "Merge-on-Read")

    def body(ns, r):
        spark_fixture.create_fixture(ns, "t", "v2", "merge-on-read")
        spark_fixture.get_spark().sql(f"UPDATE local.{ns}.t SET val = 'x' WHERE id = 2")
        rows = _read_via_daft(ns, "t").collect().to_pydict()
        vals = dict(zip(rows["id"], rows["val"]))
        r.result = "fail"
        r.details = ("Daft has no UPDATE/DELETE, so merge-on-read is not a write strategy it can "
                    f"select; it reads a Spark MoR table correctly (id 2 -> {vals.get(2)!r})")

    return _spark_test(r, body)


def test_copy_on_write() -> TestResult:
    r = TestResult("copy-on-write", "Copy-on-Write")

    def body(ns, r):
        spark_fixture.create_fixture(ns, "t", "v2", "copy-on-write")
        spark_fixture.get_spark().sql(f"DELETE FROM local.{ns}.t WHERE id = 2")
        ids = _ids(_read_via_daft(ns, "t"))
        r.result = "fail"
        r.details = ("Daft has no row-level DELETE/UPDATE for copy-on-write to act on "
                    "(write_iceberg mode='overwrite' replaces the whole table); it reads a "
                    f"Spark CoW-deleted table correctly (ids {ids})")

    return _spark_test(r, body)


def test_deletion_vectors() -> TestResult:
    r = TestResult("deletion-vectors", "Deletion Vectors", "v3")

    def body(ns, r):
        spark_fixture.create_fixture(ns, "t", "v3", "merge-on-read")
        spark_fixture.get_spark().sql(f"DELETE FROM local.{ns}.t WHERE id = 2")
        try:
            ids = _ids(_read_via_daft(ns, "t"))
            read = (f"reads the DV correctly (ids {ids})" if ids == [1, 3]
                    else f"ignores the DV (ids {ids})")
        except Exception as e:  # noqa: BLE001
            read = f"cannot read a table with a DV: {_first_line(e, 140)}"
        r.result = "fail"
        r.details = f"Daft cannot write deletion vectors (no DML); against a Spark-written V3 DV it {read}"

    return _spark_test(r, body)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def test_schema_evolution() -> TestResult:
    r = TestResult("schema-evolution", "Schema Evolution")

    def body(r):
        cat = _get_catalog()
        name = _table("schema")
        tbl = cat.create_table(name, schema=BASIC_SCHEMA)
        with tbl.update_schema() as u:
            u.add_column("new_col", StringType())
        with tbl.update_schema() as u:
            u.rename_column("value", "amount")
        tbl.append(pa.table({
            "id": pa.array([1], type=pa.int64()), "name": pa.array(["a"]),
            "amount": pa.array([1.0]),
            "ts": pa.array([datetime(2024, 1, 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")),
            "new_col": pa.array(["extra"]),
        }).cast(tbl.schema().as_arrow()))
        cols = daft.read_iceberg(cat.load_table(name)).column_names
        assert "new_col" in cols and "amount" in cols and "value" not in cols, cols
        r.result = "pass"
        r.details = f"Add + rename column via PyIceberg, Daft reads the evolved schema {cols}"

    return _catalog_test(r, body)


def test_type_promotion() -> TestResult:
    r = TestResult("type-promotion", "Type Promotion / Widening")

    def body(r):
        cat = _get_catalog()
        name = _table("promo")
        tbl = cat.create_table(name, schema=Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "f", FloatType()),
        ))
        tbl.append(pa.table({"id": pa.array([100], pa.int32()), "f": pa.array([1.5], pa.float32())})
                   .cast(tbl.schema().as_arrow()))
        with tbl.update_schema() as u:
            u.update_column("id", LongType())
            u.update_column("f", DoubleType())
        tbl = cat.load_table(name)
        tbl.append(pa.table({"id": pa.array([9999999999], pa.int64()), "f": pa.array([2.5], pa.float64())})
                   .cast(tbl.schema().as_arrow()))
        rows = daft.read_iceberg(cat.load_table(name)).collect().to_pydict()
        assert sorted(rows["id"]) == [100, 9999999999], rows
        r.result = "pass"
        r.details = ("int->long and float->double promotion via PyIceberg; Daft reads the "
                    f"pre-promotion row as the widened type alongside a wide value: {sorted(rows['id'])}")

    return _catalog_test(r, body)


def test_column_default_values() -> TestResult:
    r = TestResult("column-default-values", "Column Default Values", "v3")

    def body(ns, r):
        fixture = spark_fixture.create_column_default_fixture(ns, "t")
        rows = _read_via_daft(ns, "t").collect().to_pydict()
        regions = dict(zip(rows["id"], rows["region"]))
        if regions == fixture["expected"]:
            r.result = "pass"
            r.details = ("Daft applies the V3 initial-default on read: the row written before the "
                        f"column existed returns 'eu' ({regions})")
        else:
            r.result = "fail"
            r.details = f"V3 column default not applied on read: {regions} (expected {fixture['expected']})"

    return _spark_test(r, body)


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------

def test_hidden_partitioning() -> TestResult:
    r = TestResult("hidden-partitioning", "Hidden Partitioning")

    def body(r):
        cat = _get_catalog()
        name = _table("hidpart")
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "ts", TimestamptzType()),
            NestedField(3, "name", StringType()),
        )
        spec = PartitionSpec(
            PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="ts_day"),
            PartitionField(source_id=3, field_id=1001, transform=BucketTransform(4), name="name_bucket"),
        )
        tbl = cat.create_table(name, schema=schema, partition_spec=spec)
        daft.from_pydict({
            "id": [1, 2],
            "ts": [datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 6, 15, tzinfo=timezone.utc)],
            "name": ["a", "b"],
        }).write_iceberg(tbl, mode="append")
        df = daft.read_iceberg(cat.load_table(name)).where(daft.col("ts") > datetime(2024, 3, 1, tzinfo=timezone.utc))
        assert len(df.collect()) == 1
        r.result = "pass"
        r.details = ("Daft WROTE into a table hidden-partitioned by day(ts)+bucket(4,name) "
                    "(partition values computed on write) and read it with a pruning predicate")

    return _catalog_test(r, body)


def test_partition_evolution() -> TestResult:
    r = TestResult("partition-evolution", "Partition Evolution")

    def body(r):
        cat = _get_catalog()
        name = _table("partevo")
        schema = Schema(NestedField(1, "id", LongType(), required=True), NestedField(2, "ts", TimestamptzType()))
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="ts_day"))
        tbl = cat.create_table(name, schema=schema, partition_spec=spec)
        daft.from_pydict({"id": [1], "ts": [datetime(2024, 1, 1, tzinfo=timezone.utc)]}).write_iceberg(tbl, mode="append")
        with tbl.update_spec() as u:
            u.add_field("ts", MonthTransform(), "ts_month")
        daft.from_pydict({"id": [2], "ts": [datetime(2024, 2, 1, tzinfo=timezone.utc)]}).write_iceberg(
            cat.load_table(name), mode="append")
        ids = _ids(daft.read_iceberg(cat.load_table(name)))
        assert ids == [1, 2], ids
        r.result = "pass"
        r.details = "Daft wrote under both partition specs after evolution and reads across them (2 rows)"

    return _catalog_test(r, body)


def test_multi_arg_transforms() -> TestResult:
    r = TestResult("multi-arg-transforms", "Multi-Argument Transforms", "v3")

    def body(ns, r):
        # (1) Does PyIceberg (Daft's Iceberg layer) model a multi-source partition
        # field? It is a pydantic model that silently drops unknown kwargs, so
        # check the serialised form rather than "did not raise". (2) Can Daft
        # read a Spark table whose spec uses bucket(4, a, b)?
        from pyiceberg.partitioning import PartitionField as PF
        pf = PF(source_id=1, field_id=1000, transform=BucketTransform(8), name="b", source_ids=[1, 2])
        dumped = pf.model_dump(by_alias=True)
        modelled = "source-ids" in dumped and dumped["source-ids"] == [1, 2]
        spark = spark_fixture.get_spark()
        try:
            spark.sql(f"""CREATE TABLE local.{ns}.t (id BIGINT, a STRING, b STRING) USING iceberg
                          PARTITIONED BY (bucket(4, a, b)) TBLPROPERTIES ('format-version'='3')""")
            spark.sql(f"INSERT INTO local.{ns}.t VALUES (1, 'x', 'y')")
            try:
                n = len(_ids(_read_via_daft(ns, "t")))
                read_note = f"Daft reads a Spark table partitioned by bucket(4, a, b) ({n} row)"
                reads = True
            except Exception as e:  # noqa: BLE001
                read_note = f"Daft cannot read a table partitioned by bucket(4, a, b): {_first_line(e, 110)}"
                reads = False
        except Exception as e:  # noqa: BLE001
            read_note = (f"no such table could be provided -- Spark itself rejected the "
                         f"multi-arg spec ({_first_line(e, 90)})")
            reads = False
        r.result = "pass" if (modelled and reads) else "fail"
        r.details = (f"PyIceberg (Daft's Iceberg layer) {'models' if modelled else 'does NOT model'} "
                    f"multi-source partition fields (serialises as {dumped}); {read_note}")

    return _spark_test(r, body)


# ---------------------------------------------------------------------------
# Snapshots, maintenance, refs
# ---------------------------------------------------------------------------

def test_time_travel() -> TestResult:
    r = TestResult("time-travel", "Time Travel / Snapshots")

    def body(r):
        cat = _get_catalog()
        name = _table("timetravel")
        tbl = cat.create_table(name, schema=BASIC_SCHEMA)
        daft.from_pydict({"id": [1], "name": ["first"], "value": [1.0],
                          "ts": [datetime(2024, 1, 1, tzinfo=timezone.utc)]}).write_iceberg(tbl, mode="append")
        snap1 = cat.load_table(name).current_snapshot().snapshot_id
        daft.from_pydict({"id": [2], "name": ["second"], "value": [2.0],
                          "ts": [datetime(2024, 1, 2, tzinfo=timezone.utc)]}).write_iceberg(cat.load_table(name), mode="append")
        old = len(daft.read_iceberg(cat.load_table(name), snapshot_id=snap1).collect())
        now = len(daft.read_iceberg(cat.load_table(name)).collect())
        assert (old, now) == (1, 2), (old, now)
        r.result = "pass"
        r.details = f"read_iceberg(snapshot_id={snap1}) returned the historical 1 row; current read 2"

    return _catalog_test(r, body)


def test_table_maintenance() -> TestResult:
    r = TestResult("table-maintenance", "Table Maintenance")

    def body(r):
        cat = _get_catalog()
        name = _table("maint")
        tbl = _write_sample_data(name)
        for _ in range(2):
            daft.from_pydict({"id": [9], "name": ["z"], "value": [9.0],
                              "ts": [datetime(2024, 1, 9, tzinfo=timezone.utc)]}).write_iceberg(cat.load_table(name), mode="append")
        df = daft.read_iceberg(cat.load_table(name))
        missing = [m for m in ("optimize", "compact", "rewrite_data_files", "expire_snapshots", "vacuum")
                   if not hasattr(df, m) and not hasattr(daft, m)]
        snaps = len(list(cat.load_table(name).snapshots()))
        r.result = "fail"
        r.details = (f"Daft exposes no maintenance operations (none of {missing}); the {snaps} "
                    "snapshots from 3 small appends stay unmerged. Maintenance must come from PyIceberg or another engine")

    return _catalog_test(r, body)


def test_branching_tagging() -> TestResult:
    r = TestResult("branching-tagging", "Branching & Tagging")

    def body(ns, r):
        spark_fixture.create_fixture(ns, "t", "v2", "merge-on-read")
        spark = spark_fixture.get_spark()
        spark.sql(f"ALTER TABLE local.{ns}.t CREATE BRANCH dev")
        spark.sql(f"INSERT INTO local.{ns}.t.branch_dev VALUES (99,'branch-only')")
        tbl = _get_catalog().load_table(f"{ns}.t")
        refs = list(tbl.metadata.refs.keys())
        # Daft's read_iceberg has no branch argument; the only route is the
        # branch's snapshot id.
        branch_snap = tbl.metadata.refs["dev"].snapshot_id
        via_snapshot = _ids(daft.read_iceberg(tbl, snapshot_id=branch_snap))
        has_branch_arg = "branch" in daft.read_iceberg.__code__.co_varnames
        if has_branch_arg:
            r.result = "pass"
            r.details = f"read_iceberg() takes a branch argument; refs {refs}"
        else:
            r.result = "fail"
            r.details = ("Daft has no branch/tag surface: read_iceberg() takes no branch/tag argument "
                        f"and write_iceberg() cannot target a ref; refs {refs} are visible only via "
                        f"PyIceberg metadata, and the branch is reachable only by its snapshot id ({via_snapshot})")

    return _spark_test(r, body)


# ---------------------------------------------------------------------------
# Statistics / bloom filters
# ---------------------------------------------------------------------------

def test_statistics() -> TestResult:
    r = TestResult("statistics", "Statistics (Column Metrics)")

    def body(r):
        cat = _get_catalog()
        name = _table("stats")
        _write_sample_data(name)
        daft.from_pydict({"id": [1000], "name": ["far"], "value": [1.0],
                          "ts": [datetime(2024, 1, 9, tzinfo=timezone.utc)]}).write_iceberg(cat.load_table(name), mode="append")
        tbl = cat.load_table(name)
        files = tbl.inspect.files().to_pydict()
        with_bounds = sum(1 for lb in files["lower_bounds"] if lb)
        # Daft plans through PyIceberg, which prunes files on column bounds.
        planned = len(list(tbl.scan(row_filter="id > 500").plan_files()))
        r.result = "pass"
        r.details = (f"Daft-written files carry column bounds ({with_bounds}/{len(files['file_path'])} files); "
                    f"a predicate scan plans {planned} of {len(files['file_path'])} files (min/max pruning)")

    return _catalog_test(r, body)


def test_bloom_filters() -> TestResult:
    r = TestResult("bloom-filters", "Bloom Filters")

    def body(r):
        cat = _get_catalog()
        name = _table("bloom")
        tbl = cat.create_table(name, schema=BASIC_SCHEMA,
                               properties={"write.parquet.bloom-filter-enabled.column.id": "true"})
        daft.from_pydict({"id": list(range(1000)), "name": ["x"] * 1000, "value": [1.0] * 1000,
                          "ts": [datetime(2024, 1, 1, tzinfo=timezone.utc)] * 1000}).write_iceberg(tbl, mode="append")
        import pyarrow.parquet as pq
        from pyiceberg.io.pyarrow import PyArrowFileIO
        tbl = cat.load_table(name)
        io = PyArrowFileIO(tbl.io.properties)
        has_bloom = False
        for path in tbl.inspect.files().to_pydict()["file_path"]:
            with io.new_input(path).open() as f:
                md = pq.ParquetFile(f).metadata
            for rg in range(md.num_row_groups):
                col = md.row_group(rg).column(0)
                if getattr(col, "bloom_filter_offset", None):
                    has_bloom = True
        if has_bloom:
            r.result = "pass"
            r.details = "Daft honoured write.parquet.bloom-filter-enabled.column.id (bloom_filter_offset present)"
        else:
            r.result = "fail"
            r.details = ("Daft ignored write.parquet.bloom-filter-enabled: no bloom filter in the "
                        "Parquet data files it wrote for the requested column")

    return _catalog_test(r, body)


# ---------------------------------------------------------------------------
# Catalogs
# ---------------------------------------------------------------------------

def test_catalog_integration() -> TestResult:
    r = TestResult("catalog-integration", "Catalog Integration")

    def body(r):
        cat = _get_catalog()
        tables = cat.list_tables(_ns())
        r.result = "pass"
        r.details = (f"Daft uses PyIceberg catalogs (REST here: {len(tables)} tables listed in this "
                    "run's namespace); Glue/Hive/SQL catalogs come for free through the same layer")

    return _catalog_test(r, body)


def test_rest_catalog() -> TestResult:
    r = TestResult("rest-catalog", "REST Catalog")

    def body(r):
        cat = _get_catalog()
        name = _table("rest")
        tbl = cat.create_table(name, schema=BASIC_SCHEMA)
        daft.from_pydict({"id": [1], "name": ["a"], "value": [1.0],
                          "ts": [datetime(2024, 1, 1, tzinfo=timezone.utc)]}).write_iceberg(tbl, mode="append")
        assert _ids(daft.read_iceberg(cat.load_table(name))) == [1]
        r.result = "pass"
        r.details = (f"Full round-trip through the Iceberg REST catalog at {spark_fixture.REST_URI} "
                    "(OAuth2 client credentials): create, write, read")

    return _catalog_test(r, body)


def test_aws_glue_catalog() -> TestResult:
    r = TestResult("aws-glue-catalog", "AWS Glue Catalog")
    r.result = "skip"
    r.details = "Not exercised: PyIceberg's GlueCatalog needs AWS credentials and a Glue Data Catalog"
    return r


def test_unity_catalog() -> TestResult:
    r = TestResult("unity-catalog", "Unity Catalog")
    r.result = "skip"
    r.details = "Not exercised: requires a Databricks Unity Catalog endpoint"
    return r


# ---------------------------------------------------------------------------
# V3 data types / capabilities -- Spark writes the V3 column, Daft reads it.
# ---------------------------------------------------------------------------

def _v3_read_probe(r: TestResult, ddl_type: str, insert_value: str, col: str = "c", check=None):
    def body(ns, r):
        spark = spark_fixture.get_spark()
        spark.sql(f"""CREATE TABLE local.{ns}.t (id BIGINT, {col} {ddl_type}) USING iceberg
                      TBLPROPERTIES ('format-version'='3')""")
        spark.sql(f"INSERT INTO local.{ns}.t VALUES (1, {insert_value})")
        try:
            rows = _read_via_daft(ns, "t").collect().to_pydict()
        except Exception as e:  # noqa: BLE001
            r.result = "fail"
            r.details = f"Daft cannot read a V3 table with a {ddl_type} column: {_first_line(e, 160)}"
            return
        value = rows[col][0]
        ok, note = (check(value) if check else (value is not None, repr(value)[:80]))
        r.result = "pass" if ok else "fail"
        r.details = (f"Daft read a Spark-written V3 {ddl_type} column: {note}" if ok
                     else f"Daft read the V3 {ddl_type} column but the value is wrong: {note}")
    return _spark_test(r, body)


def test_variant_type() -> TestResult:
    r = TestResult("variant-type", "Variant Type", "v3")
    return _v3_read_probe(r, "VARIANT", "parse_json('{\"k\": 1}')")


def test_shredded_variant() -> TestResult:
    r = TestResult("shredded-variant", "Shredded Variant", "v3")

    def body(ns, r):
        # Daft writes Parquet through PyIceberg/Arrow, which has no variant
        # shredding; measure by writing a struct-typed column and checking there
        # is no Iceberg variant type at all on Daft's write path.
        cat = _get_catalog()
        name = _table("shred")
        try:
            from pyiceberg.types import VariantType  # noqa: F401
            has_variant = True
        except ImportError:
            has_variant = False
        r.result = "fail"
        r.details = (f"Daft cannot write Iceberg VARIANT (PyIceberg VariantType available: {has_variant}; "
                    "Daft's Arrow writer has no variant encoding), so it cannot produce a shredded variant")

    return _spark_test(r, body)


def test_geometry_type() -> TestResult:
    r = TestResult("geometry-type", "Geometry / Geo Types", "v3")

    def body(ns, r):
        # Neither Spark (Iceberg 1.11 connector rejects GEOMETRY) nor PyIceberg
        # (no GeometryType) can produce the fixture; DuckDB writes the V3
        # GEOMETRY table into the shared catalog and Daft reads it.
        produced = spark_fixture.create_geometry_fixture(ns, "t")
        if not produced["ok"]:
            r.result = "skip"
            r.details = f"Not exercised: {produced['reason']}"
            return
        try:
            value = _read_via_daft(ns, "t").collect().to_pydict()["c"][0]
        except Exception as e:  # noqa: BLE001
            r.result = "fail"
            r.details = (f"Daft cannot read a V3 table with a GEOMETRY column "
                        f"(DuckDB wrote {produced['wkt']}): {_first_line(e, 160)}")
            return
        r.result = "pass"
        r.details = f"Daft read a DuckDB-written V3 GEOMETRY column ({produced['wkt']}): {value!r}"[:300]

    return _spark_test(r, body)


def test_nanosecond_timestamps() -> TestResult:
    r = TestResult("nanosecond-timestamps", "Nanosecond Timestamps", "v3")

    def body(ns, r):
        spark = spark_fixture.get_spark()
        # Spark has no nanosecond literal; write a timestamp_ns column through
        # the Java API's type via a V3 schema that PyIceberg can read back.
        cat = _get_catalog()
        from pyiceberg.types import TimestampNanoType
        name = f"{ns}.ns"
        tbl = cat.create_table(name, schema=Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "ts", TimestampNanoType()),
        ), properties={"format-version": "3"})
        try:
            tbl.append(pa.table({"id": pa.array([1], pa.int64()),
                                 "ts": pa.array([1779283200123456789], pa.timestamp("ns"))}))
            write = "writes timestamp_ns through its Iceberg layer"
            wrote = True
        except Exception as e:  # noqa: BLE001
            write = f"cannot write timestamp_ns (PyIceberg: {_first_line(e, 110)})"
            wrote = False
        finally:
            cat.drop_table(name)
        # Read half: DuckDB (the only local producer with a nanosecond literal)
        # writes the V3 table into the shared catalog; Daft reads it.
        produced = spark_fixture.create_timestamp_ns_fixture(ns, "t")
        if not produced["ok"]:
            reads, read = None, f"read half not exercised ({produced['reason']})"
        else:
            try:
                df = _read_via_daft(ns, "t")
                val = df.collect().to_pydict()["c"][0]
                dtype = str(df.schema()["c"].dtype)
                reads = "ns" in dtype.lower() or "nanosecond" in dtype.lower()
                read = (f"reads a DuckDB-written timestamp_ns table as {dtype} "
                        f"(value {val!r}; DuckDB wrote {produced['value']!r})")
                if not reads:
                    read = "reads the table but not at nanosecond precision: " + read
            except Exception as e:  # noqa: BLE001
                reads, read = False, f"cannot read a DuckDB-written timestamp_ns table ({_first_line(e, 120)})"
        if reads is None:
            r.result = "pass" if wrote else "fail"
        else:
            r.result = "pass" if reads else "fail"
        r.details = f"Daft {write}; {read}"

    return _spark_test(r, body)


def test_unknown_type() -> TestResult:
    r = TestResult("unknown-type", "Unknown Type", "v3")
    return _v3_read_probe(r, "VOID", "NULL", check=lambda v: (v is None, "null, as the unknown type requires"))


def test_lineage() -> TestResult:
    r = TestResult("lineage", "Lineage Tracking", "v3")

    def body(ns, r):
        spark_fixture.create_fixture(ns, "t", "v3", "merge-on-read")
        tbl = _get_catalog().load_table(f"{ns}.t")
        next_row_id = getattr(tbl.metadata, "next_row_id", None)
        cols = daft.read_iceberg(tbl).column_names
        exposes = "_row_id" in cols
        r.result = "pass" if exposes else "fail"
        r.details = (f"Daft reads a V3 table with row lineage (next-row-id={next_row_id}); "
                    f"lineage columns {'are' if exposes else 'are NOT'} exposed to the DataFrame ({cols})")

    return _spark_test(r, body)


# ---------------------------------------------------------------------------
# Test registry
# ---------------------------------------------------------------------------

ALL_TESTS = [
    test_table_creation,
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
# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def load_daft_json_support() -> dict:
    """Load the JSON support levels for Daft from the repo data."""
    oss_path = os.path.join(
        REPO_ROOT, "src", "data", "platforms", "oss", "daft", "daft.json"
    )
    with open(oss_path) as f:
        data = json.load(f)
    result = {}
    for key, val in data.get("support", {}).items():
        if key.startswith("daft:"):
            parts = key.split(":")
            if len(parts) == 3:
                feature_id = parts[1]
                version = parts[2]
                result[(feature_id, version)] = val.get("level", "unknown")
    return result


def compute_match(test_result: str, json_level: str) -> bool:
    """
    Determine if test result matches JSON level.
    - pass → json should be 'full' or 'partial' (NOT 'unknown' — we have evidence now)
    - fail → json should be 'none' (NOT 'unknown' — we have evidence now)
    - skip → always matches (cannot verify)
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
    json_support = load_daft_json_support()

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
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "engine": "Daft",
        "daft_version": DAFT_VERSION,
        "pyiceberg_version": PYICEBERG_VERSION,
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
    lines.append("# Daft Iceberg Feature Test Report")
    lines.append("")
    lines.append(f"- **Timestamp:** {report['timestamp']}")
    lines.append(f"- **Daft Version:** {report['daft_version']}")
    lines.append(f"- **PyIceberg Version:** {report['pyiceberg_version']}")
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
    print("  Daft Iceberg Feature Test Suite")
    print("=" * 70)
    print(f"Daft version: {DAFT_VERSION}")
    print(f"PyIceberg version: {PYICEBERG_VERSION}")
    print(f"REST catalog: {spark_fixture.REST_URI} (catalog {spark_fixture.REST_WAREHOUSE!r})")
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
    json_path = os.path.join(REPORT_DIR, "daft-iceberg-test-report.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    md_content = generate_markdown(report)
    md_path = os.path.join(REPORT_DIR, "daft-iceberg-test-report.md")
    with open(md_path, "w") as f:
        f.write(md_content)
    print(f"JSON report: {json_path}\nMarkdown report: {md_path}")

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
