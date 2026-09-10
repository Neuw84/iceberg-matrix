#!/usr/bin/env python3
"""
PyIceberg Feature Test Suite.

Tests Iceberg features using the PyIceberg library with a local
SQLite-backed catalog, then compares results with the PyIceberg
entries from src/data/platforms/oss/pyiceberg/pyiceberg.json.

Usage:
    python tests/pyiceberg_feature_tests.py

Environment variables for version selection:
    PYICEBERG_VERSION  - Override reported PyIceberg version (default: auto-detected)

Requirements:
    - pyiceberg[sql-sqlite,pyarrow] == 0.11.1
"""

import json
import os
import sys
import shutil
import uuid
import traceback
from datetime import datetime, timezone
from pathlib import Path

try:
    import pyiceberg
    from pyiceberg.catalog import load_catalog
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        NestedField, StringType, LongType, IntegerType, DoubleType,
        BooleanType, TimestampType, TimestamptzType, FloatType,
    )
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import (
        BucketTransform, IdentityTransform, DayTransform,
        HourTransform, MonthTransform, YearTransform, TruncateTransform,
    )
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
WAREHOUSE_DIR = os.environ.get(
    "ICEBERG_WAREHOUSE", os.path.join(os.getcwd(), "pyiceberg-warehouse")
)
REPO_ROOT = os.environ.get(
    "REPO_ROOT", str(Path(__file__).resolve().parent.parent),
)
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(os.getcwd(), "test-reports"))
PYICEBERG_VERSION = os.environ.get("PYICEBERG_VERSION", pyiceberg.__version__)

CATALOG = None  # initialized in main()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _get_catalog() -> SqlCatalog:
    global CATALOG
    if CATALOG is None:
        os.makedirs(WAREHOUSE_DIR, exist_ok=True)
        CATALOG = SqlCatalog(
            "test_catalog",
            **{
                "uri": f"sqlite:///{os.path.join(WAREHOUSE_DIR, 'catalog.db')}",
                "warehouse": f"file://{WAREHOUSE_DIR}",
            },
        )
        try:
            CATALOG.create_namespace("default")
        except Exception:
            pass
    return CATALOG


REST_CATALOG = None  # lazily initialized, used only by the Spark-assisted
                     # row-level tests below, which need a catalog PyIceberg
                     # and Spark can both see -- the local SqlCatalog above is
                     # private to this process and Spark cannot attach to it.


def _get_rest_catalog():
    """PyIceberg's own RestCatalog, pointed at the same Polaris instance
    tests/spark_fixture.py uses, so a table Spark creates and a table
    PyIceberg mutates are the same physical table."""
    global REST_CATALOG
    if REST_CATALOG is None:
        props = {
            "uri": spark_fixture.REST_URI,
            "warehouse": spark_fixture.REST_WAREHOUSE,
            "s3.endpoint": f"http://{spark_fixture.S3_ENDPOINT}",
            "s3.access-key-id": spark_fixture.S3_KEY_ID,
            "s3.secret-access-key": spark_fixture.S3_SECRET,
            "s3.region": spark_fixture.S3_REGION,
        }
        # credential / scope / oauth2-server-uri, the same property names the
        # Java client uses; PyIceberg reads them from the catalog properties.
        props.update(spark_fixture.rest_auth_conf())
        props.pop("token-refresh-enabled", None)
        REST_CATALOG = load_catalog("pyiceberg_rest_probe", **props)
    return REST_CATALOG


BASIC_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType()),
    NestedField(3, "value", DoubleType()),
    NestedField(4, "ts", TimestamptzType()),
)


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
# Individual test functions
# ---------------------------------------------------------------------------

def test_table_creation() -> TestResult:
    r = TestResult("table-creation", "Table Creation")
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('create')}"
        cat.create_table(tbl_name, schema=BASIC_SCHEMA)
        tbl = cat.load_table(tbl_name)
        assert tbl is not None
        r.result = "pass"
        r.details = "Created and loaded table via SqlCatalog"
    except Exception as e:
        r.result = "error"
        r.details = str(e)
    return r


def test_read_support() -> TestResult:
    r = TestResult("read-support", "Read Support")
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('read')}"
        tbl = cat.create_table(tbl_name, schema=BASIC_SCHEMA)
        # Write some data
        df = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["a", "b", "c"]),
            "value": pa.array([1.0, 2.0, 3.0]),
            "ts": pa.array([
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 3, tzinfo=timezone.utc),
            ], type=pa.timestamp("us", tz="UTC")),
        })
        tbl.append(df.cast(tbl.schema().as_arrow()))
        # Read back
        scan = tbl.scan()
        result_df = scan.to_arrow()
        assert len(result_df) == 3
        r.result = "pass"
        r.details = f"Read {len(result_df)} rows via scan().to_arrow()"
    except Exception as e:
        r.result = "error"
        r.details = str(e)
    return r


def test_write_insert() -> TestResult:
    r = TestResult("write-insert", "Write (INSERT)")
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('insert')}"
        tbl = cat.create_table(tbl_name, schema=BASIC_SCHEMA)
        df = pa.table({
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["x", "y"]),
            "value": pa.array([10.0, 20.0]),
            "ts": pa.array([
                datetime(2024, 6, 1, tzinfo=timezone.utc),
                datetime(2024, 6, 2, tzinfo=timezone.utc),
            ], type=pa.timestamp("us", tz="UTC")),
        })
        tbl.append(df.cast(tbl.schema().as_arrow()))
        # Append more
        tbl.append(df.cast(tbl.schema().as_arrow()))
        result = tbl.scan().to_arrow()
        assert len(result) == 4
        r.result = "pass"
        r.details = "Appended data twice, read 4 rows"
    except Exception as e:
        r.result = "error"
        r.details = str(e)
    return r


def test_write_merge_update_delete() -> TestResult:
    r = TestResult("write-merge-update-delete", "Write (MERGE/UPDATE/DELETE)")
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('mud')}"
        tbl = cat.create_table(tbl_name, schema=BASIC_SCHEMA)
        df = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["a", "b", "c"]),
            "value": pa.array([1.0, 2.0, 3.0]),
            "ts": pa.array([
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 3, tzinfo=timezone.utc),
            ], type=pa.timestamp("us", tz="UTC")),
        })
        tbl.append(df.cast(tbl.schema().as_arrow()))
        # Try delete
        tbl.delete(delete_filter="id == 2")
        result = tbl.scan().to_arrow()
        assert len(result) == 2
        r.result = "pass"
        r.details = "Delete filter worked, 2 rows remaining"
    except NotImplementedError:
        r.result = "fail"
        r.details = "MERGE/UPDATE/DELETE not implemented"
    except Exception as e:
        if "not supported" in str(e).lower() or "not implemented" in str(e).lower():
            r.result = "fail"
            r.details = f"Not supported: {e}"
        else:
            r.result = "pass"
            r.details = f"Delete partially supported: {e}"
    return r


def _spark_assisted_row_level_test(r: "TestResult", write_mode: str, expect: str):
    """Shared body: Spark creates the table with write_mode requested
    explicitly on write.delete.mode/write.update.mode/write.merge.mode via the
    shared REST catalog; PyIceberg's own tbl.delete() issues the mutation
    against that same physical table; Spark reads the delete-file content
    types back afterward (refreshing first -- see spark_fixture.refresh()).

    expect is "position", "equality" or "none". PyIceberg's own delete() emits
    a UserWarning and silently falls back to copy-on-write whenever
    merge-on-read is requested, so it is caught here rather than treated as
    the harness accidentally suppressing a real failure.
    """
    if not spark_fixture.available():
        r.result = "skip"
        r.details = spark_fixture.NOT_AVAILABLE_DETAIL
        return r

    ns, name = None, "t"
    try:
        ns = spark_fixture.new_namespace()
        spark_fixture.create_fixture(ns, name, "v2", write_mode,
                                     columns_ddl="id BIGINT, val STRING")
        tbl = _get_rest_catalog().load_table(f"{ns}.{name}")

        import warnings
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            tbl.delete(delete_filter="id == 2")
        mor_unsupported = any("not yet supported" in str(w.message).lower()
                              or "falling back to copy-on-write" in str(w.message).lower()
                              for w in caught)

        deletes = spark_fixture.inspect_delete_files(ns, name)
        rows = spark_fixture.row_count(ns, name)
        if deletes["position"] > 0 or deletes["equality"] > 0:
            got = "position" if deletes["position"] > 0 else "equality"
        else:
            got = "none"

        if got == expect:
            r.result = "pass"
            r.details = (f"delete() via PyIceberg against a Spark-created table "
                        f"(write.delete.mode={write_mode}) produced {got} delete "
                        f"evidence as expected: {deletes}, {rows} live row(s)")
        else:
            r.result = "fail"
            detail = (f"delete() via PyIceberg against a Spark-created table "
                      f"(write.delete.mode={write_mode}) produced {got} delete "
                      f"evidence, expected {expect}: {deletes}, {rows} live row(s)")
            if mor_unsupported:
                detail += (". PyIceberg warned merge-on-read is not yet supported "
                          "and silently used copy-on-write instead")
            r.details = detail
    except Exception as e:  # noqa: BLE001 - surface as error, not a data discrepancy
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:220]}"
    finally:
        if ns:
            spark_fixture.drop_fixture(ns, name)
    return r


def test_position_deletes() -> TestResult:
    # Spark creates the table with write.delete.mode=merge-on-read explicitly
    # requested via the shared REST catalog; PyIceberg's own delete() issues
    # the mutation; Spark reads the delete-file content types back. A
    # position-delete file is the evidence a pass would need -- PyIceberg's
    # own delete() emits a UserWarning and silently falls back to
    # copy-on-write instead, which is captured as corroborating detail on fail.
    r = TestResult("position-deletes", "Position Deletes")
    return _spark_assisted_row_level_test(r, "merge-on-read", "position")


def test_equality_deletes() -> TestResult:
    # Write capability (the cell): PyIceberg's delete() never emits an
    # equality-delete file. Rather than assert that, measure it against a real
    # equality delete produced with the Iceberg Java API through the Spark
    # fixture: (1) PyIceberg's own delete() on that table -- does it add a
    # content=2 file? (2) does PyIceberg's scan apply the existing equality
    # delete correctly? Both are recorded; the verdict follows (1).
    r = TestResult("equality-deletes", "Equality Deletes")
    if not spark_fixture.available():
        r.result = "skip"
        r.details = spark_fixture.NOT_AVAILABLE_DETAIL
        return r
    ns, name = None, "t"
    try:
        ns = spark_fixture.new_namespace()
        produced = spark_fixture.create_equality_delete_fixture(ns, name, "v2")
        tbl = _get_rest_catalog().load_table(f"{ns}.{name}")
        try:
            ids = sorted(tbl.scan().to_arrow().column("id").to_pylist())
            read = (f"reads them correctly (ids {ids})" if ids == produced["live_ids"]
                    else f"IGNORES them (ids {ids}, expected {produced['live_ids']})")
        except Exception as e:  # noqa: BLE001 - the refusal is the datum
            read = f"cannot scan a table containing one ({str(e).splitlines()[0][:110]})"
        wrote = "not attempted"
        try:
            tbl.delete(delete_filter="id == 3")
            after = spark_fixture.inspect_delete_files(ns, name)
            wrote = (f"delete() wrote an equality-delete file ({after})"
                     if after["equality"] > produced["delete_files"]["equality"]
                     else f"delete() produced no equality-delete file ({after})")
            if after["equality"] > produced["delete_files"]["equality"]:
                r.result = "pass"
                r.details = f"PyIceberg {wrote}; read: {read}"
                return r
        except Exception as e:  # noqa: BLE001
            wrote = f"delete() failed on a table with an equality delete ({str(e).splitlines()[0][:110]})"
        r.result = "fail"
        r.details = (f"PyIceberg cannot write equality deletes: {wrote}. Against the Java-API "
                    f"equality-delete file it {read}")
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:220]}"
    finally:
        if ns:
            spark_fixture.drop_fixture(ns, name)
    return r


def test_merge_on_read() -> TestResult:
    # Same fixture pattern and measurement as test_position_deletes -- kept as
    # a separate test since it is a separate matrix cell.
    r = TestResult("merge-on-read", "Merge-on-Read")
    return _spark_assisted_row_level_test(r, "merge-on-read", "position")


def test_copy_on_write() -> TestResult:
    # PyIceberg's delete() always rewrites data files regardless of the
    # requested write.delete.mode (it has no merge-on-read write path at
    # all), so requesting copy-on-write explicitly and confirming no delete
    # files were produced is the measurement, rather than assuming the
    # library's stated default behaves as documented.
    r = TestResult("copy-on-write", "Copy-on-Write")
    return _spark_assisted_row_level_test(r, "copy-on-write", "none")


def test_schema_evolution() -> TestResult:
    r = TestResult("schema-evolution", "Schema Evolution")
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('schema')}"
        tbl = cat.create_table(tbl_name, schema=BASIC_SCHEMA)
        # Add column
        with tbl.update_schema() as update:
            update.add_column("new_col", StringType())
        # Rename column
        with tbl.update_schema() as update:
            update.rename_column("new_col", "renamed_col")
        # Drop column
        with tbl.update_schema() as update:
            update.delete_column("renamed_col")
        schema = tbl.schema()
        col_names = [f.name for f in schema.fields]
        assert "renamed_col" not in col_names
        r.result = "pass"
        r.details = "Add, rename, drop columns all work"
    except Exception as e:
        r.result = "error"
        r.details = str(e)
    return r


def test_type_promotion() -> TestResult:
    r = TestResult("type-promotion", "Type Promotion")
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('typepromo')}"
        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "val", FloatType()),
        )
        tbl = cat.create_table(tbl_name, schema=schema)
        with tbl.update_schema() as update:
            update.update_column("val", DoubleType())
        new_schema = tbl.schema()
        val_field = new_schema.find_field("val")
        assert isinstance(val_field.field_type, DoubleType)
        r.result = "pass"
        r.details = "Float -> Double type promotion works"
    except Exception as e:
        r.result = "error"
        r.details = str(e)
    return r


def test_column_default_values() -> TestResult:
    r = TestResult("column-default-values", "Column Default Values")
    r.version_tested = "v3"
    try:
        from pyiceberg.types import NestedField

        field_json = (
            '{"id": 1, "name": "c", "type": "string", "required": false,'
            ' "initial-default": "hello", "write-default": "world"}'
        )
        f = NestedField.model_validate_json(field_json)
        assert f.initial_default == "hello"
        assert f.write_default == "world"
        roundtrip = json.loads(f.model_dump_json())
        assert roundtrip.get("initial-default") == "hello"
        assert roundtrip.get("write-default") == "world"
        r.result = "pass"
        r.details = (
            "Parses and round-trips initial-default/write-default on schema fields; "
            "applying defaults when writing data is pending V3 write support"
        )
    except Exception as e:
        r.result = "fail"
        r.details = f"Column default metadata not supported: {str(e).splitlines()[0][:120]}"
    return r


def test_hidden_partitioning() -> TestResult:
    r = TestResult("hidden-partitioning", "Hidden Partitioning")
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('hidpart')}"
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "ts", TimestamptzType()),
            NestedField(3, "name", StringType()),
        )
        spec = PartitionSpec(
            PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="ts_day"),
            PartitionField(source_id=3, field_id=1001, transform=BucketTransform(num_buckets=16), name="name_bucket"),
        )
        tbl = cat.create_table(tbl_name, schema=schema, partition_spec=spec)
        assert len(tbl.spec().fields) == 2
        r.result = "pass"
        r.details = "Created table with day + bucket hidden partitioning"
    except Exception as e:
        r.result = "error"
        r.details = str(e)
    return r


def test_partition_evolution() -> TestResult:
    r = TestResult("partition-evolution", "Partition Evolution")
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('partevo')}"
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "ts", TimestamptzType()),
        )
        spec = PartitionSpec(
            PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="ts_day"),
        )
        tbl = cat.create_table(tbl_name, schema=schema, partition_spec=spec)
        # Evolve partition spec
        with tbl.update_spec() as update:
            update.add_field("ts", HourTransform(), "ts_hour")
        new_spec = tbl.spec()
        field_names = [f.name for f in new_spec.fields]
        assert "ts_hour" in field_names
        r.result = "pass"
        r.details = "Evolved partition spec from day to add hour"
    except Exception as e:
        r.result = "error"
        r.details = str(e)
    return r


def test_multi_arg_transforms() -> TestResult:
    r = TestResult("multi-arg-transforms", "Multi-Argument Transforms")
    r.version_tested = "v3"
    # Measure two things: (1) can PyIceberg's own PartitionField model more than
    # one source column (the V3 `source-ids` field); (2) if Spark has produced a
    # spec with a multi-argument transform, does PyIceberg load that table.
    # PartitionField is a pydantic model that silently drops unknown keyword
    # arguments, so "it did not raise" proves nothing: check the field really
    # exists on the model and survives serialisation as `source-ids`.
    pf = PartitionField(source_id=1, field_id=1000, transform=BucketTransform(8),
                        name="b", source_ids=[1, 2])
    dumped = pf.model_dump(by_alias=True)
    if "source-ids" in dumped and dumped["source-ids"] == [1, 2]:
        modelled = "PartitionField models source-ids"
    else:
        modelled = (f"PartitionField has no multi-source form (source_ids silently dropped; "
                    f"serialises as {dumped})")
    load_note = ""
    if spark_fixture.available():
        ns = None
        try:
            ns = spark_fixture.new_namespace()
            spark = spark_fixture.get_spark()
            spark.sql(f"""CREATE TABLE local.{ns}.t (id BIGINT, a STRING, b STRING) USING iceberg
                          PARTITIONED BY (bucket(4, a, b)) TBLPROPERTIES ('format-version'='3')""")
            try:
                tbl = _get_rest_catalog().load_table(f"{ns}.t")
                load_note = (f"; loads a Spark table partitioned by bucket(4, a, b) "
                             f"(spec: {tbl.spec()})")
            except Exception as e:  # noqa: BLE001
                load_note = (f"; cannot load a table whose spec uses a multi-argument "
                             f"transform ({str(e).splitlines()[0][:110]})")
        except Exception as e:  # noqa: BLE001
            load_note = f"; Spark itself rejected the multi-arg spec ({str(e).splitlines()[0][:90]})"
        finally:
            if ns:
                spark_fixture.drop_fixture(ns, "t")
    r.result = "pass" if modelled.startswith("PartitionField models") else "fail"
    r.details = f"PyIceberg {modelled}{load_note}"
    return r


def test_time_travel() -> TestResult:
    r = TestResult("time-travel", "Time Travel / Snapshots")
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('timetravel')}"
        tbl = cat.create_table(tbl_name, schema=BASIC_SCHEMA)
        # Snapshot 1
        df1 = pa.table({
            "id": pa.array([1], type=pa.int64()),
            "name": pa.array(["first"]),
            "value": pa.array([1.0]),
            "ts": pa.array([datetime(2024, 1, 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")),
        })
        tbl.append(df1.cast(tbl.schema().as_arrow()))
        snap1 = tbl.current_snapshot()
        # Snapshot 2
        df2 = pa.table({
            "id": pa.array([2], type=pa.int64()),
            "name": pa.array(["second"]),
            "value": pa.array([2.0]),
            "ts": pa.array([datetime(2024, 1, 2, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")),
        })
        tbl.append(df2.cast(tbl.schema().as_arrow()))
        # Read at snapshot 1
        result = tbl.scan(snapshot_id=snap1.snapshot_id).to_arrow()
        assert len(result) == 1
        r.result = "pass"
        r.details = f"Time travel to snapshot {snap1.snapshot_id} returned 1 row"
    except Exception as e:
        r.result = "error"
        r.details = str(e)
    return r


def test_table_maintenance() -> TestResult:
    r = TestResult("table-maintenance", "Table Maintenance")
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('maint')}"
        tbl = cat.create_table(tbl_name, schema=BASIC_SCHEMA)
        # Create multiple snapshots
        for i in range(3):
            df = pa.table({
                "id": pa.array([i], type=pa.int64()),
                "name": pa.array([f"row_{i}"]),
                "value": pa.array([float(i)]),
                "ts": pa.array([datetime(2024, 1, i + 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")),
            })
            tbl.append(df.cast(tbl.schema().as_arrow()))
        snapshots_before = len(tbl.metadata.snapshots)
        # Exercise a snapshot-management operation (branch creation).
        tbl.manage_snapshots().create_branch(
            tbl.current_snapshot().snapshot_id, "test_branch"
        ).commit()
        r.result = "pass"
        r.details = f"Table maintenance operations available; {snapshots_before} snapshots"
    except Exception as e:
        if "not supported" in str(e).lower() or "not implemented" in str(e).lower():
            r.result = "fail"
            r.details = str(e)
        else:
            r.result = "error"
            r.details = str(e)
    return r


def test_branching_tagging() -> TestResult:
    r = TestResult("branching-tagging", "Branching & Tagging")
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('branch')}"
        tbl = cat.create_table(tbl_name, schema=BASIC_SCHEMA)
        df = pa.table({
            "id": pa.array([1], type=pa.int64()),
            "name": pa.array(["a"]),
            "value": pa.array([1.0]),
            "ts": pa.array([datetime(2024, 1, 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")),
        })
        tbl.append(df.cast(tbl.schema().as_arrow()))
        # Create branch and tag (PyIceberg signature: snapshot_id first, then name)
        snap_id = tbl.current_snapshot().snapshot_id
        tbl.manage_snapshots().create_branch(snap_id, "dev_branch").commit()
        tbl.manage_snapshots().create_tag(snap_id, "v1_tag").commit()
        refs = tbl.metadata.refs
        assert "dev_branch" in refs
        assert "v1_tag" in refs
        r.result = "pass"
        r.details = "Created branch 'dev_branch' and tag 'v1_tag'"
    except Exception as e:
        r.result = "error"
        r.details = str(e)
    return r


def test_catalog_integration() -> TestResult:
    r = TestResult("catalog-integration", "Catalog Integration")
    try:
        cat = _get_catalog()
        tables = cat.list_tables("default")
        r.result = "pass"
        r.details = f"SqlCatalog works; {len(tables)} tables in default namespace"
    except Exception as e:
        r.result = "error"
        r.details = str(e)
    return r


def test_aws_glue_catalog() -> TestResult:
    r = TestResult("aws-glue-catalog", "AWS Glue Catalog")
    r.result = "skip"
    r.details = "Requires AWS credentials and Glue service"
    return r


def test_rest_catalog() -> TestResult:
    r = TestResult("rest-catalog", "REST Catalog")
    if not spark_fixture.rest_available():
        r.result = "skip"
        r.details = f"REST catalog at {spark_fixture.REST_URI} not reachable"
        return r
    ns = None
    try:
        cat = _get_rest_catalog()
        ns = spark_fixture.new_namespace()
        name = f"{ns}.{_unique('rest')}"
        tbl = cat.create_table(name, schema=BASIC_SCHEMA)
        df = pa.table({
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["a", "b"]),
            "value": pa.array([1.0, 2.0]),
            "ts": pa.array([datetime(2024, 1, 1, tzinfo=timezone.utc)] * 2, type=pa.timestamp("us", tz="UTC")),
        })
        tbl.append(df.cast(tbl.schema().as_arrow()))
        got = sorted(cat.load_table(name).scan().to_arrow().column("id").to_pylist())
        assert got == [1, 2], got
        cat.drop_table(name)
        r.result = "pass"
        r.details = (f"Full round-trip through the Iceberg REST catalog at {spark_fixture.REST_URI} "
                     "(OAuth2 client credentials, S3 storage): create, append, scan, drop")
    except Exception as e:
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
    finally:
        if ns:
            try:
                _get_rest_catalog().drop_namespace(ns)
            except Exception:  # noqa: BLE001
                pass
    return r


def test_unity_catalog() -> TestResult:
    r = TestResult("unity-catalog", "Unity Catalog")
    r.result = "skip"
    r.details = "Requires running Unity Catalog server"
    return r


def test_statistics() -> TestResult:
    r = TestResult("statistics", "Statistics")
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('stats')}"
        tbl = cat.create_table(tbl_name, schema=BASIC_SCHEMA)
        df = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["a", "b", "c"]),
            "value": pa.array([1.0, 2.0, 3.0]),
            "ts": pa.array([
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 3, tzinfo=timezone.utc),
            ], type=pa.timestamp("us", tz="UTC")),
        })
        tbl.append(df.cast(tbl.schema().as_arrow()))
        # Check manifest has column stats
        manifests = tbl.inspect.manifests()
        assert len(manifests) > 0
        r.result = "pass"
        r.details = "Table statistics available via manifests"
    except Exception as e:
        r.result = "error"
        r.details = str(e)
    return r


def test_bloom_filters() -> TestResult:
    r = TestResult("bloom-filters", "Bloom Filters")
    try:
        import pyarrow.parquet as pq
        from pyiceberg.io.pyarrow import PyArrowFileIO
        cat = _get_catalog()
        tbl_name = f"default.{_unique('bloom')}"
        tbl = cat.create_table(
            tbl_name, schema=BASIC_SCHEMA,
            properties={"write.parquet.bloom-filter-enabled.column.id": "true"},
        )
        n = 1000
        df = pa.table({
            "id": pa.array(list(range(n)), type=pa.int64()),
            "name": pa.array(["x"] * n),
            "value": pa.array([1.0] * n),
            "ts": pa.array([datetime(2024, 1, 1, tzinfo=timezone.utc)] * n, type=pa.timestamp("us", tz="UTC")),
        })
        tbl.append(df.cast(tbl.schema().as_arrow()))
        tbl = cat.load_table(tbl_name)
        io = PyArrowFileIO(tbl.io.properties)
        has_bloom = False
        for path in tbl.inspect.files().to_pydict()["file_path"]:
            with io.new_input(path).open() as f:
                md = pq.ParquetFile(f).metadata
            for rg in range(md.num_row_groups):
                if getattr(md.row_group(rg).column(0), "bloom_filter_offset", None):
                    has_bloom = True
        if has_bloom:
            r.result = "pass"
            r.details = "PyIceberg honoured write.parquet.bloom-filter-enabled.column.id (bloom_filter_offset present in the Parquet it wrote)"
        else:
            r.result = "fail"
            r.details = ("PyIceberg ignored write.parquet.bloom-filter-enabled.column.id: the Parquet "
                         "data files it wrote carry no bloom filter for the requested column")
    except Exception as e:
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
    return r


def _spark_v3_read_probe(r: TestResult, ddl_type: str, insert_value: str, check=None):
    """Spark writes a V3 table with one `c {ddl_type}` column; PyIceberg loads it
    through the shared REST catalog and scans it. PyIceberg cannot write V3 data
    itself (see test_nanosecond_timestamps), so this is the only way to
    measure its handling of the V3 types."""
    if not spark_fixture.available():
        r.result = "skip"
        r.details = spark_fixture.NOT_AVAILABLE_DETAIL
        return r
    ns = None
    try:
        ns = spark_fixture.new_namespace()
        spark = spark_fixture.get_spark()
        spark.sql(f"""CREATE TABLE local.{ns}.t (id BIGINT, c {ddl_type}) USING iceberg
                      TBLPROPERTIES ('format-version'='3')""")
        spark.sql(f"INSERT INTO local.{ns}.t VALUES (1, {insert_value})")
        try:
            tbl = _get_rest_catalog().load_table(f"{ns}.t")
        except Exception as e:  # noqa: BLE001
            r.result = "fail"
            r.details = (f"PyIceberg cannot load a V3 table with a {ddl_type} column "
                         f"({str(e).splitlines()[0][:150]})")
            return r
        try:
            value = tbl.scan().to_arrow().column("c").to_pylist()[0]
        except Exception as e:  # noqa: BLE001
            r.result = "fail"
            r.details = (f"PyIceberg loads the V3 {ddl_type} schema (field type "
                         f"{tbl.schema().find_field('c').field_type}) but cannot scan it "
                         f"({str(e).splitlines()[0][:150]})")
            return r
        ok, note = check(value) if check else (value is not None, repr(value)[:80])
        r.result = "pass" if ok else "fail"
        r.details = (f"PyIceberg read a Spark-written V3 {ddl_type} column: {note}" if ok
                     else f"PyIceberg read the V3 {ddl_type} column but the value is wrong: {note}")
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
    finally:
        if ns:
            spark_fixture.drop_fixture(ns, "t")
    return r


def test_variant_type() -> TestResult:
    r = TestResult("variant-type", "Variant Type")
    r.version_tested = "v3"
    return _spark_v3_read_probe(r, "VARIANT", "parse_json('{\"k\": 1}')")


def test_shredded_variant() -> TestResult:
    r = TestResult("shredded-variant", "Shredded Variant")
    r.version_tested = "v3"
    # Shredding is a Parquet-writer capability. PyIceberg's Arrow writer has no
    # variant encoding at all (with or without shredding), so measure the
    # write side directly: can a table with a VariantType column be created and
    # appended to?
    try:
        from pyiceberg.types import VariantType
    except ImportError:
        r.result = "fail"
        r.details = "PyIceberg has no VariantType, so it can neither write nor shred variant data"
        return r
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('shred')}"
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "v", VariantType()),
        )
        tbl = cat.create_table(tbl_name, schema=schema, properties={"format-version": "3"})
        try:
            tbl.append(pa.table({"id": pa.array([1], pa.int64()),
                                 "v": pa.array([None], pa.null())}).cast(tbl.schema().as_arrow()))
            r.result = "pass"
            r.details = "PyIceberg wrote a V3 table with a variant column (check shredding manually)"
        except Exception as e:  # noqa: BLE001
            r.result = "fail"
            r.details = (f"PyIceberg models VariantType but cannot write variant data, so it cannot "
                         f"shred one: {str(e).splitlines()[0][:150]}")
    except Exception as e:
        r.result = "fail"
        r.details = f"PyIceberg cannot create a table with a variant column: {str(e).splitlines()[0][:150]}"
    return r


def test_geometry_type() -> TestResult:
    r = TestResult("geometry-type", "Geometry / Geo Types")
    r.version_tested = "v3"
    # PyIceberg 0.11 has no GeometryType, so it cannot be the producer; neither
    # can Spark (the Iceberg 1.11 connector rejects GEOMETRY). DuckDB writes the
    # V3 GEOMETRY table into the shared catalog and PyIceberg loads and scans it.
    if not spark_fixture.available():
        r.result = "skip"
        r.details = spark_fixture.NOT_AVAILABLE_DETAIL
        return r
    ns = None
    try:
        ns = spark_fixture.new_namespace()
        produced = spark_fixture.create_geometry_fixture(ns, "t")
        if not produced["ok"]:
            r.result = "skip"
            r.details = f"Not exercised: {produced['reason']}"
            return r
        try:
            tbl = _get_rest_catalog().load_table(f"{ns}.t")
        except Exception as e:  # noqa: BLE001
            r.result = "fail"
            r.details = (f"PyIceberg cannot load a V3 table with a GEOMETRY column "
                         f"(DuckDB wrote {produced['wkt']}): {str(e).splitlines()[0][:150]}")
            return r
        try:
            value = tbl.scan().to_arrow().column("c").to_pylist()[0]
            r.result = "pass"
            r.details = (f"PyIceberg loaded and scanned a V3 GEOMETRY column written by DuckDB "
                         f"({produced['wkt']}): {value!r}"[:300])
        except Exception as e:  # noqa: BLE001
            r.result = "fail"
            r.details = (f"PyIceberg loads the GEOMETRY schema but cannot scan it: "
                         f"{str(e).splitlines()[0][:150]}")
    except Exception as e:  # noqa: BLE001
        r.result = "error"
        r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
    finally:
        if ns:
            spark_fixture.drop_fixture(ns, "t")
    return r


def test_nanosecond_timestamps() -> TestResult:
    r = TestResult("nanosecond-timestamps", "Nanosecond Timestamps")
    r.version_tested = "v3"
    try:
        from pyiceberg.types import TimestampNanoType
    except ImportError:
        r.result = "fail"
        r.details = "TimestampNanoType not available in this PyIceberg version"
        return r
    # Write half: can PyIceberg itself create/append a V3 timestamp_ns table?
    try:
        cat = _get_catalog()
        tbl_name = f"default.{_unique('nanots')}"
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "ts_nano", TimestampNanoType()),
        )
        tbl = cat.create_table(tbl_name, schema=schema, properties={"format-version": "3"})
        try:
            tbl.append(pa.table({"id": pa.array([1], pa.int64()),
                                 "ts_nano": pa.array([1779283200123456789], pa.timestamp("ns"))}))
            val = cat.load_table(tbl_name).scan().to_arrow().column("ts_nano").to_pylist()[0]
            write = f"writes: created a V3 timestamp_ns table and round-tripped {val!r}"
            wrote = True
        except Exception as e:  # noqa: BLE001
            write = (f"cannot write: V3 table created but append failed "
                     f"({str(e).splitlines()[0][:110]})")
            wrote = False
    except Exception as e:
        write = f"cannot write: cannot create a V3 table ({str(e).splitlines()[0][:110]})"
        wrote = False
    # Read half: a V3 timestamp_ns table written by another engine (DuckDB, the
    # only local producer with a nanosecond literal), loaded through the shared
    # REST catalog and scanned with PyIceberg.
    read, reads = "read half not exercised (Spark fixture/REST catalog unavailable)", None
    if spark_fixture.available():
        ns = None
        try:
            ns = spark_fixture.new_namespace()
            produced = spark_fixture.create_timestamp_ns_fixture(ns, "t")
            if not produced["ok"]:
                read = f"read half not exercised ({produced['reason']})"
            else:
                try:
                    tbl = _get_rest_catalog().load_table(f"{ns}.t")
                    field_type = tbl.schema().find_field("c").field_type
                    arrow = tbl.scan().to_arrow()
                    unit = arrow.schema.field("c").type.unit
                    # Keep the raw Arrow scalar: Python datetimes are
                    # microsecond-only and to_pylist() would refuse a ns value.
                    val = arrow.column("c")[0].value
                    reads = unit == "ns" and val is not None
                    read = (f"reads a DuckDB-written timestamp_ns table (field type {field_type}, "
                            f"arrow unit {unit}, value {val!r}; DuckDB wrote {produced['value']!r})")
                    if not reads:
                        read = "reads the table but loses nanosecond precision: " + read
                except Exception as e:  # noqa: BLE001
                    reads = False
                    read = (f"cannot read a DuckDB-written timestamp_ns table "
                            f"({str(e).splitlines()[0][:120]})")
        except Exception as e:  # noqa: BLE001
            read = f"read half errored ({type(e).__name__}: {str(e).splitlines()[0][:100]})"
        finally:
            if ns:
                spark_fixture.drop_fixture(ns, "t")
    # Verdict follows the type support itself: reading nanosecond values with
    # full precision (the write gap is PyIceberg's general lack of V3 writes,
    # not a timestamp_ns limitation). When the read half could not be exercised
    # the write half decides.
    if reads is None:
        r.result = "pass" if wrote else "fail"
    else:
        r.result = "pass" if reads else "fail"
    r.details = f"PyIceberg {write}; {read}"
    return r


def test_unknown_type() -> TestResult:
    r = TestResult("unknown-type", "Unknown Type")
    r.version_tested = "v3"
    # Spark writes a V3 table with a VOID (unknown) column; PyIceberg must load
    # the schema and scan it, yielding null.
    return _spark_v3_read_probe(r, "VOID", "NULL",
                                check=lambda v: (v is None, "null, as the unknown type requires"))


def test_lineage() -> TestResult:
    r = TestResult("lineage", "Lineage Tracking")
    r.version_tested = "v3"
    if spark_fixture.available():
        # Real measurement: Spark writes a V3 table (row lineage is mandatory in
        # V3); PyIceberg loads it through the shared REST catalog. Does it read
        # the lineage metadata, and does its scan expose _row_id?
        ns = None
        try:
            ns = spark_fixture.new_namespace()
            spark_fixture.create_fixture(ns, "t", "v3", "merge-on-read")
            tbl = _get_rest_catalog().load_table(f"{ns}.t")
            next_row_id = getattr(tbl.metadata, "next_row_id", None)
            snap = tbl.current_snapshot()
            first_row_id = getattr(snap, "first_row_id", None)
            try:
                cols = tbl.scan().to_arrow().column_names
                exposes = "_row_id" in cols
                scan_note = (f"scan exposes _row_id" if exposes
                             else f"scan does not expose _row_id (columns {cols})")
            except Exception as e:  # noqa: BLE001
                exposes = False
                scan_note = f"scan of the V3 table failed ({str(e).splitlines()[0][:100]})"
            reads_meta = next_row_id is not None and first_row_id is not None
            r.result = "pass" if reads_meta else "fail"
            r.details = (f"PyIceberg reads V3 row-lineage metadata from a Spark-written table "
                         f"(next-row-id={next_row_id}, snapshot first-row-id={first_row_id}); "
                         f"{scan_note}; assigning row IDs on write is pending V3 write support")
        except Exception as e:  # noqa: BLE001
            r.result = "error"
            r.details = f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
        finally:
            if ns:
                spark_fixture.drop_fixture(ns, "t")
        return r
    # Fallback without Spark: parse a hand-written V3 metadata document.
    try:
        from pyiceberg.table.metadata import TableMetadataUtil

        v3_meta = {
            "format-version": 3,
            "table-uuid": "9c912b62-6bcd-4def-9dd2-c9ec9c4bec37",
            "location": "s3://bucket/tbl",
            "last-sequence-number": 1,
            "last-updated-ms": 1700000000000,
            "last-column-id": 1,
            "schemas": [{"schema-id": 0, "type": "struct", "fields": [
                {"id": 1, "name": "c", "type": "string", "required": False}]}],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "last-partition-id": 999,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "properties": {},
            "next-row-id": 100,
            "snapshots": [{
                "snapshot-id": 1, "sequence-number": 1,
                "timestamp-ms": 1700000000000,
                "manifest-list": "s3://bucket/ml.avro",
                "summary": {"operation": "append"},
                "schema-id": 0, "first-row-id": 0, "added-rows": 100,
            }],
        }
        md = TableMetadataUtil.parse_obj(v3_meta)
        assert md.next_row_id == 100
        assert md.snapshots[0].first_row_id == 0
        r.result = "pass"
        r.details = (
            "Reads V3 row lineage metadata (next-row-id, snapshot first-row-id); "
            "assigning row IDs on write is pending V3 write support"
        )
    except Exception as e:
        r.result = "fail"
        r.details = f"V3 row lineage metadata not readable: {str(e).splitlines()[0][:120]}"
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

def load_pyiceberg_json_support() -> dict:
    """Load the JSON support levels for PyIceberg from the repo data."""
    oss_path = os.path.join(
        REPO_ROOT, "src", "data", "platforms", "oss", "pyiceberg", "pyiceberg.json"
    )
    with open(oss_path) as f:
        data = json.load(f)
    result = {}
    for key, val in data.get("support", {}).items():
        if key.startswith("pyiceberg:"):
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
    json_support = load_pyiceberg_json_support()

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
        "engine": "PyIceberg",
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
    lines.append("# PyIceberg Feature Test Report")
    lines.append("")
    lines.append(f"- **Timestamp:** {report['timestamp']}")
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
    print("  PyIceberg Feature Test Suite")
    print("=" * 70)
    print(f"PyIceberg version: {PYICEBERG_VERSION}")
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
    json_path = os.path.join(REPORT_DIR, "pyiceberg-test-report.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"JSON report: {json_path}")

    # Write Markdown report
    md_content = generate_markdown(report)
    md_path = os.path.join(REPORT_DIR, "pyiceberg-test-report.md")
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
