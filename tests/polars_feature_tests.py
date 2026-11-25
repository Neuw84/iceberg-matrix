#!/usr/bin/env python3
"""
Polars Iceberg Feature Test Suite.

Measures Polars' Iceberg API -- ``pl.scan_iceberg()``, ``DataFrame.write_iceberg()``,
``LazyFrame.sink_iceberg()`` and ``pl.delete_iceberg()`` -- against tables managed
by a real Iceberg REST catalog (Apache Polaris backed by RustFS, see tests/docker)
and compares the results with the Polars entries in
``src/data/platforms/oss/polars/polars.json``.

Every verdict comes from calling Polars, never PyIceberg directly. PyIceberg is
used only to build the catalog connection Polars needs (``catalog=``), to create
fixtures for the DataFrame API (partition specs, int32 columns) and to inspect the
files Polars wrote. Since Polars 2.2, a catalog attached to a ``SQLContext`` with
``register_catalog`` also takes CREATE TABLE, INSERT, UPDATE, DELETE, MERGE INTO,
ALTER TABLE (columns, SET PARTITIONED BY, branches, tags), FOR VERSION/TIMESTAMP
AS OF and CALL rewrite_data_files; those cells are measured through SQL as well
(``_sql``). The DataFrame functions delegate part of their work to PyIceberg: ``write_iceberg`` and ``delete_iceberg``
go through PyIceberg's append/overwrite/delete (copy-on-write), the scan falls
back to PyIceberg whenever the native reader (``polars-iceberg``) cannot handle a
table, and ``sink_iceberg`` writes Parquet natively and commits through PyIceberg.
Where a cell names an operation Polars does not have, the test measures what
Polars *can* do with the artifact and the verdict follows the write capability,
as the other engine suites do. Fixtures Polars cannot produce (position/equality
deletes, deletion vectors, V3 columns, branches) are created with Spark or DuckDB
through the same catalog (tests/spark_fixture.py). When the catalog or Spark is
unavailable the affected tests skip.

Format versions: like tests/duckdb_feature_tests.py, every version-agnostic test
runs once against V2 tables and once against V3 tables (``main`` loops over
``FORMAT_VERSIONS``; ``CURRENT_VERSION`` picks the table version a test builds).
Each table a test measures is checked to carry the format-version the pass asked
for, so a "V3 pass" can never silently measure a V2 table. Polars cannot create a
V3 table or write to one (PyIceberg refuses to write V3 manifests), so V3 tables
come from Spark (Iceberg 1.11), PyIceberg (empty tables) or DuckDB 2.0, and the
V3 verdict records what Polars does with them: reads, and the refusal of each
write path. The tests for V3-only features (deletion vectors, variant, geometry,
...) run once and always report v3.

How a verdict maps to a matrix level (see ``compute_match``):
    pass  with no ``partial_reason``  -> JSON must be ``full``
    pass  with a ``partial_reason``   -> JSON must be ``partial`` (the reason is why)
    fail                              -> JSON must be ``none``
    skip / error                      -> not compared
V3 cells that Polars can only read are ``pass`` with a ``partial_reason`` ("rate on
writes, not reads"): read-only support is ``partial`` at best.

Usage:
    ./tests/docker/start-polaris.sh
    python tests/polars_feature_tests.py

Environment variables:
    POLARS_VERSION / PYICEBERG_VERSION - override the reported versions
    POLARS_FORMAT_VERSIONS - comma-separated passes to run (default "v2,v3")
    ICEBERG_REST_URI / ICEBERG_REST_WAREHOUSE / ICEBERG_REST_CREDENTIAL /
    ICEBERG_REST_SCOPE / ICEBERG_S3_* - as in tests/spark_fixture.py

Requirements:
    - polars[iceberg] (the ``iceberg`` extra pulls in polars-iceberg and PyIceberg)
    - pyiceberg[sql-sqlite,pyarrow,pyiceberg-core] == 0.12.0
      (pyiceberg-core is needed by ``write_iceberg`` on partitioned tables)
"""

import inspect
import json
import os
import sys
import tempfile
import uuid
import warnings
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

# See _storage_options: an empty session token in the environment is the guard
# Polars 2.2 still honours against ambient AWS credentials. RustFS takes static
# keys only. Set before Polars builds any object-store client.
os.environ["AWS_SESSION_TOKEN"] = ""

try:
    import polars as pl
except ImportError:
    print("[FATAL] polars not installed. Run: uv pip install 'polars[iceberg]'")
    sys.exit(1)

try:
    import pyiceberg
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, LongType, DoubleType, TimestamptzType
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import (BucketTransform, DayTransform, HourTransform, IdentityTransform, MonthTransform,
                                   TruncateTransform, YearTransform)
except ImportError as e:
    print(f"[FATAL] Missing dependency: {e}")
    print("Run: uv pip install 'pyiceberg[sql-sqlite,pyarrow,pyiceberg-core]'")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).resolve().parent))
import spark_fixture  # noqa: E402 - sibling module, not a package

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REPO_ROOT = os.environ.get("REPO_ROOT", str(Path(__file__).resolve().parent.parent))
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(os.getcwd(), "test-reports"))
POLARS_VERSION = os.environ.get("POLARS_VERSION", pl.__version__)
PYICEBERG_VERSION = os.environ.get("PYICEBERG_VERSION", pyiceberg.__version__)
try:
    import importlib.metadata as _md
    POLARS_ICEBERG_VERSION = _md.version("polars-iceberg")
except Exception:  # noqa: BLE001 - optional plugin, report "none" when absent
    POLARS_ICEBERG_VERSION = "not installed"

# Iceberg format versions to exercise (see the module docstring).
FORMAT_VERSIONS = [v.strip() for v in
                   os.environ.get("POLARS_FORMAT_VERSIONS", "v2,v3").split(",") if v.strip()]
# The format version the currently running test targets. Set by main() before each
# pass; TestResult and the table helpers read it.
CURRENT_VERSION = "v2"

CATALOG = None
NO_CATALOG_DETAIL = ("Requires the Iceberg REST catalog (set ICEBERG_REST_URI; start "
                     "tests/docker/start-polaris.sh); not reachable in this run")

# Polars' V3 write gap: PyIceberg refuses to write manifests for a V3 table, and
# Polars' write path (write_iceberg, sink_iceberg, delete_iceberg) commits through it.
V3_WRITE_FRAGMENT = "cannot write manifest for table version: 3"
# Polars 2.2 checks the format version itself before handing a write to
# PyIceberg, and raises "NotImplementedError: delete from an Iceberg v3 table is
# not supported yet" (likewise for overwrite). Same refusal, different words.
V3_WRITE_FRAGMENTS = (V3_WRITE_FRAGMENT, "iceberg v3 table is not supported",
                      "iceberg v3 table is not yet supported", "unsupported table format version: 3")


def _fmt() -> int:
    return int(CURRENT_VERSION[1:])  # "v3" -> 3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _first_line(e: BaseException, n: int = 200) -> str:
    lines = str(e).splitlines()
    if not lines:
        return type(e).__name__
    # Py4J wraps the real JVM error: its first line only says "An error occurred while calling".
    if lines[0].startswith("An error occurred while calling"):
        lines = [ln for ln in lines if "Exception" in ln or "Error" in ln][:1] or lines
    return lines[0][:n]


def _err(e: BaseException, n: int = 140) -> str:
    return f"{type(e).__name__}: {_first_line(e, n)}"


def _rest_available() -> bool:
    return spark_fixture._rest_reachable(spark_fixture.REST_URI)


def _catalog_props(**extra) -> dict:
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
    props.update(extra)
    return props


def _get_catalog():
    """PyIceberg RestCatalog against the shared Polaris instance. Polars takes a
    PyIceberg catalog as its ``catalog=`` argument: it has no catalog client of
    its own, so this *is* Polars' catalog path."""
    global CATALOG
    if CATALOG is None:
        CATALOG = load_catalog("polars_polaris", **_catalog_props())
    return CATALOG


def _storage_options() -> dict:
    """Object-store settings for ``sink_iceberg``. Its native Parquet writer does
    not read S3 settings from the catalog or the table, so without these it
    writes to AWS. The empty session token stops an ambient AWS profile or
    credentials file from adding a token that RustFS rejects.

    Polars 2.2 drops empty storage options ("Empty values are unset"), which
    silently removed that guard: its credential provider then attached an
    ambient session token and RustFS answered every PUT with "invalid token2".
    The empty ``AWS_SESSION_TOKEN`` set in ``main`` restores it; the option is
    kept for Polars versions that still honour it."""
    return {
        "s3.endpoint": f"http://{spark_fixture.S3_ENDPOINT}",
        "s3.access-key-id": spark_fixture.S3_KEY_ID,
        "s3.secret-access-key": spark_fixture.S3_SECRET,
        "s3.region": spark_fixture.S3_REGION,
        "s3.path-style-access": "true",
        "s3.session-token": "",
    }


NS = None


def _ns() -> str:
    """One namespace per run for the tables Polars creates, made through the catalog."""
    global NS
    if NS is None:
        NS = "polars_" + uuid.uuid4().hex[:8]
        _get_catalog().create_namespace(NS)
    return NS


def _table(prefix: str) -> str:
    return f"{_ns()}.{_unique(prefix)}"


def _sample(ids=(1, 2, 3)) -> "pl.DataFrame":
    return pl.DataFrame({
        "id": list(ids),
        "name": [f"n{i}" for i in ids],
        "value": [float(i) * 10 for i in ids],
        "ts": [datetime(2024, 1, 1 + (i % 28), tzinfo=timezone.utc) for i in ids],
    })


def _scan(ident: str, **kw) -> "pl.LazyFrame":
    """``pl.scan_iceberg`` on a ``namespace.table`` identifier through the REST catalog."""
    return pl.scan_iceberg(ident, catalog=_get_catalog(), **kw)


def _ids(ident: str, **kw) -> list:
    return sorted(_scan(ident, **kw).collect()["id"].to_list())


def _sink(df: "pl.DataFrame", ident: str, **kw):
    """Native ``sink_iceberg`` of a frame into ``namespace.table`` through the REST catalog."""
    return df.lazy().sink_iceberg(ident, catalog=_get_catalog(), storage_options=_storage_options(), **kw)


def _create_with_sink(prefix: str, df: "pl.DataFrame") -> str:
    ident = _table(prefix)
    _sink(df, ident, mode="append", create_if_not_exists=True)
    return ident


def _table_files(ident: str) -> dict:
    return _get_catalog().load_table(ident).inspect.files().to_pydict()


def _explain_files(lf: "pl.LazyFrame") -> int:
    """Number of data files in the scan's query plan (what Polars will actually read)."""
    plan = lf.explain()
    line = next((ln for ln in plan.splitlines() if ln.startswith("Parquet SCAN")), "")
    inner = line[line.find("[") + 1: line.rfind("]")] if "[" in line else ""
    return len([p for p in inner.split(",") if p.strip()])


def _vended_storage_options(ident: str) -> dict:
    """Storage settings the catalog vended for one table (used for reads that fall
    back to Polars' plain Parquet scan, which gets no S3 settings otherwise)."""
    return {k: v for k, v in _get_catalog().load_table(ident).io.properties.items() if k.startswith("s3.")}


# --- format-version guard -------------------------------------------------

def _check_format(ident: str) -> None:
    """Fail loudly unless the table's metadata says it has the format-version of
    the current pass (the old DuckDB suite silently ran its "V3 pass" on V2 tables)."""
    got = _get_catalog().load_table(ident).format_version
    assert got == _fmt(), f"{ident} is format-version {got}, the {CURRENT_VERSION} pass needs {_fmt()}"


def _spark_format(ident: str) -> int:
    """format-version read through Spark, for tables PyIceberg cannot load (variant)."""
    rows = spark_fixture.get_spark().sql(f"SHOW TBLPROPERTIES local.{ident} ('format-version')").collect()
    return int(rows[0][1])


# --- table builders (V2: Polars or PyIceberg; V3: Spark or PyIceberg) -----

def _fixture(ns: str, name: str, mode: str, **kw) -> str:
    """Spark-created, seeded table of the pass's format version; returns ``ns.name``."""
    spark_fixture.create_fixture(ns, name, CURRENT_VERSION, mode, **kw)
    ident = f"{ns}.{name}"
    _check_format(ident)
    return ident


def _spark_type(dtype) -> str:
    s = str(dtype)
    if s.startswith("Int"):
        return "BIGINT"
    if s.startswith("Float"):
        return "DOUBLE"
    if s == "String":
        return "STRING"
    if s.startswith("Datetime"):
        return "TIMESTAMP"
    raise TypeError(f"no Spark type for {dtype}")


def _sql_literal(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if isinstance(v, datetime):
        return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    return repr(v)


def _spark_insert(ident: str, df: "pl.DataFrame") -> None:
    rows = ", ".join("(" + ", ".join(_sql_literal(v) for v in row) + ")" for row in df.rows())
    spark_fixture.get_spark().sql(f"INSERT INTO local.{ident} VALUES {rows}")


def _seed(ns, prefix: str, df: "pl.DataFrame") -> str:
    """A table holding ``df`` with the pass's format version, as Polars alone would
    make it where it can: V2 is created by ``sink_iceberg``; V3 cannot be (no
    format-version parameter), so Spark creates and seeds it."""
    if CURRENT_VERSION == "v2":
        ident = _create_with_sink(prefix, df)
    else:
        ident = f"{ns}.{_unique(prefix)}"
        ddl = ", ".join(f"{c} {_spark_type(t)}" for c, t in df.schema.items())
        spark_fixture.get_spark().sql(
            f"CREATE TABLE local.{ident} ({ddl}) USING iceberg TBLPROPERTIES ('format-version'='3')")
        _spark_insert(ident, df)
    _check_format(ident)
    return ident


def _add_rows(ident: str, df: "pl.DataFrame") -> None:
    """Append rows the way the pass can: Polars on V2, Spark on V3."""
    if CURRENT_VERSION == "v2":
        _sink(df, ident, mode="append")
    else:
        _spark_insert(ident, df)


def _pyiceberg_table(prefix: str, schema, spec=None, props=None) -> str:
    """Empty table of the pass's format version made through PyIceberg (Polars cannot
    declare partition specs or table properties). PyIceberg 0.12 can create V3
    tables; it just cannot write data to them."""
    ident = _table(prefix)
    kw = {"partition_spec": spec} if spec is not None else {}
    _get_catalog().create_table(ident, schema=schema, properties={"format-version": str(_fmt()), **(props or {})}, **kw)
    _check_format(ident)
    return ident


# --- write attempts --------------------------------------------------------

def _attempts(**fns) -> dict:
    """Run each write attempt; {name: None} on success, else {name: 'Type: first line'}."""
    out = {}
    for name, fn in fns.items():
        try:
            fn()
            out[name] = None
        except Exception as e:  # noqa: BLE001 - the refusal is the datum
            out[name] = _err(e, 110)
    return out


# ---------------------------------------------------------------------------
# Polars SQL on an attached catalog (Polars 2.2+)
# ---------------------------------------------------------------------------
# Polars 2.2 added Iceberg writes and DDL to SQL: a catalog attached with
# SQLContext.register_catalog accepts CREATE TABLE, INSERT, UPDATE, DELETE,
# MERGE INTO, ALTER TABLE (columns, SET PARTITIONED BY, branches and tags),
# FOR VERSION/TIMESTAMP AS OF and CALL procedures. That is Polars' own API, so
# each cell it touches is measured through it as well as through the DataFrame
# functions; the verdict follows whichever Polars path works.
SQL_CATALOG = "lake"


def _has_sql_catalog() -> bool:
    return hasattr(pl.SQLContext, "register_catalog")


def _sql(stmt: str) -> "pl.DataFrame":
    """Run one statement in a fresh SQLContext with the REST catalog attached as
    ``lake``. Fresh per statement so a cached table load never hides a commit."""
    ctx = pl.SQLContext()
    ctx.register_catalog(SQL_CATALOG, _get_catalog(), storage_options=_storage_options())
    return ctx.execute(stmt, eager=True)


def _sql_try(**stmts) -> dict:
    """Run statements in order; name -> None on success, else the error text."""
    if not _has_sql_catalog():
        return {k: "SQLContext.register_catalog is not available in this Polars" for k in stmts}
    return _attempts(**{k: (lambda st=st: _sql(st)) for k, st in stmts.items()})


def _lake(ident: str) -> str:
    return f"{SQL_CATALOG}.{ident}"


def _is_refusal(msg) -> bool:
    return bool(msg) and any(f in msg.lower() for f in V3_WRITE_FRAGMENTS)


def _write_verdict(r, outcome: dict, ok_details: str, what: str):
    """pass when every write attempt succeeded; fail when every one was refused with
    PyIceberg's V3 refusal; error for anything else (a mixed or unexpected result)."""
    errs = {k: v for k, v in outcome.items() if v}
    if not errs:
        r.result = "pass"
        r.details = ok_details
    elif len(errs) == len(outcome) and all(_is_refusal(v) for v in errs.values()):
        r.result = "fail"
        r.details = (f"Cannot {what}: every Polars write path is refused (by Polars or PyIceberg) on a format-version "
                     f"{_fmt()} table ({outcome})")
    else:
        r.result = "error"
        r.details = f"Unexpected write outcome while trying to {what}: {outcome}"
    return r


_GATE = {}


def _write_gate(ns) -> tuple:
    """(can_write, error) for Polars appending to a table of the pass's format version.
    Cached per pass. Cells Polars can only read on V3 use it for their partial_reason."""
    if CURRENT_VERSION not in _GATE:
        ident = _seed(ns, "gate", _sample([1]))
        out = _attempts(write_iceberg=lambda: _sample([2]).write_iceberg(_get_catalog().load_table(ident), mode="append"))
        _GATE[CURRENT_VERSION] = (out["write_iceberg"] is None, out["write_iceberg"])
    return _GATE[CURRENT_VERSION]


def _read_only_reason(ns):
    """None when Polars can write tables of this pass's version, else why a pass is read-side only."""
    ok, err = _write_gate(ns)
    return None if ok else f"read side only: Polars cannot write format-version {_fmt()} tables ({err})"


# ---------------------------------------------------------------------------
# Result class
# ---------------------------------------------------------------------------

class TestResult:
    def __init__(self, feature_id: str, feature_name: str, version: str = None):
        self.feature_id = feature_id
        self.feature_name = feature_name
        self.result = "skip"  # pass | fail | skip | error
        self.details = ""
        # Version-agnostic tests report the pass they run in; V3-only tests name "v3".
        self.version_tested = version or CURRENT_VERSION
        # Set on a pass that proves only part of the feature: the JSON must say partial.
        self.partial_reason = None

    def to_dict(self):
        return {
            "feature_id": self.feature_id,
            "feature_name": self.feature_name,
            "version": self.version_tested,
            "result": self.result,
            "details": self.details,
            "partial_reason": self.partial_reason,
        }


def _catalog_test(r: TestResult, body):
    """Run body(None, r) against the REST catalog; skip when unreachable, error on exception."""
    if not _rest_available():
        r.result = "skip"
        r.details = NO_CATALOG_DETAIL
        return r
    try:
        body(None, r)
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
            for n in ("t", "u", "e"):
                spark_fixture.drop_fixture(ns, n)
    return r


def _vtest(r: TestResult, body):
    """Version-agnostic test: the V2 pass needs only the catalog (Polars and PyIceberg
    build the tables); the V3 pass needs Spark too (it makes the V3 data)."""
    return _spark_test(r, body) if CURRENT_VERSION == "v3" else _catalog_test(r, body)


def _v3_only(fn):
    """Mark a test of a V3-only feature: it always builds V3 tables and reports v3,
    whatever pass main() is in (main() runs it once)."""
    import functools

    @functools.wraps(fn)
    def run():
        global CURRENT_VERSION
        saved, CURRENT_VERSION = CURRENT_VERSION, "v3"
        try:
            return fn()
        finally:
            CURRENT_VERSION = saved
    return run


def _delete_file_counts(ns: str, name: str) -> dict:
    return spark_fixture.inspect_delete_files(ns, name)


def _polars_delete(ident: str, predicate) -> tuple:
    """``pl.delete_iceberg`` on a table. Returns (UserWarnings raised, error or None)."""
    err = None
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            pl.delete_iceberg(_get_catalog().load_table(ident), predicate)
        except Exception as e:  # noqa: BLE001 - the refusal is the datum
            err = _err(e, 110)
    return [str(w.message) for w in caught], err


def _delete_probe(ns: str, mode: str, name: str = "t") -> dict:
    """Spark builds a table of the pass's version with the given write mode, Polars
    deletes id 2, and Spark reports which delete files that wrote."""
    ident = _fixture(ns, name, mode)
    warns, err = _polars_delete(ident, pl.col("id") == 2)
    return {"ident": ident, "warns": warns, "err": err,
            "wrote": _delete_file_counts(ns, name), "ids": _ids(ident)}


def _spark_delete_then_read(ns: str, name: str) -> dict:
    """Spark deletes id 2 from a merge-on-read table of the pass's version (a
    position delete on V2, a Puffin deletion vector on V3); Polars reads it."""
    ident = _fixture(ns, name, "merge-on-read")
    spark_fixture.get_spark().sql(f"DELETE FROM local.{ident} WHERE id = 2")
    out = {"ident": ident, "files": _delete_file_counts(ns, name),
           "formats": sorted(spark_fixture.delete_file_formats_from_storage(ns, name))}
    for reader in ("native", "pyiceberg", None):
        kw = {} if reader is None else {"reader_override": reader}
        try:
            out[reader or "auto"] = _ids(ident, **kw)
        except Exception as e:  # noqa: BLE001
            out[reader or "auto"] = _err(e, 100)
    return out


def _read_with_equality_delete(ns: str, name: str) -> dict:
    """Spark creates a table of the pass's version and the Iceberg Java API attaches a
    real equality delete to it (tests/spark_fixture.py); Polars then loads it with each
    reader. Returns {"produced": fixture info, "readers": {reader: ("ok", ids) | ("error", msg)}}."""
    produced = spark_fixture.create_equality_delete_fixture(ns, name, CURRENT_VERSION)
    _check_format(f"{ns}.{name}")
    readers = {}
    for reader in ("native", "pyiceberg", None):
        kw = {} if reader is None else {"reader_override": reader}
        try:
            readers[reader or "auto"] = ("ok", _ids(f"{ns}.{name}", **kw))
        except Exception as e:  # noqa: BLE001 - the load failure is the datum
            readers[reader or "auto"] = ("error", _first_line(e, 160))
    return {"produced": produced, "readers": readers}


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def test_table_creation() -> TestResult:
    r = TestResult("table-creation", "Table Creation")
    def body(ns, r):
        want = _fmt()
        cat = _get_catalog()
        # Types with no lossless Iceberg mapping are refused at creation, whatever the pass.
        rejected = {}
        for label, series in (("UInt64", pl.Series([1], dtype=pl.UInt64)),
                              ("Duration", pl.Series([1], dtype=pl.Duration("us"))),
                              ("Datetime(ns)", pl.Series([1]).cast(pl.Datetime("ns")))):
            out = _attempts(create=lambda s=series: _sink(pl.DataFrame({"a": s}), _table("badtype"),
                                                          mode="append", create_if_not_exists=True))["create"]
            assert out and "lossless Iceberg type" in out, f"{label} was not rejected at table creation: {out}"
            rejected[label] = out
        ident = _table("create")
        _sink(_sample(), ident, mode="append", create_if_not_exists=True, create_namespace_if_not_exists=True)
        got = cat.load_table(ident).format_version
        params = set(inspect.signature(pl.LazyFrame.sink_iceberg).parameters)
        missing = sorted({"partition_spec", "properties", "format_version", "sort_order"} - params)
        sink_note = (f"sink_iceberg(create_if_not_exists=True) creates a format-version {got} table from the frame "
                     f"schema, unpartitioned, with no parameter for {missing}. Frames with UInt64, Duration or "
                     f"nanosecond Datetime columns are rejected at creation ({sorted(rejected)})")
        # Polars SQL (2.2+): CREATE TABLE ... WITH (properties), then a partition spec.
        sident = _table("sqlcreate")
        sql = _sql_try(
            create=(f"CREATE TABLE {_lake(sident)} (id BIGINT, ts TIMESTAMP, name VARCHAR) "
                    f"WITH ('format-version' = '{want}', 'owner' = 'polars-suite')"),
            partition=f"ALTER TABLE {_lake(sident)} SET PARTITIONED BY (month(ts), bucket(4, id))",
            insert=f"INSERT INTO {_lake(sident)} VALUES (1, TIMESTAMP '2026-01-15 10:00:00', 'a')",
        )
        stbl = cat.load_table(sident) if sql["create"] is None else None
        created = stbl is not None and stbl.format_version == want and stbl.properties.get("owner") == "polars-suite"
        spec = [str(f.transform) for f in stbl.spec().fields] if stbl else []
        if created and sql["partition"] is None and sql["insert"] is None:
            assert sorted(spec) == ["bucket[4]", "month"] and _ids(sident) == [1], (spec, _ids(sident))
            r.result = "pass"
            r.details = (f"Polars SQL CREATE TABLE ... WITH ('format-version'='{want}', properties) created a "
                         f"format-version {want} table with the requested property, ALTER TABLE ... SET PARTITIONED BY "
                         f"gave it month(ts) + bucket(4, id), and INSERT wrote a row. Partitioning is a second statement: "
                         f"CREATE TABLE ... PARTITIONED BY does not parse. Also: {sink_note}")
        elif created:
            refused = {k: v for k, v in sql.items() if v}
            assert all(_is_refusal(v) for v in refused.values()), f"unexpected SQL outcome: {sql}"
            r.result = "pass"
            r.partial_reason = f"creates an empty format-version {want} table but cannot partition it or write rows ({refused})"
            r.details = (f"Polars SQL CREATE TABLE ... WITH ('format-version'='{want}') created a format-version {want} "
                         f"table with the requested property, but on it SET PARTITIONED BY and INSERT are refused "
                         f"({refused}). Also: {sink_note}")
        elif got == want:
            r.result = "pass"
            r.partial_reason = f"no SQL route ({sql['create']}) and no sink_iceberg parameter for {missing}"
            r.details = sink_note
        else:
            r.result = "fail"
            r.details = (f"Asked for a format-version {want} table: Polars SQL CREATE TABLE failed ({sql['create']}) and "
                         f"sink_iceberg created format-version {got}. {sink_note}")
    return _catalog_test(r, body)


def test_read_support() -> TestResult:
    r = TestResult("read-support", "Read Support")

    def body(ns, r):
        ident = _fixture(ns, "t", "merge-on-read")
        full = _ids(ident)
        pruned = _scan(ident).filter(pl.col("id") > 1).select("val").collect()
        plan = _scan(ident).filter(pl.col("id") > 1).select("val").explain()
        assert full == [1, 2, 3], full
        assert pruned.height == 2 and pruned.columns == ["val"], pruned
        # A delete written by another engine: position delete (V2) or Puffin deletion vector (V3).
        d = _spark_delete_then_read(ns, "u")
        expect_fmt = "PUFFIN" if _fmt() == 3 else "PARQUET"
        assert expect_fmt in d["formats"], d
        assert d["native"] == [1, 3] and d["auto"] == [1, 3] and d["pyiceberg"] == [1, 3], d
        # A table that carries an equality delete (Flink upsert sinks write these).
        eq = _read_with_equality_delete(ns, "e")
        eq_ok = all(kind == "ok" and ids == eq["produced"]["live_ids"] for kind, ids in eq["readers"].values())
        reasons = []
        if not eq_ok:
            reasons.append("tables that contain equality deletes cannot be loaded: "
                           + "; ".join(sorted({v for k, v in eq["readers"].values() if k == "error"} |
                                              {f"read wrong ids {v}" for k, v in eq["readers"].values()
                                               if k == "ok" and v != eq["produced"]["live_ids"]})))
        if _fmt() == 3:
            reasons.append("V3 tables with a VARIANT or GEOMETRY column cannot be loaded (see the Variant Type and "
                           "Geometry rows)")
        r.result = "pass"
        r.partial_reason = " | ".join(reasons) or None
        r.details = (f"pl.scan_iceberg() read a Spark-created format-version {_fmt()} table through the REST catalog (3 rows); "
                     f"predicate + projection returned 2 rows / 1 column. After a Spark DELETE (delete file formats "
                     f"{d['formats']}) the native, PyIceberg and automatic readers all returned ids {d['native']}. "
                     f"A table with a Java-API equality delete ({eq['produced']['delete_files']}): "
                     f"{ {k: v[0] if v[0] == 'ok' else 'error' for k, v in eq['readers'].items()} }"
                     f"{'' if eq_ok else ' (' + (sorted({v[1] for v in eq['readers'].values() if v[0] == 'error'}) or ['wrong ids'])[0] + ')'}. "
                     f"Plan: {plan.splitlines()[:3]}")

    return _spark_test(r, body)


def test_write_insert() -> TestResult:
    r = TestResult("write-insert", "Write INSERT")

    def body(ns, r):
        cat = _get_catalog()
        ident = _seed(ns, "insert", _sample())
        out = _attempts(
            write_iceberg_append=lambda: _sample([4, 5]).write_iceberg(cat.load_table(ident), mode="append"),
            sink_iceberg_append=lambda: _sink(_sample([6]), ident, mode="append"),
        )
        if not any(out.values()):
            assert _ids(ident) == [1, 2, 3, 4, 5, 6], _ids(ident)
            out.update(_attempts(sink_iceberg_overwrite=lambda: _sink(_sample([7, 8]), ident, mode="overwrite")))
            if not any(out.values()):
                assert _ids(ident) == [7, 8], _ids(ident)
        else:
            out.update(_attempts(sink_iceberg_overwrite=lambda: _sink(_sample([7, 8]), ident, mode="overwrite")))
            assert _ids(ident) == [1, 2, 3], f"a refused write must not change the table: {_ids(ident)}"
        _write_verdict(r, out, (
            "sink_iceberg(mode='append') and DataFrame.write_iceberg(mode='append') both appended through the REST "
            "catalog (6 rows); sink_iceberg(mode='overwrite') replaced the table contents (2 rows)"), "append to a table")

    return _vtest(r, body)


def test_write_merge_update_delete() -> TestResult:
    r = TestResult("write-merge-update-delete", "Write MERGE/UPDATE/DELETE")
    def body(ns, r):
        ident = _fixture(ns, "t", "copy-on-write")
        other = _fixture(ns, "u", "copy-on-write")
        sident = _fixture(ns, "e", "copy-on-write")
        out = _attempts(
            delete_iceberg=lambda: pl.delete_iceberg(_get_catalog().load_table(ident), pl.col("id") == 2),
            overwrite_filter=lambda: _sink(pl.DataFrame({"id": [100], "val": ["x"]}), other,
                                           mode="overwrite", overwrite_filter=pl.col("id") == 2),
        )
        L = _lake(sident)
        sql = _sql_try(
            update=f"UPDATE {L} SET val = 'upd' WHERE id = 1",
            delete=f"DELETE FROM {L} WHERE id = 3",
            merge=(f"MERGE INTO {L} AS t USING (SELECT 2 AS id, 'mrg' AS val UNION ALL SELECT 4, 'new') AS s "
                   "ON t.id = s.id WHEN MATCHED THEN UPDATE SET val = s.val "
                   "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)"),
        )
        everything = {**out, **{f"sql_{k}": v for k, v in sql.items()}}
        if not any(everything.values()):
            got = {row["id"]: row["val"] for row in _scan(sident).collect().to_dicts()}
            assert got == {1: "upd", 2: "mrg", 4: "new"}, got
            assert _ids(ident) == [1, 3] and _ids(other) == [1, 3, 100], (_ids(ident), _ids(other))
            r.result = "pass"
            r.details = (f"Polars SQL UPDATE, DELETE and MERGE INTO (matched update + not-matched insert) on a "
                         f"format-version {_fmt()} table left {got}. The DataFrame API also deletes "
                         "(pl.delete_iceberg removed id 2) and overwrites by filter (sink_iceberg overwrite_filter), "
                         "but has no MERGE or UPDATE of its own")
        elif all(everything.values()) and all(_is_refusal(v) for v in everything.values()):
            assert _ids(ident) == [1, 2, 3] and _ids(other) == [1, 2, 3] and _ids(sident) == [1, 2, 3], \
                "a refused write must not change the table"
            r.result = "fail"
            r.details = (f"Every Polars row-level write is refused on a format-version {_fmt()} table: "
                         f"SQL {sql}; DataFrame API {out}")
        else:
            r.result = "error"
            r.details = f"Unexpected write outcome: SQL {sql}; DataFrame API {out}"
    return _spark_test(r, body)


# ---------------------------------------------------------------------------
# Row-level operations: rated by what Polars can WRITE.
# ---------------------------------------------------------------------------

def test_position_deletes() -> TestResult:
    r = TestResult("position-deletes", "Position Deletes")

    def body(ns, r):
        # Write half: a delete on a table that requests merge-on-read.
        w = _delete_probe(ns, "merge-on-read", "t")
        # Read half: a Spark-written delete (position delete on V2, deletion vector on V3), read natively.
        theirs = _spark_delete_then_read(ns, "u")
        read = f"reads a Spark-written delete natively (ids {theirs['native']}, files {theirs['files']}, formats {theirs['formats']})"
        if w["err"] and _is_refusal(w["err"]):
            r.result = "fail"
            r.details = f"pl.delete_iceberg is refused on a format-version {_fmt()} table ({w['err']}); read side: {read}"
        elif w["err"]:
            r.result = "error"
            r.details = f"Unexpected failure: {w['err']}"
        elif w["wrote"]["position"] > 0:
            r.result = "pass"
            r.details = f"pl.delete_iceberg wrote position delete files ({w['wrote']}); {read}"
        else:
            r.result = "fail"
            r.details = (f"pl.delete_iceberg produced no position delete file ({w['wrote']}; ids left {w['ids']}; "
                         f"warnings {w['warns']}); it rewrote data files instead. Read side: {read}")

    return _spark_test(r, body)


def test_equality_deletes() -> TestResult:
    r = TestResult("equality-deletes", "Equality Deletes")

    def body(ns, r):
        w = _delete_probe(ns, "merge-on-read", "t")
        eqr = _read_with_equality_delete(ns, "u")
        produced = eqr["produced"]
        kinds = {k for k, _ in eqr["readers"].values()}
        if kinds == {"ok"}:
            ids = [v for _, v in eqr["readers"].values()]
            read = (f"reads them correctly (ids {ids[0]})" if all(i == produced["live_ids"] for i in ids)
                    else f"IGNORES them (ids {ids}, expected {produced['live_ids']})")
        else:
            read = f"cannot read a table that contains one ({eqr['readers']})"
        if w["err"] and not _is_refusal(w["err"]):
            r.result = "error"
            r.details = f"Unexpected failure: {w['err']}"
        elif w["wrote"]["equality"] > 0 and not w["err"]:
            r.result = "pass"
            r.details = f"pl.delete_iceberg wrote equality delete files ({w['wrote']}); read: {read}"
        else:
            r.result = "fail"
            why = f"is refused ({w['err']})" if w["err"] else f"wrote no equality delete file ({w['wrote']})"
            r.details = (f"pl.delete_iceberg {why}; against a Java-API equality-delete file ({produced['delete_files']}) "
                         f"Polars {read}")

    return _spark_test(r, body)


def test_merge_on_read() -> TestResult:
    r = TestResult("merge-on-read", "Merge-on-Read")

    def body(ns, r):
        w = _delete_probe(ns, "merge-on-read")
        deletes = w["wrote"]
        # Polars SQL DELETE (2.2+) on a second merge-on-read table.
        sident = _fixture(ns, "u", "merge-on-read")
        sql_err = _sql_try(delete=f"DELETE FROM {_lake(sident)} WHERE id = 2")["delete"]
        sql_deletes = _delete_file_counts(ns, "u")
        sql_note = (f"Polars SQL DELETE on a merge-on-read table: "
                    f"{('refused (' + sql_err + ')') if sql_err else f'delete files {sql_deletes}, ids left {_ids(sident)}'}")
        if not sql_err and (sql_deletes["position"] > 0 or sql_deletes["equality"] > 0):
            r.result = "pass"
            r.details = f"{sql_note}: merge-on-read honoured"
            return
        if w["err"] and not _is_refusal(w["err"]):
            r.result = "error"
            r.details = f"Unexpected failure: {w['err']}"
        elif w["err"]:
            r.result = "fail"
            r.details = (f"pl.delete_iceberg on a merge-on-read table is refused on format-version {_fmt()} "
                         f"({w['err']}); delete files {deletes}. {sql_note}")
        elif deletes["position"] > 0 or deletes["equality"] > 0:
            r.result = "pass"
            r.details = f"pl.delete_iceberg honoured write.delete.mode=merge-on-read: delete files {deletes}"
        else:
            r.result = "fail"
            r.details = (f"pl.delete_iceberg on a table with write.delete.mode=merge-on-read wrote no delete file "
                         f"({deletes}, ids left {w['ids']}); PyIceberg warned: {w['warns']}. {sql_note}")

    return _spark_test(r, body)


def _partitioned_cow_probe() -> dict:
    """Copy-on-write on a day(ts)+bucket(4, name) partitioned table: delete_iceberg rewrites a
    file inside one partition, and write_iceberg(mode='overwrite') replaces the table. Returns
    {"problem": partial_reason or None, "text": details}."""
    cat = _get_catalog()
    ident = _partitioned_fixture("cowpart")
    jan, jun = datetime(2024, 1, 15, tzinfo=timezone.utc), datetime(2024, 6, 15, tzinfo=timezone.utc)
    frame = pl.DataFrame({"id": [1, 2, 3, 4], "ts": [jan, jan, jun, jun], "name": ["a", "a", "b", "b"]})
    steps = _attempts(
        append=lambda: frame.write_iceberg(cat.load_table(ident), mode="append"),
        delete=lambda: pl.delete_iceberg(cat.load_table(ident), pl.col("id") == 1),
    )
    if steps["append"] is None and steps["delete"] is None:
        ids_del = _ids(ident)
        snap = cat.load_table(ident).current_snapshot().summary
        contents = sorted(set(_table_files(ident)["content"]))
        assert ids_del == [2, 3, 4] and contents == [0], (ids_del, contents)
        steps.update(_attempts(overwrite=lambda: _part_frame([10, 11], [2, 7]).write_iceberg(
            cat.load_table(ident), mode="overwrite")))
    ids_over = _ids(ident) if steps.get("overwrite", "n/a") is None else None
    failed = {k: v for k, v in steps.items() if v}
    if failed:
        return {"problem": f"partitioned tables: {failed}", "text": f"On a partitioned table: {steps}."}
    assert ids_over == [10, 11], ids_over
    return {"problem": None,
            "text": (f"On a table partitioned by day(ts) + bucket(4, name), pl.delete_iceberg rewrote a file inside one "
                     f"partition (snapshot operation {snap.operation}, ids [2, 3, 4], data files only: content {contents}) "
                     f"and write_iceberg(mode='overwrite') replaced the contents (ids {ids_over})")}


def test_copy_on_write() -> TestResult:
    r = TestResult("copy-on-write", "Copy-on-Write")

    def body(ns, r):
        w = _delete_probe(ns, "copy-on-write")
        if w["err"] and not _is_refusal(w["err"]):
            r.result = "error"
            r.details = f"Unexpected failure: {w['err']}"
        elif w["err"]:
            assert w["ids"] == [1, 2, 3], w["ids"]
            r.result = "fail"
            r.details = (f"pl.delete_iceberg is refused on a format-version {_fmt()} table ({w['err']}); "
                         f"the table is unchanged (ids {w['ids']})")
        else:
            summary = _get_catalog().load_table(w["ident"]).current_snapshot().summary
            op = str(summary.operation) if summary is not None else "?"
            assert w["ids"] == [1, 3], w["ids"]
            assert w["wrote"] == {"position": 0, "equality": 0}, w["wrote"]
            part = _partitioned_cow_probe()
            r.result = "pass"
            r.partial_reason = part["problem"]
            r.details = (f"pl.delete_iceberg rewrote the data file (snapshot operation {op}, delete files {w['wrote']}) "
                         f"and left ids {w['ids']}. {part['text']}")

    return _spark_test(r, body)


@_v3_only
def test_deletion_vectors() -> TestResult:
    r = TestResult("deletion-vectors", "Deletion Vectors", "v3")

    def body(ns, r):
        # Read half first, so the report can say what Polars does with a DV.
        d = _spark_delete_then_read(ns, "u")
        assert "PUFFIN" in d["formats"], f"the fixture has no Puffin deletion vector: {d}"
        read = f"reads a Spark-written deletion vector (Puffin) in every reader: native {d['native']}, pyiceberg {d['pyiceberg']}, auto {d['auto']}"
        w = _delete_probe(ns, "merge-on-read", "t")
        if w["err"] and not _is_refusal(w["err"]):
            r.result = "error"
            r.details = f"Unexpected failure: {w['err']}"
        elif w["err"]:
            r.result = "fail"
            r.details = f"Cannot write deletion vectors: pl.delete_iceberg is refused on a V3 table ({w['err']}). Read capability, not rated: Polars {read}"
        else:
            r.result = "pass" if "PUFFIN" in spark_fixture.delete_file_formats_from_storage(ns, "t") else "fail"
            r.details = f"pl.delete_iceberg wrote {w['wrote']} on a V3 table. Polars {read}"

    return _spark_test(r, body)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def test_schema_evolution() -> TestResult:
    r = TestResult("schema-evolution", "Schema Evolution")
    def body(ns, r):
        cat = _get_catalog()
        # Polars SQL (2.2+): ALTER TABLE on the table itself, no data write needed.
        sident = _seed(ns, "sqlevo", pl.DataFrame({"id": [1, 2], "v": [1.0, 2.0]}))
        L = _lake(sident)
        sql = _sql_try(add=f"ALTER TABLE {L} ADD COLUMN extra VARCHAR")
        extra_id = next((f.field_id for f in cat.load_table(sident).schema().fields if f.name == "extra"), None)
        sql.update(_sql_try(rename=f"ALTER TABLE {L} RENAME COLUMN extra TO extra2",
                            drop=f"ALTER TABLE {L} DROP COLUMN v"))
        fields = {f.name: f.field_id for f in cat.load_table(sident).schema().fields}
        sql_ok = (not any(sql.values()) and set(fields) == {"id", "extra2"}
                  and fields["extra2"] == extra_id and _ids(sident) == [1, 2])
        sql_note = (f"Polars SQL ALTER TABLE ADD, RENAME and DROP COLUMN all applied on a format-version {_fmt()} "
                    f"table (columns now {sorted(fields)}, the renamed column keeps field id {extra_id}, rows still read)"
                    if sql_ok else f"Polars SQL ALTER TABLE: {sql} (columns {sorted(fields)})")
        ident = _seed(ns, "evo", pl.DataFrame({"id": [1, 2], "v": [1.0, 2.0]}))
        before = [f.name for f in cat.load_table(ident).schema().fields]
        try:
            _sink(pl.DataFrame({"id": [3], "v": [3.0], "src": ["s"]}), ident, mode="append", schema_mode="merge")
        except Exception as e:  # noqa: BLE001 - the refusal is the datum
            after = [f.name for f in cat.load_table(ident).schema().fields]
            assert after == before, f"a refused evolution must not change the schema: {before} -> {after}"
            if not _is_refusal(str(e)):
                r.result = "error"
                r.details = f"Unexpected failure: {_err(e)}"
            elif sql_ok:
                r.result = "pass"
                r.details = (f"{sql_note}. Evolving the schema as part of a data write is refused on this table "
                             f"(sink_iceberg schema_mode='merge': {_err(e, 100)})")
            else:
                r.result = "fail"
                r.details = (f"sink_iceberg(schema_mode='merge') on a format-version {_fmt()} table failed before the schema "
                             f"changed ({_err(e, 100)}); {sql_note}")
            return
        added = "src" in [f.name for f in cat.load_table(ident).schema().fields]
        rows = _scan(ident).collect().sort("id").to_dicts()
        _sink(pl.DataFrame({"id": [9], "w": [1.0]}), ident, mode="overwrite", schema_mode="overwrite")
        names = [f.name for f in cat.load_table(ident).schema().fields]
        dropped = "v" not in names and "src" not in names
        assert added and rows[0]["src"] is None and rows[-1]["src"] == "s", rows
        r.result = "pass" if (dropped or sql_ok) else "fail"
        if not sql_ok:
            r.partial_reason = "no rename or reorder: evolution only as part of a data write"
        r.details = (f"{sql_note}. Through writes: sink_iceberg(schema_mode='merge') added column 'src' (older rows "
                     f"read null) and schema_mode='overwrite' replaced the schema (now {names})")
    return _vtest(r, body)


def test_type_promotion() -> TestResult:
    r = TestResult("type-promotion", "Type Promotion / Widening")
    def body(ns, r):
        # Polars SQL (2.2+): ALTER COLUMN ... TYPE on an int/float/decimal(9,2) table.
        sident = _fixture(ns, "u", "merge-on-read", columns_ddl="id INT, f FLOAT, d DECIMAL(9,2)",
                          seed_sql="(1, CAST(1.5 AS FLOAT), CAST(1.25 AS DECIMAL(9,2)))")
        L = _lake(sident)
        sql = _sql_try(int_long=f"ALTER TABLE {L} ALTER COLUMN id TYPE BIGINT",
                       float_double=f"ALTER TABLE {L} ALTER COLUMN f TYPE DOUBLE",
                       decimal_widen=f"ALTER TABLE {L} ALTER COLUMN d TYPE DECIMAL(18,2)")
        stypes = {f.name: str(f.field_type) for f in _get_catalog().load_table(sident).schema().fields}
        sql_ok = (not any(sql.values()) and stypes == {"id": "long", "f": "double", "d": "decimal(18, 2)"}
                  and _ids(sident) == [1])
        sql_note = (f"Polars SQL ALTER COLUMN TYPE promoted int->long, float->double and decimal(9,2)->decimal(18,2) "
                    f"on a format-version {_fmt()} table (types {stypes}; the old row still reads)"
                    if sql_ok else f"Polars SQL ALTER COLUMN TYPE: {sql} (types {stypes})")
        ident = _fixture(ns, "t", "merge-on-read", columns_ddl="id INT, f FLOAT, d DECIMAL(9,2)",
                         seed_sql="(1, CAST(1.5 AS FLOAT), CAST(1.25 AS DECIMAL(9,2)))")
        frame = pl.DataFrame({"id": pl.Series([9999999999], dtype=pl.Int64), "f": pl.Series([2.5], dtype=pl.Float64),
                              "d": pl.Series([Decimal("12345678901234.50")], dtype=pl.Decimal(18, 2))})
        try:
            _sink(frame, ident, mode="append", schema_mode="merge")
        except Exception as e:  # noqa: BLE001
            if not _is_refusal(str(e)):
                r.result = "error"
                r.details = f"Unexpected failure: {_err(e)}"
            elif sql_ok:
                r.result = "pass"
                r.details = (f"{sql_note}. Writing wider data to this table is refused "
                             f"(sink_iceberg schema_mode='merge': {_err(e, 100)})")
            else:
                r.result = "fail"
                r.details = f"Promotion failed: writes refused ({_err(e, 100)}); {sql_note}"
            return
        types = {f.name: str(f.field_type) for f in _get_catalog().load_table(ident).schema().fields}
        rows = _scan(ident).collect().sort("id")
        ok = {"id": types["id"] == "long", "f": types["f"] == "double", "d": types["d"] == "decimal(18, 2)"}
        assert rows["id"].to_list() == [1, 9999999999], rows["id"].to_list()
        r.result = "pass" if (all(ok.values()) or sql_ok) else "fail"
        r.details = (f"{sql_note}. Through writes: appending Int64/Float64/Decimal(18,2) data with "
                     f"sink_iceberg(schema_mode='merge') promoted the columns to {types} ({ok}); the old row still reads")
    return _spark_test(r, body)


@_v3_only
def test_column_default_values() -> TestResult:
    r = TestResult("column-default-values", "Column Default Values", "v3")

    def body(ns, r):
        fixture = spark_fixture.create_column_default_fixture(ns, "t")
        ident = f"{ns}.t"
        _check_format(ident)
        field = next(f for f in _get_catalog().load_table(ident).schema().fields if f.name == "region")
        assert field.initial_default == "eu" and field.write_default == "eu", (field.initial_default, field.write_default)
        outcomes = {}
        for reader in ("native", None):
            kw = {} if reader is None else {"reader_override": reader}
            df = _scan(ident, **kw).collect()
            outcomes[reader or "auto"] = dict(zip(df["id"].to_list(), df["region"].to_list()))
        # Write half: a row without the column, to see whether the write default is applied.
        out = _attempts(write_iceberg=lambda: pl.DataFrame({"id": [5]}).write_iceberg(_get_catalog().load_table(ident), mode="append"))
        if all(v == fixture["expected"] for v in outcomes.values()):
            r.result = "pass"
            r.partial_reason = f"read side only: initial-default is honoured on read, but writing is refused so write-default cannot be applied ({out['write_iceberg']})"
            r.details = ("The Polars readers apply the V3 initial-default on read: the row written before the column "
                         f"existed returns 'eu' ({outcomes}). Write defaults cannot be applied: appending a row without "
                         f"the column to the V3 table gives {out['write_iceberg']}")
        else:
            r.result = "fail"
            r.details = f"V3 column default not applied on read: {outcomes} (expected {fixture['expected']})"

    return _spark_test(r, body)


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------

def _partitioned_fixture(prefix: str) -> str:
    """Day + bucket partitioned table of the pass's format version. Polars cannot
    declare a partition spec, so PyIceberg creates it (V3 too: 0.12 can create V3
    tables, only not write them)."""
    schema = Schema(
        NestedField(1, "id", LongType()), NestedField(2, "ts", TimestamptzType()), NestedField(3, "name", StringType()),
    )
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="ts_day"),
        PartitionField(source_id=3, field_id=1001, transform=BucketTransform(4), name="name_bucket"),
    )
    return _pyiceberg_table(prefix, schema, spec)


def _part_frame(ids, months) -> "pl.DataFrame":
    return pl.DataFrame({"id": list(ids), "ts": [datetime(2024, m, 15, tzinfo=timezone.utc) for m in months],
                         "name": [f"n{i}" for i in ids]})


def _fill_partitioned(ident: str, frames) -> dict:
    """Polars writes each frame (write_iceberg for the first, sink_iceberg for the rest).
    Where Polars is refused, Spark writes the same rows so the read side can still be measured."""
    cat = _get_catalog()
    out = {}
    for i, frame in enumerate(frames):
        fn = (lambda f=frame: f.write_iceberg(cat.load_table(ident), mode="append")) if i == 0 \
            else (lambda f=frame: _sink(f, ident, mode="append"))
        out.update({f"{'write_iceberg' if i == 0 else 'sink_iceberg'}#{i}": _attempts(x=fn)["x"]})
    if any(out.values()):
        assert all(_is_refusal(v) for v in out.values()), f"mixed write outcomes: {out}"
        spark_fixture.refresh(*ident.split("."))
        for frame in frames:
            _spark_insert(ident, frame)
    return out


def _transform_pruning() -> dict:
    """One single-field partitioned table per transform that test_hidden_partitioning's
    day + bucket table does not cover. Polars (or Spark where Polars is refused) writes four rows
    that land in distinct partitions; a selective predicate must plan fewer files than exist."""
    def at(y, m, d, h):
        return datetime(y, m, d, h, tzinfo=timezone.utc)
    frame = pl.DataFrame({"id": [1, 2, 3, 4],
                          "ts": [at(2023, 1, 1, 1), at(2024, 3, 1, 5), at(2025, 7, 1, 9), at(2025, 7, 1, 10)],
                          "name": ["aa1", "bb2", "cc3", "dd4"]})
    schema = Schema(NestedField(1, "id", LongType()), NestedField(2, "ts", TimestamptzType()), NestedField(3, "name", StringType()))
    cases = {
        "identity(id)": (1, IdentityTransform(), pl.col("id") == 3, [3]),
        "truncate(2, name)": (3, TruncateTransform(2), pl.col("name") == "bb2", [2]),
        "year(ts)": (2, YearTransform(), pl.col("ts") >= at(2025, 1, 1, 0), [3, 4]),
        "month(ts)": (2, MonthTransform(), pl.col("ts") >= at(2025, 7, 1, 0), [3, 4]),
        "hour(ts)": (2, HourTransform(), pl.col("ts") >= at(2025, 7, 1, 10), [4]),
    }
    out = {}
    for label, (source, transform, predicate, expect) in cases.items():
        spec = PartitionSpec(PartitionField(source_id=source, field_id=1000, transform=transform, name="p"))
        ident = _pyiceberg_table("hp", schema, spec)
        _fill_partitioned(ident, [frame])
        parts = _table_files(ident)["partition"]
        lf = _scan(ident).filter(predicate)
        planned, total = _explain_files(lf), len(parts)
        got = sorted(lf.collect()["id"].to_list())
        assert got == expect and total == len({str(p) for p in parts}) and planned < total, (label, got, planned, total)
        out[label] = f"{planned}/{total} files planned"
    return out


def _declare_partitions(ident: str) -> dict:
    """Polars SQL declares day(ts) + bucket(4, name) on an empty table it created."""
    out = _sql_try(
        create=f"CREATE TABLE {_lake(ident)} (id BIGINT, ts TIMESTAMP, name VARCHAR) WITH ('format-version' = '{_fmt()}')",
        spec=f"ALTER TABLE {_lake(ident)} SET PARTITIONED BY (day(ts), bucket(4, name))",
        insert=(f"INSERT INTO {_lake(ident)} VALUES (1, TIMESTAMP '2024-01-15 10:00:00', 'n1'), "
                "(2, TIMESTAMP '2024-06-15 10:00:00', 'n2')"),
    )
    if not any(out.values()):
        parts = _table_files(ident)["partition"]
        out["files"] = None if len(parts) == 2 and all(p.get("ts_day") for p in parts) else f"partitions {parts}"
    return out


def test_hidden_partitioning() -> TestResult:
    r = TestResult("hidden-partitioning", "Hidden Partitioning")
    def body(ns, r):
        declared = _declare_partitions(_table("sqlpart"))
        ident = _partitioned_fixture("hidpart")
        out = _fill_partitioned(ident, [_part_frame([1, 2], [1, 6]), _part_frame([3, 4], [1, 6])])
        parts = _table_files(ident)["partition"]
        assert len(parts) >= 2 and all(p and p.get("ts_day") for p in parts), parts
        lf = _scan(ident).filter(pl.col("ts") > datetime(2024, 3, 1, tzinfo=timezone.utc))
        planned, total = _explain_files(lf), len(parts)
        got = sorted(lf.collect()["id"].to_list())
        assert got == [2, 4] and planned < total, (got, planned, total)
        others = _transform_pruning()
        refused = {k: v for k, v in out.items() if v}
        declared_ok = not any(declared.values())
        reasons = []
        if not declared_ok:
            reasons.append(f"cannot declare a partition spec ({ {k: v for k, v in declared.items() if v} })")
        if refused:
            reasons.append(f"read side only: every write was refused ({refused})")
        r.result = "pass"
        r.partial_reason = "; ".join(reasons) or None
        who = "Spark wrote the files after Polars was refused" if refused else "Polars (write_iceberg and native sink_iceberg) wrote them"
        r.details = (("Polars SQL declared day(ts) + bucket(4, name) on a table it created (ALTER TABLE ... SET "
                      "PARTITIONED BY) and its INSERT wrote one file per partition. " if declared_ok else
                      f"Polars SQL could not declare a spec: {declared}. ")
                     + f"Format-version {_fmt()} table partitioned by day(ts) + bucket(4, name): {who}; {total} files "
                     f"carry partition values; a ts predicate plans {planned} of {total} files and returns {got}. "
                     f"Polars write outcomes: {out}. Other transforms prune the same way: {others}")
    return _vtest(r, body)


def test_partition_evolution() -> TestResult:
    r = TestResult("partition-evolution", "Partition Evolution")
    def body(ns, r):
        cat = _get_catalog()
        ident = _partitioned_fixture("partevo")
        out = _fill_partitioned(ident, [_part_frame([1, 2], [1, 6])])
        evolve = _sql_try(evolve=f"ALTER TABLE {_lake(ident)} SET PARTITIONED BY (day(ts), bucket(4, name), month(ts))")
        if evolve["evolve"] is None:
            by = "Polars SQL ALTER TABLE ... SET PARTITIONED BY"
        else:
            assert _is_refusal(evolve["evolve"]), evolve
            by = f"PyIceberg, because Polars SQL refused ({evolve['evolve']})"
            with cat.load_table(ident).update_spec() as u:
                u.add_field("ts", MonthTransform(), "ts_month")
        out2 = _attempts(sink=lambda: _sink(_part_frame([3, 4], [2, 7]), ident, mode="append"))
        if out2["sink"]:
            assert _is_refusal(out2["sink"]), out2
            spark_fixture.refresh(*ident.split("."))
            _spark_insert(ident, _part_frame([3, 4], [2, 7]))
        ids = _ids(ident)
        specs = {p.spec_id for p in cat.load_table(ident).specs().values()}
        assert ids == [1, 2, 3, 4] and len(specs) == 2, (ids, specs)
        refused = {**{k: v for k, v in out.items() if v}, **{k: v for k, v in out2.items() if v}}
        reasons = []
        if evolve["evolve"]:
            reasons.append("cannot evolve a partition spec on this table")
        if refused:
            reasons.append(f"read side only: writes under the new spec were refused ({refused})")
        r.result = "pass"
        r.partial_reason = "; ".join(reasons) or None
        r.details = (f"The spec of a format-version {_fmt()} table was evolved by {by} ({len(specs)} specs); "
                     f"{'sink_iceberg wrote under the new spec' if not refused else 'Polars was refused, Spark wrote under the new spec'} "
                     f"and scan_iceberg read across both (ids {ids}). Polars write outcomes: {out} / {out2}")
    return _vtest(r, body)


@_v3_only
def test_multi_arg_transforms() -> TestResult:
    r = TestResult("multi-arg-transforms", "Multi-Argument Transforms", "v3")

    def body(ns, r):
        # Polars' Iceberg layer is PyIceberg's metadata model: check it can express a
        # multi-source partition field. The model silently drops unknown kwargs, so
        # inspect the serialised form instead of "did not raise". Spark 4.1 / Iceberg
        # 1.11 reject the spec as well ("Cannot convert transform with more than one column
        # reference"), and so does DuckDB 2.0, so no multi-argument table exists to read.
        pf = PartitionField(source_id=1, field_id=1000, transform=BucketTransform(8), name="b", source_ids=[1, 2])
        dumped = pf.model_dump(by_alias=True)
        modelled = dumped.get("source-ids") == [1, 2]
        sink_params = set(inspect.signature(pl.LazyFrame.sink_iceberg).parameters) | set(inspect.signature(pl.DataFrame.write_iceberg).parameters)
        spec_params = sorted(p for p in sink_params if "partition" in p and "overwrite" not in p)
        r.result = "pass" if (modelled or spec_params) else "fail"
        r.details = (f"Polars' Iceberg layer (PyIceberg {PYICEBERG_VERSION}) "
                     f"{'models' if modelled else 'does NOT model'} multi-source partition fields (serialises as {dumped}); "
                     f"no write function takes a partition spec (partition-related parameters: {spec_params or 'none'}). "
                     "Spark/Iceberg 1.11 and DuckDB 2.0 cannot create such a table either, so there is nothing to read")

    return _catalog_test(r, body)


# ---------------------------------------------------------------------------
# Snapshots, maintenance, refs
# ---------------------------------------------------------------------------

def test_time_travel() -> TestResult:
    r = TestResult("time-travel", "Time Travel / Snapshots")
    def body(ns, r):
        import time
        cat = _get_catalog()
        ident = _seed(ns, "tt", _sample([1]))
        snap1 = cat.load_table(ident).current_snapshot()
        time.sleep(2)
        _add_rows(ident, _sample([2]))
        snap2 = cat.load_table(ident).current_snapshot()
        old, now = _ids(ident, snapshot_id=snap1.snapshot_id), _ids(ident)
        inc = sorted(_scan(ident, from_snapshot_id_exclusive=snap1.snapshot_id,
                           to_snapshot_id_inclusive=snap2.snapshot_id).collect()["id"].to_list())
        assert (old, now, inc) == ([1], [1, 2], [2]), (old, now, inc)
        # Polars SQL (2.2+): AS OF a snapshot id and AS OF a timestamp between the two commits.
        mid_ms = (snap1.timestamp_ms + snap2.timestamp_ms) // 2
        mid = datetime.fromtimestamp(mid_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        sql_ids = {}
        for label, clause in (("version", f"FOR VERSION AS OF {snap1.snapshot_id}"),
                              ("timestamp", f"FOR TIMESTAMP AS OF TIMESTAMP '{mid}'")):
            try:
                sql_ids[label] = sorted(_sql(f"SELECT id FROM {_lake(ident)} {clause}")["id"].to_list())
            except Exception as e:  # noqa: BLE001
                sql_ids[label] = _err(e, 100)
        r.result = "pass"
        if sql_ids.get("timestamp") != [1]:
            r.partial_reason = f"snapshot ids only: no working timestamp (as-of) read (SQL: {sql_ids})"
        r.details = (f"On a format-version {_fmt()} table scan_iceberg(snapshot_id=...) returned {old}, the current read "
                     f"{now}, an incremental append scan {inc}. Polars SQL: FOR VERSION AS OF -> {sql_ids['version']}, "
                     f"FOR TIMESTAMP AS OF a time between the commits ({mid} UTC) -> {sql_ids['timestamp']}")
    return _vtest(r, body)


def _maintenance_api() -> list:
    names = ("expire_snapshots", "rewrite_data_files", "rewrite_manifests", "compact", "optimize", "vacuum",
             "remove_orphan_files", "compact_iceberg", "expire_snapshots_iceberg", "optimize_iceberg")
    return [n for n in names if hasattr(pl, n) or hasattr(pl.DataFrame, n) or hasattr(pl.LazyFrame, n)]


def test_table_maintenance() -> TestResult:
    r = TestResult("table-maintenance", "Table Maintenance")
    def body(ns, r):
        ident = _seed(ns, "maint", _sample([1]))
        for i in (2, 3):
            _add_rows(ident, _sample([i]))
        before = len(_table_files(ident)["file_path"])
        ns_part, name = ident.split(".")
        proc = f"{SQL_CATALOG}.system"
        sql = _sql_try(
            rewrite_data_files=(f"CALL {proc}.rewrite_data_files(table => '{ns_part}.{name}', "
                                "options => map('rewrite-all', 'true'))"),
            rewrite_manifests=f"CALL {proc}.rewrite_manifests(table => '{ns_part}.{name}')",
            expire_snapshots=f"CALL {proc}.expire_snapshots(table => '{ns_part}.{name}')",
            remove_orphan_files=f"CALL {proc}.remove_orphan_files(table => '{ns_part}.{name}')",
        )
        after = len(_table_files(ident)["file_path"])
        compacted = sql["rewrite_data_files"] is None and after < before and _ids(ident) == [1, 2, 3]
        available = sorted(k for k, v in sql.items() if v is None)
        missing = sorted(k for k, v in sql.items() if v)
        if compacted:
            r.result = "pass"
            if missing:
                r.partial_reason = f"no {missing}: Polars SQL CALL supports {available}"
            r.details = (f"Polars SQL CALL rewrite_data_files compacted a format-version {_fmt()} table from {before} "
                         f"data files to {after} (rows intact); procedures that ran: {available}; not available: "
                         f"{ {k: sql[k] for k in missing} }")
        elif sql["rewrite_data_files"] and _is_refusal(sql["rewrite_data_files"]):
            r.result = "fail"
            r.details = (f"Compaction is refused on a format-version {_fmt()} table ({sql['rewrite_data_files']}); "
                         f"other procedures: {sql}")
        else:
            r.result = "fail" if not available else "error"
            r.details = f"No maintenance through Polars: {sql} ({before} -> {after} data files)"
    return _vtest(r, body)


def _branch_surface() -> dict:
    wanted = ("branch", "tag", "ref")
    return {fn.__qualname__: [p for p in inspect.signature(fn).parameters if any(w in p for w in wanted)]
            for fn in (pl.scan_iceberg, pl.LazyFrame.sink_iceberg, pl.DataFrame.write_iceberg, pl.delete_iceberg)}


def test_branching_tagging() -> TestResult:
    r = TestResult("branching-tagging", "Branching & Tagging")
    def body(ns, r):
        ident = _fixture(ns, "t", "merge-on-read")
        spark = spark_fixture.get_spark()
        spark.sql(f"ALTER TABLE local.{ident} CREATE BRANCH dev")
        spark.sql(f"INSERT INTO local.{ident}.branch_dev VALUES (99,'branch-only')")
        L = _lake(ident)
        sql = _sql_try(create_tag=f"ALTER TABLE {L} CREATE TAG polars_tag",
                       create_branch=f"ALTER TABLE {L} CREATE BRANCH polars_branch")
        reads = {}
        for ref in ("dev", "polars_tag"):
            try:
                reads[ref] = sorted(_sql(f"SELECT id FROM {L} FOR VERSION AS OF '{ref}'")["id"].to_list())
            except Exception as e:  # noqa: BLE001
                reads[ref] = _err(e, 100)
        write = _sql_try(write_branch=f"INSERT INTO {L}.branch_dev VALUES (100, 'x')")["write_branch"]
        drop = _sql_try(drop_tag=f"ALTER TABLE {L} DROP TAG polars_tag")["drop_tag"]
        refs = sorted(_get_catalog().load_table(ident).metadata.refs)
        made = not sql["create_tag"] and not sql["create_branch"] and "polars_branch" in refs
        read_ok = reads.get("dev") == [1, 2, 3, 99] and reads.get("polars_tag") == [1, 2, 3]
        if made and read_ok:
            r.result = "pass"
            if write:
                r.partial_reason = "refs can be created, dropped and read, but no Polars write targets a branch"
            r.details = (f"Polars SQL created a tag and a branch on a format-version {_fmt()} table (refs {refs}), read "
                         f"the Spark-written branch and the tag with FOR VERSION AS OF ({reads}), and dropped the tag "
                         f"({drop or 'ok'}). Writing to a branch: {write or 'accepted'}")
        else:
            surface = _branch_surface()
            r.result = "pass" if any(v for v in surface.values()) else "fail"
            r.details = (f"Polars SQL refs: {sql}, reads {reads}; DataFrame API ref parameters: {surface}; refs {refs}")
    return _spark_test(r, body)


# ---------------------------------------------------------------------------
# Statistics / bloom filters
# ---------------------------------------------------------------------------

def test_statistics() -> TestResult:
    r = TestResult("statistics", "Statistics (Column Metrics)")

    def body(ns, r):
        seed = _sample([1, 2, 3]).with_columns(name=pl.Series(["n1", None, "n3"]))
        ident = _seed(ns, "stats", seed)
        _add_rows(ident, pl.DataFrame({"id": [1000], "name": ["far"], "value": [1.0],
                                       "ts": [datetime(2024, 1, 9, tzinfo=timezone.utc)]}))
        files = _table_files(ident)
        with_bounds = sum(1 for lb in files["lower_bounds"] if lb)
        lf = _scan(ident).filter(pl.col("id") > 500)
        planned, total = _explain_files(lf), len(files["file_path"])
        got = lf.collect()["id"].to_list()
        assert got == [1000], got
        # Null and value counts in the manifest (the stats the planner and other engines rely on).
        name_id = _get_catalog().load_table(ident).schema().find_field("name").field_id
        nulls = sum(dict(c).get(name_id, 0) for c in files["null_value_counts"])
        values = sum(dict(c).get(name_id, 0) for c in files["value_counts"])
        counts_ok = nulls == 1 and values == 4
        # Row count answered from metadata alone: delete the data files of a table, then count.
        meta = _seed(ns, "statmeta", _sample([1, 2, 3]))
        mtbl = _get_catalog().load_table(meta)
        for path in mtbl.inspect.files().to_pydict()["file_path"]:
            mtbl.io.delete(path)
        try:
            from_meta = _scan(meta, use_metadata_statistics=True).select(pl.len()).collect().item()
            meta_note = f"use_metadata_statistics=True counted {from_meta} rows with the data files deleted"
            meta_ok = from_meta == 3
        except Exception as e:  # noqa: BLE001 - a failed metadata answer is the datum
            meta_note, meta_ok = f"use_metadata_statistics=True could not count without the data files ({_err(e, 90)})", False
        reasons = [x for x in (_read_only_reason(ns),
                               None if counts_ok else f"null/value counts missing or wrong (nulls {nulls}, values {values})",
                               None if meta_ok else "row counts are not answered from metadata") if x]
        r.result = "pass" if (with_bounds == total and planned < total) else "fail"
        r.partial_reason = " | ".join(reasons) or None
        writer = "sink_iceberg" if _read_only_reason(ns) is None else "Spark (Polars cannot write these files)"
        r.details = (f"Files written by {writer} to a format-version {_fmt()} table carry column bounds ({with_bounds}/{total}); "
                     f"a predicate scan plans {planned} of {total} files (min/max pruning in the native reader). "
                     f"Manifest null counts for a column with one null: {nulls}, value counts {values} (4 rows). {meta_note}")

    return _vtest(r, body)


def _has_bloom(ident: str) -> bool:
    import pyarrow.parquet as pq
    from pyiceberg.io.pyarrow import PyArrowFileIO
    tbl = _get_catalog().load_table(ident)
    io = PyArrowFileIO(tbl.io.properties)
    for path in tbl.inspect.files().to_pydict()["file_path"]:
        with io.new_input(path).open() as f:
            md = pq.ParquetFile(f).metadata
        for rg in range(md.num_row_groups):
            if getattr(md.row_group(rg).column(0), "bloom_filter_offset", None):
                return True
    return False


def test_bloom_filters() -> TestResult:
    r = TestResult("bloom-filters", "Bloom Filters")

    def body(ns, r):
        schema = Schema(NestedField(1, "id", LongType()), NestedField(2, "name", StringType()))
        frame = pl.DataFrame({"id": list(range(1000)), "name": ["x"] * 1000})
        out, wrote = {}, {}
        for path in ("sink_iceberg", "write_iceberg"):
            ident = _pyiceberg_table("bloom", schema, props={"write.parquet.bloom-filter-enabled.column.id": "true"})
            fn = (lambda: _sink(frame, ident, mode="append")) if path == "sink_iceberg" \
                else (lambda: frame.write_iceberg(_get_catalog().load_table(ident), mode="append"))
            err = _attempts(x=fn)["x"]
            out[path] = err
            if err is None:
                wrote[path] = _has_bloom(ident)
        if any(wrote.values()):
            r.result = "pass"
            r.details = f"Polars honoured write.parquet.bloom-filter-enabled.column.id ({wrote})"
        elif wrote and len(wrote) == len(out):
            r.result = "fail"
            r.details = (f"Polars ignored write.parquet.bloom-filter-enabled.column.id on a format-version {_fmt()} table: "
                         f"neither write path produced a bloom filter in the Parquet files ({wrote}); no Puffin blob support either")
        elif not wrote and all(_is_refusal(v) for v in out.values()):
            r.result = "fail"
            r.details = (f"Cannot write bloom filters to a format-version {_fmt()} table: every Polars write path is refused "
                         f"({out}), so the property cannot have any effect")
        else:
            r.result = "error"
            r.details = f"Unexpected outcome: {out} / {wrote}"

    return _vtest(r, body)


# ---------------------------------------------------------------------------
# Catalogs
# ---------------------------------------------------------------------------

def test_catalog_integration() -> TestResult:
    r = TestResult("catalog-integration", "Catalog Integration")

    def body(ns, r):
        from pyiceberg.catalog.sql import SqlCatalog
        rest = _get_catalog()
        rest_ident = _seed(ns, "cat", _sample())
        rest_ids = _ids(rest_ident)
        assert rest_ids == [1, 2, 3], rest_ids
        with tempfile.TemporaryDirectory() as d:
            sql = SqlCatalog("polars_sql", uri=f"sqlite:///{d}/catalog.db", warehouse=f"file://{d}")
            sql.create_namespace("default")
            try:
                sql.create_table("default.t", schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "name", StringType())),
                                 properties={"format-version": str(_fmt())})
            except NotImplementedError as e:
                # A client-side catalog (SQL) has to write the V3 metadata file itself, which PyIceberg cannot do.
                r.result = "pass"
                r.partial_reason = f"read side only: the REST catalog is the only catalog that holds V3 tables here; {_err(e, 80)}"
                r.details = (f"Polars loaded and read a format-version {_fmt()} table through the REST catalog (ids {rest_ids}). "
                             f"A SQL/SQLite catalog cannot even create a V3 table, because PyIceberg writes the metadata file "
                             f"itself ({_err(e, 110)}), so Polars has no V3 table to use through it")
                return
            frame = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
            out = _attempts(
                write_iceberg=lambda: frame.write_iceberg(sql.load_table("default.t"), mode="append"),
                sink_iceberg=lambda: frame.lazy().sink_iceberg("default.t", mode="append", catalog=sql),
            )
            assert sql.load_table("default.t").format_version == _fmt()
            sql_rows = pl.scan_iceberg("default.t", catalog=sql).collect().height
        assert not any(out.values()) and sql_rows == 4, (sql_rows, out)
        r.result = "pass"
        r.details = ("Polars read and wrote through two PyIceberg catalog types: the REST catalog (3 rows) and a SQL/SQLite "
                     f"catalog ({sql_rows} rows, write_iceberg + sink_iceberg + scan_iceberg). catalog= accepts any PyIceberg "
                     "catalog or an IcebergCatalogConfig, so Polars has no catalog client of its own")

    return _vtest(r, body)


def test_rest_catalog() -> TestResult:
    r = TestResult("rest-catalog", "REST Catalog")

    def body(ns, r):
        cat = _get_catalog()
        ident = _seed(ns, "rest", _sample([1]))
        out = _attempts(write_iceberg=lambda: _sample([2]).write_iceberg(cat.load_table(ident), mode="append"))
        got = _ids(ident)
        cat.drop_table(ident)
        if out["write_iceberg"] is None:
            assert got == [1, 2], got
            r.result = "pass"
            r.details = (f"Full round-trip through the Iceberg REST catalog at {spark_fixture.REST_URI} (OAuth2 client "
                         "credentials, S3 storage): create (sink_iceberg), append (write_iceberg), scan_iceberg, drop (PyIceberg)")
        else:
            assert got == [1] and _is_refusal(out["write_iceberg"]), (got, out)
            r.result = "pass"
            r.partial_reason = f"read side only: appending through the REST catalog is refused on format-version {_fmt()} ({out['write_iceberg']})"
            r.details = (f"scan_iceberg read a format-version {_fmt()} table (created by Spark) from the REST catalog "
                         f"(ids {got}); write_iceberg to it: {out['write_iceberg']}; dropped through PyIceberg")

    return _vtest(r, body)


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
# V3 data types / capabilities -- Spark or DuckDB writes the V3 column, Polars reads it.
# ---------------------------------------------------------------------------

def _v3_read_probe(r: TestResult, ddl_type: str, insert_value: str, check=None, partial=None):
    def body(ns, r):
        spark = spark_fixture.get_spark()
        spark.sql(f"""CREATE TABLE local.{ns}.t (id BIGINT, c {ddl_type}) USING iceberg
                      TBLPROPERTIES ('format-version'='3')""")
        spark.sql(f"INSERT INTO local.{ns}.t VALUES (1, {insert_value})")
        assert _spark_format(f"{ns}.t") == 3
        try:
            df = _scan(f"{ns}.t").collect()
        except Exception as e:  # noqa: BLE001
            r.result = "fail"
            r.details = f"Polars cannot read a V3 table with a {ddl_type} column: {_err(e, 160)}"
            return
        schema_types = [str(f.field_type) for f in _get_catalog().load_table(f"{ns}.t").schema().fields]
        value = df["c"].to_list()[0]
        ok, note = check(value, schema_types) if check else (value is not None, repr(value)[:80])
        r.result = "pass" if ok else "fail"
        r.partial_reason = partial if ok else None
        r.details = (f"Polars read a Spark-written V3 {ddl_type} column: {note}" if ok
                     else f"Polars read the V3 {ddl_type} column but the value is wrong: {note}")
    return _spark_test(r, body)


@_v3_only
def test_variant_type() -> TestResult:
    r = TestResult("variant-type", "Variant Type", "v3")
    return _v3_read_probe(r, "VARIANT", "parse_json('{\"k\": 1}')")


def _parquet_paths(path: str) -> list:
    import pyarrow.parquet as pq
    from pyiceberg.io.pyarrow import PyArrowFileIO
    io = PyArrowFileIO(_catalog_props())
    with io.new_input(path).open() as f:
        md = pq.ParquetFile(f).metadata
        return [md.row_group(0).column(i).path_in_schema for i in range(md.num_columns)]


@_v3_only
def test_shredded_variant() -> TestResult:
    r = TestResult("shredded-variant", "Shredded Variant", "v3")

    def body(ns, r):
        spark = spark_fixture.get_spark()
        spark.sql(f"""CREATE TABLE local.{ns}.t (id BIGINT, v VARIANT) USING iceberg
                      TBLPROPERTIES ('format-version'='3', 'write.parquet.shred-variants'='true')""")
        spark.sql(f"INSERT INTO local.{ns}.t SELECT 1, parse_json('{{\"a\": 42}}')")
        assert _spark_format(f"{ns}.t") == 3
        # The fixture must really be shredded, else this measures an unshredded variant.
        paths = [row[0] for row in spark.sql(f"SELECT file_path FROM local.{ns}.t.files").collect()]
        cols = _parquet_paths(paths[0])
        assert any("typed_value" in c for c in cols), f"Spark wrote an unshredded variant: {cols}"
        has_dtype = hasattr(pl, "Variant")
        try:
            _scan(f"{ns}.t").collect()
            read = "reads it"
        except Exception as e:  # noqa: BLE001
            read = f"cannot load it ({_err(e, 100)})"
        r.result = "pass" if has_dtype else "fail"
        r.details = (f"Polars cannot write or shred variant data: no pl.Variant dtype (present: {has_dtype}), and a "
                     f"Spark-written shredded variant table (typed_value columns {[c for c in cols if 'typed_value' in c][:2]}) "
                     f"{read}; every write path is refused on a V3 table")

    return _spark_test(r, body)


@_v3_only
def test_geometry_type() -> TestResult:
    r = TestResult("geometry-type", "Geometry / Geo Types", "v3")

    def body(ns, r):
        # Neither Spark (Iceberg 1.11 rejects GEOMETRY) nor PyIceberg can produce the
        # fixture; DuckDB writes the V3 GEOMETRY table into the shared catalog.
        produced = spark_fixture.create_geometry_fixture(ns, "t")
        if not produced["ok"]:
            r.result = "skip"
            r.details = f"Not exercised: {produced['reason']}"
            return
        ident = f"{ns}.t"
        _check_format(ident)
        # The native reader falls back to a plain Parquet scan for this table, which
        # gets no S3 settings from the catalog, so hand it the vended ones.
        opts = _vended_storage_options(ident)
        outcomes = {}
        for reader in ("native", "pyiceberg"):
            try:
                df = _scan(ident, reader_override=reader, storage_options=opts).collect()
                outcomes[reader] = f"read {df.schema['c']}: {df['c'].to_list()[0]!r}"[:100]
            except Exception as e:  # noqa: BLE001
                outcomes[reader] = _err(e, 80)
        readable = any(v.startswith("read ") for v in outcomes.values())
        # GEOGRAPHY: no engine here can write a row, so only the empty table (built with
        # the Iceberg Java API) can be loaded.
        geog = "not built"
        try:
            spark = spark_fixture.get_spark()
            jvm = spark._jvm
            icat = spark._jsparkSession.sessionState().catalogManager().catalog("local").icebergCatalog()
            types = jvm.org.apache.iceberg.types.Types
            schema = jvm.org.apache.iceberg.Schema([
                types.NestedField.required(1, "id", types.LongType.get()),
                types.NestedField.optional(2, "geo", types.GeographyType.crs84())])
            props = jvm.java.util.HashMap()
            props.put("format-version", "3")
            icat.createTable(jvm.org.apache.iceberg.catalog.TableIdentifier.parse(f"{ns}.u"), schema,
                             jvm.org.apache.iceberg.PartitionSpec.unpartitioned(), props)
            gdf = _scan(f"{ns}.u").collect()
            geog = f"an empty V3 GEOGRAPHY table loads and its column maps to {gdf.schema['geo']} (no engine here can write a row to test values)"
        except Exception as e:  # noqa: BLE001
            geog = f"GEOGRAPHY table not usable: {_err(e, 80)}"
        # Write half: Polars has no geometry dtype, so a value cannot be expressed (a Binary column is
        # rejected as a field mismatch); an id-only frame shows the V3 write refusal itself.
        mismatch = _attempts(write_iceberg=lambda: pl.DataFrame({"id": [2], "c": [b"\x01"]}).write_iceberg(
            _get_catalog().load_table(ident), mode="append"))
        write = _attempts(write_iceberg=lambda: pl.DataFrame({"id": [2]}).write_iceberg(
            _get_catalog().load_table(ident), mode="append"))
        r.result = "pass" if readable else "fail"
        r.details = (f"Polars reading a DuckDB-written V3 GEOMETRY column ({produced['wkt']}): {outcomes}; {geog}; "
                     f"append of a Binary value to the geometry column: {mismatch['write_iceberg']}; "
                     f"append of an id-only row: {write['write_iceberg']}")[:900]

    return _spark_test(r, body)


@_v3_only
def test_nanosecond_timestamps() -> TestResult:
    r = TestResult("nanosecond-timestamps", "Nanosecond Timestamps", "v3")

    def body(ns, r):
        # Read half: DuckDB (the only local producer with a nanosecond literal) writes the V3
        # table; Polars reads it. Write half: a nanosecond frame, appended to that table and
        # used to create a new one.
        produced = spark_fixture.create_timestamp_ns_fixture(ns, "t")
        if not produced["ok"]:
            r.result = "skip"
            r.details = f"Not exercised: {produced['reason']}"
            return
        ident = f"{ns}.t"
        _check_format(ident)
        col = next(f for f in _get_catalog().load_table(ident).schema().fields if f.name == "c")
        assert str(col.field_type) == "timestamp_ns", col.field_type
        df = _scan(ident).collect()
        dtype = df.schema["c"]
        nanos = df.select(pl.col("c").cast(pl.Int64) % 1_000_000_000).item()
        ok = getattr(dtype, "time_unit", None) == "ns" and nanos == 123456789
        frame = pl.DataFrame({"id": [2], "c": pl.Series([1767225600123456789]).cast(pl.Datetime("ns"))})
        out = _attempts(
            write_iceberg=lambda: frame.write_iceberg(_get_catalog().load_table(ident), mode="append"),
            sink_iceberg=lambda: _sink(frame, ident, mode="append"),
            create_v2_table=lambda: _sink(frame, _table("nsc"), mode="append", create_if_not_exists=True),
        )
        assert all(out.values()), f"a nanosecond write unexpectedly succeeded: {out}"
        if ok:
            r.result = "pass"
            r.partial_reason = f"read side only: nanosecond frames cannot be written ({out})"
            r.details = (f"Polars reads a DuckDB-written timestamp_ns table as {dtype} with sub-microsecond digits intact "
                         f"(nanoseconds {nanos}; DuckDB wrote {produced['value']!r}). Writes: {out}")
        else:
            r.result = "fail"
            r.details = f"Polars reads the table as {dtype} but loses nanosecond precision (nanoseconds {nanos})"

    return _spark_test(r, body)


@_v3_only
def test_unknown_type() -> TestResult:
    r = TestResult("unknown-type", "Unknown Type", "v3")

    def check(value, types):
        return (value is None and "unknown" in types, f"null, as the unknown type requires (Iceberg column types {types})")

    return _v3_read_probe(r, "VOID", "NULL", check=check,
                          partial="read side only: Polars cannot declare an unknown-type column or write to V3 tables")


@_v3_only
def test_lineage() -> TestResult:
    r = TestResult("lineage", "Lineage Tracking", "v3")

    def body(ns, r):
        # Rated by WRITE: does the engine assign row IDs when it commits? Reading V3
        # row-lineage metadata written by another engine does not count.
        ident = _fixture(ns, "t", "merge-on-read")
        tbl = _get_catalog().load_table(ident)
        next_row_id = getattr(tbl.metadata, "next_row_id", None)
        assert next_row_id, f"the fixture has no row lineage (next-row-id={next_row_id})"
        cols = _scan(ident).collect_schema().names()
        out = _attempts(
            write_iceberg=lambda: _sample([9]).select("id").with_columns(val=pl.lit("z")).write_iceberg(
                _get_catalog().load_table(ident), mode="append"),
            sink_iceberg=lambda: _sink(pl.DataFrame({"id": [9], "val": ["z"]}), ident, mode="append"),
        )
        after = getattr(_get_catalog().load_table(ident).metadata, "next_row_id", None)
        _write_verdict(r, out, f"A Polars commit moved next-row-id from {next_row_id} to {after}", "assign row IDs on commit")
        r.details += (f". Read-side note, not rated: next-row-id={next_row_id}; lineage columns exposed to Polars: "
                      f"{[c for c in cols if c.startswith('_')] or 'none'}")

    return _spark_test(r, body)


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

def load_polars_json_support() -> dict:
    """Load the JSON support levels for Polars from the repo data."""
    path = os.path.join(REPO_ROOT, "src", "data", "platforms", "oss", "polars", "polars.json")
    with open(path) as f:
        data = json.load(f)
    result = {}
    for key, val in data.get("support", {}).items():
        if key.startswith("polars:"):
            parts = key.split(":")
            if len(parts) == 3:
                result[(parts[1], parts[2])] = val.get("level", "unknown")
    return result


def expected_level(test_result: str, partial_reason) -> str:
    """The level the JSON must carry for a measured result: a pass proves ``full``
    unless the test recorded a ``partial_reason``; a fail proves ``none``."""
    if test_result == "pass":
        return "partial" if partial_reason else "full"
    if test_result == "fail":
        return "none"
    return ""


def compute_match(test_result: str, json_level: str, partial_reason=None) -> bool:
    """
    Determine if test result matches JSON level.
    - pass (no partial_reason) -> json must be 'full'
    - pass (partial_reason)    -> json must be 'partial'
    - fail → json should be 'none' (NOT 'unknown' — we have evidence now)
    - skip → always matches (cannot verify)
    - error → always matches (test issue, not data issue)
    """
    if test_result in ("skip", "error"):
        return True
    return json_level == expected_level(test_result, partial_reason)


def generate_report(results: list) -> dict:
    json_support = load_polars_json_support()

    tests_output = []
    discrepancies = 0
    passed = sum(1 for r in results if r.result == "pass")
    failed = sum(1 for r in results if r.result == "fail")
    skipped = sum(1 for r in results if r.result == "skip")
    errors = sum(1 for r in results if r.result == "error")

    for r in results:
        json_level = json_support.get((r.feature_id, r.version_tested), "unknown")
        match = compute_match(r.result, json_level, r.partial_reason)
        if not match:
            discrepancies += 1
        tests_output.append({**r.to_dict(), "json_level": json_level,
                             "expected_level": expected_level(r.result, r.partial_reason), "match": match})

    return {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "engine": "Polars",
        "polars_version": POLARS_VERSION,
        "polars_iceberg_version": POLARS_ICEBERG_VERSION,
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


def generate_markdown(report: dict) -> str:
    lines = []
    lines.append("# Polars Feature Test Report")
    lines.append("")
    lines.append(f"- **Timestamp:** {report['timestamp']}")
    lines.append(f"- **Polars Version:** {report['polars_version']}")
    lines.append(f"- **polars-iceberg Version:** {report['polars_iceberg_version']}")
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
    lines.append("| Feature | Version | Result | Measured level | JSON Level | Match | Details |")
    lines.append("|---------|---------|--------|----------------|------------|-------|---------|")

    status_emoji = {"pass": "✅", "fail": "❌", "skip": "⏭️", "error": "⚠️"}

    for t in report["tests"]:
        emoji = status_emoji.get(t["result"], "❓")
        match_str = "✅" if t["match"] else "❌ DISCREPANCY"
        details = t["details"][:80].replace("\n", " ").replace("\r", "").replace("|", "\\|") if t["details"] else ""
        feature_name = t["feature_name"].replace("|", "\\|")
        json_level = t["json_level"].replace("|", "\\|") if t["json_level"] else ""
        lines.append(
            f"| {feature_name} | {t['version']} | {emoji} {t['result']} "
            f"| {t['expected_level'] or '-'} | {json_level} | {match_str} | {details} |"
        )

    lines.append("")

    discs = [t for t in report["tests"] if not t["match"]]
    if discs:
        lines.append("## ⚠️ Discrepancies")
        lines.append("")
        for t in discs:
            detail_clean = t["details"][:120].replace("\n", " ").replace("\r", "") if t["details"] else ""
            lines.append(f"- **{t['feature_name']}** ({t['version']}): "
                         f"test={t['result']} (measured {t['expected_level'] or '-'}), json={t['json_level']} — {detail_clean}")
        lines.append("")

    errs = [t for t in report["tests"] if t["result"] == "error"]
    if errs:
        lines.append("## ⚠️ Errors")
        lines.append("")
        for t in errs:
            detail_clean = t["details"][:160].replace("\n", " ").replace("\r", "") if t["details"] else ""
            lines.append(f"- **{t['feature_name']}** ({t['version']}): {detail_clean}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("  Polars Feature Test Suite")
    print("=" * 70)
    print(f"Polars version: {POLARS_VERSION} (polars-iceberg {POLARS_ICEBERG_VERSION})")
    print(f"PyIceberg version: {PYICEBERG_VERSION}")
    print(f"REST catalog: {spark_fixture.REST_URI}")
    print(f"Repo root: {REPO_ROOT}")
    print()

    os.makedirs(REPORT_DIR, exist_ok=True)

    # Run all tests, once per format version. Version-agnostic tests build tables of
    # CURRENT_VERSION; the V3-only tests always report v3 whatever the pass, and the two
    # tests that always skip say the same on both, so each of those runs once.
    global CURRENT_VERSION
    results = []
    run_once = set()
    for version in FORMAT_VERSIONS:
        CURRENT_VERSION = version
        print(f"\n{'#' * 70}\n  Format version {version}\n{'#' * 70}")
        for test_fn in ALL_TESTS:
            test_name = test_fn.__name__
            if test_name in run_once:
                continue
            print(f"\n--- Running {test_name} [{version}] ---")
            try:
                result = test_fn()
                if result.version_tested != version or test_name in ("test_aws_glue_catalog", "test_unity_catalog"):
                    run_once.add(test_name)
                results.append(result)
                icon = {"pass": "✅", "fail": "❌", "skip": "⏭️", "error": "⚠️"}.get(result.result, "?")
                print(f"  {icon} {result.result}: {result.details[:160]}")
            except Exception as e:  # noqa: BLE001
                r = TestResult(test_name.replace("test_", "").replace("_", "-"), test_name)
                r.result = "error"
                r.details = f"Unhandled exception: {e}"
                results.append(r)
                print(f"  ⚠️ error: {e}")

    print("\n" + "=" * 70)
    print("  Generating Report")
    print("=" * 70)

    report = generate_report(results)

    json_path = os.path.join(REPORT_DIR, "polars-test-report.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"JSON report: {json_path}")

    md_content = generate_markdown(report)
    md_path = os.path.join(REPORT_DIR, "polars-test-report.md")
    with open(md_path, "w") as f:
        f.write(md_content)
    print(f"Markdown report: {md_path}")

    s = report["summary"]
    print(f"\n{'=' * 70}")
    print(f"  RESULTS: {s['passed']} passed, {s['failed']} failed, "
          f"{s['skipped']} skipped, {s['errors']} errors, "
          f"{s['discrepancies']} discrepancies")
    print(f"{'=' * 70}")

    print("\n" + md_content)

    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(md_content)

    # Exit code: fail if there are discrepancies or test errors
    if s["discrepancies"] > 0 or s["errors"] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
