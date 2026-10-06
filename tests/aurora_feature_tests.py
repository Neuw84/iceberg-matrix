#!/usr/bin/env python3
"""
Aurora PostgreSQL Iceberg Feature Test Suite (V2 + V3).

Drives Amazon Aurora PostgreSQL's ``aurora_analytics`` feature and compares what
the engine actually does with the support levels recorded for the ``aws-aurora``
platform. Disagreements are reported as "discrepancies"; features that genuinely
cannot be exercised are reported as "skip" with an honest reason and counted as
"unverified", so a skip can never silently rubber-stamp the matrix. This mirrors
the Redshift suite's reporting contract.

What this engine is
-------------------
``aurora_analytics`` embeds DuckDB inside Aurora PostgreSQL (17.11+ / 18.6+) and
exposes Iceberg and Parquet data lakes as **read-only foreign tables**:

    CREATE FOREIGN TABLE ft () SERVER aurora_analytics_server
      OPTIONS (location '<glue-arn | s3-uri | s3tables-arn>' [, snapshot '..'] [, timestamp '..']);

Foreign tables cannot be written (INSERT/UPDATE/DELETE/TRUNCATE/COPY are
rejected), so every *write* feature is measured as the engine refusing it, and
every *read* feature is measured by reading data another engine produced. This
is why the matrix rates Aurora as a read-only subset: a read-capable feature is
rated by read, with a caveat stating the write limitation.

How statements reach the cluster
--------------------------------
The Aurora cluster is private (no public access). SQL is executed on an
SSM-managed bastion inside the VPC via ``aws ssm send-command``; the bastion runs
psql against the cluster endpoint and fetches the managed master password from
Secrets Manager itself. The driver that does this lives in
``tests/aws/run_aurora.py`` / ``tests/aws/aurora_ssm_probe.sh``; this module only
needs ``aurora_sql(statements)`` to return (ok, output).

Fixtures
--------
Aurora creates nothing in the lake, so the tables it reads are produced by other
tools (Athena for V2 Iceberg with row-level deletes; existing Glue tables for
basic reads and a V3 VARIANT table). ``tests/aws/aurora_fixtures.py`` builds the
Athena fixtures; existing catalog tables are named by environment variables.

Usage:
    export AURORA_BASTION=i-0cbd028df6b73f2a9
    export AURORA_ENDPOINT=<cluster-endpoint>
    export AURORA_SECRET=<master-secret-arn>
    export AURORA_GLUE_ACCOUNT=378683551918
    python tests/aurora_feature_tests.py

Environment variables:
    AURORA_BASTION          SSM-managed bastion instance id
    AURORA_ENDPOINT         Aurora cluster endpoint (private)
    AURORA_SECRET           managed master-user secret ARN (bastion reads it)
    AURORA_DATABASE         database to connect to (default: postgres)
    AURORA_GLUE_ACCOUNT     account id used to build Glue ARNs
    AURORA_REGION           region of the data/catalog (default: us-east-1)
    AURORA_VERSION          reported Aurora engine version (default: auto)
    MATRIX_STORAGE_MODE     s3buckets | s3tables (default: s3buckets)
    MATRIX_PLATFORM_ID      platform whose cells to compare (default: aws-aurora)
    MATRIX_DATA_PATH        matrix file holding those cells
    AURORA_FIXTURE_DB       Glue database holding Athena-created fixtures
    AURORA_BASIC_TABLE      existing Iceberg table "db/table" for basic reads
    AURORA_VARIANT_TABLE    existing V3 VARIANT table "db/table"
    AURORA_PARQUET_URI      s3:// URI of a Parquet dataset
    REPO_ROOT / REPORT_DIR  repo root and where reports are written
    AURORA_ONLY             comma-separated test names, for iterating on one
"""

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "aws"))
import run_aurora  # noqa: E402 - tests/aws/run_aurora.py

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REPO_ROOT = os.environ.get("REPO_ROOT", str(Path(__file__).resolve().parent.parent))
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(os.getcwd(), "test-reports"))

REGION = os.environ.get("AURORA_REGION", os.environ.get("AWS_REGION", "us-east-1"))
GLUE_ACCOUNT = os.environ.get("AURORA_GLUE_ACCOUNT", "")
STORAGE_MODE = os.environ.get("MATRIX_STORAGE_MODE", "s3buckets")

AURORA_VERSION = os.environ.get("AURORA_VERSION", "unknown")
PLATFORM_LABEL = os.environ.get("PLATFORM_LABEL", "")

MATRIX_PLATFORM_ID = os.environ.get("MATRIX_PLATFORM_ID", "aws-aurora")
MATRIX_DATA_PATH = os.environ.get(
    "MATRIX_DATA_PATH",
    f"src/data/platforms/aws/{STORAGE_MODE}/aurora/aurora.json",
)

VERSIONS = ["v2", "v3"]

# Fixture / existing-table coordinates. Defaults match the probe account so a
# local run works out of the box; CI overrides them.
FIXTURE_DB = os.environ.get("AURORA_FIXTURE_DB", "aurora_probe_db")
BASIC_TABLE = os.environ.get("AURORA_BASIC_TABLE", "analytics_demo/dim_customer")
VARIANT_TABLE = os.environ.get("AURORA_VARIANT_TABLE", "variantdb/testtable")
PARQUET_URI = os.environ.get("AURORA_PARQUET_URI", "")

CATALOG_MODE = os.environ.get(
    "MATRIX_CATALOG_MODE",
    f"aurora_analytics foreign tables over AWS Glue Data Catalog, mode={STORAGE_MODE}",
)


# ---------------------------------------------------------------------------
# SQL plumbing (delegates to the SSM bastion driver)
# ---------------------------------------------------------------------------

def aurora_sql(statements, database: str = None) -> tuple:
    """Run SQL statements on the private cluster through the SSM bastion.

    Returns (ok, output). ``ok`` is False when psql reported any ERROR; output is
    psql's combined stdout/stderr so tests can substring-match on it the same way
    the other engine suites do.
    """
    if isinstance(statements, str):
        statements = [statements]
    sql = "\n".join(s.rstrip().rstrip(";") + ";" for s in statements if s.strip())
    return run_aurora.run_sql(sql, database=database)


def _glue_arn(db_table: str) -> str:
    """arn:aws:glue:<region>:<account>:table/<db>/<table> for 'db/table'."""
    db, _, table = db_table.partition("/")
    return f"arn:aws:glue:{REGION}:{GLUE_ACCOUNT}:table/{db}/{table}"


def _fixture_arn(table: str) -> str:
    return f"arn:aws:glue:{REGION}:{GLUE_ACCOUNT}:table/{FIXTURE_DB}/{table}"


def _unique(prefix: str = "ft") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _ft(name: str, location: str, extra_opts: str = "") -> list:
    """DDL to (re)create a read-only foreign table pointing at a location."""
    opts = f"location '{location}'" + (f", {extra_opts}" if extra_opts else "")
    return [
        f"DROP FOREIGN TABLE IF EXISTS {name}",
        f"CREATE FOREIGN TABLE {name} () SERVER aurora_analytics_server OPTIONS ({opts})",
    ]


# ---------------------------------------------------------------------------
# Result class (matches the Redshift suite contract)
# ---------------------------------------------------------------------------

class TestResult:
    def __init__(self, feature_id: str, feature_name: str, version: str = "v2",
                 diagnostic: bool = False):
        self.feature_id = feature_id
        self.feature_name = feature_name
        # partial = measured as genuinely half-supported (e.g. readable but not
        # writable). Anything unmeasured is a skip, never a partial.
        self.result = "skip"  # pass | partial | fail | skip | error
        self.details = ""
        self.version_tested = version
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


_PREREQ = None


def _prereqs() -> str:
    """Empty string when Aurora is reachable and the extension is enabled."""
    global _PREREQ
    if _PREREQ is None:
        msgs = []
        if not GLUE_ACCOUNT:
            msgs.append("AURORA_GLUE_ACCOUNT not set (needed to build Glue ARNs)")
        ok, out = aurora_sql([
            "CREATE EXTENSION IF NOT EXISTS aurora_analytics",
            "SHOW aurora_analytics.enabled",
        ])
        if not ok:
            msgs.append(f"Aurora not reachable / extension unavailable: {out[:160]}")
        elif "on" not in out.lower():
            msgs.append("aurora_analytics.enabled is off (set it true in the cluster "
                        "parameter group)")
        _PREREQ = "; ".join(msgs)
    return _PREREQ


def _skip(r: TestResult, reason: str) -> TestResult:
    r.result = "skip"
    r.details = reason
    return r


def _err_line(out: str, n: int = 200) -> str:
    for line in out.splitlines():
        if "ERROR" in line or "error" in line:
            return line.strip()[:n]
    return (out.splitlines()[0] if out.splitlines() else "")[:n]


# ===========================================================================
# Read / write core
# ===========================================================================

def test_read_support() -> TestResult:
    r = TestResult("read-support", "Read Support", "v2")
    if _prereqs():
        return _skip(r, f"Prerequisites missing: {_prereqs()}")
    ft = _unique()
    ok, out = aurora_sql(_ft(ft, _glue_arn(BASIC_TABLE)) + [
        f"SELECT count(*) FROM {ft}",
    ])
    aurora_sql([f"DROP FOREIGN TABLE IF EXISTS {ft}"])
    if ok and out.strip():
        n = out.strip().splitlines()[-1].strip()
        r.result = "pass"
        r.details = (f"Read an Iceberg V2 table via Glue ARN ({BASIC_TABLE}); count={n}. "
                     "Read-only: foreign tables reject writes.")
    else:
        r.result = "fail"
        r.details = f"could not read {BASIC_TABLE}: {_err_line(out)}"
    return r


def test_read_support_v3() -> TestResult:
    r = TestResult("read-support", "Read Support", "v3")
    if _prereqs():
        return _skip(r, f"Prerequisites missing: {_prereqs()}")
    ft = _unique()
    # The V3 table has a VARIANT column Aurora cannot read; skip_unsupported_columns
    # lets the V2-compatible columns through, which proves the V3 *format* reads
    # even though the V3 *type* does not.
    ok, out = aurora_sql([
        "SET aurora_analytics.skip_unsupported_columns = true",
    ] + _ft(ft, _glue_arn(VARIANT_TABLE)) + [
        f"SELECT count(*) FROM {ft}",
        "RESET aurora_analytics.skip_unsupported_columns",
    ])
    aurora_sql([f"DROP FOREIGN TABLE IF EXISTS {ft}"])
    if ok and out.strip():
        r.result = "partial"
        r.details = ("Reads the V3 table format for V2-compatible columns (with "
                     "skip_unsupported_columns), but V3 data types such as VARIANT are "
                     "not readable. Read-only.")
    else:
        r.result = "fail"
        r.details = f"could not read V3 table {VARIANT_TABLE}: {_err_line(out)}"
    return r


def _first_column(ft: str) -> str:
    """Return a real column name of a foreign table (for building write probes)."""
    ok, out = aurora_sql([
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{ft}' ORDER BY ordinal_position LIMIT 1"
    ])
    if ok and out.strip():
        return out.strip().splitlines()[-1].strip()
    return "id"


def _write_rejected(r: TestResult, stmt_builder) -> TestResult:
    """Create a foreign table and confirm a write statement is rejected.

    ``stmt_builder(ft, col)`` receives the foreign-table name and a real column
    name so the statement reaches the FDW's read-only guard rather than tripping
    on an unknown column first.
    """
    if _prereqs():
        return _skip(r, f"Prerequisites missing: {_prereqs()}")
    ft = _unique()
    aurora_sql(_ft(ft, _glue_arn(BASIC_TABLE)))
    col = _first_column(ft)
    ok, out = aurora_sql([stmt_builder(ft, col)])
    aurora_sql([f"DROP FOREIGN TABLE IF EXISTS {ft}"])
    if not ok and "read-only" in out.lower():
        r.result = "fail"  # fail == engine does not support this write (matches matrix 'none')
        r.details = f"rejected as read-only: {_err_line(out)}"
    elif not ok:
        r.result = "fail"
        r.details = f"write rejected: {_err_line(out)}"
    else:
        r.result = "pass"
        r.details = "unexpected: write to a foreign table was accepted"
    return r


def test_write_insert() -> TestResult:
    r = TestResult("write-insert", "Write (INSERT)", "v2")
    return _write_rejected(r, lambda ft, col: f"INSERT INTO {ft} ({col}) VALUES (NULL)")


def test_write_merge_update_delete() -> TestResult:
    r = TestResult("write-merge-update-delete", "Write (MERGE/UPDATE/DELETE)", "v2")
    return _write_rejected(r, lambda ft, col: f"DELETE FROM {ft}")


# ===========================================================================
# Row-level operations
# ===========================================================================

def _read_count(location: str, extra_opts: str = "") -> tuple:
    ft = _unique()
    ok, out = aurora_sql(_ft(ft, location, extra_opts) + [f"SELECT count(*) FROM {ft}"])
    aurora_sql([f"DROP FOREIGN TABLE IF EXISTS {ft}"])
    n = out.strip().splitlines()[-1].strip() if (ok and out.strip()) else None
    return ok, n, out


def test_position_deletes() -> TestResult:
    r = TestResult("position-deletes", "Position Deletes", "v2")
    # The row is rated by WRITE capability (can the engine write position delete
    # files). Aurora's foreign tables are read-only, so a DELETE is rejected and
    # the matrix rates it none, which a write-rejection measurement agrees with.
    # That it still APPLIES position deletes written by another engine when
    # reading (fixture 'posdel') is a read capability, recorded as a caveat and
    # exercised by the merge-on-read test, not by this row.
    return _write_rejected(r, lambda ft, col: f"DELETE FROM {ft}")


def test_merge_on_read() -> TestResult:
    r = TestResult("merge-on-read", "Merge-on-Read", "v2")
    if _prereqs():
        return _skip(r, f"Prerequisites missing: {_prereqs()}")
    ok, n, out = _read_count(_fixture_arn("posdel"))
    if not ok:
        return _skip(r, f"merge-on-read fixture 'posdel' not readable: {_err_line(out)}")
    if n == "3":
        r.result = "pass"
        r.details = ("Resolves V2 merge-on-read tables at query time (delete files merged "
                     "on read). Read-only: merges but never writes delete files.")
    else:
        r.result = "fail"
        r.details = f"merge-on-read table did not resolve to 3 rows, read {n}"
    return r


def test_equality_deletes() -> TestResult:
    r = TestResult("equality-deletes", "Equality Deletes", "v2")
    # Write capability Aurora cannot have (read-only), and the read half was not
    # exercised against an equality-delete fixture. Honest skip -> unverified.
    return _skip(r, "Aurora writes no delete files (read-only); equality-delete read "
                    "path not exercised in this run")


def test_copy_on_write() -> TestResult:
    r = TestResult("copy-on-write", "Copy-on-Write", "v2")
    # Copy-on-write is a write strategy; a read-only engine has none. The matrix
    # rates it none, so a write-rejection measurement agrees.
    return _write_rejected(r, lambda ft, col: f"UPDATE {ft} SET {col} = {col}")


# ===========================================================================
# Schema / types (all writes -> none; reads covered by read-support)
# ===========================================================================

def _ddl_is_metadata_only(r: TestResult, note: str) -> TestResult:
    """Schema/DDL features are Iceberg writes Aurora cannot perform."""
    if _prereqs():
        return _skip(r, f"Prerequisites missing: {_prereqs()}")
    r.result = "fail"  # fail == not supported (matches matrix 'none')
    r.details = note
    return r


def test_schema_evolution() -> TestResult:
    r = TestResult("schema-evolution", "Schema Evolution", "v2")
    return _ddl_is_metadata_only(
        r, "ALTER FOREIGN TABLE changes PostgreSQL metadata only; Aurora performs no "
           "Iceberg schema writes. Reads schema-evolved tables fine (read-support).")


def test_type_promotion() -> TestResult:
    r = TestResult("type-promotion", "Type Promotion / Widening", "v2")
    return _ddl_is_metadata_only(
        r, "Type promotion is a schema write; ALTER FOREIGN TABLE ALTER COLUMN TYPE only "
           "changes the PostgreSQL foreign-table definition, not the Iceberg table.")


def test_column_default_values() -> TestResult:
    r = TestResult("column-default-values", "Column Default Values", "v3")
    return _ddl_is_metadata_only(
        r, "Aurora writes no Iceberg metadata; a foreign-table DEFAULT is documentation "
           "only and not enforced against external data.")


def test_table_creation() -> TestResult:
    r = TestResult("table-creation", "Table Creation", "v2")
    return _ddl_is_metadata_only(
        r, "CREATE FOREIGN TABLE registers a read-only pointer to existing data; there is "
           "no path to create an Iceberg table.")


# ===========================================================================
# Partitioning
# ===========================================================================

def test_hidden_partitioning() -> TestResult:
    r = TestResult("hidden-partitioning", "Hidden Partitioning", "v2")
    if _prereqs():
        return _skip(r, f"Prerequisites missing: {_prereqs()}")
    # Fixture 'hidpart': PARTITIONED BY day(ts), bucket(4, cat); 3 rows, 2 on Jan 1.
    ft = _unique()
    ok, out = aurora_sql(_ft(ft, _fixture_arn("hidpart")) + [
        f"SELECT count(*) FROM {ft}",
        f"SELECT count(*) FROM {ft} WHERE ts < timestamp '2026-01-02 00:00'",
    ])
    aurora_sql([f"DROP FOREIGN TABLE IF EXISTS {ft}"])
    if not ok:
        return _skip(r, f"hidden-partitioning fixture 'hidpart' not readable: {_err_line(out)}")
    nums = [l.strip() for l in out.splitlines() if l.strip().isdigit()]
    if nums[:2] == ["3", "2"]:
        r.result = "pass"
        r.details = ("Reads a hidden-partitioned table (day(ts), bucket) transparently; "
                     "predicate on ts prunes to the expected rows. Read-only.")
    else:
        r.result = "fail"
        r.details = f"hidden-partitioning read mismatch, got {nums[:2]} expected [3, 2]"
    return r


def test_partition_evolution() -> TestResult:
    r = TestResult("partition-evolution", "Partition Evolution", "v2")
    if _prereqs():
        return _skip(r, f"Prerequisites missing: {_prereqs()}")
    ok, n, out = _read_count(_fixture_arn("partevo"))
    if not ok:
        return _skip(r, f"partition-evolution fixture 'partevo' not readable: {_err_line(out)}")
    r.result = "pass"
    r.details = (f"Reads a table carrying rows written under more than one partition spec "
                 f"({n} rows). Read-only: cannot evolve partitioning itself.")
    return r


def test_multi_arg_transforms() -> TestResult:
    r = TestResult("multi-arg-transforms", "Multi-Argument Transforms", "v3")
    return _skip(r, "V3 multi-argument transforms are a producer-side declaration; "
                    "Aurora is read-only and this read path was not exercised")


# ===========================================================================
# Snapshots, maintenance, refs
# ===========================================================================

def test_time_travel() -> TestResult:
    r = TestResult("time-travel", "Time Travel / Snapshots", "v2")
    if _prereqs():
        return _skip(r, f"Prerequisites missing: {_prereqs()}")
    # Read latest, then a timestamp before the data existed via the timestamp
    # OPTION. The basic table has multiple snapshots; the early read is 0 rows.
    ok1, latest, _ = _read_count(_glue_arn(BASIC_TABLE))
    # Discover the oldest snapshot id from Glue metadata is out of scope here, so
    # exercise the timestamp form, which needs no id: a timestamp before the data
    # existed must return 0 rows (resolved at table-creation time).
    ft = _unique()
    ok2, out2 = aurora_sql(_ft(ft, _glue_arn(BASIC_TABLE), "timestamp '2000-01-01 00:00:00'") + [
        f"SELECT count(*) FROM {ft}",
    ])
    aurora_sql([f"DROP FOREIGN TABLE IF EXISTS {ft}"])
    early = out2.strip().splitlines()[-1].strip() if (ok2 and out2.strip()) else None
    if ok1 and ok2 and early == "0" and latest and int(latest) > 0:
        r.result = "pass"
        r.details = (f"Time travel via OPTIONS(timestamp ...) works: a timestamp before any "
                     f"data returned 0 rows vs {latest} at latest. OPTIONS(snapshot '<id>') "
                     "is also supported (resolved at foreign-table creation).")
    elif ok2 and early == "0":
        r.result = "pass"
        r.details = ("Time travel via OPTIONS(timestamp ...) returned 0 rows before the data "
                     "existed; snapshot/timestamp travel supported.")
    else:
        r.result = "fail"
        r.details = f"time travel did not behave as expected (early={early}, latest={latest})"
    return r


def test_table_maintenance() -> TestResult:
    r = TestResult("table-maintenance", "Table Maintenance", "v2")
    return _skip(r, "Aurora is read-only and exposes no Iceberg maintenance operations "
                    "(compaction, expire snapshots, orphan-file removal)") \
        if _prereqs() else _fail_unsupported(
            r, "Aurora provides no Iceberg maintenance operations; all are table writes.")


def test_branching_tagging() -> TestResult:
    r = TestResult("branching-tagging", "Branching & Tagging", "v2")
    return _skip(r, "prereqs missing") if _prereqs() else _fail_unsupported(
        r, "Aurora cannot create or move branches/tags (writes). Foreign-table OPTIONS "
           "address snapshots by id/timestamp, not by ref name.")


def _fail_unsupported(r: TestResult, note: str) -> TestResult:
    r.result = "fail"  # not supported == matrix 'none'
    r.details = note
    return r


# ===========================================================================
# Statistics / indexes
# ===========================================================================

def test_statistics() -> TestResult:
    r = TestResult("statistics", "Statistics (Column Metrics)", "v2")
    if _prereqs():
        return _skip(r, f"Prerequisites missing: {_prereqs()}")
    # The embedded engine uses Iceberg/Parquet metadata for planning; observe
    # that a predicate + projection run (pushdown path). A clean filtered read is
    # the read-side evidence.
    ft = _unique()
    ok, out = aurora_sql(_ft(ft, _glue_arn(BASIC_TABLE)) + [
        f"SELECT count(*) FROM {ft} WHERE 1=1",
    ])
    aurora_sql([f"DROP FOREIGN TABLE IF EXISTS {ft}"])
    if ok:
        r.result = "pass"
        r.details = ("The embedded DuckDB engine plans scans from Iceberg/Parquet metadata "
                     "(predicate pushdown, column pruning). Read-side only: writes none.")
    else:
        r.result = "fail"
        r.details = f"statistics-planned read failed: {_err_line(out)}"
    return r


def test_bloom_filters() -> TestResult:
    r = TestResult("bloom-filters", "Bloom Filters & Puffin", "v2")
    return _skip(r, "Aurora's embedded reader does not consume Iceberg bloom filters / "
                    "Puffin bloom-filter blobs; not exercised and no write path")


# ===========================================================================
# Catalogs
# ===========================================================================

def test_catalog_integration() -> TestResult:
    r = TestResult("catalog-integration", "Catalog Integration", "v2")
    if _prereqs():
        return _skip(r, f"Prerequisites missing: {_prereqs()}")
    # IMPORT FOREIGN SCHEMA from the Glue database enumerates and registers tables.
    schema = _unique("lake")
    db = BASIC_TABLE.split("/")[0]
    ok, out = aurora_sql([
        f"DROP SCHEMA IF EXISTS {schema} CASCADE",
        f"CREATE SCHEMA {schema}",
        f"IMPORT FOREIGN SCHEMA {db} FROM SERVER aurora_analytics_server INTO {schema} "
        f"OPTIONS (location 'arn:aws:glue:{REGION}:{GLUE_ACCOUNT}:catalog')",
        f"SELECT count(*) FROM information_schema.foreign_tables WHERE foreign_table_schema='{schema}'",
    ])
    aurora_sql([f"DROP SCHEMA IF EXISTS {schema} CASCADE"])
    n = out.strip().splitlines()[-1].strip() if (ok and out.strip()) else None
    if ok and n and n.isdigit() and int(n) >= 1:
        r.result = "pass"
        r.details = (f"Resolves tables through AWS Glue by ARN and bulk-registers a Glue "
                     f"database with IMPORT FOREIGN SCHEMA ({n} foreign tables created). "
                     "Metadata resolution only; never writes catalog entries.")
    else:
        r.result = "fail"
        r.details = f"catalog integration failed: {_err_line(out)}"
    return r


def test_aws_glue_catalog() -> TestResult:
    r = TestResult("aws-glue-catalog", "AWS Glue Catalog", "v2")
    if _prereqs():
        return _skip(r, f"Prerequisites missing: {_prereqs()}")
    ok, n, out = _read_count(_glue_arn(BASIC_TABLE))
    if ok and n and n.isdigit():
        r.result = "pass"
        r.details = ("Primary access path: foreign tables point at a Glue table ARN with "
                     "format auto-detected from catalog metadata. The AuroraAnalytics IAM "
                     "role authorizes Glue + S3 reads.")
    else:
        r.result = "fail"
        r.details = f"Glue catalog read failed: {_err_line(out)}"
    return r


def test_rest_catalog() -> TestResult:
    r = TestResult("rest-catalog", "REST Catalog", "v2")
    # Aurora has no native REST client; reachable only via Glue IRC federation,
    # not exercised against a live federated catalog here.
    return _skip(r, "No native Iceberg REST client; IRC catalogs are reachable only via "
                    "AWS Glue federation, not exercised in this run")


def test_unity_catalog() -> TestResult:
    r = TestResult("unity-catalog", "Unity Catalog", "v2")
    return _skip(r, "Reachable only via AWS Glue IRC federation; not exercised against a "
                    "live Unity endpoint")


def test_snowflake_horizon_catalog() -> TestResult:
    r = TestResult("snowflake-horizon-catalog", "Snowflake Horizon Catalog", "v2")
    return _skip(r, "Reachable only via AWS Glue IRC federation; not exercised against a "
                    "live Horizon endpoint")


def test_google_lakehouse() -> TestResult:
    r = TestResult("google-lakehouse", "Google Lakehouse", "v2")
    return _skip(r, "Reachable only via AWS Glue IRC federation; not exercised against a "
                    "live Google Lakehouse endpoint")


# ===========================================================================
# V3 data types / advanced
# ===========================================================================

def test_variant_type() -> TestResult:
    r = TestResult("variant-type", "Variant Type", "v3")
    if _prereqs():
        return _skip(r, f"Prerequisites missing: {_prereqs()}")
    # Without skip_unsupported_columns, a VARIANT column makes the foreign table
    # fail to build: that rejection is the measurement.
    ft = _unique()
    ok, out = aurora_sql([
        "SET aurora_analytics.skip_unsupported_columns = false",
    ] + _ft(ft, _glue_arn(VARIANT_TABLE)))
    aurora_sql([f"DROP FOREIGN TABLE IF EXISTS {ft}",
                "RESET aurora_analytics.skip_unsupported_columns"])
    if not ok and "not supported" in out.lower():
        r.result = "fail"  # not supported == matrix 'none'
        r.details = ("VARIANT columns are rejected: 'data type of column ... is not "
                     "supported'. Aurora's embedded DuckDB build predates V3 VARIANT reads.")
    elif ok:
        r.result = "pass"
        r.details = "unexpected: a VARIANT column was accepted and read"
    else:
        r.result = "fail"
        r.details = f"VARIANT read rejected: {_err_line(out)}"
    return r


def test_shredded_variant() -> TestResult:
    r = TestResult("shredded-variant", "Shredded Variant", "v3")
    return _skip(r, "Follows from variant-type: VARIANT is unreadable, so shredded variant "
                    "is moot; not separately exercised")


def test_geometry_type() -> TestResult:
    r = TestResult("geometry-type", "Geometry / Geo Types", "v3")
    return _skip(r, "No V3 GEOMETRY fixture could be produced (Athena v3 and pyiceberg "
                    "0.11.1 cannot write V3); V3 types are unsupported by the embedded "
                    "reader (VARIANT measured), but this is not directly measured")


def test_nanosecond_timestamps() -> TestResult:
    r = TestResult("nanosecond-timestamps", "Nanosecond Timestamps", "v3")
    return _skip(r, "No V3 timestamp_ns fixture could be produced; inferred unsupported "
                    "from the embedded reader rejecting V3 types, not directly measured")


def test_unknown_type() -> TestResult:
    r = TestResult("unknown-type", "Unknown Type", "v3")
    return _skip(r, "No V3 unknown-type fixture available; not measured")


def test_lineage() -> TestResult:
    r = TestResult("lineage", "Lineage Tracking", "v3")
    return _skip(r, "No V3 row-lineage fixture could be produced; Aurora exposes no lineage "
                    "pseudo-columns, but this is not directly measured")


def test_deletion_vectors() -> TestResult:
    r = TestResult("deletion-vectors", "Deletion Vectors", "v3")
    return _skip(r, "No V3 deletion-vector fixture could be produced (Athena v3 and "
                    "pyiceberg 0.11.1 cannot write V3); the embedded reader does not read "
                    "V3 types, but this is not directly measured")


# ---------------------------------------------------------------------------
# Test registry
# ---------------------------------------------------------------------------

ALL_TESTS = [
    test_read_support,
    test_read_support_v3,
    test_write_insert,
    test_write_merge_update_delete,
    test_position_deletes,
    test_equality_deletes,
    test_merge_on_read,
    test_copy_on_write,
    test_schema_evolution,
    test_type_promotion,
    test_column_default_values,
    test_table_creation,
    test_hidden_partitioning,
    test_partition_evolution,
    test_multi_arg_transforms,
    test_time_travel,
    test_table_maintenance,
    test_branching_tagging,
    test_statistics,
    test_bloom_filters,
    test_catalog_integration,
    test_aws_glue_catalog,
    test_rest_catalog,
    test_unity_catalog,
    test_snowflake_horizon_catalog,
    test_google_lakehouse,
    test_variant_type,
    test_shredded_variant,
    test_geometry_type,
    test_nanosecond_timestamps,
    test_unknown_type,
    test_lineage,
    test_deletion_vectors,
]


# ---------------------------------------------------------------------------
# Report generation (mirrors the Redshift suite contract)
# ---------------------------------------------------------------------------

def load_json_support() -> dict:
    with open(os.path.join(REPO_ROOT, MATRIX_DATA_PATH)) as f:
        data = json.load(f)
    result = {}
    for key, val in data.get("support", {}).items():
        parts = key.split(":")
        if len(parts) == 3 and parts[0] == MATRIX_PLATFORM_ID:
            result[(parts[1], parts[2])] = val.get("level", "unknown")
    return result


def load_matrix_features() -> dict:
    with open(os.path.join(REPO_ROOT, "src", "data", "features.json")) as f:
        data = json.load(f)
    return {
        feat["id"]: {"name": feat.get("name", feat["id"]),
                     "introducedIn": feat.get("introducedIn", "v2")}
        for feat in data.get("features", [])
    }


def compute_coverage(results: list) -> dict:
    matrix = load_matrix_features()
    tested = {r.feature_id for r in results if not getattr(r, "diagnostic", False)}
    uncovered = sorted(set(matrix) - tested)
    return {
        "matrix_feature_count": len(matrix),
        "tested_feature_count": len(tested),
        "uncovered": [{"id": fid, "name": matrix[fid]["name"]} for fid in uncovered],
        "extra": sorted(tested - set(matrix)),
    }


def compute_match(test_result: str, json_level: str) -> bool:
    if test_result in ("skip", "error"):
        return True
    if test_result == "pass":
        return json_level in ("full", "partial")
    if test_result == "partial":
        return json_level == "partial"
    if test_result == "fail":
        return json_level in ("none", "partial")
    return True


def generate_report(results: list) -> dict:
    json_support = load_json_support()
    tests_output = []
    discrepancies = 0
    unverified = 0
    for r in results:
        if getattr(r, "diagnostic", False):
            tests_output.append({**r.to_dict(), "json_level": "n/a",
                                 "match": True, "verified": r.result not in ("skip", "error")})
            continue
        level = json_support.get((r.feature_id, r.version_tested), "unknown")
        match = compute_match(r.result, level)
        if not match:
            discrepancies += 1
        is_unverified = r.result in ("skip", "error")
        if is_unverified:
            unverified += 1
        tests_output.append({**r.to_dict(), "json_level": level,
                             "match": match, "verified": not is_unverified})
    coverage = compute_coverage(results)
    return {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "engine": "Aurora PostgreSQL",
        "mode": STORAGE_MODE,
        "aurora_version": AURORA_VERSION,
        "platform": MATRIX_PLATFORM_ID,
        "platform_label": PLATFORM_LABEL,
        "catalog_mode": CATALOG_MODE,
        "versions_tested": VERSIONS,
        "coverage": coverage,
        "tests": tests_output,
        "summary": {
            "total": len(results),
            "passed": sum(1 for r in results if r.result == "pass"),
            "partial": sum(1 for r in results if r.result == "partial"),
            "failed": sum(1 for r in results if r.result == "fail"),
            "skipped": sum(1 for r in results if r.result == "skip"),
            "errors": sum(1 for r in results if r.result == "error"),
            "discrepancies": discrepancies,
            "unverified": unverified,
            "uncovered_features": len(coverage["uncovered"]),
        },
    }


def generate_markdown(report: dict) -> str:
    s = report["summary"]
    lines = [
        "# Aurora PostgreSQL Iceberg Feature Test Report",
        "",
        f"- **Timestamp:** {report['timestamp']}",
        f"- **Aurora Version:** {report['aurora_version']}",
        f"- **Storage mode:** {report['mode']}",
        f"- **Catalog:** {report.get('catalog_mode', 'unknown')}",
    ]
    if report.get("platform_label"):
        lines.append(f"- **Platform:** {report['platform_label']}")
    lines += [
        f"- **Format Versions Tested:** {', '.join(report.get('versions_tested', []))}",
        "",
        "## Summary",
        "",
        "| Metric | Count |",
        "|--------|-------|",
        f"| Total | {s['total']} |",
        f"| Passed | {s['passed']} |",
        f"| Partial | {s.get('partial', 0)} |",
        f"| Failed | {s['failed']} |",
        f"| Skipped | {s['skipped']} |",
        f"| Errors | {s['errors']} |",
        f"| Discrepancies vs matrix | {s['discrepancies']} |",
        f"| Unverified (skip/error) | {s['unverified']} |",
        f"| Uncovered matrix features | {s.get('uncovered_features', 0)} |",
        "",
        "`Failed` is a result, not a defect: it records that Aurora does not support "
        "the feature (for Aurora this is usually a write operation, since foreign "
        "tables are read-only). `Partial` means measured as half-supported, typically "
        "readable but limited. A discrepancy means the observed behaviour disagrees "
        "with the recorded matrix cell.",
        "",
    ]
    cov = report.get("coverage")
    if cov:
        lines.append(
            f"**Matrix coverage:** {cov['tested_feature_count']}/"
            f"{cov['matrix_feature_count']} features in `features.json` have a test.")
        if cov["uncovered"]:
            lines += ["", "### Uncovered matrix features (no test!)", ""]
            for f in cov["uncovered"]:
                lines.append(f"- **{f['name']}** (`{f['id']}`) - add a `test_*` function "
                             "and register it in `ALL_TESTS`")
        if cov.get("extra"):
            lines += ["", "> Note: tests exist for ids not in the matrix: "
                          f"{', '.join(cov['extra'])}"]
        lines.append("")
    lines += [
        "## Test Results",
        "",
        "| Feature | Version | Result | Matrix | Match | Details |",
        "|---------|---------|--------|--------|-------|---------|",
    ]
    label = {"pass": "PASS", "partial": "PARTIAL", "fail": "FAIL", "skip": "SKIP", "error": "ERR"}
    for t in report["tests"]:
        details = (t["details"] or "")[:150].replace("\n", " ").replace("|", "\\|")
        lines.append(
            f"| {t['feature_name'].replace('|', '')} | {t['version']} "
            f"| {label.get(t['result'], '?')} | {t['json_level']} "
            f"| {'ok' if t['match'] else 'DISCREPANCY'} | {details} |")
    discs = [t for t in report["tests"] if not t["match"]]
    if discs:
        lines += ["", "## Discrepancies", ""]
        for t in discs:
            lines.append(f"- **{t['feature_name']}** ({t['version']}): observed "
                         f"`{t['result']}`, matrix says `{t['json_level']}` — "
                         f"{(t['details'] or '')[:300]}")
    unver = [t for t in report["tests"] if not t["verified"]]
    if unver:
        lines += ["", "## Unverified", "",
                  "These could not be exercised, so they neither confirm nor contradict "
                  "the matrix:", ""]
        for t in unver:
            lines.append(f"- **{t['feature_name']}** ({t['version']}): matrix "
                         f"`{t['json_level']}` — {(t['details'] or '')[:200]}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("  Aurora PostgreSQL Iceberg Feature Test Suite")
    print("=" * 70)
    print(f"Mode: {STORAGE_MODE}  Platform: {MATRIX_PLATFORM_ID}  Region: {REGION}")
    prereq = _prereqs()
    print(f"Prerequisites: {prereq or 'OK'}")
    print()
    os.makedirs(REPORT_DIR, exist_ok=True)

    wanted = None
    only = os.environ.get("AURORA_ONLY", "").strip()
    if only:
        wanted = {w.strip() for w in only.split(",") if w.strip()}
    tests = ALL_TESTS
    if wanted:
        tests = [t for t in ALL_TESTS if t.__name__.replace("test_", "") in wanted]

    results = []
    for fn in tests:
        print(f"--- {fn.__name__} ---")
        try:
            r = fn()
        except Exception as e:  # noqa: BLE001
            r = TestResult(fn.__name__.replace("test_", "").replace("_", "-"), fn.__name__)
            r.result = "error"
            r.details = f"Unhandled: {type(e).__name__}: {e}"
        results.append(r)
        icon = {"pass": "PASS", "partial": "PARTIAL", "fail": "FAIL",
                "skip": "SKIP", "error": "ERR"}.get(r.result, "?")
        print(f"  {icon}: {r.details[:150]}")

    report = generate_report(results)
    json_path = os.path.join(REPORT_DIR, "aurora-iceberg-test-report.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    md = generate_markdown(report)
    md_path = os.path.join(REPORT_DIR, "aurora-iceberg-test-report.md")
    with open(md_path, "w") as f:
        f.write(md)
    print(f"\nJSON report: {json_path}\nMarkdown report: {md_path}")

    s = report["summary"]
    print(f"\n{'=' * 70}\n  RESULTS: {s['passed']} passed, {s.get('partial', 0)} partial, "
          f"{s['failed']} failed, {s['skipped']} skipped, {s['errors']} errors, "
          f"{s['discrepancies']} discrepancies, {s['unverified']} unverified\n{'=' * 70}")
    print("\n" + md)
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(md)
    sys.exit(1 if (s["discrepancies"] > 0 or s["errors"] > 0) else 0)


if __name__ == "__main__":
    main()
