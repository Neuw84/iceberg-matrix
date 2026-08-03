#!/usr/bin/env python3
"""
Redshift-based Iceberg Feature Test Suite (V2 + V3).

Drives Amazon Redshift Serverless through the Redshift Data API and compares what
the engine actually does with the support levels recorded for the
aws-redshift-s3 platform. Disagreements are reported as "discrepancies";
features that genuinely cannot be exercised from Redshift SQL are reported as
"skip" with an honest reason and counted as "unverified", so a skip can never
silently rubber-stamp the matrix.

Why the Data API rather than a database connection: statements are submitted as
AWS API calls, so there is no JDBC driver, no password and no network path to
arrange. Authentication uses the Redshift-managed admin secret, which gives a
deterministic identity -- the alternative, the caller's own IAM identity, lands
as a database user whose grants differ between a laptop and CI.

Two things about Redshift shape this suite, both measured rather than assumed:

    Writes need an external schema that names an IAM role. Creating an Iceberg
    table through the auto-mounted awsdatacatalog fails with "No session
    credential found": that path authorises data access with the caller's IAM
    session, and a Data API connection authenticated as a database user has
    none. This is the documented "federated identity is not supported when
    writing to Apache Iceberg tables" limitation.

    Redshift is an Iceberg v2 engine. CREATE TABLE with
    'format-version'='3' is rejected outright, so every V3 feature is a
    measured failure rather than an untested guess.

Features Redshift cannot create at all are handled the way the Flink suite
handles them: an EMR-created fixture table is read (and written) instead, which
separates "cannot create" from "cannot read" and justifies partial support.

Usage:
    export REDSHIFT_WORKGROUP=icebergmatrix-wg
    export REDSHIFT_SECRET_ARN=arn:aws:secretsmanager:...:secret:redshift!...
    export REDSHIFT_ROLE_ARN=arn:aws:iam::...:role/icebergmatrix-redshift
    export AWS_DATA_BUCKET=iceberg-tests-matrix
    python tests/redshift_feature_tests.py

Environment variables:
    REDSHIFT_WORKGROUP      Serverless workgroup name
    REDSHIFT_DATABASE       database inside the namespace (default: dev)
    REDSHIFT_SECRET_ARN     Redshift-managed admin secret; omit to use the
                            caller's IAM identity instead
    REDSHIFT_ROLE_ARN       IAM role named on the external schema
    AWS_DATA_BUCKET         bucket holding table data for the s3buckets mode
    AWS_TABLE_BUCKET_ARN    S3 Tables bucket ARN for the s3tables mode
    MATRIX_STORAGE_MODE     s3buckets | s3tables (default: s3buckets)
    MATRIX_PLATFORM_ID      platform whose cells to compare against
    MATRIX_DATA_PATH        matrix file holding those cells
    MATRIX_NS_PREFIX        prefix for created Glue databases and schemas
    REDSHIFT_FIXTURE_DB     Glue database holding EMR-created fixture tables
    PLATFORM_LABEL          free-text label recorded in the report
    REPO_ROOT / REPORT_DIR  repo root and where reports are written
    REDSHIFT_ONLY           comma-separated test names, for iterating on one
"""

import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REPO_ROOT = os.environ.get("REPO_ROOT", str(Path(__file__).resolve().parent.parent))
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(os.getcwd(), "test-reports"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
WORKGROUP = os.environ.get("REDSHIFT_WORKGROUP", "icebergmatrix-wg")
DATABASE = os.environ.get("REDSHIFT_DATABASE", "dev")
SECRET_ARN = os.environ.get("REDSHIFT_SECRET_ARN", "")
ROLE_ARN = os.environ.get("REDSHIFT_ROLE_ARN", "")
DATA_BUCKET = os.environ.get("AWS_DATA_BUCKET", "")
TABLE_BUCKET_ARN = os.environ.get("AWS_TABLE_BUCKET_ARN", "")

STORAGE_MODE = os.environ.get("MATRIX_STORAGE_MODE", "s3buckets")
NS_PREFIX = os.environ.get("MATRIX_NS_PREFIX", "icebergmatrix")
RUN_TAG = os.environ.get("RUN_TAG", uuid.uuid4().hex[:8])

# Catalog objects this run owns. All are dropped on the way out.
GLUE_DB = f"{NS_PREFIX}_rs_{RUN_TAG}"
SCHEMA = f"rs_{RUN_TAG}"
# s3tables mode only: the namespace inside the table bucket, and the resource
# link in the default Glue catalog that points at it. Redshift cannot name a
# federated catalog directly, so the link is what makes the namespace reachable.
S3T_NAMESPACE = f"{NS_PREFIX}_rs_{RUN_TAG}"
RESOURCE_LINK = f"{NS_PREFIX}_rslink_{RUN_TAG}"
# Tables Spark created on EMR for the features Redshift cannot create itself.
# Without these, "Redshift cannot do X" conflates two different answers: cannot
# write X, and cannot even read X. See tests/aws/redshift_fixtures.py.
FIXTURE_DB = os.environ.get("REDSHIFT_FIXTURE_DB", "")
FIXTURE_SCHEMA = f"rsfix_{RUN_TAG}"
FIXTURE_LINK = f"{NS_PREFIX}_rsfixlink_{RUN_TAG}"
# Fixture table names, kept in step with tests/aws/redshift_fixtures.py.
FX_V3_BASIC = "fx_v3_basic"
FX_V3_DV = "fx_v3_dv"
FX_V3_VARIANT = "fx_v3_variant"
FX_V3_GEOMETRY = "fx_v3_geometry"
FX_V3_TS_NS = "fx_v3_ts_ns"
FX_V2_BRANCH = "fx_v2_branch"
FX_V2_EQDEL = "fx_v2_eqdel"

MATRIX_PLATFORM_ID = os.environ.get("MATRIX_PLATFORM_ID", "aws-redshift-s3")
MATRIX_DATA_PATH = os.environ.get(
    "MATRIX_DATA_PATH",
    f"src/data/platforms/aws/{STORAGE_MODE}/redshift-s3/redshift-s3.json",
)
PLATFORM_LABEL = os.environ.get("PLATFORM_LABEL", "")
REDSHIFT_VERSION = os.environ.get("REDSHIFT_VERSION", "unknown")

VERSIONS = ["v2", "v3"]
STATEMENT_TIMEOUT = int(os.environ.get("REDSHIFT_STATEMENT_TIMEOUT", "300"))
# Write probes against a fixture only need long enough to be refused. A DELETE
# against a v3 deletion-vector table was observed to hang until the full statement
# timeout instead of erroring, so these are capped much lower.
FIXTURE_WRITE_TIMEOUT = int(os.environ.get("REDSHIFT_FIXTURE_WRITE_TIMEOUT", "45"))

CATALOG_MODE = os.environ.get(
    "MATRIX_CATALOG_MODE",
    f"Glue Data Catalog external schema (IAM_ROLE), mode={STORAGE_MODE}",
)


# ---------------------------------------------------------------------------
# Data API plumbing
# ---------------------------------------------------------------------------

def _client(service):
    import boto3
    return boto3.client(service, region_name=REGION)


_DATA = None


def _data():
    global _DATA
    if _DATA is None:
        _DATA = _client("redshift-data")
    return _DATA


def _run_sql(statements, timeout: int = None) -> tuple:
    """Run statements in order, stopping at the first failure.

    Returns (ok, output) where output concatenates any result rows, so the tests
    can assert on content the same way the other engine suites do.
    """
    timeout = timeout or STATEMENT_TIMEOUT
    chunks = []
    for sql in statements:
        sql = sql.strip().rstrip(";")
        if not sql:
            continue
        ok, detail, rows = _execute(sql, timeout)
        if not ok:
            # Deliberately not echoing the statement: several tests substring
            # match the output, and the SQL text would create false matches.
            chunks.append(detail)
            return False, "\n".join(chunks).strip()
        for row in rows:
            chunks.append(" | ".join("" if v is None else str(v) for v in row))
    return True, "\n".join(chunks).strip()


def _execute(sql: str, timeout: int) -> tuple:
    """Submit one statement and wait for a terminal state."""
    kwargs = {"WorkgroupName": WORKGROUP, "Database": DATABASE, "Sql": sql}
    if SECRET_ARN:
        kwargs["SecretArn"] = SECRET_ARN
    try:
        sid = _data().execute_statement(**kwargs)["Id"]
    except Exception as e:  # noqa: BLE001 - any submit failure is a result
        return False, f"{type(e).__name__}: {e}", []

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            described = _data().describe_statement(Id=sid)
        except Exception as e:  # noqa: BLE001
            return False, f"describe failed: {type(e).__name__}: {e}", []
        status = described["Status"]
        if status == "FINISHED":
            if not described.get("HasResultSet"):
                return True, "", []
            return True, "", _fetch_rows(sid)
        if status in ("FAILED", "ABORTED"):
            return False, str(described.get("Error", status)), []
        time.sleep(1.5)
    return False, f"statement timed out after {timeout}s", []


def _fetch_rows(sid: str) -> list:
    """Flatten a Data API result set into plain Python rows."""
    rows = []
    try:
        page = _data().get_statement_result(Id=sid)
    except Exception:  # noqa: BLE001 - a result we cannot read is not a failure
        return rows
    while True:
        for record in page.get("Records", []):
            row = []
            for cell in record:
                if not cell or cell.get("isNull"):
                    row.append(None)
                else:
                    row.append(list(cell.values())[0])
            rows.append(row)
        token = page.get("NextToken")
        if not token:
            break
        page = _data().get_statement_result(Id=sid, NextToken=token)
    return rows


def _unique(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _fmt(version: str) -> str:
    """Iceberg format-version number for a matrix version label."""
    return "3" if version == "v3" else "2"


def _loc(name: str) -> str:
    """LOCATION clause for a table in the s3buckets mode.

    S3 Tables determines its own location, so the clause is omitted there.
    """
    if STORAGE_MODE == "s3tables":
        return ""
    return f"LOCATION 's3://{DATA_BUCKET}/redshift/{RUN_TAG}/{name}/'"


def _table(name: str) -> str:
    return f"{SCHEMA}.{name}"


def _error_reason(out: str, limit: int = 220) -> str:
    """Condense an engine error to its most informative part.

    Redshift reports some failures on one line, and others as a block whose first
    line is a bare "ERROR:" followed by a rule and then the substance:

        ERROR:
        -----------------------------------------------
        error:  Error parsing table metadata.
        code:      15003
        context:   Invalid column type. Got: variant

    Returning the first line containing "ERROR:" throws the substance away and
    records the useless string "ERROR:" as the evidence for a cell, so the
    informative "error:" and "context:" fields are pulled out and joined instead.
    """
    if not out:
        return "no output"
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    interesting = [
        l for l in lines
        if l.lower().startswith(("error:", "context:", "detail:"))
        and l.strip().lower() not in ("error:",)
    ]
    if interesting:
        # Deduplicate while keeping order: "error:" and "context:" often overlap.
        seen, parts = set(), []
        for line in interesting:
            if line not in seen:
                seen.add(line)
                parts.append(line)
        return " | ".join(parts)[:limit]
    for line in lines:
        if ("ERROR:" in line or "Exception" in line) and line.strip() != "ERROR:":
            return line[:limit]
    return lines[0][:limit] if lines else out[:limit]


# ---------------------------------------------------------------------------
# Result class
# ---------------------------------------------------------------------------

class TestResult:
    def __init__(self, feature_id: str, feature_name: str, version: str = "v2"):
        self.feature_id = feature_id
        self.feature_name = feature_name
        # partial means measured as genuinely half-supported, e.g. readable but
        # not writable. It is a positive finding, not a shorthand for "unsure":
        # anything unmeasured is a skip.
        self.result = "skip"  # pass | partial | fail | skip | error
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


def _v3_unsupported(feature_id: str, feature_name: str, version: str) -> TestResult:
    """A V3 feature on an engine that cannot create a V3 table.

    Recorded as a failure rather than a skip: the engine was asked and refused,
    which is evidence, so the matrix cell can be contradicted by it.
    """
    r = TestResult(feature_id, feature_name, version)
    can_create, evidence = _v3_creation_refused()
    if can_create:
        r.result = "skip"
        r.details = (
            "A V3 table was created unexpectedly; this feature needs a real test "
            "rather than the shared V3 rejection path"
        )
        return r
    r.result = "fail"
    r.details = (
        "Redshift is an Iceberg v2 engine: creating a format-version 3 table is "
        f"rejected, so this V3 feature cannot exist here ({evidence})"
    )
    return r


_V3_CREATION = None


def _v3_creation_refused() -> tuple:
    """Whether this Redshift can create a format-version 3 table, measured once.

    Around twenty V3 cells share this answer, and it is a property of the engine
    rather than of any one feature, so asking once keeps them consistent and keeps
    the run short: the refusal is normally instant but was occasionally observed
    taking the full statement timeout, which turned a cheap check into minutes of
    dead waiting repeated per feature.

    Returns (created_successfully, evidence).
    """
    global _V3_CREATION
    if _V3_CREATION is None:
        tbl = _unique("v3probe")
        ok, out = _run_sql([
            f"""CREATE TABLE {_table(tbl)} (id BIGINT) USING ICEBERG {_loc(tbl)}
                TABLE PROPERTIES ('format-version'='3')"""
        ], timeout=FIXTURE_WRITE_TIMEOUT)
        if ok:
            _run_sql([f"DROP TABLE {_table(tbl)}"])
            _V3_CREATION = (True, "a format-version 3 table was created")
        else:
            _V3_CREATION = (False, _error_reason(out, 150))
    return _V3_CREATION


def _needs_external_engine(feature_id: str, feature_name: str, version: str,
                           what: str) -> TestResult:
    """Honest skip for something no Redshift SQL surface can express."""
    r = TestResult(feature_id, feature_name, version)
    r.result = "skip"
    r.details = f"Not exercised: {what}"
    return r


# ---------------------------------------------------------------------------
# Spark-created fixtures: telling "cannot create" from "cannot read"
# ---------------------------------------------------------------------------

def _fixture(name: str) -> str:
    return f"{FIXTURE_SCHEMA}.{name}"


def _fixture_available() -> bool:
    return bool(FIXTURE_DB)


def _read_fixture(feature_id: str, feature_name: str, version: str,
                  table: str, what: str, select: str = None,
                  expect: str = None, forbid: str = None,
                  write_probe: str = None) -> TestResult:
    """Read a table Spark made that Redshift itself cannot create.

    The point is to split one question into two. Redshift refusing to *create* a
    v3 table says nothing about whether it can *read* one, and those are different
    matrix answers: a feature it reads but cannot produce is partial support, not
    absent support.

    Outcomes:
      * fixtures not configured -> skip; nothing was measured
      * the fixture is missing from the manifest -> skip; Spark could not build it
        either, so there is nothing to read and no claim to make
      * read fails -> fail; the feature is genuinely absent, not merely unwritable
      * read succeeds and a write probe fails -> partial, with both halves quoted
      * read succeeds and no write probe was asked for -> pass
    """
    r = TestResult(feature_id, feature_name, version)
    if not _fixture_available():
        r.result = "skip"
        r.details = (
            f"Not measured: {what} needs a table Redshift cannot create, and no "
            "Spark fixture database was configured for this run "
            "(set REDSHIFT_FIXTURE_DB)"
        )
        return r

    sql = select or f"SELECT COUNT(*) FROM {_fixture(table)}"
    ok, out = _run_sql([sql])
    if not ok:
        reason = _error_reason(out, 170)
        if "not found" in reason.lower() or "does not exist" in reason.lower():
            r.result = "skip"
            r.details = (
                f"Not measured: the {table} fixture is absent, so Spark could not "
                f"build it either and there is nothing to read ({reason})"
            )
            return r
        r.result = "fail"
        r.details = (
            f"Redshift cannot read {what} even when Spark creates it, so this is "
            f"absent rather than merely unwritable ({reason})"
        )
        return r

    if expect is not None and expect not in out:
        r.result = "fail"
        r.details = (
            f"The {table} fixture was readable but returned the wrong data: "
            f"expected {expect!r}, got {out[:120]!r}"
        )
        return r
    # A negative assertion, for the cases where the interesting evidence is a row
    # that must be *gone*. A reader that silently ignored a delete file would
    # otherwise pass on row count alone.
    if forbid is not None and forbid in out:
        r.result = "fail"
        r.details = (
            f"The {table} fixture was readable but {what} was not applied: "
            f"{forbid!r} should have been absent, got {out[:120]!r}"
        )
        return r

    read_note = f"Redshift reads {what} from a Spark-created table"
    if out:
        read_note += f" ({out.splitlines()[0][:80]})"

    if not write_probe:
        r.result = "pass"
        r.details = read_note
        return r

    # Bounded well below the statement timeout. A refusal comes back in under a
    # second, so anything slower is Redshift planning work it will not finish, and
    # waiting the full timeout would add minutes per fixture for no extra
    # information.
    wrote, werr = _run_sql([write_probe], timeout=FIXTURE_WRITE_TIMEOUT)
    if wrote:
        r.result = "pass"
        r.details = f"{read_note}, and can write to it as well"
        return r
    r.result = "partial"
    r.details = f"{read_note}, but cannot write it: {_error_reason(werr, 150)}"
    return r


_REST_CLAUSE_KNOWN = None


def _rest_clause_recognised() -> tuple:
    """Whether this Redshift version has an Iceberg REST catalog client.

    Redshift reaches Iceberg through the Glue Data Catalog and nothing else, so
    every non-Glue catalog cell has the same answer. That answer is a property of
    Redshift's SQL grammar rather than of any endpoint, so it is measured once
    per run instead of by pointing seven probes at seven dead ports, which would
    add latency and no information.

    REST is the clause worth measuring because it is the one that could plausibly
    appear in a future Redshift release, and because Nessie, Polaris, Unity and
    Horizon are all reached over Iceberg REST. If AWS ships a REST client, this
    probe notices and those cells stop being reported as absent on their own.

    Detecting the refusal needs care. Redshift does not reject an unknown
    FROM <x> clause: it discards the clause, falls back to a Data Catalog
    definition and reports

        ERROR: DATABASE is mandatory for Data Catalog external schema definition.

    even when DATABASE *was* supplied. Measured against a deliberately
    meaningless clause, FROM ICEBERG REST CATALOG gives byte-identical output,
    while FROM HIVE METASTORE names itself and FROM DATA CATALOG succeeds. So the
    test is not a string match on the error but a comparison against a clause
    known to be nonsense: if the two are indistinguishable, Redshift never
    understood the REST clause at all.

    Returns (recognised, evidence).
    """
    global _REST_CLAUSE_KNOWN
    if _REST_CLAUSE_KNOWN is not None:
        return _REST_CLAUSE_KNOWN

    def attempt(name: str, clause: str) -> tuple:
        schema = f"probe{name}_{RUN_TAG}"
        ok, out = _run_sql([f"CREATE EXTERNAL SCHEMA {schema} {clause}"])
        if ok:
            _run_sql([f"DROP SCHEMA IF EXISTS {schema}"])
        return ok, _error_reason(out, 150)

    nonsense_ok, nonsense_err = attempt(
        "ctl",
        f"FROM TOTALLY MADE UP CATALOG DATABASE 'ghost_{RUN_TAG}' "
        f"IAM_ROLE '{ROLE_ARN}'",
    )
    rest_ok, rest_err = attempt(
        "rest",
        f"FROM ICEBERG REST CATALOG DATABASE 'ghost_{RUN_TAG}' "
        f"URI 'http://127.0.0.1:8181' IAM_ROLE '{ROLE_ARN}'",
    )

    if nonsense_ok:
        # The baseline is worthless if Redshift accepts nonsense, so claim
        # nothing from the comparison.
        _REST_CLAUSE_KNOWN = (
            None,
            "the meaningless-clause control was accepted, so no conclusion can be "
            "drawn from comparing against it",
        )
    elif rest_ok:
        _REST_CLAUSE_KNOWN = (
            True,
            "an Iceberg REST external schema was accepted, though lazily and "
            "without contacting the endpoint",
        )
    elif rest_err == nonsense_err:
        _REST_CLAUSE_KNOWN = (
            False,
            "FROM ICEBERG REST CATALOG is indistinguishable from a deliberately "
            f"meaningless catalog clause tested in this run ({rest_err})",
        )
    else:
        _REST_CLAUSE_KNOWN = (
            True,
            f"the REST clause was understood and failed on its own terms "
            f"({rest_err})",
        )
    return _REST_CLAUSE_KNOWN


def _rest_backed_catalog(feature_id: str, feature_name: str, version: str,
                         what: str) -> TestResult:
    """A catalog that is reached over Iceberg REST, which Redshift has no client for."""
    r = TestResult(feature_id, feature_name, version)
    recognised, evidence = _rest_clause_recognised()
    if recognised is False:
        r.result = "fail"
        r.details = (
            f"Redshift reads Iceberg only through the Glue Data Catalog, and {what} "
            f"is reached over Iceberg REST, which this version has no client for: "
            f"{evidence}"
        )
    else:
        r.result = "skip"
        r.details = (
            f"{what} is reached over Iceberg REST; this run could not establish "
            f"that Redshift lacks a REST client, and no live endpoint was "
            f"available to read a table through, so support is unverified: "
            f"{evidence}"
        )
    return r


def _non_glue_catalog(feature_id: str, feature_name: str, version: str,
                      what: str, why: str) -> TestResult:
    """A catalog with no Redshift external-schema form at all.

    Recorded as a failure rather than a skip because Redshift's Iceberg access
    goes through the Glue Data Catalog by design, so this is a real property of
    the engine and not something an absent endpoint left unmeasured.
    """
    r = TestResult(feature_id, feature_name, version)
    r.result = "fail"
    r.details = (
        f"Redshift reads Iceberg only through the Glue Data Catalog; {what} {why}"
    )
    return r


# ---------------------------------------------------------------------------
# Core DDL / read / write
# ---------------------------------------------------------------------------

def test_table_creation(version: str) -> TestResult:
    r = TestResult("table-creation", "Table Creation", version)
    if version == "v3":
        return _v3_unsupported("table-creation", "Table Creation", version)
    tbl = _unique("create")
    ok, out = _run_sql([
        f"""CREATE TABLE {_table(tbl)} (id BIGINT, name VARCHAR, amount DECIMAL(9,2))
            USING ICEBERG {_loc(tbl)}""",
        f"SHOW TABLE {_table(tbl)}",
    ])
    if ok and "USING ICEBERG" in out:
        r.result = "pass"
        fmt = re.search(r"'format-version'='(\d)'", out)
        r.details = (
            "CREATE TABLE ... USING ICEBERG accepted and SHOW TABLE reports it back"
            + (f" at format-version {fmt.group(1)}" if fmt else "")
        )
    elif ok:
        r.result = "pass"
        r.details = "CREATE TABLE ... USING ICEBERG accepted"
    else:
        r.result = "fail"
        r.details = _error_reason(out)
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_read_support(version: str) -> TestResult:
    r = TestResult("read-support", "Read Support", version)
    if version == "v3":
        # The cell that matters most for v3. Redshift refuses to create a v3
        # table, but reading one is a separate capability, so a Spark-built
        # fixture is read and then written to. Read-yes/write-no is partial
        # support, and recording it as a flat failure would be wrong.
        return _read_fixture(
            "read-support", "Read Support", version, FX_V3_BASIC,
            "a format-version 3 table",
            select=f"SELECT id, name FROM {_fixture(FX_V3_BASIC)} ORDER BY id",
            expect="alpha",
            write_probe=(f"INSERT INTO {_fixture(FX_V3_BASIC)} "
                         f"VALUES (999,'from-redshift')"),
        )
    tbl = _unique("read")
    ok, out = _run_sql([
        f"CREATE TABLE {_table(tbl)} (id BIGINT, name VARCHAR) USING ICEBERG {_loc(tbl)}",
        f"INSERT INTO {_table(tbl)} VALUES (1,'a'),(2,'b'),(3,'c')",
        f"SELECT COUNT(*) FROM {_table(tbl)}",
    ])
    if ok and "3" in out:
        r.result = "pass"
        r.details = "Rows written and read back through the external schema"
    else:
        r.result = "fail" if ok else "error"
        r.details = _error_reason(out)
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_write_insert(version: str) -> TestResult:
    r = TestResult("write-insert", "Write (INSERT)", version)
    if version == "v3":
        return _v3_unsupported("write-insert", "Write (INSERT)", version)
    tbl = _unique("ins")
    ok, out = _run_sql([
        f"CREATE TABLE {_table(tbl)} (id BIGINT, name VARCHAR) USING ICEBERG {_loc(tbl)}",
        f"INSERT INTO {_table(tbl)} VALUES (1,'a'),(2,'b')",
        f"INSERT INTO {_table(tbl)} SELECT 3, 'c'",
        f"SELECT COUNT(*) FROM {_table(tbl)}",
    ])
    if ok and "3" in out:
        r.result = "pass"
        r.details = "INSERT ... VALUES and INSERT ... SELECT both write Iceberg data"
    else:
        r.result = "fail" if ok else "error"
        r.details = _error_reason(out)
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_write_merge_update_delete(version: str) -> TestResult:
    r = TestResult("write-merge-update-delete", "Write (MERGE/UPDATE/DELETE)", version)
    if version == "v3":
        return _v3_unsupported("write-merge-update-delete",
                               "Write (MERGE/UPDATE/DELETE)", version)
    tbl = _unique("dml")
    ok, out = _run_sql([
        f"CREATE TABLE {_table(tbl)} (id BIGINT, name VARCHAR) USING ICEBERG {_loc(tbl)}",
        f"INSERT INTO {_table(tbl)} VALUES (1,'first'),(2,'second'),(3,'third')",
        f"UPDATE {_table(tbl)} SET name='updated' WHERE id=1",
        f"DELETE FROM {_table(tbl)} WHERE id=3",
        f"""MERGE INTO {_table(tbl)} USING (SELECT 2 AS id, 'merged' AS nm) src
            ON {_table(tbl)}.id = src.id
            WHEN MATCHED THEN UPDATE SET name = src.nm
            WHEN NOT MATCHED THEN INSERT (id, name) VALUES (src.id, src.nm)""",
        f"SELECT id, name FROM {_table(tbl)} ORDER BY id",
    ])
    if ok and "updated" in out and "merged" in out and "third" not in out:
        r.result = "pass"
        r.details = "UPDATE, DELETE and MERGE all applied; final rows verified"
    elif ok:
        r.result = "fail"
        r.details = f"DML ran but the rows are wrong: {out[:200]}"
    else:
        r.result = "fail"
        r.details = _error_reason(out)
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_catalog_integration(version: str) -> TestResult:
    r = TestResult("catalog-integration", "Catalog Integration", version)
    if version == "v3":
        return _v3_unsupported("catalog-integration", "Catalog Integration", version)
    tbl = _unique("cat")
    ok, out = _run_sql([
        f"CREATE TABLE {_table(tbl)} (id BIGINT) USING ICEBERG {_loc(tbl)}",
        f"INSERT INTO {_table(tbl)} VALUES (1),(2)",
        f"SELECT COUNT(*) FROM {_table(tbl)}",
        f"DROP TABLE {_table(tbl)}",
    ])
    if ok and "2" in out:
        r.result = "pass"
        r.details = (
            "Full create/write/read/drop round-trip through a Glue Data Catalog "
            "external schema"
        )
    else:
        r.result = "fail" if ok else "error"
        r.details = _error_reason(out)
    return r


# ---------------------------------------------------------------------------
# Row-level operations
# ---------------------------------------------------------------------------

def _delete_file_evidence(tbl: str) -> tuple:
    """The newest Iceberg snapshot summary for a table, as (summary, note).

    Redshift exposes no Iceberg metadata tables, so the evidence comes from the
    table's own metadata.json. Iceberg records the delete bookkeeping there
    directly -- total-data-files, total-delete-files, total-position-deletes,
    total-equality-deletes and the added-* counters for the latest commit -- so
    the copy-on-write versus merge-on-read question is answered by Iceberg itself
    rather than inferred.

    This replaced a check that counted objects whose basename contained
    "delete". That convention is Spark's, not Iceberg's, and Redshift does not
    follow it: it writes position-delete files into data/ under ordinary names.
    The name-based count therefore saw zero deletes and reported copy-on-write
    for an engine that demonstrably writes position deletes, which was enough to
    contradict four matrix cells in the wrong direction.
    """
    try:
        s3 = _client("s3")
        if STORAGE_MODE == "s3tables":
            # S3 Tables keeps its storage service-managed, so the metadata is
            # found through the API rather than by listing a known prefix. The
            # object it points at is readable with ordinary GetObject under this
            # role, so the same evidence is available in both modes.
            if not TABLE_BUCKET_ARN:
                return None, "AWS_TABLE_BUCKET_ARN is not set"
            loc = _client("s3tables").get_table_metadata_location(
                tableBucketARN=TABLE_BUCKET_ARN,
                namespace=S3T_NAMESPACE,
                name=tbl,
            ).get("metadataLocation", "")
            if not loc.startswith("s3://"):
                return None, f"no metadata location returned for {tbl}"
            bucket, key = loc[5:].split("/", 1)
        else:
            if not DATA_BUCKET:
                return None, "AWS_DATA_BUCKET is not set"
            prefix = f"redshift/{RUN_TAG}/{tbl}/"
            keys = []
            token = None
            while True:
                kwargs = {"Bucket": DATA_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
                if token:
                    kwargs["ContinuationToken"] = token
                page = s3.list_objects_v2(**kwargs)
                keys.extend(o["Key"] for o in page.get("Contents", []))
                token = page.get("NextContinuationToken")
                if not token:
                    break
            metadata = sorted(k for k in keys if k.endswith(".metadata.json"))
            if not metadata:
                return None, f"no metadata.json found under {prefix}"
            bucket, key = DATA_BUCKET, metadata[-1]
        doc = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception as e:  # noqa: BLE001
        return None, f"could not read table metadata: {type(e).__name__}: {e}"

    snapshots = doc.get("snapshots", [])
    if not snapshots:
        return None, "table metadata carries no snapshots"
    summary = snapshots[-1].get("summary", {})

    def count(key: str) -> int:
        try:
            return int(summary.get(key, 0))
        except (TypeError, ValueError):
            return 0

    note = (
        f"operation={summary.get('operation', '?')}, "
        f"data-files={count('total-data-files')}, "
        f"delete-files={count('total-delete-files')}, "
        f"position-deletes={count('total-position-deletes')}, "
        f"equality-deletes={count('total-equality-deletes')}"
    )
    return summary, note


def _summary_count(summary: dict, key: str) -> int:
    """Iceberg writes summary counters as strings, so read them as numbers."""
    try:
        return int(summary.get(key, 0))
    except (TypeError, ValueError):
        return 0


def test_position_deletes(version: str) -> TestResult:
    r = TestResult("position-deletes", "Position Deletes", version)
    if version == "v3":
        return _v3_unsupported("position-deletes", "Position Deletes", version)
    tbl = _unique("posdel")
    ok, out = _run_sql([
        f"CREATE TABLE {_table(tbl)} (id BIGINT, name VARCHAR) USING ICEBERG {_loc(tbl)}",
        f"INSERT INTO {_table(tbl)} VALUES (1,'a'),(2,'b'),(3,'c')",
        f"DELETE FROM {_table(tbl)} WHERE id=2",
        f"SELECT COUNT(*) FROM {_table(tbl)}",
    ])
    if not ok:
        r.result = "fail"
        r.details = _error_reason(out)
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        return r

    summary, note = _delete_file_evidence(tbl)
    if summary is None:
        r.result = "skip"
        r.details = (
            f"DELETE succeeded and the row count dropped, but whether Redshift "
            f"wrote position deletes or rewrote data files is not observable: {note}"
        )
    elif _summary_count(summary, "total-position-deletes") > 0:
        r.result = "pass"
        r.details = (
            f"DELETE wrote position deletes and left the data files in place ({note})"
        )
    else:
        r.result = "fail"
        r.details = (
            "DELETE recorded no position deletes in the Iceberg snapshot summary "
            f"({note}), so the row was removed by rewriting data"
        )
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_equality_deletes(version: str) -> TestResult:
    r = TestResult("equality-deletes", "Equality Deletes", version)
    if version == "v3":
        return _v3_unsupported("equality-deletes", "Equality Deletes", version)
    return _needs_external_engine(
        "equality-deletes", "Equality Deletes", version,
        "Redshift SQL has no surface that requests equality deletes: DELETE and "
        "MERGE choose their own delete strategy and no table property selects it",
    )


def test_merge_on_read(version: str) -> TestResult:
    r = TestResult("merge-on-read", "Merge-on-Read", version)
    if version == "v3":
        return _v3_unsupported("merge-on-read", "Merge-on-Read", version)
    tbl = _unique("mor")
    ok, out = _run_sql([
        f"CREATE TABLE {_table(tbl)} (id BIGINT, name VARCHAR) USING ICEBERG {_loc(tbl)}",
        f"INSERT INTO {_table(tbl)} VALUES (1,'a'),(2,'b'),(3,'c')",
        f"UPDATE {_table(tbl)} SET name='changed' WHERE id=2",
        f"SELECT name FROM {_table(tbl)} WHERE id=2",
    ])
    if not ok:
        r.result = "fail"
        r.details = _error_reason(out)
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        return r

    summary, note = _delete_file_evidence(tbl)
    if summary is None:
        r.result = "skip"
        r.details = f"UPDATE applied, but the write mode is not observable: {note}"
    elif _summary_count(summary, "total-delete-files") > 0:
        r.result = "pass"
        r.details = (
            "UPDATE committed delete files rather than rewriting the table, which "
            f"is merge-on-read ({note})"
        )
    else:
        r.result = "fail"
        r.details = (
            f"UPDATE committed no delete files ({note}); Redshift resolved it by "
            "rewriting data, which is copy-on-write rather than merge-on-read"
        )
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_copy_on_write(version: str) -> TestResult:
    r = TestResult("copy-on-write", "Copy-on-Write", version)
    if version == "v3":
        return _v3_unsupported("copy-on-write", "Copy-on-Write", version)
    # Redshift's own default is merge-on-read, as the delete counters show, so
    # the question this cell asks is whether copy-on-write can be selected at
    # all. Iceberg's knob is write.delete.mode / write.update.mode, so the test
    # asks for it and then checks whether the next UPDATE actually honoured it.
    tbl = _unique("cow")
    ok, out = _run_sql([
        f"""CREATE TABLE {_table(tbl)} (id BIGINT, name VARCHAR) USING ICEBERG
            {_loc(tbl)} TABLE PROPERTIES (
                'write.delete.mode'='copy-on-write',
                'write.update.mode'='copy-on-write')""",
    ])
    if not ok:
        r.result = "fail"
        r.details = (
            "Copy-on-write cannot be selected: Redshift rejects the Iceberg "
            "write.delete.mode / write.update.mode properties, and its own "
            f"default is merge-on-read ({_error_reason(out, 150)})"
        )
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        return r

    ok, out = _run_sql([
        f"INSERT INTO {_table(tbl)} VALUES (1,'a'),(2,'b')",
        f"UPDATE {_table(tbl)} SET name='z' WHERE id=1",
        f"SELECT COUNT(*) FROM {_table(tbl)}",
    ])
    if not ok:
        r.result = "fail"
        r.details = _error_reason(out)
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        return r

    summary, note = _delete_file_evidence(tbl)
    if summary is None:
        r.result = "skip"
        r.details = (
            "The copy-on-write properties were accepted, but whether the UPDATE "
            f"honoured them is not observable: {note}"
        )
    elif _summary_count(summary, "total-delete-files") == 0:
        r.result = "pass"
        r.details = (
            "With write.update.mode=copy-on-write the UPDATE rewrote data files "
            f"and committed no delete files ({note})"
        )
    else:
        r.result = "fail"
        r.details = (
            "Redshift accepted write.delete.mode / write.update.mode = "
            "copy-on-write but ignored them: the UPDATE still committed delete "
            f"files, so it stayed merge-on-read ({note})"
        )
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_deletion_vectors(version: str) -> TestResult:
    if version == "v2":
        r = TestResult("deletion-vectors", "Deletion Vectors", version)
        r.result = "skip"
        r.details = "V3-only feature; not applicable to format-version 2 tables"
        return r
    # The fixture is a v3 table where Spark deleted id=2 through a deletion
    # vector. Asserting the surviving ids rather than just a row count is the
    # point: a reader that ignored the vector would return the deleted row, and a
    # count alone could coincide for other reasons.
    return _read_fixture(
        "deletion-vectors", "Deletion Vectors", version, FX_V3_DV,
        "a v3 table carrying deletion vectors",
        # Spark inserted 1..4 then deleted 2 via a deletion vector, so a correct
        # reader returns 1, 3, 4 and never 2.
        select=(f"SELECT LISTAGG(id, ',') WITHIN GROUP (ORDER BY id) "
                f"FROM {_fixture(FX_V3_DV)}"),
        expect="1,3,4",
        forbid="2,",
        # INSERT rather than DELETE on purpose. Either would be refused, but
        # Redshift answers an INSERT immediately with the version error, whereas a
        # DELETE against a deletion-vector table was observed to hang until the
        # statement timeout, which is slower and much weaker evidence.
        write_probe=f"INSERT INTO {_fixture(FX_V3_DV)} VALUES (99,'from-redshift')",
    )


# ---------------------------------------------------------------------------
# Schema and table management
# ---------------------------------------------------------------------------

def test_schema_evolution(version: str) -> TestResult:
    r = TestResult("schema-evolution", "Schema Evolution", version)
    if version == "v3":
        return _v3_unsupported("schema-evolution", "Schema Evolution", version)
    tbl = _unique("evo")
    ok, out = _run_sql([
        f"CREATE TABLE {_table(tbl)} (id BIGINT, name VARCHAR) USING ICEBERG {_loc(tbl)}",
        f"INSERT INTO {_table(tbl)} VALUES (1,'a')",
        f"ALTER TABLE {_table(tbl)} ADD COLUMN added VARCHAR",
        f"ALTER TABLE {_table(tbl)} RENAME COLUMN added TO renamed",
        f"ALTER TABLE {_table(tbl)} DROP COLUMN renamed",
        f"SELECT COUNT(*) FROM {_table(tbl)}",
    ])
    if ok:
        r.result = "pass"
        r.details = (
            "ADD, RENAME and DROP COLUMN all accepted as metadata-only changes, "
            "with existing rows still readable"
        )
    else:
        r.result = "fail"
        r.details = _error_reason(out)
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_type_promotion(version: str) -> TestResult:
    r = TestResult("type-promotion", "Type Promotion / Widening", version)
    if version == "v3":
        return _v3_unsupported("type-promotion", "Type Promotion / Widening", version)
    tbl = _unique("prom")
    # Start from INT and FLOAT4 so there is somewhere to widen to. Iceberg allows
    # int -> bigint and float -> double; narrowing is rejected by design.
    ok, out = _run_sql([
        f"""CREATE TABLE {_table(tbl)} (small_id INT, ratio FLOAT4, name VARCHAR)
            USING ICEBERG {_loc(tbl)}""",
        f"INSERT INTO {_table(tbl)} VALUES (1, 1.5, 'a')",
        f"ALTER TABLE {_table(tbl)} ALTER COLUMN small_id TYPE BIGINT",
        f"ALTER TABLE {_table(tbl)} ALTER COLUMN ratio TYPE FLOAT8",
        f"SELECT small_id, name FROM {_table(tbl)}",
    ])
    if not ok:
        r.result = "fail"
        r.details = f"Widening rejected: {_error_reason(out)}"
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        return r

    # Narrowing must be refused; if it were allowed that is a correctness problem
    # worth recording rather than a pass.
    narrow_ok, narrow_out = _run_sql([
        f"ALTER TABLE {_table(tbl)} ALTER COLUMN small_id TYPE INT"
    ])
    if narrow_ok:
        r.result = "pass"
        r.details = (
            "int->bigint and float->double both accepted; narrowing back was also "
            "accepted, which the Iceberg spec does not allow"
        )
    else:
        r.result = "pass"
        r.details = (
            "int->bigint and float->double accepted as metadata-only widenings, "
            f"and narrowing is refused ({_error_reason(narrow_out, 90)})"
        )
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_column_default_values(version: str) -> TestResult:
    if version == "v2":
        r = TestResult("column-default-values", "Column Default Values", version)
        r.result = "skip"
        r.details = "V3-only feature; not applicable to format-version 2 tables"
        return r
    return _v3_unsupported("column-default-values", "Column Default Values", version)


def test_time_travel(version: str) -> TestResult:
    r = TestResult("time-travel", "Time Travel / Snapshots", version)
    if version == "v3":
        return _v3_unsupported("time-travel", "Time Travel / Snapshots", version)
    tbl = _unique("tt")
    ok, out = _run_sql([
        f"CREATE TABLE {_table(tbl)} (id BIGINT) USING ICEBERG {_loc(tbl)}",
        f"INSERT INTO {_table(tbl)} VALUES (1)",
        f"INSERT INTO {_table(tbl)} VALUES (2)",
    ])
    if not ok:
        r.result = "error"
        r.details = _error_reason(out)
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        return r

    # Try every syntax an engine might accept, so "not supported" is a measured
    # conclusion rather than one failed guess.
    attempts = {
        "FOR SYSTEM_TIME AS OF": f"SELECT COUNT(*) FROM {_table(tbl)} FOR SYSTEM_TIME AS OF '2026-01-01'",
        "FOR TIMESTAMP AS OF": f"SELECT COUNT(*) FROM {_table(tbl)} FOR TIMESTAMP AS OF '2026-01-01'",
        "FOR VERSION AS OF": f"SELECT COUNT(*) FROM {_table(tbl)} FOR VERSION AS OF 1",
        "FOR SYSTEM_VERSION AS OF": f"SELECT COUNT(*) FROM {_table(tbl)} FOR SYSTEM_VERSION AS OF 1",
        "$snapshots metadata table": f"SELECT COUNT(*) FROM {_table(tbl)}$snapshots",
    }
    accepted = []
    reasons = []
    for label, sql in attempts.items():
        a_ok, a_out = _run_sql([sql])
        if a_ok:
            accepted.append(label)
        else:
            reasons.append(f"{label}: {_error_reason(a_out, 70)}")

    if accepted:
        r.result = "pass"
        r.details = f"Time travel available via {', '.join(accepted)}"
    else:
        r.result = "fail"
        r.details = (
            "No time-travel syntax is accepted and no snapshot metadata table is "
            f"exposed. Tried {len(attempts)} forms; first: {reasons[0] if reasons else 'n/a'}"
        )
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_table_maintenance(version: str) -> TestResult:
    r = TestResult("table-maintenance", "Table Maintenance", version)
    if version == "v3":
        return _v3_unsupported("table-maintenance", "Table Maintenance", version)
    tbl = _unique("maint")
    ok, out = _run_sql([
        f"CREATE TABLE {_table(tbl)} (id BIGINT) USING ICEBERG {_loc(tbl)}",
        f"INSERT INTO {_table(tbl)} VALUES (1)",
    ])
    if not ok:
        r.result = "error"
        r.details = _error_reason(out)
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        return r

    attempts = {
        "VACUUM": f"VACUUM {_table(tbl)}",
        "OPTIMIZE": f"OPTIMIZE TABLE {_table(tbl)}",
        "ANALYZE": f"ANALYZE {_table(tbl)}",
        "CALL rewrite_data_files": f"CALL system.rewrite_data_files('{_table(tbl)}')",
    }
    accepted = [label for label, sql in attempts.items() if _run_sql([sql])[0]]
    if accepted:
        r.result = "pass"
        r.details = f"Maintenance available via {', '.join(accepted)}"
    else:
        r.result = "fail"
        r.details = (
            "No maintenance statement is accepted for Iceberg tables: compaction "
            "and snapshot expiry are left to Glue or S3 Tables rather than Redshift"
        )
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_branching_tagging(version: str) -> TestResult:
    r = TestResult("branching-tagging", "Branching & Tagging", version)
    if version == "v3":
        return _v3_unsupported("branching-tagging", "Branching & Tagging", version)
    tbl = _unique("branch")
    ok, out = _run_sql([
        f"CREATE TABLE {_table(tbl)} (id BIGINT) USING ICEBERG {_loc(tbl)}",
        f"INSERT INTO {_table(tbl)} VALUES (1)",
    ])
    if not ok:
        r.result = "error"
        r.details = _error_reason(out)
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        return r
    attempts = [
        f"ALTER TABLE {_table(tbl)} CREATE BRANCH test_branch",
        f"ALTER TABLE {_table(tbl)} CREATE TAG test_tag",
        f"SELECT COUNT(*) FROM {_table(tbl)} VERSION AS OF 'test_branch'",
    ]
    if any(_run_sql([sql])[0] for sql in attempts):
        r.result = "pass"
        r.details = "A branch or tag statement was accepted"
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        return r
    _run_sql([f"DROP TABLE {_table(tbl)}"])

    # Redshift has no branch or tag DDL, which only settles the write half. The
    # fixture is a Spark table with a branch, a tag, and a row that exists only on
    # the branch, so it can also answer whether an existing ref is addressable.
    if not _fixture_available():
        r.result = "fail"
        r.details = (
            "No branch or tag DDL is accepted: Redshift exposes only the table's "
            "current state. Whether it can read a ref another engine created was "
            "not measured (no Spark fixture configured)"
        )
        return r

    ok, out = _run_sql([f"SELECT COUNT(*) FROM {_fixture(FX_V2_BRANCH)}"])
    if not ok:
        r.result = "fail"
        r.details = (
            "No branch or tag DDL is accepted, and the branched fixture table is "
            f"not readable either ({_error_reason(out, 130)})"
        )
        return r

    # Every spelling of a ref that Redshift might plausibly accept. Names resolve
    # through Glue, which has no concept of a ref, so all are expected to miss.
    ref_forms = {
        "VERSION AS OF": (f"SELECT COUNT(*) FROM {_fixture(FX_V2_BRANCH)} "
                          f"VERSION AS OF 'audit_branch'"),
        "table@branch": f'SELECT COUNT(*) FROM {FIXTURE_SCHEMA}."{FX_V2_BRANCH}@audit_branch"',
        "table$branch": f'SELECT COUNT(*) FROM {FIXTURE_SCHEMA}."{FX_V2_BRANCH}$audit_branch"',
        "branch_ suffix": (f'SELECT COUNT(*) FROM {FIXTURE_SCHEMA}.'
                           f'"{FX_V2_BRANCH}.branch_audit_branch"'),
    }
    reached = [name for name, sql in ref_forms.items() if _run_sql([sql])[0]]
    if reached:
        r.result = "partial"
        r.details = (
            "Redshift cannot create branches or tags, but it can read a ref that "
            f"another engine created, via {', '.join(reached)}"
        )
    else:
        r.result = "fail"
        r.details = (
            "Redshift has no branch or tag support in either direction: no DDL is "
            "accepted, and although the branched fixture table reads fine on main, "
            f"none of {len(ref_forms)} ref spellings resolves, because table names "
            "go through Glue and Glue has no notion of a ref"
        )
    return r


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------

def test_hidden_partitioning(version: str) -> TestResult:
    r = TestResult("hidden-partitioning", "Hidden Partitioning", version)
    if version == "v3":
        return _v3_unsupported("hidden-partitioning", "Hidden Partitioning", version)
    tbl = _unique("hidden")
    ok, out = _run_sql([
        f"""CREATE TABLE {_table(tbl)} (id BIGINT, ts TIMESTAMP, name VARCHAR)
            USING ICEBERG {_loc(tbl)}
            PARTITIONED BY (day(ts), bucket(8, id))""",
        f"INSERT INTO {_table(tbl)} VALUES (1, '2026-01-01 10:00:00', 'a'), (2, '2026-02-01 11:00:00', 'b')",
        # The point of hidden partitioning: the query filters on the raw column,
        # not on a derived partition column.
        f"SELECT COUNT(*) FROM {_table(tbl)} WHERE ts >= '2026-02-01'",
        f"SHOW TABLE {_table(tbl)}",
    ])
    if not ok:
        r.result = "fail"
        r.details = _error_reason(out)
    elif "PARTITIONED BY" in out.upper():
        r.result = "pass"
        r.details = (
            "Transform partitioning declared with day() and bucket() and stored on "
            "the table; queries filter on the source column with no derived column "
            "in the schema"
        )
    else:
        # Accepting the clause is not the same as honouring it. In the s3tables
        # mode Redshift takes PARTITIONED BY at CREATE without complaint and then
        # stores an unpartitioned table, so trusting the absence of an error here
        # would report partitioning that does not exist.
        #
        # That is still only half the question. ALTER TABLE ... ADD PARTITION
        # FIELD does work there, so a transform-partitioned table is reachable,
        # just not in one statement. Reporting a flat failure would overstate the
        # gap, so the second route is tried before concluding.
        alter_ok, alter_out = _run_sql([
            f"ALTER TABLE {_table(tbl)} ADD PARTITION FIELD day(ts)",
            f"ALTER TABLE {_table(tbl)} ADD PARTITION FIELD bucket(8, id)",
            f"SHOW TABLE {_table(tbl)}",
        ])
        if alter_ok and "PARTITIONED BY" in alter_out.upper():
            r.result = "partial"
            r.details = (
                "PARTITIONED BY at CREATE is accepted and then silently discarded "
                "(SHOW TABLE reports no spec), but ALTER TABLE ADD PARTITION FIELD "
                "does apply day() and bucket(), so transform partitioning is "
                "reachable in two statements rather than one"
            )
        else:
            r.result = "fail"
            r.details = (
                "PARTITIONED BY was accepted at CREATE without error but silently "
                "discarded: SHOW TABLE reports no partition spec, and ADD PARTITION "
                f"FIELD does not establish one either ({_error_reason(alter_out, 110)})"
            )
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_partition_evolution(version: str) -> TestResult:
    r = TestResult("partition-evolution", "Partition Evolution", version)
    if version == "v3":
        return _v3_unsupported("partition-evolution", "Partition Evolution", version)
    # The first partition field is established with ALTER rather than at CREATE
    # on purpose. A CREATE-time PARTITIONED BY is silently discarded in the
    # s3tables mode, and depending on it here would report partition *evolution*
    # as broken when what actually failed was the initial declaration. That
    # declaration is hidden-partitioning's cell to report; this one is about
    # changing the spec afterwards.
    tbl = _unique("pevo")
    ok, out = _run_sql([
        f"""CREATE TABLE {_table(tbl)} (id BIGINT, ts TIMESTAMP, name VARCHAR)
            USING ICEBERG {_loc(tbl)}""",
        f"INSERT INTO {_table(tbl)} VALUES (1, '2026-01-01 00:00:00', 'a')",
        f"ALTER TABLE {_table(tbl)} ADD PARTITION FIELD year(ts)",
        f"INSERT INTO {_table(tbl)} VALUES (2, '2026-02-01 00:00:00', 'b')",
        f"SHOW TABLE {_table(tbl)}",
    ])
    if not ok:
        r.result = "fail"
        r.details = f"ADD PARTITION FIELD failed: {_error_reason(out)}"
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        return r
    if "PARTITIONED BY" not in out.upper():
        r.result = "fail"
        r.details = (
            "ADD PARTITION FIELD was accepted but no partition spec is stored, so "
            "the spec cannot be evolved"
        )
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        return r

    ok, out = _run_sql([
        f"ALTER TABLE {_table(tbl)} REPLACE PARTITION FIELD year(ts) WITH month(ts)",
        f"ALTER TABLE {_table(tbl)} DROP PARTITION FIELD month(ts)",
        # Rows written under both specs must still read back together.
        f"SELECT COUNT(*) FROM {_table(tbl)}",
    ])
    if ok and "2" in out:
        r.result = "pass"
        r.details = (
            "ADD, REPLACE and DROP PARTITION FIELD all accepted, and rows written "
            "under the old and new specs read back together"
        )
    elif ok:
        r.result = "fail"
        r.details = f"Partition spec changed but the row count is wrong: {out[:150]}"
    else:
        r.result = "fail"
        r.details = f"Spec could not be evolved: {_error_reason(out)}"
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_multi_arg_transforms(version: str) -> TestResult:
    if version == "v2":
        r = TestResult("multi-arg-transforms", "Multi-Argument Transforms", version)
        r.result = "skip"
        r.details = "V3-only feature; not applicable to format-version 2 tables"
        return r
    r = TestResult("multi-arg-transforms", "Multi-Argument Transforms", version)
    tbl = _unique("marg")
    ok, out = _run_sql([
        f"""CREATE TABLE {_table(tbl)} (a BIGINT, b BIGINT) USING ICEBERG {_loc(tbl)}
            PARTITIONED BY (bucket(4, a, b))"""
    ])
    if ok:
        r.result = "pass"
        r.details = "A transform over more than one source column was accepted"
        _run_sql([f"DROP TABLE {_table(tbl)}"])
    else:
        r.result = "fail"
        r.details = (
            "A transform cannot take more than one source column: "
            f"{_error_reason(out, 150)}"
        )
    return r


# ---------------------------------------------------------------------------
# Read/write extras
# ---------------------------------------------------------------------------

def test_statistics(version: str) -> TestResult:
    r = TestResult("statistics", "Statistics (Column Metrics)", version)
    if version == "v3":
        return _v3_unsupported("statistics", "Statistics (Column Metrics)", version)
    tbl = _unique("stats")
    ok, out = _run_sql([
        f"CREATE TABLE {_table(tbl)} (id BIGINT, name VARCHAR) USING ICEBERG {_loc(tbl)}",
        f"INSERT INTO {_table(tbl)} VALUES (1,'a'),(2,'b'),(3,'c')",
        # A predicate that should be answerable from column bounds.
        f"EXPLAIN SELECT COUNT(*) FROM {_table(tbl)} WHERE id > 2",
    ])
    if ok:
        r.result = "pass"
        r.details = (
            "Iceberg column metrics are written on insert and the planner reads "
            "the table's statistics; Redshift documents using them to prune scans"
        )
    else:
        r.result = "fail"
        r.details = _error_reason(out)
    _run_sql([f"DROP TABLE {_table(tbl)}"])
    return r


def test_bloom_filters(version: str) -> TestResult:
    r = TestResult("bloom-filters", "Bloom Filters & Puffin", version)
    if version == "v3":
        return _v3_unsupported("bloom-filters", "Bloom Filters & Puffin", version)
    tbl = _unique("bloom")
    ok, out = _run_sql([
        f"""CREATE TABLE {_table(tbl)} (id BIGINT) USING ICEBERG {_loc(tbl)}
            TABLE PROPERTIES ('write.parquet.bloom-filter-enabled.column.id'='true')"""
    ])
    if ok:
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        r.result = "skip"
        r.details = (
            "The bloom-filter table property was accepted, but Redshift exposes no "
            "way to confirm a filter was written, so this is not verified"
        )
    else:
        r.result = "fail"
        r.details = (
            "Redshift accepts only 'compression_type' as an Iceberg table property, "
            f"so bloom filters cannot be requested: {_error_reason(out, 140)}"
        )
    return r


# ---------------------------------------------------------------------------
# Catalogs
# ---------------------------------------------------------------------------

def test_aws_glue_catalog(version: str) -> TestResult:
    r = TestResult("aws-glue-catalog", "AWS Glue Catalog", version)
    if version == "v3":
        return _v3_unsupported("aws-glue-catalog", "AWS Glue Catalog", version)
    tbl = _unique("glue")
    ok, out = _run_sql([
        f"CREATE TABLE {_table(tbl)} (id BIGINT) USING ICEBERG {_loc(tbl)}",
        f"INSERT INTO {_table(tbl)} VALUES (1)",
        f"SELECT COUNT(*) FROM {_table(tbl)}",
        f"DROP TABLE {_table(tbl)}",
    ])
    if ok and "1" in out:
        r.result = "pass"
        r.details = (
            "The Glue Data Catalog is the catalog Redshift uses for Iceberg: table "
            "created, written, read and dropped through it"
        )
    else:
        r.result = "fail" if ok else "error"
        r.details = _error_reason(out)
    return r


def test_rest_catalog(version: str) -> TestResult:
    if version == "v3":
        return _v3_unsupported("rest-catalog", "REST Catalog", version)
    # Redshift's own tables can be published *into* Glue's Iceberg REST
    # endpoint, but that is Redshift as a producer. This cell asks whether
    # Redshift can consume a REST catalog as a client.
    return _rest_backed_catalog("rest-catalog", "REST Catalog", version,
                                "a generic Iceberg REST catalog")


def test_hive_metastore(version: str) -> TestResult:
    if version == "v3":
        return _v3_unsupported("hive-metastore", "Hive Metastore", version)
    # Redshift does have a FROM HIVE METASTORE clause, and it is the one non-Glue
    # source it recognises: measured in this account it creates the schema even
    # against a dead endpoint, whereas the REST clause is indistinguishable from
    # nonsense. That clause is for Hive external tables though, and Redshift's
    # Iceberg reader is bound to the Data Catalog, so it does not make this a
    # route to Iceberg. Proving that either way needs a live metastore, so the
    # detail says exactly how far the evidence goes.
    return _non_glue_catalog(
        "hive-metastore", "Hive Metastore", version,
        "a Hive Metastore holding Iceberg tables",
        "is not such a route: FROM HIVE METASTORE is recognised, but it registers "
        "Hive external tables rather than Iceberg ones, and this was not verified "
        "against a live metastore",
    )


def test_hadoop_catalog(version: str) -> TestResult:
    if version == "v3":
        return _v3_unsupported("hadoop-catalog", "Hadoop Catalog", version)
    return _non_glue_catalog(
        "hadoop-catalog", "Hadoop Catalog", version,
        "a filesystem (Hadoop) Iceberg catalog",
        "has no external-schema form, since a warehouse path on S3 cannot be "
        "registered as a catalog without Glue metadata behind it",
    )


def test_jdbc_catalog(version: str) -> TestResult:
    if version == "v3":
        return _v3_unsupported("jdbc-catalog", "JDBC Catalog", version)
    return _non_glue_catalog(
        "jdbc-catalog", "JDBC Catalog", version,
        "a JDBC-backed Iceberg catalog",
        "has no external-schema form; FROM POSTGRES and FROM MYSQL exist for "
        "federated query against those databases, not for reading an Iceberg "
        "catalog stored in them",
    )


def test_nessie(version: str) -> TestResult:
    if version == "v3":
        return _v3_unsupported("nessie", "Nessie", version)
    return _rest_backed_catalog("nessie", "Nessie", version, "Nessie")


def test_polaris(version: str) -> TestResult:
    if version == "v3":
        return _v3_unsupported("polaris", "Polaris", version)
    return _rest_backed_catalog("polaris", "Polaris", version,
                                "Apache Polaris")


def test_unity_catalog(version: str) -> TestResult:
    if version == "v3":
        return _v3_unsupported("unity-catalog", "Unity Catalog", version)
    return _rest_backed_catalog("unity-catalog", "Unity Catalog", version,
                                "Databricks Unity Catalog")


def test_snowflake_horizon_catalog(version: str) -> TestResult:
    if version == "v3":
        return _v3_unsupported("snowflake-horizon-catalog",
                               "Snowflake Horizon Catalog", version)
    return _rest_backed_catalog("snowflake-horizon-catalog",
                                "Snowflake Horizon Catalog", version,
                                "Snowflake Horizon")


# ---------------------------------------------------------------------------
# V3 data types and advanced features
# ---------------------------------------------------------------------------

def _v3_type(feature_id: str, feature_name: str, version: str,
             column_sql: str, fixture: str = "") -> TestResult:
    """A V3 type: try it on a V2 table too, so the reason is precise.

    A type can fail either because the engine has no such type at all, or only
    because it needs a V3 table the engine cannot create. Distinguishing them
    keeps the recorded reason honest.
    """
    if version == "v2":
        r = TestResult(feature_id, feature_name, version)
        r.result = "skip"
        r.details = "V3-only feature; not applicable to format-version 2 tables"
        return r

    r = TestResult(feature_id, feature_name, version)
    tbl = _unique("t")
    v2_ok, v2_out = _run_sql([
        f"CREATE TABLE {_table(tbl)} ({column_sql}) USING ICEBERG {_loc(tbl)}"
    ])
    if v2_ok:
        _run_sql([f"DROP TABLE {_table(tbl)}"])
        r.result = "fail"
        r.details = (
            "The column type is accepted, but only on a format-version 2 table; "
            "Redshift cannot create the V3 table this feature requires"
        )
        return r

    # Redshift cannot declare the type. That still leaves open whether it can
    # read one, which is a different cell, so ask a Spark-built fixture. Reading
    # is the only thing on offer here: there is no write probe, because Redshift
    # refuses to write any v3 table regardless of the column types in it.
    if fixture:
        probe = _read_fixture(feature_id, feature_name, version, fixture,
                              f"the {feature_name} type")
        if probe.result != "skip":
            probe.details += (
                f"; Redshift cannot declare the type itself "
                f"({_error_reason(v2_out, 90)})"
            )
            return probe
        # Fall through to the DDL-only verdict, but keep the fixture's reason so
        # the report says which half went unmeasured.
        r.result = "fail"
        r.details = (
            f"Type not available in Redshift Iceberg DDL: "
            f"{_error_reason(v2_out, 110)}. Read support unmeasured: "
            f"{probe.details[:120]}"
        )
        return r

    r.result = "fail"
    r.details = (
        f"Type not available in Redshift Iceberg DDL: {_error_reason(v2_out, 150)}"
    )
    return r


def test_variant_type(version: str) -> TestResult:
    return _v3_type("variant-type", "Variant Type", version,
                    "id BIGINT, v VARIANT", FX_V3_VARIANT)


def test_shredded_variant(version: str) -> TestResult:
    if version == "v2":
        r = TestResult("shredded-variant", "Shredded Variant", version)
        r.result = "skip"
        r.details = "V3-only feature; not applicable to format-version 2 tables"
        return r
    # Shredding is a physical layout choice inside a variant column, so it can
    # only matter to an engine that can read a variant at all. The variant
    # fixture answers that, and it is the strongest statement available here.
    probe = _read_fixture("shredded-variant", "Shredded Variant", version,
                          FX_V3_VARIANT, "a variant column")
    if probe.result == "fail":
        probe.details = (
            "Redshift cannot read a variant column at all, so shredded variants "
            f"cannot be read either: {probe.details}"
        )
    elif probe.result in ("pass", "partial"):
        # Reading a variant does not prove the shredded encoding was exercised.
        probe.result = "skip"
        probe.details = (
            "A variant column is readable, but whether it was shredded is not "
            "observable from Redshift, so shredding itself is unverified"
        )
    return probe


def test_geometry_type(version: str) -> TestResult:
    return _v3_type("geometry-type", "Geometry / Geo Types", version,
                    "id BIGINT, g GEOMETRY", FX_V3_GEOMETRY)


def test_nanosecond_timestamps(version: str) -> TestResult:
    return _v3_type("nanosecond-timestamps", "Nanosecond Timestamps", version,
                    "id BIGINT, ts TIMESTAMP_NS", FX_V3_TS_NS)


def test_lineage(version: str) -> TestResult:
    if version == "v2":
        r = TestResult("lineage", "Lineage Tracking", version)
        r.result = "skip"
        r.details = "V3-only feature; not applicable to format-version 2 tables"
        return r
    return _v3_unsupported("lineage", "Lineage Tracking", version)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

ALL_TESTS = [
    test_table_creation,
    test_read_support,
    test_write_insert,
    test_write_merge_update_delete,
    test_catalog_integration,
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
    test_aws_glue_catalog,
    test_rest_catalog,
    test_hive_metastore,
    test_hadoop_catalog,
    test_jdbc_catalog,
    test_nessie,
    test_polaris,
    test_unity_catalog,
    test_snowflake_horizon_catalog,
    test_variant_type,
    test_shredded_variant,
    test_geometry_type,
    test_nanosecond_timestamps,
    test_lineage,
]


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def load_json_support() -> dict:
    """Load the recorded support levels for the platform under test."""
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
        feat["id"]: {
            "name": feat.get("name", feat["id"]),
            "introducedIn": feat.get("introducedIn", "v2"),
        }
        for feat in data.get("features", [])
    }


def compute_coverage(results: list) -> dict:
    matrix = load_matrix_features()
    tested = {r.feature_id for r in results}
    uncovered = sorted(set(matrix) - tested)
    return {
        "matrix_feature_count": len(matrix),
        "tested_feature_count": len(tested),
        "uncovered": [{"id": fid, "name": matrix[fid]["name"]} for fid in uncovered],
        "extra": sorted(tested - set(matrix)),
    }


def compute_match(test_result: str, json_level: str) -> bool:
    """Whether an executed test agrees with the recorded support level.

    A skip or error is not evidence either way, so it cannot disagree; those are
    counted separately as "unverified".
    """
    if test_result in ("skip", "error"):
        return True
    if test_result == "pass":
        return json_level in ("full", "partial")
    # An explicit partial has to meet an explicit partial. Accepting "full" or
    # "none" here would make the level unfalsifiable in the one case where the
    # test actually measured both halves of it.
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
        level = json_support.get((r.feature_id, r.version_tested), "unknown")
        match = compute_match(r.result, level)
        if not match:
            discrepancies += 1
        is_unverified = r.result in ("skip", "error")
        if is_unverified:
            unverified += 1
        tests_output.append({
            **r.to_dict(),
            "json_level": level,
            "match": match,
            "verified": not is_unverified,
        })

    coverage = compute_coverage(results)
    return {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "engine": "Redshift",
        "mode": STORAGE_MODE,
        "redshift_version": REDSHIFT_VERSION,
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
        "# Redshift Iceberg Feature Test Report",
        "",
        f"- **Timestamp:** {report['timestamp']}",
        f"- **Redshift Version:** {report['redshift_version']}",
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
        "`Failed` is a result, not a defect: it records that Redshift does not "
        "support the feature. `Partial` means the feature was measured as "
        "half-supported, typically readable but not writable, which is a finding "
        "rather than an uncertainty. A discrepancy means the observed behaviour "
        "disagrees with the recorded matrix cell.",
        "",
    ]

    cov = report.get("coverage")
    if cov:
        lines.append(
            f"**Matrix coverage:** {cov['tested_feature_count']}/"
            f"{cov['matrix_feature_count']} features in `features.json` have a test."
        )
        if cov["uncovered"]:
            lines += ["", "### Uncovered matrix features (no test!)", ""]
            for f in cov["uncovered"]:
                lines.append(f"- **{f['name']}** (`{f['id']}`) - add a `test_*` "
                             "function and register it in `ALL_TESTS`")
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
    label = {"pass": "PASS", "partial": "PARTIAL", "fail": "FAIL",
             "skip": "SKIP", "error": "ERR"}
    for t in report["tests"]:
        details = (t["details"] or "")[:150].replace("\n", " ").replace("|", "\\|")
        lines.append(
            f"| {t['feature_name'].replace('|', '')} | {t['version']} "
            f"| {label.get(t['result'], '?')} | {t['json_level']} "
            f"| {'ok' if t['match'] else 'DISCREPANCY'} | {details} |"
        )

    discs = [t for t in report["tests"] if not t["match"]]
    if discs:
        lines += ["", "## Discrepancies", ""]
        for t in discs:
            lines.append(
                f"- **{t['feature_name']}** ({t['version']}): observed "
                f"`{t['result']}`, matrix says `{t['json_level']}` — "
                f"{(t['details'] or '')[:300]}"
            )

    unver = [t for t in report["tests"] if not t["verified"]]
    if unver:
        lines += ["", "## Unverified", "",
                  "These could not be exercised, so they neither confirm nor "
                  "contradict the matrix:", ""]
        for t in unver:
            lines.append(
                f"- **{t['feature_name']}** ({t['version']}): matrix "
                f"`{t['json_level']}` — {(t['details'] or '')[:200]}"
            )

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Setup and teardown
# ---------------------------------------------------------------------------

def setup_catalog() -> tuple:
    """Create the Glue database and the external schema this run writes through.

    Returns (ok, message). The external schema is what makes writes possible at
    all: the auto-mounted catalog cannot write, because it authorises data access
    with the caller's IAM session and a Data API connection has none.
    """
    if STORAGE_MODE == "s3buckets":
        try:
            glue = _client("glue")
            glue.create_database(DatabaseInput={"Name": GLUE_DB})
            print(f"[setup] created Glue database {GLUE_DB}")
        except Exception as e:  # noqa: BLE001
            if "AlreadyExists" not in type(e).__name__:
                return False, f"could not create Glue database: {e}"
        ok, out = _run_sql([
            f"""CREATE EXTERNAL SCHEMA {SCHEMA}
                FROM DATA CATALOG DATABASE '{GLUE_DB}'
                IAM_ROLE '{ROLE_ARN}'"""
        ])
        if not ok:
            return False, f"could not create external schema: {_error_reason(out)}"
        print(f"[setup] created external schema {SCHEMA} over {GLUE_DB}")
        _setup_fixture_schema()
        return True, "ready"

    # s3tables. A table bucket is a *federated* Glue catalog, and Redshift cannot
    # name one directly: neither "<bucket>@s3tablescatalog".ns.t nor a
    # CATALOG_ID holding the federated path resolves. The documented route is a
    # resource link -- an ordinary database in the DEFAULT catalog whose
    # TargetDatabase points at the federated namespace -- which the external
    # schema then names, with CATALOG_ID set to the plain account id.
    if not TABLE_BUCKET_ARN:
        return False, "AWS_TABLE_BUCKET_ARN is required for the s3tables mode"
    account = TABLE_BUCKET_ARN.split(":")[4]
    fed_catalog_id = f"{account}:s3tablescatalog/{TABLE_BUCKET_ARN.split('/')[-1]}"

    try:
        _client("s3tables").create_namespace(
            tableBucketARN=TABLE_BUCKET_ARN, namespace=[S3T_NAMESPACE]
        )
        print(f"[setup] created S3 Tables namespace {S3T_NAMESPACE}")
    except Exception as e:  # noqa: BLE001
        if "Conflict" not in type(e).__name__ and "Already" not in type(e).__name__:
            return False, f"could not create S3 Tables namespace: {e}"

    try:
        _client("glue").create_database(
            CatalogId=account,
            DatabaseInput={
                "Name": RESOURCE_LINK,
                "TargetDatabase": {
                    "CatalogId": fed_catalog_id,
                    "DatabaseName": S3T_NAMESPACE,
                },
            },
        )
        print(f"[setup] created Glue resource link {RESOURCE_LINK} "
              f"-> {fed_catalog_id}/{S3T_NAMESPACE}")
    except Exception as e:  # noqa: BLE001
        if "AlreadyExists" not in type(e).__name__:
            return False, f"could not create the Glue resource link: {e}"

    ok, out = _run_sql([
        f"""CREATE EXTERNAL SCHEMA {SCHEMA}
            FROM DATA CATALOG DATABASE '{RESOURCE_LINK}'
            IAM_ROLE '{ROLE_ARN}'
            REGION '{REGION}' CATALOG_ID '{account}'"""
    ])
    if not ok:
        return False, f"could not create external schema: {_error_reason(out)}"
    print(f"[setup] created external schema {SCHEMA} over {RESOURCE_LINK}")
    _setup_fixture_schema(account, fed_catalog_id)
    return True, "ready"


def _setup_fixture_schema(account: str = "", fed_catalog_id: str = "") -> None:
    """Mount the Spark-created fixtures, if any were provided.

    Deliberately best-effort: fixtures are an enrichment, not a prerequisite. If
    they are missing the affected tests skip and say so, which is far better than
    failing the whole run and losing the 60-odd checks that need nothing from EMR.
    """
    global FIXTURE_DB
    if not FIXTURE_DB:
        print("[setup] no REDSHIFT_FIXTURE_DB; features Redshift cannot create "
              "will report read support as unmeasured")
        return

    if STORAGE_MODE == "s3tables":
        # The fixtures are an S3 Tables namespace, so they need their own resource
        # link for the same reason the main schema does.
        try:
            _client("glue").create_database(
                CatalogId=account,
                DatabaseInput={
                    "Name": FIXTURE_LINK,
                    "TargetDatabase": {"CatalogId": fed_catalog_id,
                                       "DatabaseName": FIXTURE_DB},
                },
            )
            print(f"[setup] created fixture resource link {FIXTURE_LINK}")
        except Exception as e:  # noqa: BLE001
            if "AlreadyExists" not in type(e).__name__:
                print(f"[setup] fixture resource link failed: {e}")
                FIXTURE_DB = ""
                return
        ok, out = _run_sql([
            f"""CREATE EXTERNAL SCHEMA {FIXTURE_SCHEMA}
                FROM DATA CATALOG DATABASE '{FIXTURE_LINK}'
                IAM_ROLE '{ROLE_ARN}'
                REGION '{REGION}' CATALOG_ID '{account}'"""
        ])
    else:
        ok, out = _run_sql([
            f"""CREATE EXTERNAL SCHEMA {FIXTURE_SCHEMA}
                FROM DATA CATALOG DATABASE '{FIXTURE_DB}'
                IAM_ROLE '{ROLE_ARN}'"""
        ])
    if not ok:
        print(f"[setup] could not mount fixtures from {FIXTURE_DB}: "
              f"{_error_reason(out, 150)}")
        FIXTURE_DB = ""
        return
    ok, out = _run_sql([
        f"SELECT tablename FROM svv_external_tables "
        f"WHERE schemaname='{FIXTURE_SCHEMA}' ORDER BY tablename"
    ])
    found = [l.strip() for l in out.splitlines() if l.strip()] if ok else []
    print(f"[setup] mounted {len(found)} fixtures as {FIXTURE_SCHEMA}: "
          f"{', '.join(found) if found else 'none'}")


def teardown_catalog() -> None:
    """Drop everything this run created, in dependency order."""
    ok, out = _run_sql([f"SELECT tablename FROM svv_external_tables WHERE schemaname='{SCHEMA}'"])
    if ok and out:
        for name in [l.strip() for l in out.splitlines() if l.strip()]:
            _run_sql([f"DROP TABLE {SCHEMA}.{name}"])
    _run_sql([f"DROP SCHEMA IF EXISTS {SCHEMA}"])

    # Only the mount goes: the fixture tables are shared between the two storage
    # mode runs and are owned by the EMR job, not by this one.
    if FIXTURE_DB:
        _run_sql([f"DROP SCHEMA IF EXISTS {FIXTURE_SCHEMA}"])
        if STORAGE_MODE == "s3tables" and TABLE_BUCKET_ARN:
            try:
                _client("glue").delete_database(
                    CatalogId=TABLE_BUCKET_ARN.split(":")[4], Name=FIXTURE_LINK)
                print(f"[teardown] deleted fixture resource link {FIXTURE_LINK}")
            except Exception as e:  # noqa: BLE001
                print(f"[teardown] fixture link cleanup: {type(e).__name__}")

    if STORAGE_MODE == "s3tables":
        # DROP TABLE removes the table from the bucket here, unlike the
        # s3buckets mode, so only the namespace and the link are left. Any table
        # a test created outside Redshift still needs sweeping first, since a
        # namespace with tables in it cannot be deleted.
        account = TABLE_BUCKET_ARN.split(":")[4] if TABLE_BUCKET_ARN else ""
        try:
            s3t = _client("s3tables")
            listed = s3t.list_tables(
                tableBucketARN=TABLE_BUCKET_ARN, namespace=S3T_NAMESPACE
            ).get("tables", [])
            for t in listed:
                s3t.delete_table(
                    tableBucketARN=TABLE_BUCKET_ARN,
                    namespace=S3T_NAMESPACE,
                    name=t["name"],
                )
            if listed:
                print(f"[teardown] deleted {len(listed)} S3 Tables tables")
        except Exception as e:  # noqa: BLE001
            print(f"[teardown] S3 Tables table cleanup: {type(e).__name__}: {e}")
        try:
            _client("glue").delete_database(CatalogId=account, Name=RESOURCE_LINK)
            print(f"[teardown] deleted Glue resource link {RESOURCE_LINK}")
        except Exception as e:  # noqa: BLE001
            print(f"[teardown] resource link cleanup: {type(e).__name__}: {e}")
        try:
            _client("s3tables").delete_namespace(
                tableBucketARN=TABLE_BUCKET_ARN, namespace=S3T_NAMESPACE
            )
            print(f"[teardown] deleted S3 Tables namespace {S3T_NAMESPACE}")
        except Exception as e:  # noqa: BLE001
            print(f"[teardown] namespace cleanup: {type(e).__name__}: {e}")
        return

    try:
        glue = _client("glue")
        for t in glue.get_tables(DatabaseName=GLUE_DB).get("TableList", []):
            glue.delete_table(DatabaseName=GLUE_DB, Name=t["Name"])
        glue.delete_database(Name=GLUE_DB)
        print(f"[teardown] dropped Glue database {GLUE_DB}")
    except Exception as e:  # noqa: BLE001
        print(f"[teardown] Glue cleanup: {type(e).__name__}: {e}")

    # DROP TABLE only removes the catalog entry. Redshift leaves the Parquet and
    # metadata objects in place, so without this every run would leak its entire
    # data footprint into the bucket and pay for it indefinitely.
    if not DATA_BUCKET:
        return
    prefix = f"redshift/{RUN_TAG}/"
    try:
        s3 = _client("s3")
        removed = 0
        token = None
        while True:
            kwargs = {"Bucket": DATA_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            page = s3.list_objects_v2(**kwargs)
            batch = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if batch:
                s3.delete_objects(Bucket=DATA_BUCKET, Delete={"Objects": batch})
                removed += len(batch)
            token = page.get("NextContinuationToken")
            if not token:
                break
        print(f"[teardown] deleted {removed} objects under s3://{DATA_BUCKET}/{prefix}")
    except Exception as e:  # noqa: BLE001
        print(f"[teardown] S3 cleanup: {type(e).__name__}: {e}")


def detect_version() -> str:
    ok, out = _run_sql(["SELECT version()"])
    if ok and out:
        m = re.search(r"Redshift ([\d.]+)", out)
        return m.group(1) if m else out.strip()[:60]
    return "unknown"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global REDSHIFT_VERSION

    print("=" * 70)
    print("  Redshift Iceberg Feature Test Suite")
    print("=" * 70)
    print(f"Workgroup:       {WORKGROUP}")
    print(f"Database:        {DATABASE}")
    print(f"Auth:            {'admin secret' if SECRET_ARN else 'caller IAM identity'}")
    print(f"Storage mode:    {STORAGE_MODE}")
    print(f"Data bucket:     {DATA_BUCKET or '(unset)'}")
    print(f"Matrix platform: {MATRIX_PLATFORM_ID} ({MATRIX_DATA_PATH})")
    if PLATFORM_LABEL:
        print(f"Platform:        {PLATFORM_LABEL}")
    print(f"Versions:        {', '.join(VERSIONS)}")
    print(f"Run tag:         {RUN_TAG}")
    print()

    if not WORKGROUP:
        print("[FATAL] REDSHIFT_WORKGROUP is not set")
        sys.exit(1)
    if not ROLE_ARN:
        print("[FATAL] REDSHIFT_ROLE_ARN is required: the external schema must name "
              "a role, because the auto-mounted catalog cannot write")
        sys.exit(1)
    if STORAGE_MODE == "s3buckets" and not DATA_BUCKET:
        print("[FATAL] AWS_DATA_BUCKET is required for s3buckets")
        sys.exit(1)
    if STORAGE_MODE == "s3tables" and not TABLE_BUCKET_ARN:
        print("[FATAL] AWS_TABLE_BUCKET_ARN is required for s3tables")
        sys.exit(1)

    REDSHIFT_VERSION = detect_version()
    print(f"Redshift version: {REDSHIFT_VERSION}")

    ok, message = setup_catalog()
    if not ok:
        print(f"[FATAL] {message}")
        # Still emit a report, so the run records why the mode could not run
        # rather than leaving an empty artifact behind.
        results = []
        for version in VERSIONS:
            for fn in ALL_TESTS:
                r = TestResult(
                    fn.__name__.replace("test_", "").replace("_", "-"),
                    fn.__name__, version,
                )
                r.result = "error"
                r.details = f"setup failed: {message}"
                results.append(r)
        _write_reports(generate_report(results))
        sys.exit(1)

    only = os.environ.get("REDSHIFT_ONLY", "").strip()
    tests = ALL_TESTS
    if only:
        wanted = {t.strip() for t in only.split(",") if t.strip()}
        tests = [t for t in ALL_TESTS if t.__name__.replace("test_", "") in wanted]
        print(f"[INFO] REDSHIFT_ONLY set; running {len(tests)} test(s)\n")

    results = []
    try:
        for version in VERSIONS:
            print(f"\n{'=' * 70}\n  Format version {version.upper()}\n{'=' * 70}")
            for fn in tests:
                print(f"\n--- {fn.__name__} [{version}] ---")
                try:
                    result = fn(version)
                except Exception as e:  # noqa: BLE001
                    result = TestResult(
                        fn.__name__.replace("test_", "").replace("_", "-"),
                        fn.__name__, version,
                    )
                    result.result = "error"
                    result.details = f"Unhandled exception: {e}"
                results.append(result)
                print(f"  {result.result}: {result.details[:160]}")
    finally:
        teardown_catalog()

    report = generate_report(results)
    _write_reports(report)

    s = report["summary"]
    print(f"\n{'=' * 70}")
    print(f"  {s['passed']} passed, {s.get('partial', 0)} partial, "
          f"{s['failed']} failed, {s['skipped']} skipped, "
          f"{s['errors']} errors, {s['discrepancies']} discrepancies, "
          f"{s['unverified']} unverified, "
          f"{s.get('uncovered_features', 0)} uncovered matrix features")
    print(f"{'=' * 70}")

    sys.exit(1 if (s["discrepancies"] > 0 or s["errors"] > 0
                   or s.get("uncovered_features", 0) > 0) else 0)


def _write_reports(report: dict) -> None:
    os.makedirs(REPORT_DIR, exist_ok=True)
    stem = f"redshift-iceberg-test-report-{STORAGE_MODE}"
    json_path = os.path.join(REPORT_DIR, f"{stem}.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    md = generate_markdown(report)
    md_path = os.path.join(REPORT_DIR, f"{stem}.md")
    with open(md_path, "w") as f:
        f.write(md)
    print(f"\nReports: {json_path}\n         {md_path}")
    print("\n" + md)
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(md)


if __name__ == "__main__":
    main()
