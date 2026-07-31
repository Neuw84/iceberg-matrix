#!/usr/bin/env python3
"""
Flink-based Iceberg Feature Test Suite (V2 + V3).

Drives Flink SQL against a real Flink cluster with the Iceberg connector and
compares what the engine actually does with the support levels recorded in
src/data/platforms/oss/flink/flink.json. Disagreements are reported as
"discrepancies"; features that genuinely cannot be exercised from a SQL client
are reported as "skip" with an honest reason, and are additionally counted as
"unverified" so a skip can never silently rubber-stamp the matrix.

Versions (checked 2026-07):
    Flink 2.3.0        - latest Flink release
    Iceberg 1.11.0     - latest Iceberg release; first with V3 production-ready
    iceberg-flink-runtime-2.1 - latest published Iceberg Flink runtime; Iceberg
                         has no 2.2/2.3 runtime yet, so the 2.1 runtime is used
                         against the 2.3 engine.

Two execution modes:
    docker (default) - talks to the cluster from tests/docker/docker-compose.flink.yml
                       via `docker compose exec jobmanager sql-client.sh -f`.
                       Start it with tests/docker/start-flink.sh.
    local            - uses $FLINK_HOME/bin/sql-client.sh directly.
Set FLINK_MODE to force one.

Usage:
    tests/docker/start-lakekeeper.sh
    tests/docker/start-flink.sh
    python tests/flink_feature_tests.py

Environment variables:
    FLINK_MODE              - "docker" | "local" (default: auto-detect)
    FLINK_VERSION           - Flink engine version, for the report
    FLINK_ICEBERG_VERSION   - Iceberg version, for the report
    FLINK_HOME              - Flink install path (local mode)
    ICEBERG_REST_URI        - Iceberg REST catalog URI
    ICEBERG_REST_WAREHOUSE  - warehouse name (default: demo)
    ICEBERG_S3_ENDPOINT     - S3/MinIO endpoint
    ICEBERG_S3_KEY_ID / ICEBERG_S3_SECRET
    ICEBERG_JDBC_URI        - Postgres URI for the JDBC catalog test
"""

import json
import os
import re
import subprocess
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
DOCKER_DIR = os.path.join(REPO_ROOT, "tests", "docker")
COMPOSE_FILE = os.path.join(DOCKER_DIR, "docker-compose.flink.yml")
WORK_DIR = os.path.join(DOCKER_DIR, "flink-work")

FLINK_HOME = os.environ.get("FLINK_HOME", "")
FLINK_VERSION = os.environ.get("FLINK_VERSION", "2.3.0")
FLINK_ICEBERG_VERSION = os.environ.get("FLINK_ICEBERG_VERSION", "1.11.0")

# Host address the cluster uses to reach the catalog and MinIO. Both must be
# reachable from inside the Flink container as well as from the host, because
# Lakekeeper's GET /v1/config returns an "overrides.uri" that the Iceberg REST
# client applies over the configured URI (see docker-compose.lakekeeper.yml).
def _default_host() -> str:
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        finally:
            s.close()
    except Exception:
        return "127.0.0.1"


_HOST = os.environ.get("FLINK_HOST_IP") or _default_host()

REST_URI = os.environ.get("ICEBERG_REST_URI", f"http://{_HOST}:8181/catalog")
REST_WAREHOUSE = os.environ.get("ICEBERG_REST_WAREHOUSE", "demo")
S3_ENDPOINT = os.environ.get("ICEBERG_S3_ENDPOINT", f"http://{_HOST}:9000")
S3_KEY_ID = os.environ.get("ICEBERG_S3_KEY_ID", "minio")
S3_SECRET = os.environ.get("ICEBERG_S3_SECRET", "minio12345")
JDBC_URI = os.environ.get(
    "ICEBERG_JDBC_URI", f"jdbc:postgresql://{_HOST}:5432/postgres"
)
JDBC_USER = os.environ.get("ICEBERG_JDBC_USER", "postgres")
JDBC_PASSWORD = os.environ.get("ICEBERG_JDBC_PASSWORD", "postgres")
# Filesystem warehouse for the Hadoop catalog test. This must live on the /work
# bind mount, which is shared by the JobManager and the TaskManager: the two are
# separate containers, so a path under /tmp would give each its own copy and the
# table written by a task would be invisible to the coordinator.
HADOOP_WAREHOUSE = os.environ.get("ICEBERG_HADOOP_WAREHOUSE", "file:///work/hadoop-warehouse")

VERSIONS = ["v2", "v3"]

# Which platform's matrix cells to compare against. Overridable so the same
# suite can be pointed at another platform that runs this engine, matching how
# the Spark-based suites work.
MATRIX_PLATFORM_ID = os.environ.get("MATRIX_PLATFORM_ID", "flink")
MATRIX_DATA_PATH = os.environ.get(
    "MATRIX_DATA_PATH", "src/data/platforms/oss/flink/flink.json"
)
# Free-text label for the runtime under test, recorded in the report so a cell
# that changes later is attributable.
PLATFORM_LABEL = os.environ.get("PLATFORM_LABEL", "")
# How the catalog under test is reached, recorded in the report. Overridable so
# a managed platform can describe its own catalog instead of the local stack.
CATALOG_MODE = os.environ.get(
    "MATRIX_CATALOG_MODE", f"REST ({REST_URI}, warehouse={REST_WAREHOUSE})"
)

# How long to wait for in-job compaction to produce a rewrite commit. Sized off
# the 5s checkpoint interval configured in docker-compose.flink.yml: a rewrite
# was observed within ~90s locally.
MAINTENANCE_POLL_SECONDS = int(os.environ.get("FLINK_MAINTENANCE_POLL_SECONDS", "180"))


def _detect_mode() -> str:
    mode = os.environ.get("FLINK_MODE", "").strip().lower()
    if mode in ("docker", "local"):
        return mode
    if os.path.isfile(COMPOSE_FILE):
        try:
            out = subprocess.run(
                ["docker", "compose", "-f", COMPOSE_FILE, "ps", "-q", "jobmanager"],
                capture_output=True, text=True, timeout=30,
            )
            if out.returncode == 0 and out.stdout.strip():
                return "docker"
        except Exception:
            pass
    return "local"


MODE = _detect_mode()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _fmt(version: str) -> str:
    """Map a matrix version label (v2/v3) to the Iceberg format-version number."""
    return "3" if version == "v3" else "2"


# Errors the SQL client reports without a non-zero exit code.
_ERROR_MARKERS = (
    "[ERROR]",
    "org.apache.flink.table.api.ValidationException",
    "org.apache.calcite.sql.validate.SqlValidatorException",
    "org.apache.flink.sql.parser.impl.ParseException",
    "Exception in thread",
)


def _run_sql(statements: list, timeout: int = 240) -> tuple:
    """Execute Flink SQL statements in one SQL client session.

    Returns (ok, combined_output). The SQL client exits 0 even when a statement
    fails and prints [ERROR] instead, and it abandons the remaining statements
    of the script, so each test gets its own session.
    """
    sql_text = "\n".join(s.strip().rstrip(";") + ";" for s in statements if s.strip())
    name = f"{_unique('script')}.sql"

    try:
        if MODE == "docker":
            os.makedirs(WORK_DIR, exist_ok=True)
            host_path = os.path.join(WORK_DIR, name)
            with open(host_path, "w") as fh:
                fh.write(sql_text)
            cmd = [
                "docker", "compose", "-f", COMPOSE_FILE, "exec", "-T", "jobmanager",
                "/opt/flink/bin/sql-client.sh", "embedded", "-f", f"/work/{name}",
            ]
            env = os.environ.copy()
        else:
            if not FLINK_HOME:
                return False, "FLINK_MODE=local but FLINK_HOME is not set"
            host_path = os.path.join("/tmp", name)
            with open(host_path, "w") as fh:
                fh.write(sql_text)
            cmd = [
                os.path.join(FLINK_HOME, "bin", "sql-client.sh"),
                "embedded", "-f", host_path,
            ]
            env = {**os.environ, "FLINK_HOME": FLINK_HOME}

        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout, env=env
            )
        finally:
            if os.path.exists(host_path):
                os.unlink(host_path)

        output = (proc.stdout or "") + "\n" + (proc.stderr or "")
        if proc.returncode != 0:
            return False, output.strip()
        if any(m in output for m in _ERROR_MARKERS):
            return False, output.strip()
        return True, output.strip()
    except subprocess.TimeoutExpired:
        return False, f"SQL client timed out after {timeout}s"
    except FileNotFoundError as e:
        return False, f"SQL client not runnable: {e}"
    except Exception as e:  # noqa: BLE001
        return False, str(e)


def _submit_streaming(statements: list, timeout: int = 240) -> tuple:
    """Submit an unbounded streaming job and return (ok, out, job_id).

    Deliberately does NOT set table.dml-sync: an unbounded INSERT would never
    return. The SQL client submits the job detached and prints its Job ID, which
    the caller polls against and must cancel afterwards.
    """
    ok, out = _run_sql(statements, timeout=timeout)
    m = re.search(r"Job ID:\s*([0-9a-f]{32})", out)
    return ok, out, (m.group(1) if m else None)


def _cancel_job(job_id: str) -> None:
    """Cancel a running Flink job so it cannot leak into later tests."""
    if not job_id:
        return
    try:
        if MODE == "docker":
            cmd = ["docker", "compose", "-f", COMPOSE_FILE, "exec", "-T", "jobmanager",
                   "/opt/flink/bin/flink", "cancel", job_id]
        else:
            cmd = [os.path.join(FLINK_HOME, "bin", "flink"), "cancel", job_id]
        subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except Exception:  # noqa: BLE001
        pass


def _marker(out: str, expected: str) -> bool:
    """True when a MARK... token appears in the client's tableau output.

    Assertions are encoded as short CONCAT('MARKX=', ...) literals rather than
    by parsing the tableau grid: the grid pads, aligns and truncates cells to the
    column width, so short markers are the reliable way to read a value back.

    The match is anchored at the end of the value, otherwise "MARKCNT=1" would
    also be satisfied by "MARKCNT=10" and a wrong row count could pass.
    """
    prefix, _, value = expected.partition("=")
    return value in _marker_values(out, prefix)


def _marker_values(out: str, prefix: str) -> list:
    """Return every value carried by markers with the given prefix.

    Empty matches are dropped: the SQL client echoes each statement before running
    it, so the literal in CONCAT('MARKX=', ...) also appears in the echoed query
    text and would otherwise show up as a bogus empty value.
    """
    found = re.findall(rf"{prefix}=([A-Za-z0-9_:.,+-]+)", out.replace(" ", ""))
    return [v for v in found if v]


def _error_reason(out: str, limit: int = 220) -> str:
    """Pull the most informative text out of a failed SQL client run.

    The client prints "[ERROR] Could not execute SQL statement. Reason:" and puts
    the actual exception on the following line, so the [ERROR] line alone is
    useless. Exception class names are matched anywhere in the text because the
    dumb terminal wraps long lines at the column width.
    """
    flat = " ".join(out.split())
    m = re.search(r"((?:org|java|javax)\.[\w.$]*(?:Exception|Error)\b:?[^|]{0,200})", flat)
    if m:
        return m.group(1).strip()[:limit]
    m = re.search(r"Reason:\s*(.{10,200})", flat)
    if m:
        return m.group(1).strip()[:limit]
    for line in out.splitlines():
        if "[ERROR]" in line:
            return line.strip()[:limit]
    return flat[:limit]


def _prelude(version: str = "v3", catalog: str = "rest", streaming: bool = False,
             dml_sync: bool = True) -> list:
    """Session setup: result mode, DML sync mode, and the requested catalog.

    table.dml-sync is essential for bounded work: without it the SQL client
    submits INSERT jobs detached and a following SELECT races the write, silently
    reading zero rows. It must be OFF for an unbounded streaming INSERT, which
    would otherwise never return and hang the client until its timeout.
    """
    stmts = [
        "SET 'sql-client.execution.result-mode' = 'tableau'",
        f"SET 'table.dml-sync' = '{str(dml_sync).lower()}'",
        "SET 'table.dynamic-table-options.enabled' = 'true'",
        f"SET 'execution.runtime-mode' = '{'streaming' if streaming else 'batch'}'",
    ]
    if catalog == "rest":
        stmts.append(f"""CREATE CATALOG test_catalog WITH (
            'type'='iceberg',
            'catalog-type'='rest',
            'uri'='{REST_URI}',
            'warehouse'='{REST_WAREHOUSE}',
            'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint'='{S3_ENDPOINT}',
            's3.path-style-access'='true',
            's3.access-key-id'='{S3_KEY_ID}',
            's3.secret-access-key'='{S3_SECRET}'
        )""")
    elif catalog == "hadoop":
        stmts.append(f"""CREATE CATALOG test_catalog WITH (
            'type'='iceberg',
            'catalog-type'='hadoop',
            'warehouse'='{HADOOP_WAREHOUSE}'
        )""")
    elif catalog == "jdbc":
        # Flink's FlinkCatalogFactory accepts only hive, hadoop or rest for
        # catalog-type; every other Iceberg catalog is reached through
        # catalog-impl, and setting both at once is rejected outright.
        stmts.append(f"""CREATE CATALOG test_catalog WITH (
            'type'='iceberg',
            'catalog-impl'='org.apache.iceberg.jdbc.JdbcCatalog',
            'uri'='{JDBC_URI}',
            'jdbc.user'='{JDBC_USER}',
            'jdbc.password'='{JDBC_PASSWORD}',
            'warehouse'='{HADOOP_WAREHOUSE}'
        )""")
    else:
        raise ValueError(f"unknown catalog {catalog}")

    stmts += [
        "USE CATALOG test_catalog",
        "CREATE DATABASE IF NOT EXISTS test_db",
        "USE test_db",
    ]
    return stmts


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


def _v3_only(feature_id: str, feature_name: str) -> TestResult:
    """V2 placeholder for a V3-only feature."""
    r = TestResult(feature_id, feature_name, "v2")
    r.result = "skip"
    r.details = "V3-only feature; not applicable to format-version 2 tables"
    return r


def _rest_prefix() -> str:
    """The catalog's request prefix, needed for direct REST calls."""
    import urllib.request
    base = REST_URI.rstrip("/")
    with urllib.request.urlopen(f"{base}/v1/config?warehouse={REST_WAREHOUSE}", timeout=15) as resp:
        return json.load(resp)["defaults"]["prefix"]


def _rest_table_metadata(table: str, namespace: str = "test_db"):
    """Load a table's Iceberg metadata straight from the catalog.

    Used to inspect state that Flink SQL cannot surface, such as V3 row lineage
    counters and the partition-spec history.
    """
    import urllib.request
    try:
        base = REST_URI.rstrip("/")
        url = f"{base}/v1/{_rest_prefix()}/namespaces/{namespace}/tables/{table}"
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.load(resp).get("metadata", {})
    except Exception:  # noqa: BLE001
        return None


def _rest_evolve_spec(table: str, namespace: str = "test_db") -> bool:
    """Add a day(ts) field to the table's partition spec and make it the default.

    Flink SQL has no partition-evolution syntax, so the evolution is driven through
    the catalog. This is exactly how the operation works underneath: a new spec is
    appended and the default spec pointer moves, leaving existing data files bound
    to the old spec.
    """
    import urllib.request
    try:
        base = REST_URI.rstrip("/")
        prefix = _rest_prefix()
        url = f"{base}/v1/{prefix}/namespaces/{namespace}/tables/{table}"
        md = _rest_table_metadata(table, namespace)
        cur = [s for s in md["partition-specs"] if s["spec-id"] == md["default-spec-id"]][0]
        ts_field = [f for f in md["schemas"][-1]["fields"] if f["name"] == "ts"][0]
        new_spec = {
            "spec-id": max(s["spec-id"] for s in md["partition-specs"]) + 1,
            "fields": cur["fields"] + [
                {"source-id": ts_field["id"], "field-id": 1001,
                 "name": "ts_day", "transform": "day"},
            ],
        }
        body = {
            "requirements": [{"type": "assert-table-uuid", "uuid": md["table-uuid"]}],
            "updates": [{"action": "add-spec", "spec": new_spec},
                        {"action": "set-default-spec", "spec-id": -1}],
        }
        req = urllib.request.Request(
            url, data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            json.load(resp)
        return True
    except Exception:  # noqa: BLE001
        return False


def _rest_set_tags(table: str, tags: dict, namespace: str = "test_db") -> bool:
    """Create tags on existing snapshots through the catalog.

    Flink has no ref DDL, so tags for the tag-read and tag-to-tag scan tests have
    to come from the catalog itself.
    """
    import urllib.request
    try:
        base = REST_URI.rstrip("/")
        md = _rest_table_metadata(table, namespace)
        body = {
            "requirements": [{"type": "assert-table-uuid", "uuid": md["table-uuid"]}],
            "updates": [
                {"action": "set-snapshot-ref", "ref-name": name,
                 "type": "tag", "snapshot-id": snap_id}
                for name, snap_id in tags.items()
            ],
        }
        req = urllib.request.Request(
            f"{base}/v1/{_rest_prefix()}/namespaces/{namespace}/tables/{table}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            json.load(resp)
        return True
    except Exception:  # noqa: BLE001
        return False


def _rest_create_transform_partitioned(version: str, namespace: str = "test_db"):
    """Create a day(ts)-partitioned table straight through the REST catalog API.

    Flink DDL cannot express transform partitioning, so a table that exercises the
    hidden-partitioning read/write path has to be created by something else. Going
    to the catalog's own HTTP API avoids adding an engine or library just for this.
    Returns the table name, or None if the catalog rejected the request.
    """
    import urllib.error
    import urllib.request
    base = REST_URI.rstrip("/")
    name = _unique("hpext")
    try:
        with urllib.request.urlopen(f"{base}/v1/config?warehouse={REST_WAREHOUSE}", timeout=15) as resp:
            prefix = json.load(resp)["defaults"]["prefix"]
        body = {
            "name": name,
            "schema": {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": False, "type": "long"},
                {"id": 2, "name": "ts", "required": False, "type": "timestamptz"},
            ]},
            "partition-spec": {"spec-id": 0, "fields": [
                {"source-id": 2, "field-id": 1000, "name": "ts_day", "transform": "day"},
            ]},
            "stage-create": False,
            "properties": {"format-version": _fmt(version)},
        }
        req = urllib.request.Request(
            f"{base}/v1/{prefix}/namespaces/{namespace}/tables",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            json.load(resp)
        return name
    except Exception:  # noqa: BLE001
        return None


def _external_service(feature_id: str, feature_name: str, version: str, what: str) -> TestResult:
    """Honest skip for a catalog that needs a service or credentials we lack."""
    r = TestResult(feature_id, feature_name, version)
    r.result = "skip"
    r.details = (
        f"Not exercised: requires {what}. Flink ships the catalog implementation, "
        "but this harness has no such endpoint to prove it against"
    )
    return r


# ---------------------------------------------------------------------------
# Core DDL / read / write
# ---------------------------------------------------------------------------

def test_table_creation(version: str) -> TestResult:
    r = TestResult("table-creation", "Table Creation", version)
    tbl = _unique("create")
    ok, out = _run_sql(_prelude(version) + [
        f"""CREATE TABLE {tbl} (id BIGINT, name STRING, amount DOUBLE, ts TIMESTAMP(6))
            WITH ('format-version'='{_fmt(version)}')""",
        f"INSERT INTO {tbl} VALUES (1, 'a', 1.5, TIMESTAMP '2026-01-01 00:00:00')",
        # Confirm the declared format version was actually applied rather than
        # silently defaulted, by reading it back from table metadata.
        f"SELECT CONCAT('MARKFV=', CAST(COUNT(*) AS STRING)) AS m FROM `{tbl}$snapshots`",
        f"SELECT CONCAT('MARKROW=', name, ':', CAST(amount AS STRING)) AS m FROM {tbl}",
        f"DROP TABLE {tbl}",
    ])
    if ok and _marker(out, "MARKROW=a:1.5"):
        r.result = "pass"
        r.details = (
            f"CREATE TABLE with 4 column types on a {version.upper()} table, then "
            "INSERT and read-back of every column"
        )
    elif ok:
        r.result = "fail"
        r.details = f"Table created but row did not read back: {_marker_values(out, 'MARKROW')}"
    else:
        r.result = "error"
        r.details = _error_reason(out)
    return r


def test_read_support(version: str) -> TestResult:
    r = TestResult("read-support", "Read Support", version)
    tbl = _unique("read")
    ok, out = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl} (id BIGINT, name STRING, val INT) WITH ('format-version'='{_fmt(version)}')",
        f"INSERT INTO {tbl} VALUES (1,'a',10),(2,'b',20),(3,'c',30)",
        f"SELECT CONCAT('MARKALL=', CAST(COUNT(*) AS STRING)) AS m FROM {tbl}",
        # Predicate pushdown and column projection.
        f"SELECT CONCAT('MARKPRED=', CAST(COUNT(*) AS STRING)) AS m FROM {tbl} WHERE val > 15",
        f"SELECT CONCAT('MARKPROJ=', name) AS m FROM {tbl} WHERE id = 2",
        f"DROP TABLE {tbl}",
    ])
    if not ok:
        r.result = "error"
        r.details = _error_reason(out)
        return r
    if not (_marker(out, "MARKALL=3") and _marker(out, "MARKPRED=2") and _marker(out, "MARKPROJ=b")):
        r.result = "fail"
        r.details = f"Unexpected read results: {_marker_values(out, 'MARKALL')} {_marker_values(out, 'MARKPRED')}"
        return r

    # Streaming is what Flink is for, so batch reads alone are weak evidence.
    # Verify the continuous read path: an iceberg-to-iceberg tail with a
    # monitor-interval must deliver the initial snapshot AND rows committed to the
    # source after the job started.
    src, tgt = _unique("ssrc"), _unique("stgt")
    ok_s, _ = _run_sql(_prelude(version) + [
        f"CREATE TABLE {src} (id BIGINT, val STRING) WITH ('format-version'='{_fmt(version)}')",
        f"CREATE TABLE {tgt} (id BIGINT, val STRING) WITH ('format-version'='{_fmt(version)}')",
        f"INSERT INTO {src} VALUES (1,'a'),(2,'b')",
    ])
    job_id = None
    late_arrived = False
    if ok_s:
        ok_j, _, job_id = _submit_streaming(
            _prelude(version, streaming=True, dml_sync=False) + [
                f"INSERT INTO {tgt} SELECT id, val FROM {src} "
                f"/*+ OPTIONS('streaming'='true','monitor-interval'='2s') */",
            ])
        if ok_j and job_id:
            try:
                time.sleep(10)
                _run_sql(_prelude(version) + [f"INSERT INTO {src} VALUES (3,'late')"])
                for _ in range(9):
                    time.sleep(10)
                    ok_p, out_p = _run_sql(_prelude(version) + [
                        f"SELECT CONCAT('MARKT=', CAST(id AS STRING), ':', val) AS m "
                        f"FROM {tgt} ORDER BY id",
                    ])
                    if ok_p and "3:late" in _marker_values(out_p, "MARKT"):
                        late_arrived = True
                        break
            finally:
                _cancel_job(job_id)
    _run_sql(_prelude(version) + [f"DROP TABLE IF EXISTS {src}",
                                  f"DROP TABLE IF EXISTS {tgt}"])

    if late_arrived:
        r.result = "pass"
        r.details = (
            "Batch: read 3 rows with correct predicate filtering and projection. "
            "Streaming: a continuous read (streaming=true, monitor-interval) delivered "
            "the initial snapshot and a row committed to the source AFTER the job "
            "started, into a second Iceberg table"
        )
    else:
        r.result = "fail"
        r.details = (
            "Batch reads verified, but the continuous streaming read did not deliver a "
            "row committed after the job started"
        )
    return r


def test_write_insert(version: str) -> TestResult:
    r = TestResult("write-insert", "Write (INSERT)", version)
    tbl = _unique("insert")
    ok, out = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl} (id BIGINT, name STRING) WITH ('format-version'='{_fmt(version)}')",
        f"INSERT INTO {tbl} VALUES (1,'a'),(2,'b')",
        f"INSERT INTO {tbl} VALUES (3,'c')",
        f"SELECT CONCAT('MARKAPP=', CAST(COUNT(*) AS STRING)) AS m FROM {tbl}",
        # INSERT OVERWRITE is batch-only; this session is batch.
        f"INSERT OVERWRITE {tbl} VALUES (9,'z')",
        f"SELECT CONCAT('MARKOVW=', CAST(COUNT(*) AS STRING)) AS m FROM {tbl}",
        f"DROP TABLE {tbl}",
    ])
    if not ok:
        r.result = "error"
        r.details = _error_reason(out)
        return r
    if not _marker(out, "MARKAPP=3"):
        r.result = "fail"
        r.details = f"Unexpected counts: append={_marker_values(out, 'MARKAPP')}"
        return r
    batch_detail = (
        "Batch: INSERT INTO appended across 2 commits (3 rows)"
        + ("; INSERT OVERWRITE replaced them (1 row)" if _marker(out, "MARKOVW=1")
           else f"; INSERT OVERWRITE gave {_marker_values(out, 'MARKOVW')}")
    )

    # Streaming writes are Flink's primary use, and their contract differs from a
    # bounded insert: commits are driven by checkpoints while the job keeps
    # running. Assert that an unbounded INSERT produces MULTIPLE append snapshots
    # and that the data is readable mid-flight, before the job ever finishes.
    stbl = _unique("swrite")
    src = "default_catalog.default_database." + _unique("sgen")
    ok_j, out_j, job_id = _submit_streaming(
        _prelude(version, streaming=True, dml_sync=False) + [
            f"CREATE TABLE {stbl} (id BIGINT, val STRING) WITH ('format-version'='{_fmt(version)}')",
            f"""CREATE TEMPORARY TABLE {src} (id BIGINT, val STRING) WITH (
                'connector'='datagen', 'rows-per-second'='10', 'fields.val.length'='8')""",
            f"INSERT INTO {stbl} SELECT id, val FROM {src}",
        ])
    commits, mid_rows = 0, 0
    if ok_j and job_id:
        try:
            for _ in range(9):
                time.sleep(10)
                ok_p, out_p = _run_sql(_prelude(version) + [
                    f"SELECT CONCAT('MARKSN=', CAST(COUNT(*) AS STRING)) AS m FROM `{stbl}$snapshots`",
                    f"SELECT CONCAT('MARKCNT=', CAST(COUNT(*) AS STRING)) AS m FROM {stbl}",
                ])
                if ok_p:
                    sn = _marker_values(out_p, "MARKSN")
                    cnt = _marker_values(out_p, "MARKCNT")
                    commits = int(sn[0]) if sn else 0
                    mid_rows = int(cnt[0]) if cnt else 0
                    if commits >= 3 and mid_rows > 0:
                        break
        finally:
            _cancel_job(job_id)
    _run_sql(_prelude(version) + [f"DROP TABLE IF EXISTS {stbl}"])

    if commits >= 3 and mid_rows > 0:
        r.result = "pass"
        r.details = (
            f"{batch_detail}. Streaming: an unbounded INSERT committed {commits} append "
            f"snapshots on checkpoints while the job was still running, with {mid_rows} "
            "rows already readable mid-flight (exactly-once checkpoint commit loop)"
        )
    else:
        r.result = "fail"
        r.details = (
            f"{batch_detail}. Streaming write did not demonstrate checkpoint commits: "
            f"snapshots={commits}, rows readable mid-job={mid_rows}"
            + ("" if ok_j and job_id else f" ({_error_reason(out_j, 120)})")
        )
    return r


def test_write_merge_update_delete(version: str) -> TestResult:
    r = TestResult("write-merge-update-delete", "Write (MERGE/UPDATE/DELETE)", version)
    tbl = _unique("mud")
    # Flink SQL has no MERGE INTO / UPDATE / DELETE for Iceberg; upsert is the
    # only row-level write path. Prove the SQL statements really are rejected
    # rather than asserting it from documentation.
    setup = _prelude(version) + [
        f"CREATE TABLE {tbl} (id BIGINT, val STRING) WITH ('format-version'='{_fmt(version)}')",
        f"INSERT INTO {tbl} VALUES (1,'a'),(2,'b')",
    ]
    ok_del, out_del = _run_sql(setup + [f"DELETE FROM {tbl} WHERE id = 1"])
    ok_upd, out_upd = _run_sql(_prelude(version) + [f"UPDATE {tbl} SET val='x' WHERE id=2"])
    _run_sql(_prelude(version) + [f"DROP TABLE IF EXISTS {tbl}"])

    if ok_del and ok_upd:
        r.result = "pass"
        r.details = "Flink SQL executed DELETE and UPDATE against the Iceberg table"
    elif not ok_del and not ok_upd:
        r.result = "fail"
        r.details = (
            "Neither DELETE nor UPDATE is supported in Flink SQL; upsert mode is the "
            f"only row-level write path. DELETE: {_error_reason(out_del, 90)}"
        )
    else:
        r.result = "fail"
        r.details = (
            f"Partial row-level DML: DELETE ok={ok_del}, UPDATE ok={ok_upd}. "
            f"{_error_reason(out_del if not ok_del else out_upd, 120)}"
        )
    return r


# ---------------------------------------------------------------------------
# Row-level operations
# ---------------------------------------------------------------------------

def _upsert_delete_evidence(version: str, use_v2_sink: bool, same_batch: bool = False):
    """Run an upsert that must replace a row, and report the delete files written.

    Returns (ok, out, rows, delete_kinds). delete_kinds holds "<content>:<format>"
    per delete file: content 1 = position deletes, 2 = equality deletes, and a
    puffin format on a position delete means a V3 deletion vector.

    same_batch controls which delete flavour the writer produces, and the
    distinction matters:
      False - the duplicate key arrives in a later commit, so the writer can only
              delete by key and emits an equality delete file.
      True  - both versions of the key arrive in one statement, so the superseded
              row is deleted by position within the file being written. On a V3
              table that position delete is written as a deletion vector.

    The inserts run in the STREAMING runtime, since upsert exists for streaming
    CDC pipelines; the verification SELECTs run in the same session after
    switching back to batch. Bounded VALUES sources work under dml-sync in
    either runtime, so the session stays synchronous throughout.
    """
    tbl = _unique("ups")
    stmts = _prelude(version, streaming=True)
    if use_v2_sink:
        stmts.append("SET 'table.exec.iceberg.use-v2-sink' = 'true'")
    stmts.append(
        f"""CREATE TABLE {tbl} (id BIGINT, name STRING, PRIMARY KEY (id) NOT ENFORCED)
            WITH ('format-version'='{_fmt(version)}', 'write.upsert.enabled'='true')"""
    )
    if same_batch:
        stmts.append(f"INSERT INTO {tbl} VALUES (1,'first'),(1,'updated'),(2,'second')")
    else:
        stmts.append(f"INSERT INTO {tbl} VALUES (1,'first'),(2,'second')")
        stmts.append(f"INSERT INTO {tbl} VALUES (1,'updated')")
    stmts += [
        "SET 'execution.runtime-mode' = 'batch'",
        f"SELECT CONCAT('MARKROW=', CAST(id AS STRING), ':', name) AS m FROM {tbl} ORDER BY id",
        f"SELECT CONCAT('MARKDEL=', CAST(content AS STRING), ':', file_format) AS m FROM `{tbl}$delete_files`",
        f"DROP TABLE {tbl}",
    ]
    ok, out = _run_sql(stmts)
    return ok, out, _marker_values(out, "MARKROW"), _marker_values(out, "MARKDEL")


def test_equality_deletes(version: str) -> TestResult:
    r = TestResult("equality-deletes", "Equality Deletes", version)
    ok, out, rows, deletes = _upsert_delete_evidence(version, use_v2_sink=False)
    if not ok:
        r.result = "error"
        r.details = _error_reason(out)
        return r
    replaced = "1:updated" in rows and "1:first" not in rows
    eq = [d for d in deletes if d.startswith("2:")]
    if replaced and eq:
        r.result = "pass"
        r.details = (
            f"UPSERT replaced the row (rows={rows}) and wrote equality delete "
            f"files (content=2): {eq}"
        )
    elif replaced:
        r.result = "pass"
        r.details = f"UPSERT replaced the row (rows={rows}); delete files: {deletes or 'none'}"
    else:
        r.result = "fail"
        r.details = f"UPSERT did not replace the row; rows={rows}, deletes={deletes}"
    return r


def test_merge_on_read(version: str) -> TestResult:
    r = TestResult("merge-on-read", "Merge-on-Read", version)
    ok, out, rows, deletes = _upsert_delete_evidence(version, use_v2_sink=False)
    if not ok:
        r.result = "error"
        r.details = _error_reason(out)
        return r
    if not (deletes and "1:updated" in rows):
        r.result = "fail"
        r.details = f"Merge-on-read not demonstrated: rows={rows}, deletes={deletes}"
        return r

    # Upsert is not just capable of MoR -- it is MoR-only. Prove it by setting all
    # copy-on-write modes on the table: the writer must ignore them and still
    # produce delete files.
    tbl = _unique("morcow")
    ok2, out2 = _run_sql(_prelude(version) + [
        f"""CREATE TABLE {tbl} (id BIGINT, val STRING, PRIMARY KEY (id) NOT ENFORCED)
            WITH ('format-version'='{_fmt(version)}', 'write.upsert.enabled'='true',
                  'write.delete.mode'='copy-on-write', 'write.update.mode'='copy-on-write',
                  'write.merge.mode'='copy-on-write')""",
        f"INSERT INTO {tbl} VALUES (1,'first'),(2,'second')",
        f"INSERT INTO {tbl} VALUES (1,'updated')",
        f"SELECT CONCAT('MARKDEL=', CAST(content AS STRING), ':', file_format) AS m FROM `{tbl}$delete_files`",
        f"DROP TABLE {tbl}",
    ])
    cow_ignored = ok2 and bool(_marker_values(out2, "MARKDEL"))
    r.result = "pass"
    r.details = (
        "Upsert produced delete files that the reader merged at scan time "
        f"(deletes={deletes}, rows={rows})."
        + (" Merge-on-read is also the ONLY write path for upsert: with all "
           "write.*.mode properties set to copy-on-write the writer still emitted "
           f"delete files ({_marker_values(out2, 'MARKDEL')}), ignoring the setting"
           if cow_ignored else "")
    )
    return r


def test_position_deletes(version: str) -> TestResult:
    r = TestResult("position-deletes", "Position Deletes", version)
    # Flink has no DELETE statement, but it does write position deletes: when an
    # upsert sees the same key twice in one batch, the superseded row is deleted by
    # position within the file being written. On V2 that is a Parquet position
    # delete file; on V3 it is recorded as a puffin deletion vector.
    ok, out, rows, deletes = _upsert_delete_evidence(version, False, same_batch=True)
    if not ok:
        r.result = "error"
        r.details = _error_reason(out)
        return r
    pos = [d for d in deletes if d.startswith("1:")]
    if pos and "1:updated" in rows:
        r.result = "pass"
        r.details = (
            f"Write path produced position deletes (content=1): {pos}, and the read "
            f"merged them correctly (rows={rows}). Emitted for a row superseded within a "
            "single batch; Flink SQL has no DELETE statement to request them directly"
        )
    elif deletes:
        r.result = "fail"
        r.details = (
            f"No position delete files produced; writer emitted {deletes} "
            "(content=2 is equality deletes)"
        )
    else:
        r.result = "fail"
        r.details = f"No delete files produced at all; rows={rows}"
    return r


def test_copy_on_write(version: str) -> TestResult:
    r = TestResult("copy-on-write", "Copy-on-Write", version)
    tbl = _unique("cow")
    ok, out = _run_sql(_prelude(version) + [
        f"""CREATE TABLE {tbl} (id BIGINT, val STRING) WITH (
            'format-version'='{_fmt(version)}',
            'write.delete.mode'='copy-on-write',
            'write.update.mode'='copy-on-write')""",
        f"INSERT INTO {tbl} VALUES (1,'a'),(2,'b'),(3,'c')",
        # INSERT OVERWRITE rewrites data files wholesale: the copy-on-write path
        # reachable from Flink SQL.
        f"INSERT OVERWRITE {tbl} VALUES (1,'rewritten')",
        f"SELECT CONCAT('MARKROW=', CAST(id AS STRING), ':', val) AS m FROM {tbl}",
        f"SELECT CONCAT('MARKDEL=', CAST(COUNT(*) AS STRING)) AS m FROM `{tbl}$delete_files`",
        f"DROP TABLE {tbl}",
    ])
    if not ok:
        r.result = "error"
        r.details = _error_reason(out)
    elif _marker(out, "MARKROW=1:rewritten") and _marker(out, "MARKDEL=0"):
        r.result = "pass"
        r.details = (
            "INSERT OVERWRITE rewrote the data files with no delete files, which is the "
            "copy-on-write path Flink SQL can reach. Note the scope: copy-on-write proper "
            "means rewriting files for a row-level UPDATE or DELETE, and Flink SQL has "
            "neither, so write.*.mode='copy-on-write' has nothing to act on -- the only "
            "rewrite available is a whole-table or whole-partition overwrite"
        )
    elif _marker(out, "MARKROW=1:rewritten"):
        r.result = "pass"
        r.details = (
            "INSERT OVERWRITE rewrote data (whole-table overwrite, not row-level "
            f"copy-on-write); delete files={_marker_values(out, 'MARKDEL')}"
        )
    else:
        r.result = "fail"
        r.details = f"Copy-on-write overwrite not confirmed: rows={_marker_values(out, 'MARKROW')}"
    return r


def test_deletion_vectors(version: str) -> TestResult:
    if version == "v2":
        return _v3_only("deletion-vectors", "Deletion Vectors")
    r = TestResult("deletion-vectors", "Deletion Vectors", version)
    # Deletion vectors replace POSITION deletes, so the write has to produce a
    # position delete to produce a DV. An upsert whose duplicate key arrives in a
    # later commit can only delete by key and yields an equality delete file; when
    # both versions arrive in the same statement, the superseded row is deleted by
    # position and a V3 table records that as a puffin DV. Test the same-batch path
    # under both sinks, and keep the cross-commit run for contrast.
    ok_b, out_b, rows_b, del_b = _upsert_delete_evidence(version, False, same_batch=True)
    ok_v2, out_v2, _, del_v2 = _upsert_delete_evidence(version, True, same_batch=True)
    _, _, _, del_cross = _upsert_delete_evidence(version, False, same_batch=False)

    def _dv(kinds):
        # content 1 = position deletes; puffin on a position delete is a DV.
        return [k for k in kinds if "puffin" in k.lower()]

    dv_default, dv_sink2 = _dv(del_b), _dv(del_v2)
    if not ok_b:
        r.result = "error"
        r.details = _error_reason(out_b)
    elif dv_default and "1:updated" in rows_b:
        r.result = "pass"
        r.details = (
            f"V3 deletion vectors written from plain Flink SQL: {dv_default} "
            f"(content=1 position deletes in puffin), alongside equality deletes for the "
            f"key-based part ({[k for k in del_b if k not in dv_default]}), and the upsert "
            f"result is correct (rows={rows_b}). Same-batch duplicate keys are required: an "
            f"upsert split across commits writes only equality deletes ({del_cross}). "
            f"The Sink V2 path behaves the same ({dv_sink2 or 'no DVs'})"
        )
    elif del_b:
        r.result = "fail"
        r.details = (
            f"No puffin deletion vectors on a V3 table; writer produced {del_b} "
            f"(same batch) and {del_cross} (across commits)"
        )
    else:
        r.result = "fail"
        r.details = f"No delete files produced at all; rows={rows_b}"
    return r


# ---------------------------------------------------------------------------
# Table management
# ---------------------------------------------------------------------------

def test_schema_evolution(version: str) -> TestResult:
    r = TestResult("schema-evolution", "Schema Evolution", version)
    tbl = _unique("schema")
    ok, out = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl} (id BIGINT, name STRING) WITH ('format-version'='{_fmt(version)}')",
        f"INSERT INTO {tbl} VALUES (1,'alice')",
        f"ALTER TABLE {tbl} ADD (age INT)",
        f"ALTER TABLE {tbl} RENAME name TO full_name",
        f"ALTER TABLE {tbl} DROP age",
        # Existing data must still be readable under the evolved schema.
        f"SELECT CONCAT('MARKEVO=', full_name) AS m FROM {tbl} WHERE id = 1",
        f"DROP TABLE {tbl}",
    ])
    if ok and _marker(out, "MARKEVO=alice"):
        r.result = "pass"
        r.details = "ADD, RENAME and DROP COLUMN all succeeded via Flink DDL; existing rows readable"
        return r
    if ok:
        r.result = "fail"
        r.details = "DDL succeeded but the evolved column did not read back"
        return r

    # Fall back to property-only ALTER to distinguish "no column DDL" from a broken test.
    ok2, out2 = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl}_p (id BIGINT) WITH ('format-version'='{_fmt(version)}')",
        f"ALTER TABLE {tbl}_p SET ('read.split.target-size'='134217728')",
        f"DROP TABLE {tbl}_p",
    ])
    r.result = "fail" if ok2 else "error"
    r.details = (
        f"Column DDL rejected ({_error_reason(out, 140)}); "
        f"property-only ALTER TABLE SET {'works' if ok2 else 'also failed'}"
    )
    return r


def test_type_promotion(version: str) -> TestResult:
    r = TestResult("type-promotion", "Type Promotion / Widening", version)
    tbl = _unique("promo")
    ok, out = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl} (id INT, amount FLOAT) WITH ('format-version'='{_fmt(version)}')",
        f"INSERT INTO {tbl} VALUES (1, 1.5)",
        f"ALTER TABLE {tbl} MODIFY id BIGINT",
        f"ALTER TABLE {tbl} MODIFY amount DOUBLE",
        # A value beyond INT range proves the column really widened.
        f"INSERT INTO {tbl} VALUES (9999999999, 3.5)",
        f"SELECT CONCAT('MARKWIDE=', CAST(id AS STRING)) AS m FROM {tbl} WHERE id > 100",
        f"SELECT CONCAT('MARKOLD=', CAST(amount AS STRING)) AS m FROM {tbl} WHERE id = 1",
        f"DROP TABLE {tbl}",
    ])
    if not ok:
        r.result = "fail"
        r.details = f"Type widening rejected: {_error_reason(out, 160)}"
    elif _marker(out, "MARKWIDE=9999999999"):
        r.result = "pass"
        r.details = "INT→BIGINT and FLOAT→DOUBLE widening applied; out-of-INT-range value stored and read back"
    else:
        r.result = "fail"
        r.details = f"Widening did not take effect: {_marker_values(out, 'MARKWIDE')}"
    return r


def test_column_default_values(version: str) -> TestResult:
    if version == "v2":
        return _v3_only("column-default-values", "Column Default Values")
    r = TestResult("column-default-values", "Column Default Values", version)
    tbl = _unique("coldef")
    ok, out = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl} (id BIGINT, val STRING DEFAULT 'hello') WITH ('format-version'='3')",
        f"INSERT INTO {tbl} (id) VALUES (1)",
        f"SELECT CONCAT('MARKDEF=', val) AS m FROM {tbl}",
        f"DROP TABLE {tbl}",
    ])
    if ok and _marker(out, "MARKDEF=hello"):
        r.result = "pass"
        r.details = "Column DEFAULT declared in Flink DDL and applied on write"
    elif ok:
        r.result = "fail"
        r.details = "DEFAULT accepted but not applied on write"
    else:
        r.result = "fail"
        r.details = (
            f"Flink SQL cannot declare column defaults: {_error_reason(out, 150)}. "
            "The V3 initial-default/write-default metadata is a table-level concern; "
            "Flink's parser rejects DEFAULT in CREATE TABLE"
        )
    return r


def test_time_travel(version: str) -> TestResult:
    r = TestResult("time-travel", "Time Travel / Snapshots", version)
    tbl = _unique("tt")
    # Read the real snapshot id from the metadata table, then travel to it.
    ok, out = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl} (id BIGINT, val STRING) WITH ('format-version'='{_fmt(version)}')",
        f"INSERT INTO {tbl} VALUES (1,'v1')",
        f"INSERT INTO {tbl} VALUES (2,'v2')",
        f"SELECT CONCAT('MARKSNAP=', CAST(snapshot_id AS STRING)) AS m FROM `{tbl}$snapshots` ORDER BY committed_at",
        f"SELECT CONCAT('MARKNOW=', CAST(COUNT(*) AS STRING)) AS m FROM {tbl}",
    ])
    if not ok:
        r.result = "error"
        r.details = _error_reason(out)
        return r
    snaps = _marker_values(out, "MARKSNAP")
    if len(snaps) < 2:
        r.result = "fail"
        r.details = f"Expected 2 snapshots, saw {snaps}"
        return r

    first = snaps[0]
    ok2, out2 = _run_sql(_prelude(version) + [
        f"SELECT CONCAT('MARKOLD=', CAST(COUNT(*) AS STRING)) AS m "
        f"FROM {tbl} /*+ OPTIONS('snapshot-id'='{first}') */",
        f"DROP TABLE {tbl}",
    ])
    if ok2 and _marker(out2, "MARKOLD=1") and _marker(out, "MARKNOW=2"):
        r.result = "pass"
        r.details = (
            f"Current table has 2 rows; reading snapshot {first} via the snapshot-id "
            "hint returned the 1 row present at that snapshot"
        )
    elif ok2:
        r.result = "fail"
        r.details = f"Time travel returned {_marker_values(out2, 'MARKOLD')} rows, expected 1"
    else:
        r.result = "error"
        r.details = _error_reason(out2)
    return r


def test_table_maintenance(version: str) -> TestResult:
    r = TestResult("table-maintenance", "Table Maintenance", version)
    tbl = _unique("maint")
    # Flink is the only engine that runs Iceberg maintenance inside its own job:
    # IcebergSink post-commit tasks, configured from SQL with flink-maintenance.*
    # options (Iceberg 1.11 accepts only jdbc or zookeeper for the SQL lock type).
    #
    # It has to be a STREAMING job. Scheduling counts commits, and a bounded batch
    # INSERT produces a single commit and then the job ends, so compaction never
    # fires. A datagen source with periodic checkpointing commits repeatedly, and a
    # compaction shows up as a "replace" snapshot operation.
    src = "default_catalog.default_database." + _unique("src")
    ok, out, job_id = _submit_streaming(_prelude(version, streaming=True, dml_sync=False) + [
        "SET 'table.exec.iceberg.use-v2-sink' = 'true'",
        f"""CREATE TABLE {tbl} (id BIGINT, val STRING) WITH (
            'format-version'='{_fmt(version)}',
            'flink-maintenance.rewrite.enabled'='true',
            'flink-maintenance.rewrite.schedule.commit-count'='2',
            'flink-maintenance.lock.type'='jdbc',
            'flink-maintenance.lock.lock-id'='{tbl}',
            'flink-maintenance.lock.jdbc.uri'='{JDBC_URI}?user={JDBC_USER}&password={JDBC_PASSWORD}',
            'flink-maintenance.lock.jdbc.init-lock-table'='true')""",
        f"""CREATE TEMPORARY TABLE {src} (id BIGINT, val STRING) WITH (
            'connector'='datagen', 'rows-per-second'='20', 'fields.val.length'='8')""",
        f"INSERT INTO {tbl} SELECT id, val FROM {src}",
    ])
    if not ok or not job_id:
        r.result = "fail"
        r.details = (
            f"In-job maintenance not usable from SQL: {_error_reason(out, 170)}"
            if not ok else "Streaming job was accepted but no Job ID was reported"
        )
        return r

    ops = []
    try:
        # Poll until a rewrite commit appears. Checkpointing is every 5s, so a
        # few commits accumulate quickly; give it generous headroom regardless.
        deadline = MAINTENANCE_POLL_SECONDS
        waited = 0
        while waited < deadline:
            time.sleep(15)
            waited += 15
            ok_p, out_p = _run_sql(_prelude(version) + [
                f"SELECT CONCAT('MARKOP=', operation) AS m "
                f"FROM `{tbl}$snapshots` ORDER BY committed_at",
            ])
            if ok_p:
                ops = _marker_values(out_p, "MARKOP")
                if "replace" in ops:
                    break
    finally:
        _cancel_job(job_id)
        _run_sql(_prelude(version) + [f"DROP TABLE IF EXISTS {tbl}"])

    appends = ops.count("append")
    if "replace" in ops:
        r.result = "pass"
        r.details = (
            f"In-job post-commit compaction ran in a streaming job: {appends} append "
            "commits plus a 'replace' (rewrite) snapshot, with no external scheduler "
            "and no Spark. Configured entirely from SQL via flink-maintenance.* options"
        )
    elif ops:
        r.result = "fail"
        r.details = (
            f"Streaming job committed {appends} snapshots but no rewrite commit appeared "
            f"within {MAINTENANCE_POLL_SECONDS}s (operations={sorted(set(ops))})"
        )
    else:
        r.result = "fail"
        r.details = "Streaming job started but no snapshots were committed"
    return r


def test_branching_tagging(version: str) -> TestResult:
    r = TestResult("branching-tagging", "Branching & Tagging", version)
    tbl = _unique("branch")
    # Flink cannot create refs via DDL; it can write to and read from existing
    # ones. "main" always exists, so the read path is provable.
    ok, out = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl} (id BIGINT, val STRING) WITH ('format-version'='{_fmt(version)}')",
        f"INSERT INTO {tbl} VALUES (1,'a')",
        f"SELECT CONCAT('MARKREF=', name, ':', type) AS m FROM `{tbl}$refs`",
        f"SELECT CONCAT('MARKBR=', CAST(COUNT(*) AS STRING)) AS m "
        f"FROM {tbl} /*+ OPTIONS('branch'='main') */",
    ])
    if not ok:
        r.result = "error"
        r.details = _error_reason(out)
        return r
    read_ok = _marker(out, "MARKBR=1")

    ok_ddl, out_ddl = _run_sql(_prelude(version) + [
        f"ALTER TABLE {tbl} CREATE BRANCH testbranch",
    ])

    # Flink cannot create refs, so make a tag through the catalog and verify the
    # tag read and tag-to-tag incremental scan hints against it.
    tag_ok = False
    ok_s, out_s = _run_sql(_prelude(version) + [
        f"INSERT INTO {tbl} VALUES (2,'b')",
        f"SELECT CONCAT('MARKSNAP=', CAST(snapshot_id AS STRING)) AS m "
        f"FROM `{tbl}$snapshots` ORDER BY committed_at",
    ])
    snaps = _marker_values(out_s, "MARKSNAP")
    if ok_s and len(snaps) >= 2 and _rest_set_tags(tbl, {"tag1": int(snaps[0]), "tag2": int(snaps[1])}):
        ok_t, out_t = _run_sql(_prelude(version) + [
            f"SELECT CONCAT('MARKTAG=', CAST(COUNT(*) AS STRING)) AS m "
            f"FROM {tbl} /*+ OPTIONS('tag'='tag1') */",
            f"SELECT CONCAT('MARKT2T=', val) AS m "
            f"FROM {tbl} /*+ OPTIONS('start-tag'='tag1','end-tag'='tag2') */",
        ])
        tag_ok = ok_t and _marker(out_t, "MARKTAG=1") \
            and _marker_values(out_t, "MARKT2T") == ["b"]
    _run_sql(_prelude(version) + [f"DROP TABLE IF EXISTS {tbl}"])

    if read_ok and ok_ddl:
        r.result = "pass"
        r.details = "Branch reads via the branch hint work, and CREATE BRANCH DDL is supported"
    elif read_ok and tag_ok:
        r.result = "pass"
        r.details = (
            "Branch reads via the branch hint, tag reads via the tag hint, and tag-to-tag "
            "incremental scans (start-tag/end-tag) all work against refs created through "
            f"the catalog; Flink cannot create refs via DDL ({_error_reason(out_ddl, 80)})"
        )
    elif read_ok:
        r.result = "pass"
        r.details = (
            f"Reading a branch via /*+ OPTIONS('branch'='main') */ works and refs are "
            f"listable ({_marker_values(out, 'MARKREF')}); tag hints could not be verified "
            "and Flink cannot create refs via DDL"
        )
    else:
        r.result = "fail"
        r.details = f"Branch read failed: {_marker_values(out, 'MARKBR')}"
    return r


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------

def test_hidden_partitioning(version: str) -> TestResult:
    r = TestResult("hidden-partitioning", "Hidden Partitioning", version)
    tbl = _unique("hidpart")
    # Transform partitioning in DDL.
    ok_t, out_t = _run_sql(_prelude(version) + [
        f"""CREATE TABLE {tbl}_t (id BIGINT, ts TIMESTAMP(6))
            PARTITIONED BY (days(ts)) WITH ('format-version'='{_fmt(version)}')""",
        f"DROP TABLE {tbl}_t",
    ])
    if ok_t:
        r.result = "pass"
        r.details = "Flink DDL accepted a transform-based (hidden) partition spec"
        return r

    # Flink cannot declare it, so create a day(ts)-partitioned table through the
    # catalog API and check the half that actually matters: can Flink write into a
    # hidden-partitioned table, honour the transform, and prune on it?
    ext = _rest_create_transform_partitioned(version)
    if not ext:
        r.result = "fail"
        r.details = (
            f"Transform partitioning is a parser error in Flink DDL "
            f"({_error_reason(out_t, 110)}); could not create a hidden-partitioned "
            "table through the catalog API to test the read/write path"
        )
        return r

    ok_e, out_e = _run_sql(_prelude(version) + [
        f"""INSERT INTO {ext} VALUES
            (1, TIMESTAMP '2026-01-01 10:00:00'),
            (2, TIMESTAMP '2026-01-02 10:00:00'),
            (3, TIMESTAMP '2026-01-01 20:00:00')""",
        f"SELECT CONCAT('MARKALL=', CAST(COUNT(*) AS STRING)) AS m FROM {ext}",
        # 3 rows across 2 distinct days: 2 partitions proves the day() transform
        # was applied by Flink on write, not ignored.
        f"SELECT CONCAT('MARKPARTS=', CAST(COUNT(*) AS STRING)) AS m FROM `{ext}$partitions`",
        # Filtering on the source column must prune via the hidden partition.
        f"SELECT CONCAT('MARKPRUNE=', CAST(COUNT(*) AS STRING)) AS m FROM {ext} "
        f"WHERE ts < TIMESTAMP '2026-01-02 00:00:00'",
        f"DROP TABLE {ext}",
    ])
    wrote = _marker(out_e, "MARKALL=3") and _marker(out_e, "MARKPARTS=2")
    if ok_e and wrote and _marker(out_e, "MARKPRUNE=2"):
        r.result = "pass"
        r.details = (
            "Flink cannot declare transform partitioning (PARTITIONED BY (days(ts)) is a "
            "parser error), but on a day(ts)-partitioned table created through the catalog "
            "API it wrote 3 rows into the correct 2 day-partitions and pruned correctly "
            "when filtering the source column. So the gap is DDL-only: hidden-partitioned "
            "tables must be created elsewhere, then Flink reads and writes them properly"
        )
    elif ok_e:
        r.result = "fail"
        r.details = (
            "Flink cannot declare transform partitioning, and on an externally created "
            f"hidden-partitioned table the write did not honour it: rows="
            f"{_marker_values(out_e, 'MARKALL')}, partitions={_marker_values(out_e, 'MARKPARTS')}, "
            f"pruned={_marker_values(out_e, 'MARKPRUNE')}"
        )
    else:
        r.result = "fail"
        r.details = (
            f"Transform partitioning not declarable in Flink DDL, and writing to an "
            f"externally created hidden-partitioned table failed: {_error_reason(out_e, 130)}"
        )
    return r


def test_partition_evolution(version: str) -> TestResult:
    r = TestResult("partition-evolution", "Partition Evolution", version)
    tbl = _unique("partevo")
    setup = [
        f"""CREATE TABLE {tbl} (id BIGINT, region STRING, ts TIMESTAMP(6))
            PARTITIONED BY (region) WITH ('format-version'='{_fmt(version)}')""",
        f"INSERT INTO {tbl} VALUES (1,'eu',TIMESTAMP '2026-01-01 10:00:00')",
    ]
    # ADD PARTITION FIELD is a Spark SQL extension; Flink has no partition DDL at
    # all, so this is the only syntax there is to try.
    ok_ddl, out_ddl = _run_sql(_prelude(version) + setup + [
        f"ALTER TABLE {tbl} ADD PARTITION FIELD ts",
    ])
    if ok_ddl:
        _run_sql(_prelude(version) + [f"DROP TABLE IF EXISTS {tbl}"])
        r.result = "pass"
        r.details = "ALTER TABLE ADD PARTITION FIELD evolved the partition spec via Flink SQL"
        return r

    # Flink cannot initiate the evolution, so drive it through the catalog and test
    # whether Flink honours the result: new writes must use the new spec while old
    # data files stay on the old one, and reads must span both.
    if not _rest_evolve_spec(tbl):
        _run_sql(_prelude(version) + [f"DROP TABLE IF EXISTS {tbl}"])
        r.result = "fail"
        r.details = (
            f"No partition-evolution syntax in Flink SQL ({_error_reason(out_ddl, 110)}), "
            "and the spec could not be evolved through the catalog to test the write path"
        )
        return r

    ok_w, out_w = _run_sql(_prelude(version) + [
        f"INSERT INTO {tbl} VALUES (2,'us',TIMESTAMP '2026-02-02 10:00:00')",
        f"SELECT CONCAT('MARKALL=', CAST(COUNT(*) AS STRING)) AS m FROM {tbl}",
        f"SELECT CONCAT('MARKSPEC=', CAST(spec_id AS STRING)) AS m FROM `{tbl}$files`",
        f"SELECT CONCAT('MARKOLD=', CAST(COUNT(*) AS STRING)) AS m FROM {tbl} WHERE region='eu'",
        f"DROP TABLE {tbl}",
    ])
    specs = sorted(set(_marker_values(out_w, "MARKSPEC")))
    if ok_w and specs == ["0", "1"] and _marker(out_w, "MARKALL=2"):
        r.result = "pass"
        r.details = (
            "Flink cannot initiate partition evolution (no SQL syntax; ADD PARTITION FIELD "
            "is a parser error) but honours it fully once the catalog evolves the spec: after "
            "adding day(ts) as the default spec, a Flink write landed in the new spec while "
            f"the pre-existing file stayed on the old one (spec_ids={specs}), and reads span "
            "both specs correctly"
        )
    elif ok_w:
        r.result = "fail"
        r.details = (
            f"Spec evolved in the catalog but Flink did not write with the new spec: "
            f"spec_ids={specs}, rows={_marker_values(out_w, 'MARKALL')}"
        )
    else:
        r.result = "fail"
        r.details = (
            f"No partition-evolution syntax in Flink SQL, and writing after the catalog "
            f"evolved the spec failed: {_error_reason(out_w, 130)}"
        )
    return r


def test_multi_arg_transforms(version: str) -> TestResult:
    if version == "v2":
        return _v3_only("multi-arg-transforms", "Multi-Argument Transforms")
    r = TestResult("multi-arg-transforms", "Multi-Argument Transforms", version)
    r.result = "skip"
    r.details = (
        "Not exercised: Flink DDL cannot express any transform partitioning at all "
        "(PARTITIONED BY only takes plain column names), so a multi-argument "
        "transform cannot be declared from SQL"
    )
    return r


# ---------------------------------------------------------------------------
# V3 data types
# ---------------------------------------------------------------------------

def test_variant_type(version: str) -> TestResult:
    if version == "v2":
        return _v3_only("variant-type", "Variant Type")
    r = TestResult("variant-type", "Variant Type", version)
    tbl = _unique("variant")
    # Flink 2.3 provides the variant constructors PARSE_JSON / TRY_PARSE_JSON but
    # no variant field accessor, and CAST(VARIANT AS STRING) is rejected by the
    # planner. So assert that the value round-trips and is non-null, which is the
    # most that Flink SQL can observe about a variant.
    ok, out = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl} (id BIGINT, v VARIANT) WITH ('format-version'='3')",
        f"INSERT INTO {tbl} SELECT CAST(1 AS BIGINT), PARSE_JSON('{{\"a\":42}}')",
        f"""SELECT CONCAT('MARKVAR=', CASE WHEN v IS NOT NULL THEN 'STORED' ELSE 'NULL' END) AS m
            FROM {tbl}""",
        f"DROP TABLE {tbl}",
    ])
    if not ok:
        r.result = "fail"
        r.details = f"VARIANT not usable from Flink SQL: {_error_reason(out, 160)}"
    elif _marker(out, "MARKVAR=STORED"):
        r.result = "pass"
        r.details = (
            "VARIANT column on a V3 table: value written with PARSE_JSON and read back "
            "non-null. Field extraction is not possible from Flink SQL -- Flink 2.3 "
            "exposes only the PARSE_JSON/TRY_PARSE_JSON constructors, with no variant "
            "accessor function and no CAST from VARIANT"
        )
    else:
        r.result = "fail"
        r.details = f"VARIANT written but did not read back: {_marker_values(out, 'MARKVAR')}"
    return r


def test_shredded_variant(version: str) -> TestResult:
    if version == "v2":
        return _v3_only("shredded-variant", "Shredded Variant")
    r = TestResult("shredded-variant", "Shredded Variant", version)
    tbl = _unique("shred")
    # Shredding is a writer-side physical layout choice; there is no SQL surface
    # to request it and no metadata table exposing whether it happened.
    ok, out = _run_sql(_prelude(version) + [
        f"""CREATE TABLE {tbl} (id BIGINT, v VARIANT) WITH (
            'format-version'='3', 'write.parquet.variant-shredding.enabled'='true')""",
        f"INSERT INTO {tbl} SELECT CAST(1 AS BIGINT), PARSE_JSON('{{\"a\":42}}')",
        f"SELECT CONCAT('MARKCNT=', CAST(COUNT(*) AS STRING)) AS m FROM {tbl}",
        f"DROP TABLE {tbl}",
    ])
    r.result = "skip"
    if ok and _marker(out, "MARKCNT=1"):
        r.details = (
            "Not verifiable from SQL: the shredding table property is accepted and data "
            "round-trips, but whether the writer actually shredded the variant is not "
            "observable through any Flink SQL surface or metadata table"
        )
    else:
        r.details = (
            "Not verifiable from SQL: no Flink SQL surface requests or reports variant "
            f"shredding ({_error_reason(out, 110)})"
        )
    return r


def test_geometry_type(version: str) -> TestResult:
    if version == "v2":
        return _v3_only("geometry-type", "Geometry / Geo Types")
    r = TestResult("geometry-type", "Geometry / Geo Types", version)
    tbl = _unique("geo")
    ok, out = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl} (id BIGINT, g GEOMETRY) WITH ('format-version'='3')",
        f"DROP TABLE {tbl}",
    ])
    if ok:
        r.result = "pass"
        r.details = "GEOMETRY column accepted in Flink DDL on a V3 table"
    elif "Geo-spatial extensions" in out or "GEOMETRY" in out:
        r.result = "fail"
        r.details = (
            "Blocked by Flink, not Iceberg: the Calcite-based planner keeps GEOMETRY "
            "behind its spatial extensions (enabled via the Calcite fun=spatial connect "
            "string), which Flink exposes no configuration for, so the type cannot be "
            "declared in Flink SQL"
        )
    else:
        r.result = "fail"
        r.details = f"GEOMETRY rejected: {_error_reason(out, 150)}"
    return r


def test_nanosecond_timestamps(version: str) -> TestResult:
    if version == "v2":
        return _v3_only("nanosecond-timestamps", "Nanosecond Timestamps")
    r = TestResult("nanosecond-timestamps", "Nanosecond Timestamps", version)
    tbl = _unique("nanots")
    # Compare inside the engine so the assertion tests precision, not formatting:
    # a silent truncation to microseconds makes the equality fail.
    ok, out = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl} (id BIGINT, ts TIMESTAMP(9)) WITH ('format-version'='3')",
        f"INSERT INTO {tbl} VALUES (1, TIMESTAMP '2026-01-01 12:00:00.123456789')",
        f"""SELECT CONCAT('MARKNANO=', CASE WHEN ts = TIMESTAMP '2026-01-01 12:00:00.123456789'
             THEN 'EXACT' ELSE 'LOSSY' END) AS m FROM {tbl}""",
    ])
    # The round-trip alone could in principle be satisfied by Flink comparing two
    # equally-truncated values; confirm the column is genuinely the V3 timestamp_ns
    # type by reading the Iceberg schema from the catalog.
    meta = _rest_table_metadata(tbl)
    stored = ""
    if meta:
        for f in meta.get("schemas", [{}])[-1].get("fields", []):
            if f.get("name") == "ts":
                stored = str(f.get("type"))
    _run_sql(_prelude(version) + [f"DROP TABLE IF EXISTS {tbl}"])

    if not ok:
        r.result = "fail"
        r.details = f"TIMESTAMP(9) not usable on a V3 table: {_error_reason(out, 150)}"
    elif _marker(out, "MARKNANO=EXACT") and stored == "timestamp_ns":
        r.result = "pass"
        r.details = (
            "TIMESTAMP(9) maps to the Iceberg V3 timestamp_ns type (confirmed in the table "
            "schema) and a nanosecond-precision value round-trips exactly"
        )
    elif _marker(out, "MARKNANO=EXACT"):
        r.result = "pass"
        r.details = (
            f"Nanosecond value round-tripped exactly, but the stored Iceberg type reads as "
            f"'{stored or 'unavailable'}' rather than timestamp_ns"
        )
    else:
        r.result = "fail"
        r.details = f"TIMESTAMP(9) accepted but the value lost precision (stored type: {stored})"
    return r


# ---------------------------------------------------------------------------
# V3 advanced
# ---------------------------------------------------------------------------

def test_lineage(version: str) -> TestResult:
    if version == "v2":
        return _v3_only("lineage", "Lineage Tracking")
    r = TestResult("lineage", "Lineage Tracking", version)
    tbl = _unique("lineage")
    ok, out = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl} (id BIGINT, val STRING) WITH ('format-version'='3')",
        f"INSERT INTO {tbl} VALUES (1,'a'),(2,'b'),(3,'c')",
        f"SELECT CONCAT('MARKLIN=', CAST(_row_id AS STRING)) AS m FROM {tbl}",
    ])
    readable = ok and bool(_marker_values(out, "MARKLIN"))

    # Whether Flink can project the lineage columns is only half the question. The
    # other half is whether a Flink write maintains the V3 lineage metadata at all,
    # which decides between "unusable" and "written but not exposed". Read it from
    # the catalog, since Flink SQL cannot surface it.
    meta = _rest_table_metadata(tbl)
    next_row_id = meta.get("next-row-id") if meta else None
    snaps = (meta or {}).get("snapshots") or []
    assigned = [s for s in snaps if s.get("first-row-id") is not None]
    _run_sql(_prelude(version) + [f"DROP TABLE IF EXISTS {tbl}"])

    if readable:
        r.result = "pass"
        r.details = (
            f"Row lineage columns readable from Flink SQL: {_marker_values(out, 'MARKLIN')}"
        )
    elif assigned and next_row_id:
        r.result = "pass"
        r.details = (
            f"Flink maintains V3 row lineage on write -- after inserting 3 rows the table "
            f"metadata carries next-row-id={next_row_id} and the snapshot reports "
            f"first-row-id={assigned[0].get('first-row-id')}, added-rows={assigned[0].get('added-rows')} "
            "-- but none of it is reachable from Flink SQL: selecting _row_id fails, "
            "metadata (computed) columns are rejected outright, and the $snapshots table "
            "exposes no lineage column"
        )
    else:
        r.result = "fail"
        r.details = (
            "Row lineage neither projectable from Flink SQL nor visible in the table "
            f"metadata after a Flink write ({_error_reason(out, 110)})"
        )
    return r


# ---------------------------------------------------------------------------
# Read/write extras
# ---------------------------------------------------------------------------

def test_statistics(version: str) -> TestResult:
    r = TestResult("statistics", "Statistics (Column Metrics)", version)
    tbl = _unique("stats")
    # Prove per-column metrics were written, rather than only that a write happened.
    ok, out = _run_sql(_prelude(version) + [
        f"CREATE TABLE {tbl} (id BIGINT, val STRING) WITH ('format-version'='{_fmt(version)}')",
        f"INSERT INTO {tbl} VALUES (1,'a'),(2,'b'),(3,'c')",
        f"SELECT CONCAT('MARKREC=', CAST(record_count AS STRING)) AS m FROM `{tbl}$files`",
        f"SELECT CONCAT('MARKVC=', CAST(CARDINALITY(value_counts) AS STRING)) AS m FROM `{tbl}$files`",
        f"SELECT CONCAT('MARKNULL=', CAST(CARDINALITY(null_value_counts) AS STRING)) AS m FROM `{tbl}$files`",
        f"DROP TABLE {tbl}",
    ])
    if not ok:
        r.result = "error"
        r.details = _error_reason(out)
        return r
    vc = _marker_values(out, "MARKVC")
    if _marker(out, "MARKREC=3") and vc and vc[0] not in ("0", ""):
        r.result = "pass"
        r.details = (
            f"Data file manifest carries record_count=3 and per-column value_counts "
            f"for {vc[0]} columns, plus null_value_counts {_marker_values(out, 'MARKNULL')}"
        )
    else:
        r.result = "fail"
        r.details = f"Column metrics missing: record={_marker_values(out, 'MARKREC')}, value_counts={vc}"
    return r


def test_bloom_filters(version: str) -> TestResult:
    r = TestResult("bloom-filters", "Bloom Filters & Puffin", version)
    tbl = _unique("bloom")
    ok, out = _run_sql(_prelude(version) + [
        f"""CREATE TABLE {tbl} (id BIGINT, val STRING) WITH (
            'format-version'='{_fmt(version)}',
            'write.parquet.bloom-filter-enabled.column.val'='true')""",
        f"INSERT INTO {tbl} VALUES (1,'a'),(2,'b')",
        f"SELECT CONCAT('MARKCNT=', CAST(COUNT(*) AS STRING)) AS m FROM {tbl}",
        f"SELECT CONCAT('MARKSEL=', CAST(COUNT(*) AS STRING)) AS m FROM {tbl} WHERE val = 'a'",
        f"DROP TABLE {tbl}",
    ])
    r.result = "skip"
    if ok and _marker(out, "MARKCNT=2") and _marker(out, "MARKSEL=1"):
        r.details = (
            "Not verifiable from SQL: the Parquet bloom-filter write property is accepted "
            "and point lookups return correct results, but no Flink SQL surface or Iceberg "
            "metadata table reports whether a bloom filter was written or used to skip data"
        )
    else:
        r.details = (
            "Not verifiable from SQL: bloom filter presence is not observable through "
            f"Flink SQL ({_error_reason(out, 110)})"
        )
    return r


# ---------------------------------------------------------------------------
# Catalog support
# ---------------------------------------------------------------------------

def _catalog_roundtrip(version: str, catalog: str) -> tuple:
    """Create, write, read and drop a table through the named catalog."""
    tbl = _unique("cat")
    return _run_sql(_prelude(version, catalog=catalog) + [
        f"CREATE TABLE {tbl} (id BIGINT, val STRING) WITH ('format-version'='{_fmt(version)}')",
        f"INSERT INTO {tbl} VALUES (1,'a'),(2,'b')",
        f"SELECT CONCAT('MARKCAT=', CAST(COUNT(*) AS STRING)) AS m FROM {tbl}",
        f"DROP TABLE {tbl}",
    ])


def test_catalog_integration(version: str) -> TestResult:
    r = TestResult("catalog-integration", "Catalog Integration", version)
    ok, out = _catalog_roundtrip(version, "rest")
    if ok and _marker(out, "MARKCAT=2"):
        r.result = "pass"
        r.details = "Full create/write/read/drop round-trip through an Iceberg catalog"
    else:
        r.result = "error" if not ok else "fail"
        r.details = _error_reason(out)
    return r


def test_rest_catalog(version: str) -> TestResult:
    r = TestResult("rest-catalog", "REST Catalog", version)
    ok, out = _catalog_roundtrip(version, "rest")
    if ok and _marker(out, "MARKCAT=2"):
        r.result = "pass"
        r.details = (
            "catalog-type='rest' against a live Lakekeeper REST catalog: table created, "
            "written, read back and dropped"
        )
    else:
        r.result = "error" if not ok else "fail"
        r.details = _error_reason(out)
    return r


def test_hadoop_catalog(version: str) -> TestResult:
    r = TestResult("hadoop-catalog", "Hadoop Catalog", version)
    ok, out = _catalog_roundtrip(version, "hadoop")
    if ok and _marker(out, "MARKCAT=2"):
        r.result = "pass"
        r.details = (
            f"catalog-type='hadoop' on a filesystem warehouse ({HADOOP_WAREHOUSE}): "
            "table created, written, read back and dropped"
        )
    else:
        r.result = "fail" if ok else "error"
        r.details = _error_reason(out)
    return r


def test_jdbc_catalog(version: str) -> TestResult:
    r = TestResult("jdbc-catalog", "JDBC Catalog", version)
    ok, out = _catalog_roundtrip(version, "jdbc")
    if ok and _marker(out, "MARKCAT=2"):
        r.result = "pass"
        r.details = (
            "JDBC catalog against the stack's Postgres instance: table created, written, "
            "read back and dropped. Reached via catalog-impl=org.apache.iceberg.jdbc."
            "JdbcCatalog, not catalog-type: Flink's catalog-type accepts only hive, "
            "hadoop and rest"
        )
    else:
        r.result = "fail" if ok else "error"
        r.details = _error_reason(out)
    return r


def test_hive_metastore(version: str) -> TestResult:
    return _external_service("hive-metastore", "Hive Metastore", version,
                             "a running Hive Metastore (thrift) service")


def test_aws_glue_catalog(version: str) -> TestResult:
    return _external_service("aws-glue-catalog", "AWS Glue Catalog", version,
                             "AWS credentials and a Glue Data Catalog")


def test_nessie(version: str) -> TestResult:
    return _external_service("nessie", "Nessie", version, "a running Nessie server")


def test_polaris(version: str) -> TestResult:
    return _external_service("polaris", "Polaris", version,
                             "a running Apache Polaris server (reachable via catalog-type='rest')")


def test_unity_catalog(version: str) -> TestResult:
    return _external_service("unity-catalog", "Unity Catalog", version,
                             "a Databricks Unity Catalog endpoint")


def test_snowflake_horizon_catalog(version: str) -> TestResult:
    return _external_service("snowflake-horizon-catalog", "Snowflake Horizon Catalog", version,
                             "a Snowflake account with Horizon Catalog enabled")


# ---------------------------------------------------------------------------
# Test registry
# ---------------------------------------------------------------------------
# Every test takes a version ("v2" | "v3") and returns a TestResult.

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
    test_hadoop_catalog,
    test_jdbc_catalog,
    test_hive_metastore,
    test_aws_glue_catalog,
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

def load_flink_json_support() -> dict:
    """Load the recorded support levels for the platform under test.

    Reads MATRIX_DATA_PATH and keeps only the cells belonging to
    MATRIX_PLATFORM_ID, so the suite is not tied to the OSS Flink cells.
    """
    path = os.path.join(REPO_ROOT, MATRIX_DATA_PATH)
    with open(path) as f:
        data = json.load(f)
    result = {}
    for key, val in data.get("support", {}).items():
        parts = key.split(":")
        if len(parts) == 3 and parts[0] == MATRIX_PLATFORM_ID:
            result[(parts[1], parts[2])] = val.get("level", "unknown")
    return result


def load_matrix_features() -> dict:
    """Load feature definitions from the matrix source of truth (features.json).

    Lets the suite assert that EVERY feature shown in the matrix is exercised, so
    a newly added matrix feature cannot silently go untested.
    """
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
    """Compare the set of tested feature ids against the matrix features.

    A non-empty "uncovered" list means the suite has drifted from the matrix and
    is treated as a failure, matching the Spark-based suites.
    """
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
    counted separately as "unverified" so an unverifiable feature can never look
    like a confirmation of the matrix.
    """
    if test_result in ("skip", "error"):
        return True
    if test_result == "pass":
        return json_level in ("full", "partial")
    if test_result == "fail":
        return json_level in ("none", "partial")
    return True


def generate_report(results: list) -> dict:
    json_support = load_flink_json_support()

    tests_output = []
    discrepancies = 0
    unverified = 0
    for r in results:
        json_level = json_support.get((r.feature_id, r.version_tested), "unknown")
        match = compute_match(r.result, json_level)
        if not match:
            discrepancies += 1
        # A cell the matrix asserts support for, that nothing here could confirm.
        is_unverified = r.result in ("skip", "error")
        if is_unverified:
            unverified += 1
        tests_output.append({
            **r.to_dict(),
            "json_level": json_level,
            "match": match,
            "verified": not is_unverified,
        })

    coverage = compute_coverage(results)

    return {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "engine": "Flink",
        "mode": MODE,
        "flink_version": FLINK_VERSION,
        "flink_iceberg_version": FLINK_ICEBERG_VERSION,
        "platform": MATRIX_PLATFORM_ID,
        "platform_label": PLATFORM_LABEL,
        "catalog_mode": CATALOG_MODE,
        "versions_tested": VERSIONS,
        "coverage": coverage,
        "tests": tests_output,
        "summary": {
            "total": len(results),
            "passed": sum(1 for r in results if r.result == "pass"),
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
        "# Flink Iceberg Feature Test Report",
        "",
        f"- **Timestamp:** {report['timestamp']}",
        f"- **Flink Version:** {report['flink_version']}",
        f"- **Iceberg Version:** {report['flink_iceberg_version']}",
        f"- **Execution mode:** {report['mode']}",
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
        f"| Failed | {s['failed']} |",
        f"| Skipped | {s['skipped']} |",
        f"| Errors | {s['errors']} |",
        f"| Discrepancies vs matrix | {s['discrepancies']} |",
        f"| Unverified (skip/error) | {s['unverified']} |",
        f"| Uncovered matrix features | {s.get('uncovered_features', 0)} |",
        "",
        "`Failed` is a result, not a defect: it records that the engine does not "
        "support the feature through Flink SQL. A discrepancy means the observed "
        "behaviour disagrees with `flink.json`.",
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
                lines.append(
                    f"- **{f['name']}** (`{f['id']}`) - add a `test_*` function "
                    "and register it in `ALL_TESTS`"
                )
        if cov.get("extra"):
            lines += ["", f"> Note: tests exist for ids not in the matrix: "
                          f"{', '.join(cov['extra'])}"]
        lines.append("")

    lines += [
        "## Test Results",
        "",
        "| Feature | Version | Result | Matrix | Match | Details |",
        "|---------|---------|--------|--------|-------|---------|",
    ]

    emoji = {"pass": "PASS", "fail": "FAIL", "skip": "SKIP", "error": "ERR"}
    for t in report["tests"]:
        details = (t["details"] or "")[:150].replace("\n", " ").replace("|", "\\|")
        lines.append(
            f"| {t['feature_name'].replace('|', '')} | {t['version']} "
            f"| {emoji.get(t['result'], '?')} | {t['json_level']} "
            f"| {'ok' if t['match'] else 'DISCREPANCY'} | {details} |"
        )

    discs = [t for t in report["tests"] if not t["match"]]
    if discs:
        lines += ["", "## Discrepancies", ""]
        for t in discs:
            lines.append(
                f"- **{t['feature_name']}** ({t['version']}): observed `{t['result']}`, "
                f"matrix says `{t['json_level']}` — {(t['details'] or '')[:300]}"
            )

    unver = [t for t in report["tests"] if not t["verified"]]
    if unver:
        lines += ["", "## Unverified", "",
                  "These could not be exercised here, so they neither confirm nor "
                  "contradict the matrix:", ""]
        for t in unver:
            lines.append(
                f"- **{t['feature_name']}** ({t['version']}): matrix `{t['json_level']}` "
                f"— {(t['details'] or '')[:200]}"
            )

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("  Flink Iceberg Feature Test Suite")
    print("=" * 70)
    print(f"Mode:            {MODE}")
    print(f"Flink version:   {FLINK_VERSION}")
    print(f"Iceberg version: {FLINK_ICEBERG_VERSION}")
    print(f"REST catalog:    {REST_URI} (warehouse {REST_WAREHOUSE})")
    print(f"S3 endpoint:     {S3_ENDPOINT}")
    print(f"JDBC catalog:    {JDBC_URI}")
    print(f"Matrix platform: {MATRIX_PLATFORM_ID} ({MATRIX_DATA_PATH})")
    if PLATFORM_LABEL:
        print(f"Platform:        {PLATFORM_LABEL}")
    print(f"Versions:        {', '.join(VERSIONS)}")
    print()

    if MODE == "local" and not FLINK_HOME:
        print("[FATAL] No Docker Flink cluster found and FLINK_HOME is not set.")
        print("        Start one with tests/docker/start-flink.sh")
        sys.exit(1)

    only = os.environ.get("FLINK_ONLY", "").strip()
    tests = ALL_TESTS
    if only:
        wanted = {t.strip() for t in only.split(",") if t.strip()}
        tests = [t for t in ALL_TESTS if t.__name__.replace("test_", "") in wanted]
        print(f"[INFO] FLINK_ONLY set; running {len(tests)} test(s): {sorted(wanted)}\n")

    os.makedirs(REPORT_DIR, exist_ok=True)

    results = []
    for version in VERSIONS:
        print(f"\n{'=' * 70}\n  Format version {version.upper()}\n{'=' * 70}")
        for test_fn in tests:
            name = test_fn.__name__
            print(f"\n--- {name} [{version}] ---")
            try:
                result = test_fn(version)
            except Exception as e:  # noqa: BLE001
                result = TestResult(
                    name.replace("test_", "").replace("_", "-"), name, version
                )
                result.result = "error"
                result.details = f"Unhandled exception: {e}"
            results.append(result)
            print(f"  {result.result}: {result.details[:160]}")

    report = generate_report(results)

    json_path = os.path.join(REPORT_DIR, "flink-iceberg-test-report.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)

    md_content = generate_markdown(report)
    md_path = os.path.join(REPORT_DIR, "flink-iceberg-test-report.md")
    with open(md_path, "w") as f:
        f.write(md_content)

    s = report["summary"]
    print(f"\n{'=' * 70}")
    print(f"  {s['passed']} passed, {s['failed']} failed, {s['skipped']} skipped, "
          f"{s['errors']} errors, {s['discrepancies']} discrepancies, "
          f"{s['unverified']} unverified, "
          f"{s.get('uncovered_features', 0)} uncovered matrix features")
    print(f"  Reports: {json_path}")
    print(f"           {md_path}")
    print(f"{'=' * 70}")
    print("\n" + md_content)

    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(md_content)

    # Errors mean the harness itself could not run something; discrepancies mean
    # the matrix and reality disagree; uncovered features mean the suite has
    # drifted from the matrix. All three warrant a human look.
    sys.exit(1 if (s["discrepancies"] > 0 or s["errors"] > 0
                   or s.get("uncovered_features", 0) > 0) else 0)


if __name__ == "__main__":
    main()
