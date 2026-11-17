#!/usr/bin/env python3
"""
BigQuery Iceberg Feature Test Suite (V2 + V3).

Drives BigQuery through the google-cloud-bigquery client and compares what it
actually does with the support levels recorded for the google-bigquery
platform. Disagreements are "discrepancies"; anything that cannot be exercised
is a "skip" with the reason, counted as unverified, so a skip never silently
confirms a matrix cell.

BigQuery has two Iceberg table types, and they behave differently enough that
each is its own storage mode (MATRIX_STORAGE_MODE):

    managed     Apache Iceberg managed tables in BigQuery: created in a
                BigQuery dataset WITH CONNECTION, data in a bucket you own,
                only BigQuery writes, Iceberg metadata is exported for others.
    lakehouse   Tables in the Lakehouse runtime catalog (Iceberg REST):
                BigQuery addresses them as project.catalog.namespace.table and
                shares them with Spark, Trino or Flink.

Ratings follow the conventions the other suites use. Row-level-operation cells
(position/equality deletes, merge-on-read, copy-on-write, deletion vectors)
describe what BigQuery's *own* DML writes, so being able to read another
engine's delete files is reported in the details but never earns the cell. V3
column types and features BigQuery cannot declare are probed by reading a
table Spark made (lakehouse mode), which separates "cannot write" from "cannot
read": readable but not writable is partial.

Evidence about what a write produced comes from the table's own Iceberg
metadata (manifest list and manifests, read with fastavro), not from file
names, so copy-on-write versus merge-on-read is answered by Iceberg itself.

Environment:
    GCP_PROJECT            (required)
    GCP_REGION             (default us-central1)
    GCP_DATASET            dataset for managed tables
    GCP_CONNECTION         project.region.connection for managed tables
    GCP_DATA_BUCKET        bucket for managed table data
    GCP_LAKEHOUSE_CATALOG  Lakehouse runtime catalog id
    MATRIX_STORAGE_MODE    managed | lakehouse   (default lakehouse)
    MATRIX_DATA_PATH       matrix JSON to compare against
    BIGQUERY_ONLY          comma-separated test names to run (debugging)
    RUN_TAG                unique per run (default random)

Spark fixtures (lakehouse mode) need pyspark and Java 17; the suite sets
ICEBERG_CATALOG_FLAVOR=gcp-lakehouse for tests/spark_fixture.py itself.
"""
import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone

PROJECT = os.environ.get("GCP_PROJECT", "")
REGION = os.environ.get("GCP_REGION", "us-central1")
DATASET = os.environ.get("GCP_DATASET", "icebergmatrix_tests")
CONNECTION = os.environ.get("GCP_CONNECTION", "")
DATA_BUCKET = os.environ.get("GCP_DATA_BUCKET", "")
CATALOG = os.environ.get("GCP_LAKEHOUSE_CATALOG", "")
STORAGE_MODE = os.environ.get("MATRIX_STORAGE_MODE", "lakehouse")
RUN_TAG = os.environ.get("RUN_TAG", uuid.uuid4().hex[:8])
NS = f"bq_{RUN_TAG}"
REST_URI = "https://biglake.googleapis.com/iceberg/v1/restcatalog"
REST_PREFIX = f"{REST_URI}/v1/projects/{PROJECT}/catalogs/{CATALOG}"

MATRIX_PLATFORM_ID = os.environ.get("MATRIX_PLATFORM_ID", "google-bigquery")
MATRIX_DATA_PATH = os.environ.get(
    "MATRIX_DATA_PATH", f"src/data/platforms/gcp/{STORAGE_MODE}/bigquery/bigquery.json")
VERSIONS = ["v2", "v3"]
STATEMENT_TIMEOUT = int(os.environ.get("BIGQUERY_STATEMENT_TIMEOUT", "300"))

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(REPO_ROOT, "test-reports"))

# The Spark fixture reads its catalog flavour at import time.
os.environ.setdefault("ICEBERG_CATALOG_FLAVOR", "gcp-lakehouse")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

LAKEHOUSE = STORAGE_MODE == "lakehouse"


class TestResult:
    def __init__(self, feature_id: str, feature_name: str, version: str = "v2",
                 diagnostic: bool = False):
        self.feature_id = feature_id
        self.feature_name = feature_name
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


def _names(r: TestResult) -> tuple:
    """(feature id, name) of a declared result. Tests start with a literal
    TestResult("<feature-id>", ...) because src/data/test-coverage.ts finds the
    cells a suite tests by reading those calls."""
    return r.feature_id, r.feature_name


def _res(fid: str, name: str, version: str, result: str, details: str) -> TestResult:
    r = TestResult(fid, name, version)
    r.result, r.details = result, details
    return r


# ---------------------------------------------------------------------------
# BigQuery, REST catalog and storage plumbing
# ---------------------------------------------------------------------------

_BQ = None
_SESSION = None
_GCS = None


def _bq():
    global _BQ
    if _BQ is None:
        from google.cloud import bigquery
        _BQ = bigquery.Client(project=PROJECT, location=REGION)
    return _BQ


def _gcs():
    global _GCS
    if _GCS is None:
        from google.cloud import storage
        _GCS = storage.Client(project=PROJECT)
    return _GCS


def _rest():
    """An authorised HTTP session for the Lakehouse Iceberg REST endpoint."""
    global _SESSION
    if _SESSION is None:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        _SESSION = AuthorizedSession(creds)
        # The catalog runs in credential-vending mode and rejects any request
        # that does not ask for vended credentials.
        _SESSION.headers.update({"x-goog-user-project": PROJECT,
                                 "X-Iceberg-Access-Delegation": "vended-credentials"})
    return _SESSION


def _reason(err, limit: int = 400) -> str:
    """One readable line from a BigQuery or HTTP error, without job ids."""
    msg = getattr(err, "message", None) or str(err)
    msg = re.sub(r"\s+", " ", msg)
    msg = re.sub(r"(Error processing job|Job) '?[\w:.-]+'?:? ?", "", msg)
    msg = re.sub(r"https?://\S+", "", msg).strip(" :")
    return msg[:limit]


_TRANSIENT = ("Retrying may solve the problem", "internal error", "backendError",
              "transient issue")


def _run(sql: str, timeout: int = None, retries: int = 2) -> tuple:
    """Run one statement. Returns (True, rows as dicts) or (False, reason).

    Errors BigQuery itself labels transient are retried a bounded number of
    times; the client's own job_retry is off (it resubmits for up to 10
    minutes, which turned one refused statement into 25 identical jobs).
    """
    for attempt in range(retries + 1):
        try:
            job = _bq().query(sql, job_retry=None)
            rows = [dict(r.items()) for r in job.result(timeout=timeout or STATEMENT_TIMEOUT,
                                                        job_retry=None)]
            return True, rows
        except Exception as e:  # noqa: BLE001 - the reason is the measurement
            reason = _reason(e)
            if attempt < retries and any(t in str(e) for t in _TRANSIENT):
                time.sleep(5 * (attempt + 1))
                continue
            return False, reason
    return False, "unreachable"


def _run_all(statements: list) -> tuple:
    """Run statements in order; stop at the first failure.

    Returns (ok, rows of the last statement | reason, index that failed or -1).
    """
    rows = []
    for i, sql in enumerate(statements):
        ok, rows = _run(sql)
        if not ok:
            return False, rows, i
    return True, rows, -1


def _ids(rows: list, col: str = "id") -> list:
    return sorted(int(r[col]) for r in rows)


def _unique(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


_CREATED = []  # managed-mode tables this run owns


def _t(name: str) -> str:
    """Fully qualified BigQuery name of a table this run owns."""
    if LAKEHOUSE:
        return f"`{PROJECT}.{CATALOG}.{NS}.{name}`"
    return f"`{PROJECT}.{DATASET}.{name}`"


def _storage_uri(name: str) -> str:
    return f"gs://{DATA_BUCKET}/managed/{RUN_TAG}/{name}"


def _create(name: str, columns: str, partition: str = "", options: str = "") -> str:
    """CREATE TABLE in the active mode; BigQuery picks the format version."""
    part = f" PARTITION BY {partition}" if partition else ""
    if LAKEHOUSE:
        opts = f" OPTIONS ({options})" if options else ""
        return f"CREATE TABLE {_t(name)} ({columns}){part}{opts}"
    _CREATED.append(name)
    extra = f", {options}" if options else ""
    return (f"CREATE TABLE {_t(name)} ({columns}){part} "
            f"WITH CONNECTION `{CONNECTION}` OPTIONS (file_format='PARQUET', "
            f"table_format='ICEBERG', storage_uri='{_storage_uri(name)}'{extra})")


def _drop(name: str) -> None:
    _run(f"DROP TABLE IF EXISTS {_t(name)}")


# --- Iceberg metadata -------------------------------------------------------

def _rest_load(ns: str, name: str) -> dict:
    resp = _rest().get(f"{REST_PREFIX}/namespaces/{ns}/tables/{name}")
    if resp.status_code != 200:
        raise RuntimeError(f"REST load {ns}.{name}: HTTP {resp.status_code} {resp.text[:200]}")
    return resp.json()


def _managed_metadata(name: str) -> tuple:
    """(metadata dict, location) of a managed table, exported fresh."""
    ok, out = _run(f"EXPORT TABLE METADATA FROM {_t(name)}")
    if not ok:
        raise RuntimeError(f"EXPORT TABLE METADATA failed: {out}")
    bucket = _gcs().bucket(DATA_BUCKET)
    prefix = f"managed/{RUN_TAG}/{name}/metadata/"
    metas = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".metadata.json")]
    if not metas:
        raise RuntimeError(f"no metadata.json under gs://{DATA_BUCKET}/{prefix}")
    newest = max(metas, key=lambda b: b.updated)
    return json.loads(newest.download_as_bytes()), f"gs://{DATA_BUCKET}/{newest.name}"


def _metadata(name: str, ns: str = None) -> dict:
    """Current Iceberg table metadata for a table in the active mode."""
    if LAKEHOUSE:
        return _rest_load(ns or NS, name)["metadata"]
    return _managed_metadata(name)[0]


def _read_avro(uri: str) -> list:
    import fastavro
    import io
    bucket, key = uri[len("gs://"):].split("/", 1)
    data = _gcs().bucket(bucket).blob(key).download_as_bytes()
    return list(fastavro.reader(io.BytesIO(data)))


def _live_files(meta: dict) -> list:
    """Live files of the current snapshot: dicts with content, format, path, stats.

    content 0 = data, 1 = position deletes (a PUFFIN one is a deletion
    vector), 2 = equality deletes. Read from the manifests themselves, because
    BigQuery's snapshot summaries carry only the operation, not the counters.
    """
    snap_id = meta.get("current-snapshot-id")
    snap = next((s for s in meta.get("snapshots", []) if s["snapshot-id"] == snap_id), None)
    if snap is None:
        return []
    files = []
    for m in _read_avro(snap["manifest-list"]):
        for entry in _read_avro(m["manifest_path"]):
            if entry.get("status") == 2:  # DELETED
                continue
            df = entry["data_file"]
            files.append({
                "content": df.get("content", 0),
                "format": str(df.get("file_format", "")).upper(),
                "path": df["file_path"],
                "lower_bounds": df.get("lower_bounds"),
                "upper_bounds": df.get("upper_bounds"),
                "value_counts": df.get("value_counts"),
            })
    return files


def _file_note(files: list) -> str:
    data = sum(1 for f in files if f["content"] == 0)
    pos = sum(1 for f in files if f["content"] == 1 and f["format"] != "PUFFIN")
    dv = sum(1 for f in files if f["content"] == 1 and f["format"] == "PUFFIN")
    eq = sum(1 for f in files if f["content"] == 2)
    return f"data-files={data}, position-delete-files={pos}, deletion-vectors={dv}, equality-delete-files={eq}"


def _delete_files(files: list) -> int:
    return sum(1 for f in files if f["content"] != 0)


# ---------------------------------------------------------------------------
# Spark fixtures (lakehouse mode): tables BigQuery cannot make itself
# ---------------------------------------------------------------------------

_SPARK_STATE = {"checked": False, "ok": False, "reason": ""}
_FIXTURES = {}


def _spark_ok() -> tuple:
    """A local Spark session (fixtures in lakehouse mode, parquet-mr in both)."""
    if not _SPARK_STATE["checked"]:
        _SPARK_STATE["checked"] = True
        try:
            import spark_fixture as sf
            sf.get_spark().sparkContext.setLogLevel("ERROR")
            _SPARK_STATE["ok"] = True
        except Exception as e:  # noqa: BLE001
            _SPARK_STATE["reason"] = f"no Spark session: {_reason(e, 150)}"
    return _SPARK_STATE["ok"], _SPARK_STATE["reason"]


def _spark():
    import spark_fixture as sf
    return sf.get_spark()


def _jcatalog():
    return _spark()._jsparkSession.sessionState().catalogManager().catalog("local").icebergCatalog()


def _java_table(spark, jvm, name, fields, props):
    """Create a schema-only table through the Iceberg Java API.

    For V3 column types Spark SQL cannot declare (nanosecond timestamps,
    geometry, unknown). ``fields`` is a list of (name, jvm Type).
    """
    arr = jvm.java.util.ArrayList()
    for i, (fname, ftype) in enumerate(fields, start=1):
        arr.add(jvm.org.apache.iceberg.types.Types.NestedField.optional(i, fname, ftype))
    schema = jvm.org.apache.iceberg.Schema(arr)
    jprops = jvm.java.util.HashMap()
    for k, v in props.items():
        jprops.put(k, v)
    # parse() rather than of(): of() is varargs, which py4j cannot call.
    ident = jvm.org.apache.iceberg.catalog.TableIdentifier.parse(f"{NS}.{name}")
    _jcatalog().createTable(ident, schema, jvm.org.apache.iceberg.PartitionSpec.unpartitioned(), jprops)


def _fixture(key: str) -> dict:
    """Build (once) and describe a Spark-made fixture table in NS.

    Returns {"ok": bool, "table": name, "reason": str, ...extras}.
    """
    if key in _FIXTURES:
        return _FIXTURES[key]
    if not LAKEHOUSE:
        return {"ok": False, "reason": "Spark fixtures need the lakehouse mode"}
    ok, why = _spark_ok()
    if not ok:
        _FIXTURES[key] = {"ok": False, "reason": why}
        return _FIXTURES[key]
    import spark_fixture as sf
    spark = _spark()
    jvm = spark._jvm
    T = jvm.org.apache.iceberg.types.Types
    name = f"fx_{key}"
    fq = f"local.{NS}.{name}"
    out = {"ok": True, "table": name, "reason": ""}
    try:
        if key in ("v2_basic", "v3_basic"):
            v = key[:2]
            sf.create_fixture(NS, name, v, "copy-on-write")
        elif key in ("v2_mor", "v3_dv"):
            v = key[:2]
            sf.create_fixture(NS, name, v, "merge-on-read")
            spark.sql(f"DELETE FROM {fq} WHERE id = 2")
            out["delete_files"] = sf.inspect_delete_files(NS, name)
        elif key in ("v2_eq", "v3_eq"):
            out.update(sf.create_equality_delete_fixture(NS, name, key[:2]))
        elif key == "v2_dml":
            # A Spark table opted in to BigQuery DML, the documented interop path.
            spark.sql(f"""CREATE TABLE {fq} (id BIGINT, i INT, val STRING) USING iceberg
                          TBLPROPERTIES ('format-version'='2',
                                         'gcp.biglake.bigquery-dml.enabled'='true')""")
            spark.sql(f"INSERT INTO {fq} VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')")
        elif key == "v2_bucket":
            spark.sql(f"""CREATE TABLE {fq} (id BIGINT, val STRING) USING iceberg
                          PARTITIONED BY (bucket(4, id))
                          TBLPROPERTIES ('format-version'='2')""")
            spark.sql(f"INSERT INTO {fq} VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
        elif key == "v2_partevo":
            spark.sql(f"""CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP) USING iceberg
                          PARTITIONED BY (days(ts)) TBLPROPERTIES ('format-version'='2')""")
            spark.sql(f"INSERT INTO {fq} VALUES (1, TIMESTAMP '2026-01-01 10:00:00')")
            spark.sql(f"ALTER TABLE {fq} ADD PARTITION FIELD bucket(4, id)")
            spark.sql(f"INSERT INTO {fq} VALUES (2, TIMESTAMP '2026-01-02 10:00:00')")
        elif key == "v2_promoted":
            spark.sql(f"""CREATE TABLE {fq} (id BIGINT, i INT, f FLOAT) USING iceberg
                          TBLPROPERTIES ('format-version'='2')""")
            spark.sql(f"INSERT INTO {fq} VALUES (1, 7, 1.5)")
            spark.sql(f"ALTER TABLE {fq} ALTER COLUMN i TYPE BIGINT")
            spark.sql(f"ALTER TABLE {fq} ALTER COLUMN f TYPE DOUBLE")
            spark.sql(f"INSERT INTO {fq} VALUES (2, 9000000000, 2.5)")
        elif key == "v3_variant":
            spark.sql(f"""CREATE TABLE {fq} (id BIGINT, v VARIANT) USING iceberg
                          TBLPROPERTIES ('format-version'='3')""")
            spark.sql(f"""INSERT INTO {fq} SELECT 1, parse_json('{{"a": 1, "b": "x"}}')""")
        elif key == "v3_lineage":
            sf.create_fixture(NS, name, "v3", "merge-on-read")
        elif key == "v3_defaults":
            spark.sql(f"CREATE TABLE {fq} (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='3')")
            spark.sql(f"INSERT INTO {fq} VALUES (1)")
            tbl = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, fq)
            lit = jvm.org.apache.iceberg.expressions.Literal.of("eu")
            tbl.updateSchema().addColumn("region", T.StringType.get(), "doc", lit).commit()
            spark.sql(f"REFRESH TABLE {fq}")
            spark.sql(f"INSERT INTO {fq} VALUES (2, 'us')")
            out["expected"] = {1: "eu", 2: "us"}
        elif key == "v3_ts_ns":
            _java_table(spark, jvm, name, [("id", T.LongType.get()),
                                           ("c", T.TimestampNanoType.withoutZone())],
                        {"format-version": "3"})
            out["schema_only"] = True
        elif key == "v3_geometry":
            _java_table(spark, jvm, name, [("id", T.LongType.get()),
                                           ("c", T.GeometryType.crs84())],
                        {"format-version": "3"})
            out["schema_only"] = True
        elif key == "v3_unknown":
            _java_table(spark, jvm, name, [("id", T.LongType.get()),
                                           ("c", T.UnknownType.get())],
                        {"format-version": "3"})
            out["schema_only"] = True
        else:
            raise ValueError(f"unknown fixture {key}")
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        if "BadRequestException" in msg or "Malformed request" in msg:
            # The catalog, not Spark, refused it: such a table cannot exist in
            # the Lakehouse runtime catalog, so no engine (BigQuery included)
            # can read one there.
            out = {"ok": False, "catalog_refused": True,
                   "reason": f"the Lakehouse runtime catalog refuses to create the {key} table "
                             f"({_reason(msg.split(chr(10))[1] if chr(10) in msg else msg, 110)})"}
        else:
            out = {"ok": False, "reason": f"Spark could not build the {key} fixture: {_reason(e, 160)}"}
    _FIXTURES[key] = out
    return out


def _read_fixture(key: str, select: str = None) -> tuple:
    """BigQuery reads a fixture. Returns (status, rows | reason, fixture).

    status: "ok" | "fail" (BigQuery refused) | "skip" (no fixture).
    """
    fx = _fixture(key)
    if not fx["ok"]:
        return "skip", fx["reason"], fx
    sql = (select or "SELECT * FROM {t} ORDER BY id").format(t=_t(fx["table"]))
    ok, out = _run(sql)
    return ("ok" if ok else "fail"), out, fx


# ---------------------------------------------------------------------------
# Pre-flight: can BigQuery produce a V3 table in this mode?
# ---------------------------------------------------------------------------

_V3 = None


def _v3_creatable() -> tuple:
    """Whether BigQuery DDL can create a format-version 3 table here.

    Measured, not assumed: create a table the default way and read back the
    format version it stores, then ask for V3 explicitly through the
    properties option. Returns (creatable, evidence).
    """
    global _V3
    if _V3 is not None:
        return _V3
    name = _unique("v3probe")
    ok, out, _ = _run_all([_create(name, "id INT64")])
    if not ok:
        _V3 = (False, f"BigQuery could not create any Iceberg table: {out}")
        return _V3
    try:
        stored = _metadata(name).get("format-version")
    except Exception as e:  # noqa: BLE001
        stored = f"? ({_reason(e, 80)})"
    _drop(name)
    name3 = _unique("v3probe")
    ok3, out3, _ = _run_all([_create(name3, "id INT64",
                                     options="`properties.format-version`='3'")])
    if ok3:
        try:
            got = _metadata(name3).get("format-version")
        except Exception:  # noqa: BLE001
            got = None
        _drop(name3)
        if got == 3:
            _V3 = (True, "a table created with properties.format-version=3 is stored as v3")
            return _V3
        _V3 = (False, f"format-version 3 was requested and accepted but stored as {got}")
        return _V3
    _V3 = (False, f"BigQuery creates format-version {stored} tables and rejects a "
                  f"request for v3 ({out3[:110]})")
    return _V3


def _spark_enable_dml(table: str) -> tuple:
    """Opt a Spark-made table in to BigQuery DML, the documented way (from Spark).

    BigQuery's own ALTER TABLE ... SET OPTIONS refuses this option on any table
    it did not create, so the property has to come from the engine that owns
    the table. Returns (accepted, reason).
    """
    try:
        _spark().sql(f"ALTER TABLE local.{NS}.{table} SET TBLPROPERTIES "
                     "('gcp.biglake.bigquery-dml.enabled'='true')")
        return True, ""
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        line = next((ln for ln in msg.splitlines() if "Exception" in ln), msg)
        return False, _reason(line, 140)


_V3_REFUSAL = None


def _v3_write_refusal() -> str:
    """Why BigQuery cannot write a V3 table, measured once against a Spark V3 table."""
    global _V3_REFUSAL
    if _V3_REFUSAL is not None:
        return _V3_REFUSAL
    can, evidence = _v3_creatable()
    if can:
        _V3_REFUSAL = ""
        return _V3_REFUSAL
    create = "BigQuery DDL creates only v2 tables (properties.format-version is an unknown option)"
    if not LAKEHOUSE:
        _V3_REFUSAL = f"{create}, and only BigQuery writes managed tables"
        return _V3_REFUSAL
    fx = _fixture("v3_basic")
    if not fx["ok"]:
        _V3_REFUSAL = create
        return _V3_REFUSAL
    ok, out = _run(f"INSERT INTO {_t(fx['table'])} (id, val) VALUES (99, 'bq')")
    if ok:
        _V3_REFUSAL = ""  # BigQuery can write a V3 table after all
        return _V3_REFUSAL
    accepted, why = _spark_enable_dml(fx["table"])
    if accepted:
        ok2, out2 = _run(f"INSERT INTO {_t(fx['table'])} (id, val) VALUES (98, 'bq')")
        if ok2:
            _V3_REFUSAL = ""
            return _V3_REFUSAL
        tail = f"even with gcp.biglake.bigquery-dml.enabled set it still fails ({out2[:90]})"
    else:
        tail = f"and the catalog refuses gcp.biglake.bigquery-dml.enabled on a v3 table ({why[:100]})"
    _V3_REFUSAL = f"{create}; INSERT into a Spark-made v3 table fails ({out[:80]}), {tail}"
    return _V3_REFUSAL


# ---------------------------------------------------------------------------
# Tables BigQuery writes to, per format version
# ---------------------------------------------------------------------------
#
# BigQuery DDL only creates v2 tables. It still *writes* v3 tables in the
# lakehouse mode, as long as the table was created by another engine and opted
# in to BigQuery DML (gcp.biglake.bigquery-dml.enabled, which only the owning
# engine can set). Measured: INSERT, UPDATE and DELETE all commit and the table
# stays format-version 3. So every write cell at v3 is measured on such a
# table, and only the cells about *creating* something credit Spark's part.

V3_HOW = "a Spark-created v3 table opted in to BigQuery DML"

_SPARK_TYPES = {"INT64": "BIGINT", "STRING": "STRING", "TIMESTAMP": "TIMESTAMP",
                "NUMERIC(10,2)": "DECIMAL(10,2)", "NUMERIC(9,2)": "DECIMAL(9,2)"}


def _split_cols(cols: str) -> list:
    """Split a column list on top-level commas (NUMERIC(10,2) stays whole)."""
    parts, depth, cur = [], 0, ""
    for ch in cols:
        depth += ch == "("
        depth -= ch == ")"
        if ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    return parts + [cur] if cur.strip() else parts


def _spark_cols(bq_cols: str) -> str:
    out = []
    for col in _split_cols(bq_cols):
        name, typ = col.strip().split(None, 1)
        out.append(f"{name} {_SPARK_TYPES.get(typ.strip().upper(), typ)}")
    return ", ".join(out)


def _spark_err(e: Exception) -> str:
    msg = str(e)
    line = next((ln for ln in msg.splitlines() if "Exception" in ln), msg)
    return _reason(line, 160)


def _make(version: str, name: str, bq_cols: str, partition: str = "",
          spark_partition: str = "") -> tuple:
    """Create a table BigQuery can write at ``version``. Returns (ok, reason)."""
    if version == "v2":
        ok, out, _ = _run_all([_create(name, bq_cols, partition=partition)])
        return ok, ("" if ok else out)
    if not LAKEHOUSE:
        return False, _v3_write_refusal()
    ok, why = _spark_ok()
    if not ok:
        return False, why
    part = f" PARTITIONED BY ({spark_partition})" if spark_partition else ""
    try:
        _spark().sql(f"""CREATE TABLE local.{NS}.{name} ({_spark_cols(bq_cols)}) USING iceberg{part}
                         TBLPROPERTIES ('format-version'='3',
                                        'gcp.biglake.bigquery-dml.enabled'='true')""")
        return True, ""
    except Exception as e:  # noqa: BLE001
        return False, f"Spark could not create {V3_HOW}: {_spark_err(e)}"


def _how(version: str) -> str:
    return "" if version == "v2" else f" on {V3_HOW}"


def _stored_version(name: str) -> str:
    try:
        return str(_metadata(name).get("format-version"))
    except Exception:  # noqa: BLE001
        return "?"


def _version_kept(name: str, version: str) -> str:
    """'' when the table is still at ``version``; otherwise the problem."""
    got = _stored_version(name)
    want = "3" if version == "v3" else "2"
    # "?" means the metadata could not be read (a managed table's export can
    # lag a DDL statement); that is not evidence of a version change.
    return "" if got in (want, "?") else f"the table is now format-version {got}, not {want}"


def _v3_only(fid: str, name: str, version: str):
    if version == "v2":
        return _res(fid, name, version, "skip",
                    "V3-only feature; not applicable to format-version 2 tables")
    return None


def _cannot(fid: str, fname: str, version: str, why: str) -> TestResult:
    return _res(fid, fname, version, "fail", why)


# ---------------------------------------------------------------------------
# Core DDL / read / write
# ---------------------------------------------------------------------------

def test_table_creation(version: str) -> TestResult:
    fid, fname = _names(TestResult("table-creation", "Table Creation", version))
    if version == "v3":
        can, evidence = _v3_creatable()
        if can:
            return _res(fid, fname, version, "pass", evidence)
        tail = (". It does write v3 tables another engine created and opted in to BigQuery DML"
                if LAKEHOUSE else "")
        return _res(fid, fname, version, "fail",
                    f"BigQuery DDL cannot create a v3 Iceberg table: {evidence}{tail}")
    tbl = _unique("create")
    ok, out, _ = _run_all([_create(tbl, "id INT64, name STRING, amount NUMERIC(9,2)")])
    if not ok:
        return _res(fid, fname, version, "fail", out)
    try:
        meta = _metadata(tbl)
        detail = (f"CREATE TABLE produced an Iceberg table at format-version "
                  f"{meta.get('format-version')} with {len(meta['schemas'][-1]['fields'])} columns")
        result = "pass" if meta.get("format-version") == 2 else "fail"
    except Exception as e:  # noqa: BLE001
        result, detail = "pass", f"CREATE TABLE accepted; metadata not readable ({_reason(e, 80)})"
    _drop(tbl)
    return _res(fid, fname, version, result, detail)


def test_read_support(version: str) -> TestResult:
    fid, fname = _names(TestResult("read-support", "Read Support", version))
    tbl = _unique("read")
    ok, out = _make(version, tbl, "id INT64, name STRING")
    if not ok:
        return _cannot(fid, fname, version, f"No {version} table BigQuery can use: {out}")
    ok, out, _ = _run_all([f"INSERT INTO {_t(tbl)} (id, name) VALUES (1,'a'),(2,'b'),(3,'c')",
                           f"SELECT id FROM {_t(tbl)} ORDER BY id"])
    kept = _version_kept(tbl, version) if ok else ""
    _drop(tbl)
    if not ok or _ids(out) != [1, 2, 3] or kept:
        return _res(fid, fname, version, "fail", out if not ok else kept or f"wrong rows {out}")
    detail = f"Rows written and read back by BigQuery{_how(version)}"
    if LAKEHOUSE:
        st, rows, _ = _read_fixture(f"{version}_basic")
        if st == "ok":
            detail += f"; a table Spark wrote is read too ({len(rows)} rows)"
        elif st == "fail":
            return _res(fid, fname, version, "partial",
                        f"{detail}, but a Spark-written {version} table is not readable: {rows}")
    return _res(fid, fname, version, "pass", detail)


def test_write_insert(version: str) -> TestResult:
    fid, fname = _names(TestResult("write-insert", "Write (INSERT)", version))
    tbl = _unique("ins")
    ok, out = _make(version, tbl, "id INT64, name STRING")
    if not ok:
        return _cannot(fid, fname, version, f"No {version} table BigQuery can write: {out}")
    ok, out, _ = _run_all([
        f"INSERT INTO {_t(tbl)} (id, name) VALUES (1,'a'),(2,'b')",
        f"INSERT INTO {_t(tbl)} (id, name) SELECT 3, 'c'",
        f"SELECT id FROM {_t(tbl)}",
    ])
    kept = _version_kept(tbl, version) if ok else ""
    _drop(tbl)
    if ok and _ids(out) == [1, 2, 3] and not kept:
        detail = f"INSERT ... VALUES and INSERT ... SELECT both write Iceberg data{_how(version)}"
        if version == "v2" and LAKEHOUSE:
            fx = _fixture("v2_dml")
            if fx["ok"]:
                ok2, out2 = _run(f"INSERT INTO {_t(fx['table'])} (id, i, val) VALUES (4, 40, 'bq')")
                detail += ("; also into a Spark-created table with gcp.biglake.bigquery-dml.enabled"
                           if ok2 else f"; but not into a DML-enabled Spark table ({out2[:100]})")
        return _res(fid, fname, version, "pass", detail)
    return _res(fid, fname, version, "fail", out if not ok else kept or f"wrong rows {out}")


def test_write_merge_update_delete(version: str) -> TestResult:
    fid, fname = _names(TestResult("write-merge-update-delete", "Write (MERGE/UPDATE/DELETE)", version))
    tbl = _unique("dml")
    ok, out = _make(version, tbl, "id INT64, name STRING")
    if not ok:
        return _cannot(fid, fname, version, f"No {version} table BigQuery can write: {out}")
    ok, out, i = _run_all([
        f"INSERT INTO {_t(tbl)} (id, name) VALUES (1,'first'),(2,'second'),(3,'third')",
        f"UPDATE {_t(tbl)} SET name='updated' WHERE id=1",
        f"DELETE FROM {_t(tbl)} WHERE id=3",
        f"""MERGE {_t(tbl)} t USING (SELECT 2 AS id, 'merged' AS nm UNION ALL SELECT 4, 'new') s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET name = s.nm
            WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.nm)""",
        f"SELECT id, name FROM {_t(tbl)} ORDER BY id",
    ])
    kept = _version_kept(tbl, version) if ok else ""
    _drop(tbl)
    if not ok:
        return _res(fid, fname, version, "fail", f"statement {i} failed: {out}")
    got = {int(r["id"]): r["name"] for r in out}
    if got == {1: "updated", 2: "merged", 4: "new"} and not kept:
        return _res(fid, fname, version, "pass",
                    f"UPDATE, DELETE and MERGE all applied{_how(version)}; final rows verified")
    return _res(fid, fname, version, "fail", kept or f"DML ran but rows are wrong: {got}")


def test_catalog_integration(version: str) -> TestResult:
    fid, fname = _names(TestResult("catalog-integration", "Catalog Integration", version))
    tbl = _unique("cat")
    ok, out = _make(version, tbl, "id INT64")
    if not ok:
        return _cannot(fid, fname, version, f"No {version} table BigQuery can use: {out}")
    ok, out, _ = _run_all([
        f"INSERT INTO {_t(tbl)} (id) VALUES (1),(2)",
        f"SELECT COUNT(*) AS n FROM {_t(tbl)}",
    ])
    if not ok:
        _drop(tbl)
        return _res(fid, fname, version, "fail", out)
    if LAKEHOUSE:
        try:
            loc = _rest_load(NS, tbl)["metadata-location"]
            made = "BigQuery" if version == "v2" else "Spark (BigQuery DDL cannot create v3)"
            detail = (f"Table created by {made}, then written, read and dropped by BigQuery through "
                      f"the Lakehouse runtime catalog, which serves it over Iceberg REST "
                      f"({loc.rsplit('/', 1)[-1]})")
        except Exception as e:  # noqa: BLE001
            _drop(tbl)
            return _res(fid, fname, version, "partial",
                        f"BigQuery round trip worked but the REST catalog does not serve the table: {_reason(e)}")
    else:
        try:
            _, loc = _managed_metadata(tbl)
            detail = ("Create/write/read/drop round trip in a BigQuery dataset; Iceberg metadata is "
                      f"exported to storage for other engines ({loc.rsplit('/', 1)[-1]})")
        except Exception as e:  # noqa: BLE001
            detail = f"Round trip worked; metadata export failed ({_reason(e, 100)})"
    _drop(tbl)
    return _res(fid, fname, version, "pass", detail)


# ---------------------------------------------------------------------------
# Row-level operations: what BigQuery's own DML writes
# ---------------------------------------------------------------------------

_DML_EVIDENCE = {}


def _bq_dml_files(version: str) -> tuple:
    """DELETE then UPDATE through BigQuery; returns (files, note, error)."""
    if version not in _DML_EVIDENCE:
        tbl = _unique("rowops")
        ok, out = _make(version, tbl, "id INT64, k STRING, val STRING")
        if not ok:
            _DML_EVIDENCE[version] = (None, "", f"No {version} table BigQuery can write: {out}")
            return _DML_EVIDENCE[version]
        ok, out, i = _run_all([
            f"INSERT INTO {_t(tbl)} (id, k, val) VALUES (1,'a','x'),(2,'b','y'),(3,'c','z')",
            f"DELETE FROM {_t(tbl)} WHERE id=2",
            f"UPDATE {_t(tbl)} SET val='changed' WHERE id=3",
            f"SELECT id FROM {_t(tbl)}",
        ])
        if not ok:
            _DML_EVIDENCE[version] = (None, "", f"statement {i} failed: {out}")
        else:
            try:
                meta = _metadata(tbl)
                files = _live_files(meta)
                props = meta.get("properties", {})
                modes = ", ".join(f"{k}={props[k]}" for k in
                                  ("write.delete.mode", "write.update.mode") if k in props)
                note = _file_note(files) + (f"; table properties {modes}" if modes else "")
                _DML_EVIDENCE[version] = (files, note, "")
            except Exception as e:  # noqa: BLE001
                _DML_EVIDENCE[version] = (None, "", f"metadata not readable: {_reason(e)}")
        _drop(tbl)
    return _DML_EVIDENCE[version]


def _read_note(key: str, expect_ids: list, what: str) -> str:
    """Whether BigQuery reads a Spark table carrying ``what`` correctly."""
    if not LAKEHOUSE:
        return ""
    st, rows, fx = _read_fixture(key)
    if st == "skip":
        return f" Reading {what}: not measured ({rows[:100]})."
    if st == "fail":
        return f" BigQuery cannot even read {what} written by Spark ({rows[:120]})."
    got = _ids(rows)
    if got == expect_ids:
        return f" BigQuery does read {what} written by Spark correctly (rows {got})."
    return f" BigQuery reads a table with {what} but returns the wrong rows ({got}, expected {expect_ids})."


def _rowop(fid: str, fname: str, version: str, wrote, yes: str, no: str,
           read_key: str = "", read_what: str = "") -> TestResult:
    files, note, err = _bq_dml_files(version)
    if files is None:
        return _res(fid, fname, version, "fail" if version == "v3" else "error", err)
    reads = _read_note(read_key, [1, 3], read_what) if read_key else ""
    if wrote(files):
        return _res(fid, fname, version, "pass", f"{yes}{_how(version)} ({note}).{reads}")
    return _res(fid, fname, version, "fail", f"{no}{_how(version)} ({note}).{reads}")


def test_position_deletes(version: str) -> TestResult:
    fid, fname = _names(TestResult("position-deletes", "Position Deletes", version))
    return _rowop(fid, fname, version,
                  lambda fs: any(f["content"] == 1 for f in fs),
                  "BigQuery DELETE wrote position deletes",
                  "BigQuery DELETE/UPDATE rewrote data files and wrote no position deletes",
                  f"{version}_mor" if version == "v2" else "v3_dv",
                  "position delete files" if version == "v2" else "a v3 deletion vector")


def test_equality_deletes(version: str) -> TestResult:
    fid, fname = _names(TestResult("equality-deletes", "Equality Deletes", version))
    return _rowop(fid, fname, version,
                  lambda fs: any(f["content"] == 2 for f in fs),
                  "BigQuery DML wrote equality deletes",
                  "BigQuery DML never wrote an equality delete",
                  f"{version}_eq", "equality delete files")


def test_merge_on_read(version: str) -> TestResult:
    fid, fname = _names(TestResult("merge-on-read", "Merge-on-Read", version))
    r = _rowop(fid, fname, version, _delete_files,
               "BigQuery DML committed delete files",
               "BigQuery DML committed no delete files, so it is copy-on-write only")
    if r.result != "fail" or not LAKEHOUSE:
        return r
    # Read first: the write probe below changes the fixture.
    r.details += _read_note(f"{version}_mor" if version == "v2" else "v3_dv", [1, 3],
                            "merge-on-read deletes")
    fx = _fixture(f"{version}_mor" if version == "v2" else "v3_dv")
    if fx["ok"]:
        accepted, why = _spark_enable_dml(fx["table"])
        r.details += (" A merge-on-read table can be opted in to BigQuery DML" if accepted else
                      f" The catalog refuses to opt a merge-on-read table in to BigQuery DML ({why[:120]})")
    return r


def test_copy_on_write(version: str) -> TestResult:
    fid, fname = _names(TestResult("copy-on-write", "Copy-on-Write", version))
    return _rowop(fid, fname, version,
                  lambda fs: not _delete_files(fs),
                  "DELETE and UPDATE rewrote data files and committed no delete files",
                  "BigQuery DML committed delete files")


def test_deletion_vectors(version: str) -> TestResult:
    fid, fname = _names(TestResult("deletion-vectors", "Deletion Vectors", version))
    skip = _v3_only(fid, fname, version)
    if skip:
        return skip
    return _rowop(fid, fname, version,
                  lambda fs: any(f["content"] == 1 and f["format"] == "PUFFIN" for f in fs),
                  "BigQuery DELETE wrote a deletion vector",
                  "BigQuery DELETE rewrote data files and wrote no deletion vector",
                  "v3_dv", "a deletion vector")


# ---------------------------------------------------------------------------
# Schema and partitioning
# ---------------------------------------------------------------------------

def test_schema_evolution(version: str) -> TestResult:
    fid, fname = _names(TestResult("schema-evolution", "Schema Evolution", version))
    tbl = _unique("schema")
    ok, out = _make(version, tbl, "id INT64, a STRING, b STRING")
    if not ok:
        return _cannot(fid, fname, version, f"No {version} table BigQuery can alter: {out}")
    ok, out = _run(f"INSERT INTO {_t(tbl)} (id, a, b) VALUES (1,'x','y')")
    if not ok:
        _drop(tbl)
        return _res(fid, fname, version, "fail", out)
    ops = {
        "add": f"ALTER TABLE {_t(tbl)} ADD COLUMN c INT64",
        "rename": f"ALTER TABLE {_t(tbl)} RENAME COLUMN a TO a2",
        "drop": f"ALTER TABLE {_t(tbl)} DROP COLUMN b",
    }
    done, refused = [], []
    for op, sql in ops.items():
        ok, out = _run(sql)
        (done if ok else refused).append(op if ok else f"{op} ({out[:90]})")
    ids_note = ""
    try:
        fields = {f["name"]: f["id"] for f in _metadata(tbl)["schemas"][-1]["fields"]}
        if "rename" in done:
            ids_note = f"; Iceberg field ids kept across the rename (a2 id={fields.get('a2')})"
    except Exception:  # noqa: BLE001
        pass
    kept = _version_kept(tbl, version)
    _drop(tbl)
    if kept:
        return _res(fid, fname, version, "fail", kept)
    if len(done) == 3:
        return _res(fid, fname, version, "pass",
                    f"ADD, RENAME and DROP COLUMN all applied{_how(version)}{ids_note}")
    if done:
        return _res(fid, fname, version, "partial", f"applied: {', '.join(done)}; refused: {'; '.join(refused)}")
    return _res(fid, fname, version, "fail", "; ".join(refused))


def _field_type(name: str, column: str) -> str:
    try:
        return str(next(f["type"] for f in _metadata(name)["schemas"][-1]["fields"] if f["name"] == column))
    except Exception:  # noqa: BLE001
        return "?"


def test_type_promotion(version: str) -> list:
    fid, fname = _names(TestResult("type-promotion", "Type Promotion", version))
    probes = []
    tbl = _unique("promo")
    ok, out = _make(version, tbl, "id INT64, n NUMERIC(10,2)")
    if ok:
        ok, out = _run(f"INSERT INTO {_t(tbl)} (id, n) VALUES (1, 1.25)")
    if ok:
        ok2, out2 = _run(f"ALTER TABLE {_t(tbl)} ALTER COLUMN n SET DATA TYPE NUMERIC(12,2)")
        stored = _field_type(tbl, "n") if ok2 else ""
        probes.append(("decimal(10,2)->decimal(12,2)", ok2 and stored.replace(" ", "") == "decimal(12,2)",
                       f"stored {stored}" if ok2 else out2[:90]))
    else:
        probes.append(("decimal(10,2)->decimal(12,2)", False, f"table not usable: {out[:90]}"))
    _drop(tbl)
    if LAKEHOUSE:
        # int -> long: BigQuery has only INT64, so the 32-bit column has to come
        # from Spark. ALTER ... SET DATA TYPE INT64 is a no-op to BigQuery
        # (measured: the Iceberg type stays int); the promotion happens when
        # BigQuery first writes a value that needs 64 bits, so that is the probe.
        name = _unique("promoint")
        label = "int->long (Spark-made INT column)"
        try:
            v = "3" if version == "v3" else "2"
            _spark().sql(f"""CREATE TABLE local.{NS}.{name} (id BIGINT, i INT) USING iceberg
                             TBLPROPERTIES ('format-version'='{v}',
                                            'gcp.biglake.bigquery-dml.enabled'='true')""")
            _spark().sql(f"INSERT INTO local.{NS}.{name} VALUES (1, 7)")
            ok3, out3, _ = _run_all([
                f"INSERT INTO {_t(name)} (id, i) VALUES (2, 9000000000)",
                f"SELECT id, i FROM {_t(name)} ORDER BY id",
            ])
            stored = _field_type(name, "i")
            vals = [int(r["i"]) for r in out3] if ok3 else []
            good = ok3 and stored == "long" and vals == [7, 9000000000]
            probes.append((label, good,
                           f"writing 9000000000 promoted the Iceberg column to {stored}, rows {vals}"
                           if ok3 else out3[:90]))
        except Exception as e:  # noqa: BLE001
            probes.append((label, False, _spark_err(e)[:90]))
        _drop(name)
    results = []
    for label, passed, ev in probes:
        d = TestResult(fid, f"{fname}: {label}", version, diagnostic=True)
        d.result, d.details = ("pass" if passed else "fail"), ev
        results.append(d)
    good = [p for p in probes if p[1]]
    summary = "; ".join(f"{p[0]}: {'yes' if p[1] else 'no'} ({p[2]})" for p in probes)
    if version == "v2" and LAKEHOUSE:
        st, rows, _ = _read_fixture("v2_promoted")
        summary += (f"; reads a table Spark promoted int->long and float->double ({len(rows)} rows)"
                    if st == "ok" else f"; Spark-promoted table: {str(rows)[:90]}")
    verdict = "pass" if good and len(good) == len(probes) else "partial" if good else "fail"
    return [_res(fid, fname, version, verdict, summary + _how(version))] + results


def test_column_default_values(version: str) -> TestResult:
    fid, fname = _names(TestResult("column-default-values", "Column Default Values", version))
    skip = _v3_only(fid, fname, version)
    if skip:
        return skip
    read = ""
    if LAKEHOUSE:
        st, rows, fx = _read_fixture("v3_defaults", "SELECT id, region FROM {t} ORDER BY id")
        if st == "ok":
            got = {int(r["id"]): r["region"] for r in rows}
            read = ("BigQuery applies a Spark v3 table's initial-default when reading"
                    if got == fx.get("expected") else
                    f"BigQuery reads a v3 table with a column default but returns {got}, not "
                    f"{fx.get('expected')}, so the default is ignored")
        else:
            read = f"BigQuery cannot read a v3 table that has a column default ({str(rows)[:200]})"
    tbl = _unique("dflt")
    ok, out = _make(version, tbl, "id INT64")
    if not ok:
        return _res(fid, fname, version, "fail", f"No v3 table BigQuery can alter: {out}. {read}")
    ok, out = _run(f"ALTER TABLE {_t(tbl)} ADD COLUMN region STRING DEFAULT 'eu'")
    field = {}
    if ok:
        try:
            field = next(f for f in _metadata(tbl)["schemas"][-1]["fields"] if f["name"] == "region")
        except Exception:  # noqa: BLE001
            field = {}
    _drop(tbl)
    has = "initial-default" in field or "write-default" in field
    if ok and has:
        return _res(fid, fname, version, "pass" if "cannot" not in read else "partial",
                    f"ADD COLUMN ... DEFAULT records an Iceberg default ({field}). {read}")
    why = (f"ADD COLUMN ... DEFAULT was accepted but no Iceberg default was recorded ({field})"
           if ok else f"ADD COLUMN ... DEFAULT is refused{_how(version)} ({out[:120]})")
    return _res(fid, fname, version, "fail", f"{why}. {read}")


_PARTITION_FORMS = [
    ("DATE(ts)", "day", "days(ts)"),
    ("TIMESTAMP_TRUNC(ts, HOUR)", "hour", "hours(ts)"),
    ("TIMESTAMP_TRUNC(ts, MONTH)", "month", "months(ts)"),
    ("TIMESTAMP_TRUNC(ts, YEAR)", "year", "years(ts)"),
]


def test_hidden_partitioning(version: str) -> TestResult:
    fid, fname = _names(TestResult("hidden-partitioning", "Hidden Partitioning", version))
    if version == "v3":
        # BigQuery cannot declare a partition spec on a v3 table, so the cell
        # can at most be partial: whether it writes rows into the right
        # partitions of one Spark declared, and prunes on them.
        tbl = _unique("hp")
        ok, out = _make(version, tbl, "id INT64, ts TIMESTAMP", spark_partition="days(ts)")
        if not ok:
            return _cannot(fid, fname, version, f"No v3 table BigQuery can write: {out}")
        ok, out, _ = _run_all([
            f"INSERT INTO {_t(tbl)} (id, ts) VALUES (1, TIMESTAMP '2026-01-01 10:00:00'), (2, TIMESTAMP '2026-02-01 10:00:00')",
            f"SELECT id FROM {_t(tbl)} WHERE ts >= TIMESTAMP '2026-02-01'",
        ])
        parts = set()
        if ok:
            try:
                parts = {json.dumps(f.get("partition"), default=str)
                         for f in _live_files_with_partition(_metadata(tbl))}
            except Exception:  # noqa: BLE001
                parts = set()
        _drop(tbl)
        if ok and _ids(out) == [2] and len(parts) >= 2:
            return _res(fid, fname, version, "partial",
                        f"BigQuery writes rows into the day partitions of {V3_HOW} ({len(parts)} partitions) "
                        "and filters on them, but cannot declare a partition spec on a v3 table")
        return _res(fid, fname, version, "fail",
                    out if not ok else f"rows {out}, partitions {sorted(parts)}")
    made, refused = [], []
    for expr, transform, _ in _PARTITION_FORMS:
        tbl = _unique("hp")
        ok, out, _ = _run_all([
            _create(tbl, "id INT64, ts TIMESTAMP", partition=expr),
            f"INSERT INTO {_t(tbl)} VALUES (1, TIMESTAMP '2026-01-01 10:00:00'), (2, TIMESTAMP '2026-02-01 10:00:00')",
            f"SELECT id FROM {_t(tbl)} WHERE ts >= TIMESTAMP '2026-02-01'",
        ])
        if ok:
            try:
                spec = _metadata(tbl)["partition-specs"][-1]["fields"]
                got = spec[0]["transform"] if spec else "none"
            except Exception:  # noqa: BLE001
                got = "?"
            (made if got == transform else refused).append(
                transform if got == transform else f"{expr} -> Iceberg transform {got}")
        else:
            refused.append(f"{expr} ({out[:70]})")
        _drop(tbl)
    # Integer-range is BigQuery's only non-time partitioning. Managed tables
    # refuse it at once; the Lakehouse catalog was measured hanging for 40
    # minutes before an internal error, so the probe is bounded.
    tbl = _unique("hpb")
    ok, out = _run(_create(tbl, "id INT64", partition="RANGE_BUCKET(id, GENERATE_ARRAY(0, 100, 10))"),
                   timeout=180, retries=0)
    _drop(tbl)
    other = ("integer-range partitioning was accepted" if ok
             else f"no identity/bucket/truncate form: integer-range partitioning is refused ({out[:90]})")
    if LAKEHOUSE:
        st, rows, _ = _read_fixture("v2_bucket", "SELECT id FROM {t} WHERE id = 3")
        other += ("; BigQuery reads a Spark bucket(4, id) table with a filter" if st == "ok"
                  else f"; Spark bucket table: {str(rows)[:80]}")
    if not made:
        return _res(fid, fname, version, "fail", f"No time transform created: {'; '.join(refused)}; {other}")
    detail = f"BigQuery DDL creates Iceberg {', '.join(made)} transforms on a timestamp; {other}"
    if refused:
        detail += f"; refused: {'; '.join(refused)}"
    return _res(fid, fname, version, "pass" if ok else "partial", detail)


def _live_files_with_partition(meta: dict) -> list:
    snap_id = meta.get("current-snapshot-id")
    snap = next((s for s in meta.get("snapshots", []) if s["snapshot-id"] == snap_id), None)
    out = []
    for m in _read_avro(snap["manifest-list"]) if snap else []:
        for entry in _read_avro(m["manifest_path"]):
            if entry.get("status") != 2 and entry["data_file"].get("content", 0) == 0:
                out.append(entry["data_file"])
    return out


def test_partition_evolution(version: str) -> TestResult:
    fid, fname = _names(TestResult("partition-evolution", "Partition Evolution", version))
    tbl = _unique("pe")
    ok, out = _make(version, tbl, "id INT64, ts TIMESTAMP", partition="DATE(ts)",
                    spark_partition="days(ts)")
    if not ok:
        return _cannot(fid, fname, version, f"No {version} table BigQuery can alter: {out}")
    ok, out = _run(f"INSERT INTO {_t(tbl)} (id, ts) VALUES (1, TIMESTAMP '2026-01-01 10:00:00')")
    replace = (_create(tbl, "id INT64, ts TIMESTAMP", partition="TIMESTAMP_TRUNC(ts, MONTH)")
               .replace("CREATE TABLE", "CREATE OR REPLACE TABLE", 1))
    ok2, out2 = _run(replace)
    specs = 0
    if ok2:
        try:
            specs = len(_metadata(tbl)["partition-specs"])
        except Exception:  # noqa: BLE001
            specs = 0
    _drop(tbl)
    read = ""
    if LAKEHOUSE and version == "v2":
        st, rows, _ = _read_fixture("v2_partevo")
        read = (f" BigQuery reads a table Spark evolved from days(ts) to days(ts)+bucket(id) (rows {_ids(rows)})."
                if st == "ok" else f" Spark-evolved table: {str(rows)[:100]}.")
    if ok2 and specs > 1:
        return _res(fid, fname, version, "pass", f"The partition spec evolved in place ({specs} specs).{read}")
    why = ("CREATE OR REPLACE with a new spec replaced the table rather than evolving it"
           if ok2 else f"changing the partition spec is refused ({out2[:110]})")
    return _res(fid, fname, version, "fail",
                f"BigQuery DDL has no partition-evolution statement and {why}{_how(version)}.{read}")


def test_multi_arg_transforms(version: str) -> TestResult:
    fid, fname = _names(TestResult("multi-arg-transforms", "Multi-Argument Transforms", version))
    skip = _v3_only(fid, fname, version)
    if skip:
        return skip
    return _res(fid, fname, version, "fail",
                "BigQuery partitioning takes a single time-unit column (DATE / TIMESTAMP_TRUNC), "
                "and integer-range partitioning is refused for Iceberg tables, so no multi-argument "
                "transform can be declared; it cannot declare any partition spec on a v3 table")


# ---------------------------------------------------------------------------
# Table management
# ---------------------------------------------------------------------------

def test_time_travel(version: str) -> TestResult:
    fid, fname = _names(TestResult("time-travel", "Time Travel", version))
    tbl = _unique("tt")
    ok, out = _make(version, tbl, "id INT64")
    if not ok:
        return _cannot(fid, fname, version, f"No {version} table BigQuery can write: {out}")
    ok, out = _run(f"INSERT INTO {_t(tbl)} (id) VALUES (1)")
    if not ok:
        _drop(tbl)
        return _res(fid, fname, version, "fail", out)
    time.sleep(3)
    _, now = _run("SELECT CURRENT_TIMESTAMP() AS ts")
    ts = str(now[0]["ts"])
    time.sleep(3)
    _run(f"INSERT INTO {_t(tbl)} (id) VALUES (2)")
    ok, out = _run(f"SELECT id FROM {_t(tbl)} FOR SYSTEM_TIME AS OF TIMESTAMP '{ts}'")
    snaps = "?"
    try:
        snaps = len(_metadata(tbl).get("snapshots", []))
    except Exception:  # noqa: BLE001
        pass
    _drop(tbl)
    history = (f"; the Iceberg metadata keeps {snaps} snapshot(s), "
               + ("so other engines cannot time travel" if snaps == 1
                  else "so other engines can time travel too"))
    if ok and _ids(out) == [1]:
        return _res(fid, fname, version, "pass",
                    f"FOR SYSTEM_TIME AS OF returned the earlier state{_how(version)}{history}")
    return _res(fid, fname, version, "fail", (out if not ok else f"wrong rows {out}") + history)


def test_table_maintenance(version: str) -> TestResult:
    fid, fname = _names(TestResult("table-maintenance", "Table Maintenance", version))
    if not LAKEHOUSE:
        if version == "v3":
            return _cannot(fid, fname, version, _v3_write_refusal())
        return _res(fid, fname, version, "skip",
                    "Managed tables are compacted and garbage-collected by BigQuery in the background; "
                    "there is no statement to trigger or observe it in a short test")
    if version == "v3":
        tbl = _unique("maint")
        ok, out = _make(version, tbl, "id INT64")
        if not ok:
            return _cannot(fid, fname, version, out)
        try:
            _spark().sql(f"ALTER TABLE local.{NS}.{tbl} SET TBLPROPERTIES "
                         "('gcp.biglake.table-management.enabled'='true')")
            accepted, why = True, ""
        except Exception as e:  # noqa: BLE001
            accepted, why = False, _spark_err(e)
        _drop(tbl)
        if not accepted:
            return _res(fid, fname, version, "fail",
                        f"Automatic table management cannot be enabled on a v3 table ({why}); "
                        "BigQuery has no maintenance statement of its own")
        return _res(fid, fname, version, "skip",
                    "The catalog accepts gcp.biglake.table-management.enabled on a v3 table, though "
                    "Google documents managed capabilities as v2-only; the background jobs only run "
                    "above ~1 GB, so whether they run on v3 is unverified")
    tbl = _unique("maint")
    ok, out, _ = _run_all([_create(tbl, "id INT64"), f"INSERT INTO {_t(tbl)} VALUES (1)"])
    if not ok:
        _drop(tbl)
        return _res(fid, fname, version, "fail", out)
    props = _metadata(tbl).get("properties", {})
    ok2, out2 = _run(f"ALTER TABLE {_t(tbl)} SET OPTIONS (`properties.gcp.biglake.table-management` = 'disabled')")
    after = _metadata(tbl).get("properties", {}).get("gcp.biglake.table-management.enabled")
    _drop(tbl)
    enabled = props.get("gcp.biglake.table-management.enabled") == "true"
    if enabled and ok2:
        return _res(fid, fname, version, "pass",
                    "BigQuery-created tables have automatic table management (compaction, snapshot "
                    f"expiry) on by default and BigQuery DDL can switch it (after disabling: {after}); "
                    "the jobs themselves only run above ~1 GB and were not observed")
    return _res(fid, fname, version, "fail",
                f"table-management property={props.get('gcp.biglake.table-management.enabled')}; "
                f"toggle: {'ok' if ok2 else out2[:100]}")


def test_branching_tagging(version: str) -> TestResult:
    fid, fname = _names(TestResult("branching-tagging", "Branching & Tagging", version))
    tbl = _unique("ref")
    ok, out = _make(version, tbl, "id INT64")
    if not ok:
        return _cannot(fid, fname, version, f"No {version} table BigQuery can use: {out}")
    okb, outb = _run(f"ALTER TABLE {_t(tbl)} CREATE BRANCH audit")
    okt, outt = _run(f"ALTER TABLE {_t(tbl)} CREATE TAG v1")
    _drop(tbl)
    if okb or okt:
        return _res(fid, fname, version, "pass", "BigQuery created an Iceberg branch or tag")
    return _res(fid, fname, version, "fail",
                f"BigQuery SQL has no branch or tag DDL (CREATE BRANCH: {outb[:60]}; CREATE TAG: {outt[:60]}) "
                "and no syntax to read a ref")


# ---------------------------------------------------------------------------
# Read / write details
# ---------------------------------------------------------------------------

def _bq_written_files(version: str, tbl: str) -> list:
    ok, out = _make(version, tbl, "id INT64, name STRING")
    if not ok:
        raise RuntimeError(out)
    ok, out = _run(f"INSERT INTO {_t(tbl)} (id, name) SELECT x, CONCAT('n', CAST(x AS STRING)) "
                   "FROM UNNEST(GENERATE_ARRAY(1, 500)) x")
    if not ok:
        raise RuntimeError(out)
    return [f for f in _live_files(_metadata(tbl)) if f["content"] == 0]


def test_statistics(version: str) -> TestResult:
    fid, fname = _names(TestResult("statistics", "Statistics", version))
    tbl = _unique("stats")
    try:
        files = _bq_written_files(version, tbl)
    except Exception as e:  # noqa: BLE001
        _drop(tbl)
        return _res(fid, fname, version, "fail" if version == "v3" else "error", _reason(e))
    _drop(tbl)
    with_bounds = [f for f in files if f["lower_bounds"] and f["upper_bounds"]]
    with_counts = [f for f in files if f["value_counts"]]
    where = _how(version)
    if files and len(with_bounds) == len(files):
        return _res(fid, fname, version, "pass",
                    f"Every data file BigQuery wrote{where} carries Iceberg column bounds in its manifest "
                    f"({len(files)} file(s), value counts on {len(with_counts)})")
    if with_bounds or with_counts:
        return _res(fid, fname, version, "partial",
                    f"Only some column metrics written{where}: bounds on {len(with_bounds)}/{len(files)}, "
                    f"counts on {len(with_counts)}/{len(files)}")
    return _res(fid, fname, version, "fail",
                f"BigQuery wrote {len(files)} data file(s){where} with no column metrics "
                "(empty value_counts, lower_bounds and upper_bounds) in the manifest")


def _parquet_bloom_columns(local_path: str) -> tuple:
    """Columns with a Parquet bloom filter, read with parquet-mr in Spark's JVM."""
    jvm = _spark()._jvm
    conf = jvm.org.apache.hadoop.conf.Configuration()
    path = jvm.org.apache.hadoop.fs.Path(f"file://{local_path}")
    reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(
        jvm.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(path, conf))
    try:
        cols, total = set(), 0
        for block in reader.getFooter().getBlocks():
            for col in block.getColumns():
                total += 1
                if col.getBloomFilterOffset() >= 0:
                    cols.add(str(col.getPath().toDotString()))
        return cols, total
    finally:
        reader.close()


def test_bloom_filters(version: str) -> TestResult:
    fid, fname = _names(TestResult("bloom-filters", "Bloom Filters", version))
    ok, why = _spark_ok()
    if not ok:
        return _res(fid, fname, version, "skip", f"Parquet footers are inspected with parquet-mr: {why}")
    tbl = _unique("bloom")
    try:
        files = _bq_written_files(version, tbl)
        f = files[0]["path"]
        bucket, key = f[len("gs://"):].split("/", 1)
        local = f"/tmp/{RUN_TAG}_{os.path.basename(key)}"
        _gcs().bucket(bucket).blob(key).download_to_filename(local)
        cols, total = _parquet_bloom_columns(local)
        os.remove(local)
    except Exception as e:  # noqa: BLE001
        _drop(tbl)
        return _res(fid, fname, version, "fail" if version == "v3" else "error", _reason(e))
    _drop(tbl)
    if cols:
        return _res(fid, fname, version, "pass",
                    f"BigQuery-written Parquet{_how(version)} carries bloom filters on {sorted(cols)}")
    return _res(fid, fname, version, "fail",
                f"No bloom filter in any of {total} column chunk(s) of a data file BigQuery wrote"
                f"{_how(version)}, and BigQuery DDL has no option to request one")


# ---------------------------------------------------------------------------
# Catalogs
# ---------------------------------------------------------------------------

def _federated_reason(what: str) -> str:
    return (f"{what} is reachable from BigQuery only by federating it into the Lakehouse runtime "
            "catalog (borderless Lakehouse, Preview); no federated catalog is configured in this "
            "project, so the path is unverified here")


# Always skipped, so they mark no matrix cell as tested.
def test_aws_glue_catalog(version: str) -> TestResult:
    r = TestResult("aws-glue-catalog", "AWS Glue Catalog", version)
    r.result = "skip"
    r.details = _federated_reason("AWS Glue")
    return r


def test_rest_catalog(version: str) -> TestResult:
    r = TestResult("rest-catalog", "REST Catalog", version)
    r.result = "skip"
    r.details = _federated_reason("A third-party Iceberg REST catalog")
    return r


def test_unity_catalog(version: str) -> TestResult:
    r = TestResult("unity-catalog", "Unity Catalog", version)
    r.result = "skip"
    r.details = _federated_reason("Databricks Unity Catalog")
    return r


def test_snowflake_horizon_catalog(version: str) -> TestResult:
    r = TestResult("snowflake-horizon-catalog", "Snowflake Horizon Catalog", version)
    r.result = "skip"
    r.details = _federated_reason("Snowflake Horizon Catalog")
    return r


def test_google_lakehouse(version: str) -> TestResult:
    fid, fname = _names(TestResult("google-lakehouse", "Google Lakehouse", version))
    if not LAKEHOUSE:
        return _res(fid, fname, version, "skip",
                    "Managed tables live in a BigQuery dataset; the lakehouse mode measures this cell")
    r = test_catalog_integration(version)
    r.feature_id, r.feature_name = fid, fname
    return r


# ---------------------------------------------------------------------------
# V3 types and lineage
# ---------------------------------------------------------------------------

def _v3_type(fid: str, fname: str, version: str, fixture: str, ddl_type: str) -> TestResult:
    skip = _v3_only(fid, fname, version)
    if skip:
        return skip
    tbl = _unique("ty")
    ok, out, _ = _run_all([_create(tbl, f"id INT64, c {ddl_type}")])
    if ok:
        stored = _field_type(tbl, "c")
        _drop(tbl)
        declare = f"BigQuery's nearest type, {ddl_type}, maps to Iceberg {stored!r}"
    else:
        declare = f"BigQuery cannot declare {ddl_type} on an Iceberg table ({out[:80]})"
    if not LAKEHOUSE:
        return _res(fid, fname, version, "fail", f"{_v3_write_refusal()}; {declare}")
    st, rows, fx = _read_fixture(fixture)
    if st == "skip" and fx.get("catalog_refused"):
        return _res(fid, fname, version, "fail",
                    f"{rows[:200]}, so no such table can exist for BigQuery to read or write; {declare}")
    if st == "skip":
        return _res(fid, fname, version, "fail",
                    f"BigQuery cannot declare the type ({declare}); reading it is unmeasured: {rows[:120]}")
    if st == "fail":
        return _res(fid, fname, version, "fail",
                    f"BigQuery cannot read a v3 {fname} column Spark wrote ({rows[:160]}); {declare}")
    if fx.get("schema_only"):
        return _res(fid, fname, version, "skip",
                    f"BigQuery binds a v3 table with a {fname} column (schema only), so value reads "
                    f"are unverified; {declare}")
    return _res(fid, fname, version, "partial",
                f"BigQuery reads a v3 {fname} column Spark wrote ({str(rows)[:80]}) but cannot declare it; {declare}")


def test_variant_type(version: str) -> TestResult:
    fid, fname = _names(TestResult("variant-type", "Variant Type", version))
    return _v3_type(fid, fname, version, "v3_variant", "JSON")


def test_shredded_variant(version: str) -> TestResult:
    fid, fname = _names(TestResult("shredded-variant", "Shredded Variant", version))
    skip = _v3_only(fid, fname, version)
    if skip:
        return skip
    base = test_variant_type(version)
    if base.result in ("pass", "partial"):
        return _res(fid, fname, version, "skip",
                    "A variant column is readable, but whether it was shredded is not observable from "
                    "BigQuery, so shredding itself is unverified")
    base.feature_id, base.feature_name = fid, fname
    return base


def test_geometry_type(version: str) -> TestResult:
    fid, fname = _names(TestResult("geometry-type", "Geometry / Geo Types", version))
    return _v3_type(fid, fname, version, "v3_geometry", "GEOGRAPHY")


def test_nanosecond_timestamps(version: str) -> TestResult:
    fid, fname = _names(TestResult("nanosecond-timestamps", "Nanosecond Timestamps", version))
    return _v3_type(fid, fname, version, "v3_ts_ns", "TIMESTAMP")


def test_unknown_type(version: str) -> TestResult:
    fid, fname = _names(TestResult("unknown-type", "Unknown Type", version))
    skip = _v3_only(fid, fname, version)
    if skip:
        return skip
    if not LAKEHOUSE:
        return _res(fid, fname, version, "fail", _v3_write_refusal())
    st, rows, fx = _read_fixture("v3_unknown")
    if st == "ok":
        return _res(fid, fname, version, "skip",
                    "BigQuery binds a v3 table with an unknown-typed column (schema only); "
                    "BigQuery has no unknown type of its own to declare")
    return _res(fid, fname, version, "fail",
                f"{str(rows)[:200]}; BigQuery has no unknown type of its own to declare")


def _spark_lineage(fq: str) -> dict:
    _spark().sql(f"REFRESH TABLE {fq}")
    return {int(r[0]): (int(r[1]), int(r[2])) for r in _spark().sql(
        f"SELECT id, _row_id, _last_updated_sequence_number FROM {fq}").collect()}


def test_lineage(version: str) -> TestResult:
    fid, fname = _names(TestResult("lineage", "Lineage Tracking", version))
    skip = _v3_only(fid, fname, version)
    if skip:
        return skip
    if not LAKEHOUSE:
        return _res(fid, fname, version, "fail", _v3_write_refusal())
    # Row lineage requires a copy-on-write writer to carry each surviving
    # row's _row_id and _last_updated_sequence_number into the rewritten file.
    # Spark reads them back, so a BigQuery UPDATE of one row must leave the
    # other rows' lineage untouched.
    tbl = _unique("lin")
    ok, out = _make(version, tbl, "id INT64, val STRING")
    if not ok:
        return _cannot(fid, fname, version, out)
    fq = f"local.{NS}.{tbl}"
    try:
        _spark().sql(f"INSERT INTO {fq} VALUES (1,'a'),(2,'b'),(3,'c')")
        before = _spark_lineage(fq)
        ok, out = _run(f"UPDATE {_t(tbl)} SET val='u' WHERE id = 1")
        after = _spark_lineage(fq) if ok else {}
    except Exception as e:  # noqa: BLE001
        _drop(tbl)
        return _res(fid, fname, version, "error", _spark_err(e))
    okq, outq = _run(f"SELECT _row_id FROM {_t(tbl)}")
    _drop(tbl)
    exposed = ("BigQuery exposes _row_id" if okq
               else f"BigQuery does not expose the lineage fields ({outq[:70]})")
    if not ok:
        return _res(fid, fname, version, "fail", f"BigQuery UPDATE on {V3_HOW} failed: {out}; {exposed}")
    untouched = [i for i in (2, 3) if after.get(i) == before.get(i)]
    if len(untouched) == 2 and after.get(1, (None,))[0] == before.get(1, (None,))[0]:
        return _res(fid, fname, version, "pass",
                    f"A BigQuery UPDATE kept every row's _row_id and the untouched rows' sequence numbers; {exposed}")
    return _res(fid, fname, version, "fail",
                f"A BigQuery UPDATE of row 1 reassigned lineage for rows it did not change "
                f"(before {before}, after {after}), so row lineage is not preserved; {exposed}")


ALL_TESTS = [
    test_table_creation, test_read_support, test_write_insert, test_write_merge_update_delete,
    test_catalog_integration, test_position_deletes, test_equality_deletes, test_merge_on_read,
    test_copy_on_write, test_deletion_vectors, test_schema_evolution, test_type_promotion,
    test_column_default_values, test_hidden_partitioning, test_partition_evolution,
    test_multi_arg_transforms, test_time_travel, test_table_maintenance, test_branching_tagging,
    test_statistics, test_bloom_filters, test_aws_glue_catalog, test_rest_catalog,
    test_unity_catalog, test_snowflake_horizon_catalog, test_google_lakehouse,
    test_variant_type, test_shredded_variant, test_geometry_type, test_nanosecond_timestamps,
    test_unknown_type, test_lineage,
]


# ---------------------------------------------------------------------------
# Reporting (same shape as the other suites)
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
    return {feat["id"]: {"name": feat.get("name", feat["id"])} for feat in data.get("features", [])}


def compute_coverage(results: list) -> dict:
    matrix = load_matrix_features()
    tested = {r.feature_id for r in results if not r.diagnostic}
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


def generate_report(results: list, bq_note: str) -> dict:
    json_support = load_json_support()
    tests_output, discrepancies, unverified = [], 0, 0
    for r in results:
        if r.diagnostic:
            tests_output.append({**r.to_dict(), "json_level": "n/a", "match": True,
                                 "verified": r.result not in ("skip", "error")})
            continue
        level = json_support.get((r.feature_id, r.version_tested), "unknown")
        match = compute_match(r.result, level)
        discrepancies += 0 if match else 1
        is_unverified = r.result in ("skip", "error")
        unverified += 1 if is_unverified else 0
        tests_output.append({**r.to_dict(), "json_level": level, "match": match,
                             "verified": not is_unverified})
    coverage = compute_coverage(results)
    counted = [r for r in results if not r.diagnostic]
    return {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "engine": "BigQuery",
        "mode": STORAGE_MODE,
        "bigquery_version": bq_note,
        "platform": MATRIX_PLATFORM_ID,
        "catalog_mode": (f"Lakehouse runtime catalog {CATALOG} (Iceberg REST)" if LAKEHOUSE
                         else f"BigQuery dataset {DATASET}, connection {CONNECTION}"),
        "versions_tested": VERSIONS,
        "coverage": coverage,
        "tests": tests_output,
        "summary": {
            "total": len(counted),
            "passed": sum(1 for r in counted if r.result == "pass"),
            "partial": sum(1 for r in counted if r.result == "partial"),
            "failed": sum(1 for r in counted if r.result == "fail"),
            "skipped": sum(1 for r in counted if r.result == "skip"),
            "errors": sum(1 for r in counted if r.result == "error"),
            "discrepancies": discrepancies,
            "unverified": unverified,
            "uncovered_features": len(coverage["uncovered"]),
        },
    }


def generate_markdown(report: dict) -> str:
    s = report["summary"]
    lines = [
        "# BigQuery Iceberg Feature Test Report", "",
        f"- **Timestamp:** {report['timestamp']}",
        f"- **Storage mode:** {report['mode']}",
        f"- **Catalog:** {report['catalog_mode']}",
        f"- **Region:** {REGION}",
        f"- **Format Versions Tested:** {', '.join(report['versions_tested'])}", "",
        "## Summary", "", "| Metric | Count |", "|--------|-------|",
        f"| Total | {s['total']} |", f"| Passed | {s['passed']} |", f"| Partial | {s['partial']} |",
        f"| Failed | {s['failed']} |", f"| Skipped | {s['skipped']} |", f"| Errors | {s['errors']} |",
        f"| Discrepancies vs matrix | {s['discrepancies']} |",
        f"| Unverified (skip/error) | {s['unverified']} |",
        f"| Uncovered matrix features | {s['uncovered_features']} |", "",
        "`Failed` is a result, not a defect: it records that BigQuery does not support the "
        "feature. `Partial` means measured as half-supported, typically readable but not "
        "writable. A discrepancy means the observed behaviour disagrees with the matrix cell.", "",
    ]
    cov = report["coverage"]
    lines.append(f"**Matrix coverage:** {cov['tested_feature_count']}/{cov['matrix_feature_count']} "
                 "features in `features.json` have a test.")
    for f in cov["uncovered"]:
        lines.append(f"- uncovered: **{f['name']}** (`{f['id']}`)")
    lines += ["", "## Test Results", "",
              "| Feature | Version | Result | Matrix | Match | Details |",
              "|---------|---------|--------|--------|-------|---------|"]
    label = {"pass": "PASS", "partial": "PARTIAL", "fail": "FAIL", "skip": "SKIP", "error": "ERR"}
    for t in report["tests"]:
        details = (t["details"] or "")[:180].replace("\n", " ").replace("|", "\\|")
        lines.append(f"| {t['feature_name']} | {t['version']} | {label.get(t['result'], '?')} "
                     f"| {t['json_level']} | {'ok' if t['match'] else 'DISCREPANCY'} | {details} |")
    discs = [t for t in report["tests"] if not t["match"]]
    if discs:
        lines += ["", "## Discrepancies", ""]
        for t in discs:
            lines.append(f"- **{t['feature_name']}** ({t['version']}): observed `{t['result']}`, "
                         f"matrix says `{t['json_level']}` — {(t['details'] or '')[:400]}")
    unver = [t for t in report["tests"] if not t["verified"] and not t["diagnostic"]]
    if unver:
        lines += ["", "## Unverified", ""]
        for t in unver:
            lines.append(f"- **{t['feature_name']}** ({t['version']}): matrix `{t['json_level']}` — "
                         f"{(t['details'] or '')[:250]}")
    return "\n".join(lines) + "\n"


def _write_reports(report: dict) -> None:
    os.makedirs(REPORT_DIR, exist_ok=True)
    stem = f"bigquery-iceberg-test-report-{STORAGE_MODE}"
    with open(os.path.join(REPORT_DIR, f"{stem}.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)
    md = generate_markdown(report)
    with open(os.path.join(REPORT_DIR, f"{stem}.md"), "w") as f:
        f.write(md)
    print(f"\nReports: {REPORT_DIR}/{stem}.json|.md\n")
    print(md)
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(md)


# ---------------------------------------------------------------------------
# Setup and teardown
# ---------------------------------------------------------------------------

def setup() -> tuple:
    if not PROJECT:
        return False, "GCP_PROJECT is not set"
    if LAKEHOUSE:
        if not CATALOG:
            return False, "GCP_LAKEHOUSE_CATALOG is required for the lakehouse mode"
        resp = _rest().post(f"{REST_PREFIX}/namespaces", json={"namespace": [NS], "properties": {}})
        if resp.status_code not in (200, 409):
            return False, f"namespace {NS} not created: HTTP {resp.status_code} {resp.text[:150]}"
        return True, ""
    if not (CONNECTION and DATA_BUCKET):
        return False, "GCP_CONNECTION and GCP_DATA_BUCKET are required for the managed mode"
    return True, ""


def teardown() -> None:
    if LAKEHOUSE:
        try:
            resp = _rest().get(f"{REST_PREFIX}/namespaces/{NS}/tables")
            for ident in resp.json().get("identifiers", []):
                _rest().delete(f"{REST_PREFIX}/namespaces/{NS}/tables/{ident['name']}",
                               params={"purgeRequested": "true"})
            _rest().delete(f"{REST_PREFIX}/namespaces/{NS}")
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] teardown of namespace {NS} incomplete: {_reason(e)}")
        try:
            prefix = f"lakehouse/{NS}/"
            for blob in _gcs().bucket(DATA_BUCKET).list_blobs(prefix=prefix) if DATA_BUCKET else []:
                blob.delete()
        except Exception:  # noqa: BLE001
            pass
        return
    for name in set(_CREATED):
        _drop(name)
    try:
        for blob in _gcs().bucket(DATA_BUCKET).list_blobs(prefix=f"managed/{RUN_TAG}/"):
            blob.delete()
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] could not clean gs://{DATA_BUCKET}/managed/{RUN_TAG}/: {_reason(e)}")


def main():
    print("=" * 70)
    print("  BigQuery Iceberg Feature Test Suite")
    print("=" * 70)
    print(f"Project/region:  {PROJECT} / {REGION}")
    print(f"Storage mode:    {STORAGE_MODE}")
    print(f"Catalog:         {CATALOG + ' namespace ' + NS if LAKEHOUSE else DATASET}")
    print(f"Matrix platform: {MATRIX_PLATFORM_ID} ({MATRIX_DATA_PATH})")
    print(f"Run tag:         {RUN_TAG}\n")

    ok, message = setup()
    if not ok:
        print(f"[FATAL] {message}")
        sys.exit(1)

    only = os.environ.get("BIGQUERY_ONLY", "").strip()
    tests = ALL_TESTS
    if only:
        wanted = {t.strip() for t in only.split(",") if t.strip()}
        tests = [t for t in ALL_TESTS if t.__name__.replace("test_", "") in wanted]
    results = []
    try:
        can_v3, evidence = _v3_creatable()
        print(f"BigQuery can create v3 tables: {'yes' if can_v3 else 'NO'} ({evidence})")
        for version in VERSIONS:
            print(f"\n{'=' * 70}\n  Format version {version.upper()}\n{'=' * 70}")
            for fn in tests:
                print(f"\n--- {fn.__name__} [{version}] ---", flush=True)
                try:
                    result = fn(version)
                except Exception as e:  # noqa: BLE001
                    result = _res(fn.__name__.replace("test_", "").replace("_", "-"),
                                  fn.__name__, version, "error", f"Unhandled exception: {_reason(e)}")
                batch = result if isinstance(result, list) else [result]
                results.extend(batch)
                for res in batch:
                    print(f"  {res.result}: {res.details[:200]}")
    finally:
        teardown()

    report = generate_report(results, "serverless (no version exposed)")
    _write_reports(report)
    s = report["summary"]
    print(f"{s['passed']} passed, {s['partial']} partial, {s['failed']} failed, {s['skipped']} skipped, "
          f"{s['errors']} errors, {s['discrepancies']} discrepancies, {s['unverified']} unverified")
    sys.exit(1 if (s["discrepancies"] > 0 or s["errors"] > 0) else 0)


if __name__ == "__main__":
    main()
