"""Shared Spark fixture helper for the OSS engine feature-test suites.

Some row-level-operations cells (position-deletes, merge-on-read,
copy-on-write) are about what an engine's own DML actually writes. Measuring
that honestly means: create the table with Spark, requesting the write
strategy explicitly via table properties; run the *target* engine's own
DELETE against that same table through the shared Iceberg REST catalog; then
read the result back with Spark, which can see delete-file content types
(0=data, 1=position-delete, 2=equality-delete) and snapshot operations that
the target engine's own SQL surface may not expose.

This mirrors the pattern already used for the AWS live-platform suites
(tests/iceberg_feature_tests.py creates with explicit properties, mutates, and
inspects $files/.snapshots) and for Redshift (tests/redshift_feature_tests.py
reads metadata.json directly since Redshift exposes no Iceberg metadata
tables) -- here Spark plays the role of "authoritative inspector" for engines
whose own SQL surface cannot see delete-file content types (DuckDB, PyIceberg).

Both Spark and the target engine must point at the *same* Iceberg REST
catalog for this to mean anything: they are two different SQL dialects
addressing one physical table, not two independent copies. That catalog is
the Lakekeeper + MinIO stack in tests/docker (see start-lakekeeper.sh),
which every catalog-backed suite in this directory already targets.

Environment variables (matching tests/duckdb_feature_tests.py, so one running
Lakekeeper instance serves every suite without separate configuration):
    ICEBERG_REST_URI        - Iceberg REST catalog endpoint
                              (default: "http://127.0.0.1:8181/catalog")
    ICEBERG_REST_WAREHOUSE  - Warehouse identifier to attach (default: "demo")
    ICEBERG_S3_ENDPOINT     - S3 endpoint for data files (default: "127.0.0.1:9000")
    ICEBERG_S3_KEY_ID       - S3 access key id (default: "minio")
    ICEBERG_S3_SECRET       - S3 secret access key (default: "minio12345")
    ICEBERG_S3_REGION       - S3 region (default: "us-east-1")
    SPARK_VERSION           - Iceberg Spark runtime artifact suffix (default: "4.1")
    ICEBERG_VERSION         - Iceberg version (default: "1.11.0")
    ICEBERG_JAR             - Comma-separated local jar path(s); when unset the
                              Maven coordinates above are resolved via
                              spark.jars.packages instead (same fallback
                              tests/iceberg_feature_tests.py uses)

Nothing here fabricates a result: callers must treat `available()` returning
False, or `create_fixture`/`inspect_delete_files` raising, as grounds for
`skip`/`error` -- never a `pass` or `fail`.
"""

import os
import urllib.error
import urllib.request
import uuid

try:
    from pyspark.sql import SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

REST_URI = os.environ.get("ICEBERG_REST_URI", "http://127.0.0.1:8181/catalog")
REST_WAREHOUSE = os.environ.get("ICEBERG_REST_WAREHOUSE", "demo")
S3_ENDPOINT = os.environ.get("ICEBERG_S3_ENDPOINT", "127.0.0.1:9000")
S3_KEY_ID = os.environ.get("ICEBERG_S3_KEY_ID", "minio")
S3_SECRET = os.environ.get("ICEBERG_S3_SECRET", "minio12345")
S3_REGION = os.environ.get("ICEBERG_S3_REGION", "us-east-1")
SPARK_VERSION_SHORT = os.environ.get("SPARK_VERSION", "4.1")
ICEBERG_VERSION = os.environ.get("ICEBERG_VERSION", "1.11.0")
ICEBERG_JAR = os.environ.get("ICEBERG_JAR", "")

NOT_AVAILABLE_DETAIL = (
    "Requires a local PySpark session against an Iceberg REST catalog "
    "(set ICEBERG_REST_URI; start tests/docker/start-lakekeeper.sh); "
    "not configured in this run"
)

_spark_session = None
_rest_checked = False
_rest_ok = False


def _rest_reachable(uri: str, timeout: float = 2.0) -> bool:
    if not uri:
        return False
    url = f"{uri.rstrip('/')}/v1/config?warehouse={REST_WAREHOUSE}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
            return resp.status < 500
    except urllib.error.HTTPError as e:
        return e.code < 500
    except Exception:
        return False


def available() -> bool:
    """True when PySpark is importable and the REST catalog answers.

    Cached after the first check (a catalog that is down does not come back
    mid-run, and re-probing before every test would just slow the suite down).
    """
    global _rest_checked, _rest_ok
    if not PYSPARK_AVAILABLE:
        return False
    if not _rest_checked:
        _rest_ok = _rest_reachable(REST_URI)
        _rest_checked = True
    return _rest_ok


def get_spark():
    """A lazily-created, cached local SparkSession against the REST catalog."""
    global _spark_session
    if _spark_session is not None:
        return _spark_session
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("pyspark is not installed")

    builder = (
        SparkSession.builder
        .appName("spark-fixture")
        .master("local[1]")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "rest")
        .config("spark.sql.catalog.local.uri", REST_URI)
        .config("spark.sql.catalog.local.warehouse", REST_WAREHOUSE)
        .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.local.s3.endpoint", f"http://{S3_ENDPOINT}")
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
        .config("spark.sql.catalog.local.client.region", S3_REGION)
        .config("spark.sql.defaultCatalog", "local")
        .config("spark.ui.enabled", "false")
    )

    jar_paths = [os.path.abspath(j.strip()) for j in ICEBERG_JAR.split(",")
                if j.strip() and os.path.isfile(j.strip())]
    if jar_paths:
        builder = builder.config("spark.jars", ",".join(jar_paths))
    else:
        jar_coord = (f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION_SHORT}_2.13:"
                    f"{ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}")
        builder = builder.config("spark.jars.packages", jar_coord)

    _spark_session = builder.getOrCreate()
    return _spark_session


def new_namespace() -> str:
    """Create and return a fresh, uniquely-named namespace via Spark."""
    ns = "sf_" + uuid.uuid4().hex[:10]
    get_spark().sql(f"CREATE NAMESPACE IF NOT EXISTS local.{ns}")
    return ns


def create_fixture(ns: str, name: str, version: str, write_mode: str,
                   columns_ddl: str = "id BIGINT, val STRING",
                   seed_sql: str = "(1,'a'),(2,'b'),(3,'c')") -> None:
    """Create and seed a table with an explicit row-level write strategy.

    write_mode is applied to write.delete.mode, write.update.mode and
    write.merge.mode uniformly, since the point is to pin down what the
    *target* engine's DML actually does once that strategy is requested, not
    to test partial configurations Spark itself never exercises.
    """
    spark = get_spark()
    fqn = f"local.{ns}.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {fqn}")
    spark.sql(f"""
        CREATE TABLE {fqn} ({columns_ddl})
        USING iceberg
        TBLPROPERTIES (
          'format-version' = '{"3" if version == "v3" else "2"}',
          'write.delete.mode' = '{write_mode}',
          'write.update.mode' = '{write_mode}',
          'write.merge.mode'  = '{write_mode}'
        )
    """)
    spark.sql(f"INSERT INTO {fqn} VALUES {seed_sql}")


def refresh(ns: str, name: str) -> None:
    """Force Spark to reload this table's metadata from the catalog.

    Each engine's REST catalog client caches table metadata independently, so
    Spark can still see the pre-mutation snapshot immediately after another
    engine commits a change through the same catalog. Without this, inspecting
    delete files right after the target engine's DML silently measures stale
    state instead of what was actually written.
    """
    get_spark().sql(f"REFRESH TABLE local.{ns}.{name}")


def inspect_delete_files(ns: str, name: str) -> dict:
    """Delete-file counts by Iceberg content type, read back through Spark.

    content: 0=data (never returned by all_delete_files), 1=position-delete,
    2=equality-delete. Refreshes first -- see refresh().
    """
    refresh(ns, name)
    fqn = f"local.{ns}.{name}"
    rows = get_spark().sql(f"SELECT content FROM {fqn}.all_delete_files").collect()
    counts = {"position": 0, "equality": 0}
    for row in rows:
        if row[0] == 1:
            counts["position"] += 1
        elif row[0] == 2:
            counts["equality"] += 1
    return counts


def row_count(ns: str, name: str) -> int:
    refresh(ns, name)
    fqn = f"local.{ns}.{name}"
    return get_spark().sql(f"SELECT count(*) FROM {fqn}").collect()[0][0]


def drop_fixture(ns: str, name: str) -> None:
    try:
        get_spark().sql(f"DROP TABLE IF EXISTS local.{ns}.{name}")
    except Exception:  # noqa: BLE001 - best-effort cleanup
        pass
