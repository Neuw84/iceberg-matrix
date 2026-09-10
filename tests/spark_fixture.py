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
the Apache Polaris + MinIO stack in tests/docker (see start-polaris.sh),
which every catalog-backed suite in this directory already targets.

Environment variables (matching tests/duckdb_feature_tests.py, so one running
Polaris instance serves every suite without separate configuration):
    ICEBERG_REST_URI        - Iceberg REST catalog endpoint
                              (default: "http://127.0.0.1:8181/api/catalog")
    ICEBERG_REST_WAREHOUSE  - Catalog (warehouse) name to attach (default: "demo")
    ICEBERG_REST_CREDENTIAL - OAuth2 client credentials "id:secret"
                              (default: "root:s3cr3t", Polaris' bootstrap root)
    ICEBERG_REST_SCOPE      - OAuth2 scope (default: "PRINCIPAL_ROLE:ALL")
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

REST_URI = os.environ.get("ICEBERG_REST_URI", "http://127.0.0.1:8181/api/catalog")
REST_WAREHOUSE = os.environ.get("ICEBERG_REST_WAREHOUSE", "demo")
REST_CREDENTIAL = os.environ.get("ICEBERG_REST_CREDENTIAL", "root:s3cr3t")
REST_SCOPE = os.environ.get("ICEBERG_REST_SCOPE", "PRINCIPAL_ROLE:ALL")
S3_ENDPOINT = os.environ.get("ICEBERG_S3_ENDPOINT", "127.0.0.1:9000")
S3_KEY_ID = os.environ.get("ICEBERG_S3_KEY_ID", "minio")
S3_SECRET = os.environ.get("ICEBERG_S3_SECRET", "minio12345")
S3_REGION = os.environ.get("ICEBERG_S3_REGION", "us-east-1")
SPARK_VERSION_SHORT = os.environ.get("SPARK_VERSION", "4.1")
ICEBERG_VERSION = os.environ.get("ICEBERG_VERSION", "1.11.0")
ICEBERG_JAR = os.environ.get("ICEBERG_JAR", "")

NOT_AVAILABLE_DETAIL = (
    "Requires a local PySpark session against an Iceberg REST catalog "
    "(set ICEBERG_REST_URI; start tests/docker/start-polaris.sh); "
    "not configured in this run"
)

_spark_session = None
_rest_checked = False
_rest_ok = False


def _rest_reachable(uri: str, timeout: float = 2.0) -> bool:
    """True when something answers the Iceberg REST config endpoint at ``uri``.

    Polaris answers 401 to an unauthenticated GET /v1/config; any 4xx still
    proves a catalog is listening, which is all this check is for.
    """
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


def rest_auth_conf() -> dict:
    """Iceberg REST client properties for OAuth2 client-credentials auth.

    Returned as plain Iceberg catalog property names (``credential``, ``scope``,
    ``oauth2-server-uri``) so every engine that speaks the Iceberg REST protocol
    through the Java/Python client -- Spark, Flink, PyIceberg -- can spread the
    same dict into its own catalog config. Empty when ICEBERG_REST_CREDENTIAL is
    blank, for catalogs that run without auth.
    """
    if not REST_CREDENTIAL:
        return {}
    return {
        "credential": REST_CREDENTIAL,
        "scope": REST_SCOPE,
        "oauth2-server-uri": f"{REST_URI.rstrip('/')}/v1/oauth/tokens",
        # The bootstrap token is long-lived enough for a test run; refreshing
        # against Polaris' token endpoint is not needed and can 401 mid-run.
        "token-refresh-enabled": "false",
    }


def rest_client_id_secret() -> tuple:
    """(client_id, client_secret) split out of ICEBERG_REST_CREDENTIAL."""
    if not REST_CREDENTIAL or ":" not in REST_CREDENTIAL:
        return ("", "")
    client_id, _, secret = REST_CREDENTIAL.partition(":")
    return (client_id, secret)


def rest_available() -> bool:
    """True when the REST catalog answers, regardless of whether PySpark is
    importable -- for suites (PyIceberg, Daft) that talk to the catalog directly.

    Cached after the first check (a catalog that is down does not come back
    mid-run, and re-probing before every test would just slow the suite down).
    """
    global _rest_checked, _rest_ok
    if not _rest_checked:
        _rest_ok = _rest_reachable(REST_URI)
        _rest_checked = True
    return _rest_ok


def available() -> bool:
    """True when PySpark is importable and the REST catalog answers."""
    return PYSPARK_AVAILABLE and rest_available()


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
        .config("spark.sql.catalog.local.s3.access-key-id", S3_KEY_ID)
        .config("spark.sql.catalog.local.s3.secret-access-key", S3_SECRET)
        .config("spark.sql.catalog.local.client.region", S3_REGION)
        .config("spark.sql.defaultCatalog", "local")
        .config("spark.ui.enabled", "false")
        # Spark 4.1 gates GEOMETRY/GEOGRAPHY and DEFAULT clauses behind these
        # flags; the fixture creates V3 tables other engines then read, so keep
        # it capable of every V3 type.
        .config("spark.sql.geospatial.enabled", "true")
        .config("spark.sql.defaultColumn.enabled", "true")
    )
    for key, value in rest_auth_conf().items():
        builder = builder.config(f"spark.sql.catalog.local.{key}", value)

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


def create_equality_delete_fixture(ns: str, name: str, version: str = "v2") -> dict:
    """Create a seeded table and attach a real Iceberg *equality* delete to it.

    Neither Spark SQL nor DuckDB nor PyIceberg can write an equality-delete
    file (content=2): Spark's DELETE emits positional deletes on v2 and
    deletion vectors on v3, and the other two have no equality-delete write
    path at all. The canonical producer is a streaming upsert sink (Flink), but
    the delete-file format is engine-agnostic once written, so we build one
    directly with the Iceberg Java API through Spark's JVM gateway -- the same
    library Flink would use -- rather than standing up a second engine.

    The table is created via Spark (so it is a normal REST-catalog table any
    engine can attach to), then an EqualityDeleteWriter writes a single delete
    record keyed on a STRING column ``k`` and the file is committed with
    ``RowDelta.addDeletes``. A string key is used deliberately: py4j collapses
    small Java longs to Integer when a value crosses the Python/JVM boundary,
    which the Parquet writer then rejects, so an integer key cannot be written
    this way -- a string key has no such ambiguity.

    Returns a dict describing what was produced so the caller can assert the
    engine under test reads it correctly::

        {"deleted_key": "b", "deleted_id": 2, "live_ids": [1, 3],
         "delete_files": {"position": 0, "equality": 1}}

    Raises on any failure; callers treat that as ``error`` (never a fabricated
    pass/fail). ``drop_fixture(ns, name)`` cleans up as usual.
    """
    # id 2 / k='b' is the row the equality delete removes; 1 and 3 stay live.
    create_fixture(
        ns, name, version, "merge-on-read",
        columns_ddl="id BIGINT, k STRING, val STRING",
        seed_sql="(1,'a','x'),(2,'b','y'),(3,'c','z')",
    )
    write_equality_delete(get_spark(), f"local.{ns}.{name}", "k", "b")
    counts = inspect_delete_files(ns, name)
    return {
        "deleted_key": "b",
        "deleted_id": 2,
        "live_ids": [1, 3],
        "delete_files": counts,
    }


def write_equality_delete(spark, fqn: str, key_column: str, key_value: str) -> dict:
    """Commit one equality-delete file (content=2) to ``fqn`` via the Iceberg Java API.

    Session-agnostic so any Spark-based suite can call it with its own
    SparkSession: ``spark`` must have an Iceberg SparkCatalog configured under
    the catalog name that ``fqn`` (``catalog.namespace.table``) refers to.
    ``key_column`` must be a STRING column (see create_equality_delete_fixture
    for why an integer key cannot cross the py4j boundary). The table must be
    unpartitioned.

    Returns ``{"record_count": n, "content": "EQUALITY_DELETES", "path": ...}``
    describing the committed delete file. Raises on any failure.
    """
    jvm = spark._jvm
    jspark = spark._jsparkSession
    gateway = spark._sc._gateway

    tbl = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(jspark, fqn)
    schema = tbl.schema()
    spec = tbl.spec()
    key_field = schema.findField(key_column)

    # Equality-delete row schema is the projection of the equality columns.
    names_arr = gateway.new_array(jvm.java.lang.String, 1)
    names_arr[0] = key_column
    eq_schema = schema.select(names_arr)

    eq_field_ids = gateway.new_array(jvm.int, 1)
    eq_field_ids[0] = key_field.fieldId()

    file_format = jvm.org.apache.iceberg.FileFormat
    appender_factory = jvm.org.apache.iceberg.data.GenericAppenderFactory(
        schema, spec, eq_field_ids, eq_schema, None
    )
    out_file = (
        jvm.org.apache.iceberg.io.OutputFileFactory
        .builderFor(tbl, 1, 1)
        .format(file_format.PARQUET)
        .build()
        .newOutputFile()
    )
    # Unpartitioned table -> null partition tuple.
    writer = appender_factory.newEqDeleteWriter(out_file, file_format.PARQUET, None)
    record = jvm.org.apache.iceberg.data.GenericRecord.create(eq_schema)
    record.setField(key_column, key_value)
    writer.write(record)
    writer.close()
    delete_file = writer.toDeleteFile()
    tbl.newRowDelta().addDeletes(delete_file).commit()
    return {
        "record_count": int(delete_file.recordCount()),
        "content": str(delete_file.content().toString()),
        "path": str(delete_file.location()),
    }


def table_location(ns: str, name: str) -> str:
    """The table's root location (``s3://bucket/...``) as recorded by the catalog.

    Engines without a REST client -- or with a broken one -- can still read an
    Iceberg table straight from its storage location; this is how they find it.
    """
    spark = get_spark()
    tbl = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, f"local.{ns}.{name}"
    )
    return str(tbl.location())


def s3_http_location(ns: str, name: str) -> str:
    """table_location() rewritten as the path-style HTTP URL MinIO serves it at.

    ``s3://bucket/key`` becomes ``http://<S3_ENDPOINT>/bucket/key/``, which is
    the form ClickHouse's icebergS3() / IcebergS3 engine and DuckDB's
    iceberg_scan take when pointed at S3-compatible storage without a catalog.
    """
    loc = table_location(ns, name)
    assert loc.startswith("s3://"), f"unexpected table location scheme: {loc}"
    return f"http://{S3_ENDPOINT}/{loc[len('s3://'):].rstrip('/')}/"


def _latest_metadata_file(ns: str, name: str) -> str:
    """Newest ``metadata/*.metadata.json`` under the table location, by version.

    Engines that write to storage without going through the REST catalog
    (ClickHouse's IcebergS3 engine) leave the catalog pointing at an older
    metadata.json. Listing the metadata directory and taking the highest
    version number finds what they actually committed.
    """
    spark = get_spark()
    jvm = spark._jvm
    loc = table_location(ns, name).rstrip("/") + "/metadata/"
    tbl = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, f"local.{ns}.{name}"
    )
    io = tbl.io()
    # S3FileIO implements SupportsPrefixOperations; listPrefix returns FileInfo.
    files = [str(f.location()) for f in _jiter(io.listPrefix(loc))]
    metas = [f for f in files if f.endswith(".metadata.json")]
    if not metas:
        raise RuntimeError(f"no metadata.json under {loc}")

    def version(path: str) -> int:
        # Iceberg Java writes "00003-<uuid>.metadata.json"; ClickHouse's Iceberg
        # engine writes "v3.metadata.json" (the Hadoop-catalog convention).
        base = path.rsplit("/", 1)[-1]
        head = base.split("-", 1)[0].split(".", 1)[0]
        if head.startswith("v") and head[1:].isdigit():
            return int(head[1:])
        return int(head) if head.isdigit() else -1

    return max(metas, key=version)


def _storage_table(ns: str, name: str):
    """A Java Iceberg Table loaded straight from the newest metadata.json.

    Bypasses the catalog entirely, so it reflects commits made by engines
    that wrote to storage without notifying the catalog.
    """
    spark = get_spark()
    jvm = spark._jvm
    catalog_tbl = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, f"local.{ns}.{name}"
    )
    ops = jvm.org.apache.iceberg.StaticTableOperations(
        _latest_metadata_file(ns, name), catalog_tbl.io()
    )
    return jvm.org.apache.iceberg.BaseTable(ops, f"{ns}.{name}")


def inspect_delete_files_from_storage(ns: str, name: str) -> dict:
    """Like inspect_delete_files, but reading the newest metadata in storage
    (see _storage_table). Counts every delete file across all snapshots."""
    tbl = _storage_table(ns, name)
    counts = {"position": 0, "equality": 0}
    if tbl.currentSnapshot() is None:
        return counts
    # Walk every snapshot so deletes from earlier commits are included.
    for s in _jiter(tbl.snapshots()):
        for df in _jiter(s.addedDeleteFiles(tbl.io())):
            content = str(df.content().toString())
            if content == "POSITION_DELETES":
                counts["position"] += 1
            elif content == "EQUALITY_DELETES":
                counts["equality"] += 1
    return counts


def _jiter(java_iterable):
    """Iterate a java.lang.Iterable through py4j (which does not make them
    Python-iterable on its own)."""
    it = java_iterable.iterator()
    while it.hasNext():
        yield it.next()


def delete_file_formats_from_storage(ns: str, name: str) -> set:
    """File formats (e.g. {'PARQUET', 'PUFFIN'}) of every delete file in the
    newest storage metadata; a PUFFIN position delete is a V3 deletion vector."""
    tbl = _storage_table(ns, name)
    formats = set()
    for s in _jiter(tbl.snapshots()):
        for df in _jiter(s.addedDeleteFiles(tbl.io())):
            formats.add(str(df.format().toString()))
    return formats


def row_count_from_storage(ns: str, name: str) -> int:
    """Live row count from the newest storage metadata (see _storage_table):
    sum of data-file record counts minus nothing -- so only meaningful when the
    table has no delete files. Callers that need delete-aware counts should read
    the table with an engine instead."""
    tbl = _storage_table(ns, name)
    snap = tbl.currentSnapshot()
    if snap is None:
        return 0
    return int(snap.summary().get("total-records") or 0)


def create_column_default_fixture(ns: str, name: str) -> dict:
    """A V3 table whose first row predates a column that carries an initial-default.

    OSS Spark's Iceberg connector refuses DEFAULT clauses and PyIceberg cannot
    write V3 data files, so neither can produce this fixture alone. Spark
    creates and seeds the V3 table; PyIceberg then adds ``region STRING`` with
    ``initial-default='eu'`` through the same REST catalog (a metadata-only
    commit); Spark appends one more row with an explicit value. A reader that
    honours V3 defaults must return 'eu' for id 1 and 'us' for id 2.

    Returns {"expected": {1: "eu", 2: "us"}, "column": "region"}. Requires
    pyiceberg; raises if it is not importable.
    """
    from pyiceberg.catalog import load_catalog
    from pyiceberg.types import StringType

    spark = get_spark()
    spark.sql(f"DROP TABLE IF EXISTS local.{ns}.{name}")
    spark.sql(f"""CREATE TABLE local.{ns}.{name} (id BIGINT) USING iceberg
                  TBLPROPERTIES ('format-version'='3')""")
    spark.sql(f"INSERT INTO local.{ns}.{name} VALUES (1)")

    props = {
        "uri": REST_URI, "warehouse": REST_WAREHOUSE,
        "s3.endpoint": f"http://{S3_ENDPOINT}", "s3.access-key-id": S3_KEY_ID,
        "s3.secret-access-key": S3_SECRET, "s3.region": S3_REGION,
    }
    props.update(rest_auth_conf())
    props.pop("token-refresh-enabled", None)
    tbl = load_catalog("spark_fixture_defaults", **props).load_table(f"{ns}.{name}")
    with tbl.update_schema() as update:
        update.add_column("region", StringType(), default_value="eu")

    refresh(ns, name)
    spark.sql(f"INSERT INTO local.{ns}.{name} VALUES (2, 'us')")
    return {"expected": {1: "eu", 2: "us"}, "column": "region"}


def create_duckdb_fixture(ns: str, name: str, columns_ddl: str, values_sql: str,
                          probe_expr: str, extensions: tuple = ()) -> dict:
    """Write a V3 table into the shared REST catalog through DuckDB.

    Some V3 column types have no Spark producer here: the Iceberg 1.11 Spark
    connector rejects ``GEOMETRY(4326)`` ("Not a supported type") and Spark has
    no nanosecond timestamp literal. DuckDB 1.5 can write both against a REST
    catalog, so it is the fallback producer when the ``duckdb`` module is
    importable. ``columns_ddl`` / ``values_sql`` are DuckDB SQL fragments;
    ``probe_expr`` is selected back and returned as ``value`` so the caller
    can quote what was written.

    Returns ``{"ok": True, "value": <probe>}`` on success or
    ``{"ok": False, "reason": ...}`` when DuckDB is unavailable or refuses, so
    callers can report "not exercised" rather than a fabricated result. The
    namespace must already exist (use new_namespace()).
    """
    try:
        import duckdb  # noqa: WPS433 - optional producer, only needed here
    except ImportError:
        return {"ok": False, "reason": f"duckdb module not installed; no engine available to write ({columns_ddl})"}
    try:
        con = duckdb.connect(":memory:")
        con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
        for ext in extensions:
            con.execute(f"INSTALL {ext}; LOAD {ext};")
        con.execute(f"""
            CREATE SECRET s3sec (TYPE s3, KEY_ID '{S3_KEY_ID}', SECRET '{S3_SECRET}',
                                 ENDPOINT '{S3_ENDPOINT}', URL_STYLE 'path', USE_SSL false,
                                 REGION '{S3_REGION}')""")
        client_id, client_secret = rest_client_id_secret()
        if client_id:
            con.execute(f"""
                CREATE SECRET restsec (TYPE iceberg, CLIENT_ID '{client_id}',
                                       CLIENT_SECRET '{client_secret}',
                                       OAUTH2_SERVER_URI '{REST_URI.rstrip('/')}/v1/oauth/tokens',
                                       OAUTH2_SCOPE '{REST_SCOPE}')""")
            auth = "SECRET restsec"
        else:
            auth = "AUTHORIZATION_TYPE 'none'"
        con.execute(f"""
            ATTACH '{REST_WAREHOUSE}' AS ib (TYPE iceberg, ENDPOINT '{REST_URI}', {auth},
                                            ACCESS_DELEGATION_MODE 'none')""")
        con.execute(f"CREATE TABLE ib.{ns}.{name} ({columns_ddl}) WITH ('format-version'='3')")
        con.execute(f"INSERT INTO ib.{ns}.{name} VALUES {values_sql}")
        value = con.execute(f"SELECT {probe_expr} FROM ib.{ns}.{name}").fetchone()[0]
        con.close()
        return {"ok": True, "value": value}
    except Exception as e:  # noqa: BLE001 - the reason is what the caller reports
        return {"ok": False, "reason": f"DuckDB could not produce the ({columns_ddl}) fixture: {str(e).splitlines()[0][:160]}"}


def create_geometry_fixture(ns: str, name: str) -> dict:
    """V3 table ``(id BIGINT, c GEOMETRY)`` holding POINT (1.5 2.5), written by
    DuckDB (see create_duckdb_fixture). Adds ``wkt`` to the result on success."""
    out = create_duckdb_fixture(ns, name, "id BIGINT, c GEOMETRY", "(1, ST_Point(1.5, 2.5))",
                                "ST_AsText(c)", extensions=("spatial",))
    if out["ok"]:
        out["wkt"] = out["value"]
    return out


def create_timestamp_ns_fixture(ns: str, name: str) -> dict:
    """V3 table ``(id BIGINT, c TIMESTAMP_NS)`` holding
    2026-05-20 12:00:00.123456789, written by DuckDB (see create_duckdb_fixture)."""
    return create_duckdb_fixture(ns, name, "id BIGINT, c TIMESTAMP_NS",
                                 "(1, TIMESTAMP_NS '2026-05-20 12:00:00.123456789')", "c")
