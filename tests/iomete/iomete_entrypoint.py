"""Job-side entry point: run the iceberg-matrix Spark suite inside an IOMETE Spark job.

Submitted by tests/iomete/run_on_iomete.py.

Mounted into the job with configMaps next to the unmodified suite and the matrix data:
    /etc/configs/matrix/tests/iceberg_feature_tests.py
    /etc/configs/matrix/src/data/features.json
    /etc/configs/matrix/platform/<spark3|spark4>.json
It points the suite's `local` catalog at the same Iceberg REST catalog IOMETE configures as
spark_catalog (read from the driver's spark.properties), so no credentials are involved.

Modes (first argument):
    suite     run the suite; print the JSON report between markers in the driver log
    dv-write  create a V3 merge-on-read table, insert 5 rows, delete 2 (deletion vectors)
    dv-read   read that table's row count (expect 3), then drop it
Environment (set on the job): MATRIX_PLATFORM_ID, PLATFORM_LABEL.
"""
import base64
import glob
import json
import os
import re
import runpy
import sys

ROOT = "/etc/configs/matrix"
WORK = "/tmp/icebergmatrix"
REPORT_DIR = os.path.join(WORK, "reports")
BEGIN, END = "@@ICEBERG_MATRIX_REPORT_BEGIN@@", "@@ICEBERG_MATRIX_REPORT_END@@"
CATALOG_PREFIX = "spark.sql.catalog.spark_catalog."
# Catalog properties copied from spark_catalog; anything secret-looking is never copied.
COPY_KEYS = ("type", "uri", "warehouse", "io-impl", "client.region", "s3.endpoint",
             "s3.path-style-access", "header.X-Iceberg-Access-Delegation")
INTEROP_TABLE = "spark_catalog.icebergmatrix_interop.dv_probe"


def spark_properties() -> dict:
    """Read the driver's spark.properties (Spark on Kubernetes mounts it under SPARK_CONF_DIR)."""
    props = {}
    for path in [os.path.join(os.environ.get("SPARK_CONF_DIR", "/opt/spark/conf"), "spark.properties"),
                 "/opt/spark/conf/spark.properties"]:
        if os.path.isfile(path):
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    props[k.strip()] = v.strip().replace("\\:", ":").replace("\\=", "=")
            print(f"[entrypoint] read {len(props)} properties from {path}")
            return props
    # Fallback: ask the driver JVM (spark-submit passes confs as spark.* system properties).
    from pyspark import SparkConf, SparkContext
    SparkContext._ensure_initialized()
    props = dict(SparkConf().getAll())
    print(f"[entrypoint] no spark.properties found; read {len(props)} properties from the JVM")
    return props


def catalog_env(props: dict) -> dict:
    cat = {k[len(CATALOG_PREFIX):]: v for k, v in props.items() if k.startswith(CATALOG_PREFIX)}
    ctype = cat.get("type", "")
    if ctype != "rest" or not cat.get("uri"):
        raise SystemExit(f"[entrypoint] spark_catalog is not a REST catalog here (type={ctype!r}); stopping")
    extra = [f"{k}={cat[k]}" for k in COPY_KEYS if k in cat and k not in ("type", "warehouse")]
    print(f"[entrypoint] local catalog -> REST {cat['uri']} (warehouse {cat.get('warehouse', '')})")
    return {
        "ICEBERG_CATALOG_TYPE": "rest",
        "ICEBERG_CATALOG_WAREHOUSE": cat.get("warehouse", ""),
        "ICEBERG_CATALOG_PROPS": ",".join(extra),
        # Lets test_rest_catalog run: `local` is this REST catalog.
        "ICEBERG_REST_URI": cat["uri"],
    }


def iceberg_version() -> str:
    for path in sorted(glob.glob("/opt/spark/jars/iceberg-spark-runtime-*.jar")):
        m = re.search(r"iceberg-spark-runtime-[\d.]+_[\d.]+-(.+)\.jar$", os.path.basename(path))
        if m:
            print(f"[entrypoint] iceberg runtime: {path}")
            return m.group(1)
    return "in-image"


def prepare_suite(catalog: str) -> str:
    """Copy the suite and its spark_fixture sibling into one writable folder.

    With catalog != "local", only the SQL references to the suite's primary catalog are
    renamed (local.{ns}..., SHOW NAMESPACES IN local, spark.sql.defaultCatalog). IOMETE's data
    security plugin authorizes by catalog name, and an IOMETE user's grants are on the
    platform's own catalogs (spark_catalog), not on a catalog the suite invents.
    """
    out_dir = os.path.join(WORK, "suite")
    os.makedirs(out_dir, exist_ok=True)
    text = open(os.path.join(ROOT, "tests", "iceberg_feature_tests.py")).read()
    if catalog != "local":
        text, n_refs = re.subn(r"(?<![\w.])local\.(?=\{|system\.)", catalog + ".", text)
        n_show = text.count('SHOW NAMESPACES IN local"')
        text = text.replace('SHOW NAMESPACES IN local"', f'SHOW NAMESPACES IN {catalog}"')
        n_default = text.count('"spark.sql.defaultCatalog", "local"')
        text = text.replace('"spark.sql.defaultCatalog", "local"', f'"spark.sql.defaultCatalog", "{catalog}"')
        print(f"[entrypoint] catalog renamed local -> {catalog}: {n_refs} table/namespace refs, "
              f"{n_show} SHOW NAMESPACES, {n_default} defaultCatalog")
        if n_show != 1 or n_default != 1 or n_refs < 90:
            raise SystemExit("[entrypoint] unexpected suite shape; refusing to run a partial rename")
    suite = os.path.join(out_dir, "iceberg_feature_tests.py")
    open(suite, "w").write(text)
    with open(os.path.join(ROOT, "fixture", "spark_fixture.py")) as src, \
            open(os.path.join(out_dir, "spark_fixture.py"), "w") as dst:
        dst.write(src.read())
    return suite


def run_suite(env: dict) -> int:
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.environ.update(env)
    os.environ.update({
        "REPO_ROOT": ROOT,
        "REPORT_DIR": REPORT_DIR,
        "ICEBERG_VERSION": iceberg_version(),
        "ICEBERG_WAREHOUSE": os.path.join(WORK, "hadoop-warehouse"),
        "MATRIX_DATA_PATH": os.environ.get("MATRIX_DATA_PATH", ""),
        "MATRIX_NS_PREFIX": "icebergmatrix_",
    })
    suite = prepare_suite(os.environ.get("MATRIX_CATALOG", "local"))
    code = 0
    try:
        runpy.run_path(suite, run_name="__main__")
    except SystemExit as e:
        code = int(e.code or 0)
    report = os.path.join(REPORT_DIR, "iceberg-test-report.json")
    if os.path.isfile(report):
        blob = base64.b64encode(open(report, "rb").read()).decode()
        print(BEGIN)
        for i in range(0, len(blob), 4000):
            print(blob[i:i + 4000])
        print(END)
    print(f"[entrypoint] suite exit code: {code}")
    return 0


def interop(mode: str, env: dict) -> int:
    # Uses the platform's spark_catalog exactly as IOMETE configures it.
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(f"icebergmatrix-{mode}").getOrCreate()
    if mode == "dv-write":
        spark.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.icebergmatrix_interop")
        spark.sql(f"DROP TABLE IF EXISTS {INTEROP_TABLE}")
        spark.sql(f"""CREATE TABLE {INTEROP_TABLE} (id BIGINT) USING iceberg TBLPROPERTIES (
            'format-version'='3', 'write.delete.mode'='merge-on-read')""")
        spark.sql(f"INSERT INTO {INTEROP_TABLE} VALUES (1),(2),(3),(4),(5)")
        spark.sql(f"DELETE FROM {INTEROP_TABLE} WHERE id IN (2, 4)")
        fmt = [r[0] for r in spark.sql(f"SELECT file_format FROM {INTEROP_TABLE}.delete_files").collect()]
        cnt = spark.sql(f"SELECT count(*) FROM {INTEROP_TABLE}").collect()[0][0]
        print(f"@@DV_WRITE@@ spark={spark.version} count={cnt} delete_file_formats={fmt}")
    else:
        try:
            ids = sorted(r[0] for r in spark.sql(f"SELECT id FROM {INTEROP_TABLE}").collect())
            print(f"@@DV_READ@@ spark={spark.version} ids={ids}")
        except Exception as e:  # report, then clean up
            print(f"@@DV_READ@@ spark={spark.version} error={str(e)[:300]}")
        spark.sql(f"DROP TABLE IF EXISTS {INTEROP_TABLE}")
        spark.sql("DROP NAMESPACE IF EXISTS spark_catalog.icebergmatrix_interop")
    spark.stop()
    return 0


def shred_probe() -> int:
    """Same footer check as the suite's test_shredded_variant, once per table property."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("icebergmatrix-shred-probe").getOrCreate()
    ns = "spark_catalog.icebergmatrix_shred"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
    try:
        for name, prop in (("suite_property", "write.parquet.variant-shredding.enabled"),
                           ("iceberg_property", "write.parquet.shred-variants")):
            tbl = f"{ns}.{name}"
            spark.sql(f"DROP TABLE IF EXISTS {tbl}")
            spark.sql(f"CREATE TABLE {tbl} (id BIGINT, data VARIANT) USING iceberg "
                      f"TBLPROPERTIES ('format-version'='3', '{prop}'='true')")
            spark.sql(f"INSERT INTO {tbl} SELECT 1, parse_json('{{\"user\":\"alice\",\"n\":2}}')")
            path = spark.sql(f"SELECT file_path FROM {tbl}.files LIMIT 1").collect()[0][0]
            jvm = spark.sparkContext._jvm
            jtable = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, tbl)
            f = jtable.io().newInputFile(path)
            length = f.getLength()
            stream = f.newStream()
            tail_len = min(length, 65536)
            stream.seek(length - tail_len)
            buf = bytearray()
            while len(buf) < tail_len:
                b = stream.read()
                if b < 0:
                    break
                buf.append(b)
            stream.close()
            value = spark.sql(f"SELECT variant_get(data, '$.user', 'string') FROM {tbl}").collect()[0][0]
            print(f"@@SHRED@@ spark={spark.version} property={prop} typed_value_in_footer="
                  f"{b'typed_value' in bytes(buf)} read_back={value!r} file={path.rsplit('/', 1)[-1]}")
            spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    finally:
        spark.sql(f"DROP NAMESPACE IF EXISTS {ns}")
    spark.stop()
    return 0


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "suite"
    if mode == "shred-probe":
        return shred_probe()
    env = catalog_env(spark_properties())
    if mode == "suite":
        return run_suite(env)
    return interop(mode, env)


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
