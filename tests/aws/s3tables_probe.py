"""Report which Iceberg catalog implementation Spark actually resolves.

Submitted as an EMR Serverless entry point with the same --conf set as the real
s3tables job. It answers one question: when the configuration says type=rest,
does Spark build a RESTCatalog, and if not, what does it build instead?

Written because the s3tables job kept failing inside
GlueCatalog.defaultWarehouseLocation even though both spark-submit and the
SparkSession builder passed spark.sql.catalog.local.type=rest and no
catalog-impl. SparkCatalog wraps the real catalog in a CachingCatalog, so the
class has to be unwrapped by reflection to see what is underneath -- reading the
wrapper only tells you it is cached, not what it caches.

Namespace listing succeeding proves nothing on its own: namespace and table
operations can take different paths. So this also attempts a real CREATE TABLE,
which is the operation that actually fails in the suite.
"""

import uuid

from pyspark.sql import SparkSession


def unwrap(obj, depth=0):
    """Print the class of obj, then follow any wrapped catalog fields inwards."""
    indent = "  " + "  " * depth
    cls = obj.getClass()
    print(f"{indent}{cls.getName()}")
    for field_name in ("catalog", "wrapped", "delegate", "icebergCatalog"):
        try:
            field = cls.getDeclaredField(field_name)
            field.setAccessible(True)
            inner = field.get(obj)
            if inner is not None and depth < 5:
                print(f"{indent}  .{field_name} ->")
                unwrap(inner, depth + 2)
                return
        except Exception:  # noqa: BLE001 - field simply absent on this class
            continue


def main() -> int:
    spark = SparkSession.builder.appName("s3tables-catalog-probe").getOrCreate()
    sc = spark.sparkContext

    # The catalog name is a variable under test: "local" may collide with
    # something in the image, so the submitter picks the name and tells us
    # through defaultCatalog rather than this script assuming one.
    catalog = spark.conf.get("spark.sql.defaultCatalog", "local")
    print(f"=== catalog under test: {catalog} ===")

    print("\n=== effective catalog configuration ===")
    for key, value in sorted(sc.getConf().getAll()):
        if "catalog" in key.lower() or "iceberg" in key.lower() or "jars" in key.lower():
            print(f"  {key} = {value}")

    print("\n=== resolved catalog chain (unwrapped) ===")
    try:
        manager = spark._jsparkSession.sessionState().catalogManager()
        plugin = manager.catalog(catalog)
        unwrap(plugin)
    except Exception as e:  # noqa: BLE001
        print(f"  could not resolve: {type(e).__name__}: {str(e)[:300]}")

    # Must carry the resource prefix: the job role is scoped to icebergmatrix*,
    # so any other namespace name fails on IAM and hides the real result.
    ns = f"icebergmatrix_probe{uuid.uuid4().hex[:6]}"
    tbl = f"{catalog}.{ns}.t_{uuid.uuid4().hex[:8]}"
    print("\n=== catalog operations ===")
    steps = [
        ("list namespaces", f"SHOW NAMESPACES IN {catalog}"),
        ("create namespace", f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}"),
        ("create table", f"CREATE TABLE {tbl} (id BIGINT, val STRING) USING iceberg "
                         "TBLPROPERTIES ('format-version'='2')"),
        ("describe table", f"DESCRIBE EXTENDED {tbl}"),
        ("insert", f"INSERT INTO {tbl} VALUES (1,'a'),(2,'b')"),
        ("select", f"SELECT count(*) FROM {tbl}"),
        ("drop table", f"DROP TABLE IF EXISTS {tbl}"),
        ("drop namespace", f"DROP NAMESPACE IF EXISTS {catalog}.{ns}"),
    ]
    for label, stmt in steps:
        try:
            rows = spark.sql(stmt).collect()
            preview = [str(r[0]) for r in rows][:5] if rows else "ok"
            print(f"  {label}: OK -> {preview}")
        except Exception as e:  # noqa: BLE001
            print(f"  {label}: FAILED -> {str(e)[:500]}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
