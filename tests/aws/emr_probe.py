"""Diagnostic job: report what the EMR Serverless image actually provides.

Submitted by run_emr_serverless.py when PROBE=1. Costs one short job run and
answers the questions that otherwise turn into guesswork:

  - where the Iceberg runtime jar lives, and what it is called
  - whether Iceberg is already on the default classpath (no spark.jars needed)
  - whether the S3 Tables catalog implementation is present
  - which Spark and Iceberg versions the release label actually gives you

Reads nothing and writes nothing. Deliberately does not create a SparkSession
with Iceberg extensions, so it cannot fail for reasons unrelated to discovery.
"""

import glob
import os
import subprocess
import sys

CANDIDATE_DIRS = [
    "/usr/share/aws/iceberg/lib",
    "/usr/share/aws/iceberg",
    "/usr/lib/spark/jars",
    "/usr/share/aws/aws-java-sdk",
    "/usr/share/aws/s3tables",
]

PATTERNS = ["*iceberg*", "*s3tables*", "*s3-tables*"]


def section(title: str) -> None:
    print(f"\nPROBE ===== {title} =====")


def main() -> int:
    section("python / spark")
    print(f"PROBE python: {sys.version.split()[0]}")
    try:
        import pyspark
        print(f"PROBE pyspark: {pyspark.__version__}")
        print(f"PROBE SPARK_HOME: {os.environ.get('SPARK_HOME', '(unset)')}")
    except Exception as e:  # noqa: BLE001
        print(f"PROBE pyspark import failed: {e}")

    section("candidate directories")
    for d in CANDIDATE_DIRS:
        if os.path.isdir(d):
            entries = sorted(os.listdir(d))
            print(f"PROBE dir {d}: {len(entries)} entries")
            for name in entries[:40]:
                print(f"PROBE     {name}")
        else:
            print(f"PROBE dir {d}: MISSING")

    section("jar search")
    for pattern in PATTERNS:
        hits = []
        for root in ("/usr/share/aws", "/usr/lib/spark", "/usr/lib", "/opt"):
            if os.path.isdir(root):
                hits += glob.glob(os.path.join(root, "**", pattern + ".jar"), recursive=True)
        print(f"PROBE pattern {pattern}: {len(hits)} hit(s)")
        for h in sorted(set(hits))[:25]:
            print(f"PROBE     {h}")

    section("classpath resolution")
    # Ask the JVM whether the Iceberg classes are already reachable without any
    # spark.jars: if they are, the extension config alone is enough.
    for cls in (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "org.apache.iceberg.aws.glue.GlueCatalog",
        "software.amazon.s3tables.iceberg.S3TablesCatalog",
    ):
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("probe").getOrCreate()
            jvm = spark.sparkContext._jvm
            loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
            try:
                jvm.java.lang.Class.forName(cls, False, loader)
                print(f"PROBE class {cls}: PRESENT")
            except Exception:
                print(f"PROBE class {cls}: NOT on default classpath")
        except Exception as e:  # noqa: BLE001
            print(f"PROBE class {cls}: check failed ({type(e).__name__}: {e})")

    section("iceberg version hint")
    try:
        out = subprocess.run(
            ["bash", "-lc", "ls /usr/share/aws/iceberg/lib 2>/dev/null || true"],
            capture_output=True, text=True, timeout=30,
        )
        print(f"PROBE ls: {out.stdout.strip() or '(empty)'}")
    except Exception as e:  # noqa: BLE001
        print(f"PROBE ls failed: {e}")

    print("\nPROBE done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
