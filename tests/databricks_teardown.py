"""Delete everything billable that a Databricks test run created.

Runs with `if: always()` so it must never raise: every step is best-effort and
reports what it did. Anything it cannot remove is printed loudly, because the
alternative is silent spend.

What the suite creates, and therefore what this removes:
  - Unity Catalog schemas    <run-tag>_<n> inside DATABRICKS_CATALOG, one per
                             test, each holding managed Iceberg tables
  - Managed table storage    purged by UC when the schema is dropped CASCADE;
                             a direct S3 sweep is deliberately NOT done here
                             because deleting managed-table files behind Unity
                             Catalog's back corrupts its bookkeeping

The SQL warehouse itself is long-lived workspace infrastructure (serverless
warehouses bill per query, nothing while idle), so it is left alone, exactly
like the Redshift Serverless workgroup on the AWS side.

Prefix-scoped, not run-scoped: any schema starting with RESOURCE_PREFIX is
swept, so a crashed earlier run's leftovers are collected by the next run.

Environment: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID,
             DATABRICKS_CATALOG (default: icebergmatrix),
             RESOURCE_PREFIX (default: icebergmatrix)
"""

import os
import sys

HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "icebergmatrix")
RESOURCE_PREFIX = os.environ.get("RESOURCE_PREFIX", "icebergmatrix")

problems: list[str] = []


def note(msg: str) -> None:
    print(f"[teardown] {msg}")


def failed(what: str, e: Exception) -> None:
    msg = f"{what}: {type(e).__name__}: {e}"
    problems.append(msg)
    print(f"[teardown] COULD NOT CLEAN {msg}")


def main() -> int:
    if not (HOST and TOKEN and WAREHOUSE_ID):
        note("Databricks connection not configured; nothing to do")
        return 0

    try:
        from databricks import sql as dbsql

        conn = dbsql.connect(
            server_hostname=HOST.replace("https://", ""),
            http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
            access_token=TOKEN,
        )
    except Exception as e:  # noqa: BLE001 - a dead connection must not raise
        failed("connect to the SQL warehouse", e)
        print("\n[teardown] could not connect; schemas may remain — "
              f"check catalog '{CATALOG}' in the workspace by hand")
        return 0

    try:
        with conn.cursor() as c:
            c.execute(f"SHOW SCHEMAS IN {CATALOG}")
            schemas = [row[0] for row in c.fetchall()
                       if str(row[0]).startswith(RESOURCE_PREFIX)]
        note(f"schemas to drop in {CATALOG}: {len(schemas)}")

        for schema in schemas:
            try:
                with conn.cursor() as c:
                    # CASCADE drops the managed Iceberg tables inside; Unity
                    # Catalog purges their storage asynchronously.
                    c.execute(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")
                note(f"dropped {CATALOG}.{schema}")
            except Exception as e:  # noqa: BLE001 - keep sweeping
                failed(f"drop schema {CATALOG}.{schema}", e)
    except Exception as e:  # noqa: BLE001
        failed(f"list schemas in {CATALOG}", e)
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001, S110 - closing is best-effort
            pass

    if problems:
        print("\n[teardown] the following could not be cleaned and may still bill:")
        for p in problems:
            print(f"  - {p}")
        print(f"[teardown] check catalog '{CATALOG}' in the workspace by hand")
    else:
        note("all run schemas removed; UC purges managed storage asynchronously")
    # Never fail the workflow: a teardown error must not mask the test report.
    return 0


if __name__ == "__main__":
    sys.exit(main())
