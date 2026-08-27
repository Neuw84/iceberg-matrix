"""Delete everything billable that a Snowflake test run created.

Runs with `if: always()` so it must never raise: every step is best-effort and
reports what it did. Anything it cannot remove is printed loudly, because the
alternative is silent spend and orphaned S3 objects.

What the suite creates, and therefore what this removes:
  - Schemas          <run-tag>_<n> inside SNOWFLAKE_DATABASE, one per test,
                     each holding managed Iceberg tables
  - Managed tables   dropped by DROP SCHEMA ... CASCADE; Snowflake removes the
                     table's data and metadata files on the external volume

DROP SCHEMA CASCADE is the primary cleanup path, exactly like the Databricks
teardown drops UC schemas. This matters specifically for Snowflake-managed
Iceberg: Snowflake owns the files under snowflake/<base>/ on the bucket and
tracks them through its own catalog, so deleting those objects directly with
boto3 would corrupt the catalog's view rather than clean up. The S3 lifecycle
rule on snowflake/managed/ is only a backstop for files orphaned by a crashed
run; the correct teardown is always the SQL DROP, which lets Snowflake remove
its own storage.

The virtual warehouse and the external volume are long-lived infrastructure
(a warehouse bills only while running and auto-suspends), so they are left
alone, exactly like the Redshift Serverless workgroup and the SQL warehouse on
the other engines.

Prefix-scoped, not run-scoped: any schema starting with RESOURCE_PREFIX is
swept, so a crashed earlier run's leftovers are collected by the next run.

Environment: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and either SNOWFLAKE_PASSWORD or
             SNOWFLAKE_PRIVATE_KEY (+ optional SNOWFLAKE_PRIVATE_KEY_PASSPHRASE),
             SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE (default: ICEBERGMATRIX),
             SNOWFLAKE_ROLE (optional),
             RESOURCE_PREFIX (default: ICEBERGMATRIX)
"""

import os
import re
import sys

ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")
USER = os.environ.get("SNOWFLAKE_USER", "")
PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "")
PRIVATE_KEY = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "")
PRIVATE_KEY_PASSPHRASE = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
ROLE = os.environ.get("SNOWFLAKE_ROLE", "")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "")
DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "ICEBERGMATRIX")
# Schemas are created upper-cased with non-identifier chars replaced, so match
# teardown's sweep prefix the same way the suite builds NS_PREFIX.
RESOURCE_PREFIX = re.sub(
    r"[^A-Za-z0-9_]", "_", os.environ.get("RESOURCE_PREFIX", "ICEBERGMATRIX").upper()
)

problems: list[str] = []


def note(msg: str) -> None:
    print(f"[teardown] {msg}")


def failed(what: str, e: Exception) -> None:
    msg = f"{what}: {type(e).__name__}: {e}"
    problems.append(msg)
    print(f"[teardown] COULD NOT CLEAN {msg}")


def _private_key_der():
    if not PRIVATE_KEY:
        return None
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    passphrase = PRIVATE_KEY_PASSPHRASE.encode() if PRIVATE_KEY_PASSPHRASE else None
    key = serialization.load_pem_private_key(
        PRIVATE_KEY.encode(), password=passphrase, backend=default_backend()
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def main() -> int:
    have_auth = bool(PASSWORD or PRIVATE_KEY)
    if not (ACCOUNT and USER and WAREHOUSE and have_auth):
        note("Snowflake connection not configured; nothing to do")
        return 0

    try:
        import snowflake.connector

        kwargs = {
            "account": ACCOUNT,
            "user": USER,
            "warehouse": WAREHOUSE,
            "database": DATABASE,
        }
        if ROLE:
            kwargs["role"] = ROLE
        der = _private_key_der()
        if der is not None:
            kwargs["private_key"] = der
        else:
            kwargs["password"] = PASSWORD
        conn = snowflake.connector.connect(**kwargs)
    except Exception as e:  # noqa: BLE001 - a dead connection must not raise
        failed("connect to Snowflake", e)
        print("\n[teardown] could not connect; schemas may remain - "
              f"check database '{DATABASE}' by hand")
        return 0

    try:
        with conn.cursor() as c:
            c.execute(f"SHOW SCHEMAS IN DATABASE {DATABASE}")
            rows = c.fetchall()
            # SHOW SCHEMAS returns name in column index 1 (created_on, name, ...).
            schemas = [row[1] for row in rows
                       if str(row[1]).upper().startswith(RESOURCE_PREFIX)]
        note(f"schemas to drop in {DATABASE}: {len(schemas)}")

        for schema in schemas:
            try:
                with conn.cursor() as c:
                    # CASCADE drops the managed Iceberg tables inside; Snowflake
                    # removes their data and metadata on the external volume.
                    c.execute(f"DROP SCHEMA IF EXISTS {DATABASE}.{schema} CASCADE")
                note(f"dropped {DATABASE}.{schema}")
            except Exception as e:  # noqa: BLE001 - keep sweeping
                failed(f"drop schema {DATABASE}.{schema}", e)
    except Exception as e:  # noqa: BLE001
        failed(f"list schemas in {DATABASE}", e)
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001, S110 - closing is best-effort
            pass

    if problems:
        print("\n[teardown] the following could not be cleaned and may still bill:")
        for p in problems:
            print(f"  - {p}")
        print(f"[teardown] check database '{DATABASE}' by hand; the snowflake/ "
              "lifecycle rule will expire orphaned files as a backstop")
    else:
        note("all run schemas removed; Snowflake removed managed table storage")
    # Never fail the workflow: a teardown error must not mask the test report.
    return 0


if __name__ == "__main__":
    sys.exit(main())
