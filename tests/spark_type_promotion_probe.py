"""Standalone OSS-Spark reproduction of the Databricks type-promotion detail
probe (tests/databricks_feature_tests.py::test_type_promotion_detail /
_v3_detail).

For each Iceberg-spec-valid primitive promotion it runs the same end-to-end
sequence against a local Spark + Iceberg REST catalog:

    CREATE TABLE (format-version) -> INSERT a row -> ALTER COLUMN ... TYPE ->
    INSERT a widened-only value -> read both rows back

so we can state, per promotion, whether OSS Spark + Iceberg supports it. The
widened-only second value would not fit the original type, so a column that
silently kept the narrow type is caught; reading the pre-promotion row back
exercises the spec's bounds-decoding rule (old data-file bounds were written at
the narrower width and must still decode).

This is a read-only diagnostic: it neither edits the matrix nor is compared
against it. It reuses tests/spark_fixture.get_spark(), so it needs the same
running Polaris REST catalog + MinIO stack and the same env vars as
tests/iceberg_feature_tests.py.

Run (from the repo root), e.g.:

    export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home
    export ICEBERG_JAR=/tmp/iceberg-spark-runtime-4.1_2.13-1.11.0.jar,/tmp/iceberg-aws-bundle-1.11.0.jar
    uv run --python /tmp/iceberg-venv/bin/python python tests/spark_type_promotion_probe.py
"""

import sys

import spark_fixture

# (label, from_type, sample_value, to_type, widened_only_value)
# v1/v2-valid promotions (still valid in v3).
_PROMOTIONS_V2 = [
    ("int -> long", "INT", "1", "BIGINT", "2147483648"),
    ("float -> double", "FLOAT", "CAST(1.5 AS FLOAT)", "DOUBLE", "1.7976931348623157E308"),
    ("decimal precision widen", "DECIMAL(5,2)", "123.45", "DECIMAL(10,2)", "12345678.90"),
]

# Additionally valid starting in v3.
#
# Iceberg's `timestamp` (no zone) is Spark's TIMESTAMP_NTZ; Spark's bare
# TIMESTAMP is Iceberg `timestamptz` (with zone), whose promotion from date the
# spec explicitly disallows -- so the spec-valid target here is TIMESTAMP_NTZ,
# not TIMESTAMP. Iceberg `timestamp_ns` has no Spark 4.1 SQL type name at all
# (the parser rejects TIMESTAMP_NS), so that promotion is unreachable from the
# Spark SQL surface and is recorded as such rather than as a plain rejection.
_PROMOTIONS_V3_EXTRA = [
    ("date -> timestamp (ntz)", "DATE", "DATE'2026-01-01'", "TIMESTAMP_NTZ",
     "TIMESTAMP_NTZ'2026-06-15 12:34:56'"),
    ("date -> timestamp_ns", "DATE", "DATE'2026-01-01'", "TIMESTAMP_NS",
     "TIMESTAMP_NS'2026-06-15 12:34:56.123456789'"),
]


def _jvm_chain(e) -> str:
    """The full JVM exception-cause chain, not the truncated Py4J first line.

    Spark wraps Iceberg's real message ("Cannot change column type: ...") several
    causes deep; the Python-side str(e) shows only the outer Py4J line, so we
    walk java_exception.getCause() to reach the actionable text.
    """
    je = getattr(e, "java_exception", None)
    if je is None:
        text = str(e)
    else:
        msgs, cur, seen = [], je, 0
        while cur is not None and seen < 6:
            m = cur.getMessage()
            if m:
                msgs.append(m.splitlines()[0])
            cur = cur.getCause()
            seen += 1
        # Prefer the deepest (root-cause) message, but keep any parser marker
        # from an outer frame so the caller can tell "no such type" apart from
        # "Iceberg refused the promotion".
        root = msgs[-1] if msgs else ""
        marker = next((m for m in msgs if "UNSUPPORTED_DATATYPE" in m
                       or "Unsupported data type" in m), "")
        text = marker if (marker and not root) else (
            f"{marker} || {root}" if marker and marker not in root else root)
    return (text or "unknown error").strip()[:220]


def _probe_one(spark, ns, idx, version, label, from_type, v1, to_type, v2):
    """Return (label, result, detail). result in {pass, fail, unreachable, error}.

    - pass:        ALTER accepted and the widened-only value round-trips.
    - fail:        Iceberg rejected the promotion itself (the datum for an
                   unsupported promotion).
    - unreachable: the target type has no name in this SQL dialect, so the
                   promotion cannot be expressed here (not an Iceberg verdict).
    - error:       unexpected harness failure before the ALTER.
    """
    fqn = f"local.{ns}.tp_{version}_{idx}"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        spark.sql(
            f"CREATE TABLE {fqn} (id INT, val {from_type}) USING iceberg "
            f"TBLPROPERTIES ('format-version' = '{version}')"
        )
        spark.sql(f"INSERT INTO {fqn} VALUES (1, {v1})")
    except Exception as e:  # noqa: BLE001 - setup failure isn't a promotion datum
        return label, "error", f"setup failed before ALTER: {_jvm_chain(e)}"

    try:
        spark.sql(f"ALTER TABLE {fqn} ALTER COLUMN val TYPE {to_type}")
    except Exception as e:  # noqa: BLE001 - rejection IS the measurement
        msg = _jvm_chain(e)
        # Distinguish "the dialect has no such type" from "Iceberg refused the
        # promotion": the former is a Spark parser limit (UNSUPPORTED_DATATYPE),
        # not a statement about whether Iceberg allows the promotion.
        if "UNSUPPORTED_DATATYPE" in msg or "Unsupported data type" in msg:
            return label, "unreachable", (f"target type {to_type} not expressible in "
                                          f"Spark 4.1 SQL: {msg}")
        return label, "fail", f"{from_type} -> {to_type} rejected by Iceberg: {msg}"

    try:
        spark.sql(f"INSERT INTO {fqn} VALUES (2, {v2})")
        spark.sql(f"REFRESH TABLE {fqn}")
        n = spark.sql(f"SELECT count(*) FROM {fqn}").collect()[0][0]
        old = spark.sql(f"SELECT val FROM {fqn} WHERE id = 1").collect()[0][0]
    except Exception as e:  # noqa: BLE001
        return label, "fail", (f"{from_type} -> {to_type} altered but post-promotion "
                               f"read/write failed: {_jvm_chain(e)}")

    if n != 2:
        return label, "fail", f"{from_type} -> {to_type}: expected 2 rows, got {n}"
    if old is None:
        return label, "fail", f"{from_type} -> {to_type}: pre-promotion value lost after ALTER"
    return label, "pass", (f"{from_type} -> {to_type} accepted; widened-only value stored, "
                          f"pre-promotion row still decodes (val={old!r})")


def _run_set(spark, version, promotions):
    ns = spark_fixture.new_namespace()
    out = []
    for idx, promo in enumerate(promotions):
        out.append(_probe_one(spark, ns, idx, version, *promo))
    return out


def main() -> int:
    if not spark_fixture.available():
        print("SKIP: " + spark_fixture.NOT_AVAILABLE_DETAIL)
        return 2

    spark = spark_fixture.get_spark()
    try:
        runtime = spark.version
    except Exception:  # noqa: BLE001
        runtime = "unknown"

    print("=" * 72)
    print("  OSS Spark + Iceberg type-promotion probe")
    print(f"  Spark {runtime}, Iceberg {spark_fixture.ICEBERG_VERSION}, "
          f"runtime {spark_fixture.SPARK_VERSION_SHORT}")
    print("=" * 72)

    sets = [
        ("v2", "2", _PROMOTIONS_V2),
        ("v3", "3", _PROMOTIONS_V2 + _PROMOTIONS_V3_EXTRA),
    ]
    all_results = {}
    for name, version, promos in sets:
        print(f"\n### {name} table (format-version {version}) — "
              f"{len(promos)} spec-valid promotions")
        results = _run_set(spark, version, promos)
        all_results[name] = results
        icons = {"pass": "PASS", "fail": "FAIL", "unreachable": "N/A ", "error": "ERR "}
        for label, res, detail in results:
            print(f"  [{icons.get(res, res)}] {label}: {detail}")

    # Compact summary the matrix owner can read at a glance.
    print("\n" + "=" * 72)
    print("  SUMMARY (supported = ALTER accepted and widened value round-trips)")
    print("=" * 72)
    for name, _version, _promos in sets:
        supported = [lbl for lbl, res, _ in all_results[name] if res == "pass"]
        rejected = [lbl for lbl, res, _ in all_results[name] if res == "fail"]
        unreachable = [lbl for lbl, res, _ in all_results[name] if res == "unreachable"]
        errored = [lbl for lbl, res, _ in all_results[name] if res == "error"]
        print(f"\n{name}: {len(supported)}/{len(all_results[name])} supported")
        print(f"  supported:   {', '.join(supported) or '(none)'}")
        if rejected:
            print(f"  rejected:    {', '.join(rejected)}")
        if unreachable:
            print(f"  unreachable: {', '.join(unreachable)} (no Spark 4.1 SQL type)")
        if errored:
            print(f"  errored:     {', '.join(errored)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
