# Flink image preloaded with the Iceberg Flink runtime and the jars it needs.
#
# Version pinning rationale (checked 2026-07):
#   - Flink 2.3.0 is the latest Flink release.
#   - Iceberg 1.11.0 is the latest Iceberg release and the first release where
#     the V3 spec is production-ready.
#   - Iceberg publishes no Flink 2.2/2.3 runtime yet, so the latest published
#     runtime (iceberg-flink-runtime-2.1) is used against the 2.3 engine.
#     ICEBERG_FLINK_MAJOR is the runtime's Flink minor and intentionally
#     differs from the engine version.
ARG FLINK_IMAGE=flink:2.3.0-scala_2.12-java17
FROM ${FLINK_IMAGE}

ARG ICEBERG_VERSION=1.11.0
ARG ICEBERG_FLINK_MAJOR=2.1
# Hadoop 3.x client jars, NOT flink-shaded-hadoop-2-uber. The shaded 2.8.3 uber
# jar is Hadoop 2.8, and Iceberg 1.11's Hadoop file IO calls
# FileSystem.openFile(), which only exists from Hadoop 3.3 on: with the old jar
# the Hadoop and JDBC catalogs write fine and then fail every read with
# NoSuchMethodError. (The REST catalog hides the problem because it goes through
# S3FileIO instead of the Hadoop FileSystem API.)
ARG HADOOP_VERSION=3.4.1
ARG POSTGRES_JDBC_VERSION=42.7.4

USER root

RUN set -eux; \
    if ! command -v curl >/dev/null 2>&1; then \
      apt-get update; \
      apt-get install -y --no-install-recommends curl ca-certificates; \
      rm -rf /var/lib/apt/lists/*; \
    fi

# Iceberg Flink runtime plus the AWS bundle that provides S3FileIO, so Iceberg
# reads/writes data files on S3 (MinIO). Iceberg's Flink catalog loader
# instantiates org.apache.hadoop.conf.Configuration regardless of catalog type,
# so Hadoop classes must also be on the classpath. The Postgres driver backs the
# JDBC catalog test against the stack's Postgres instance.
RUN set -eux; \
    APACHE=https://repo1.maven.org/maven2/org/apache; \
    curl -fsSL -o /opt/flink/lib/iceberg-flink-runtime-${ICEBERG_FLINK_MAJOR}-${ICEBERG_VERSION}.jar \
      "${APACHE}/iceberg/iceberg-flink-runtime-${ICEBERG_FLINK_MAJOR}/${ICEBERG_VERSION}/iceberg-flink-runtime-${ICEBERG_FLINK_MAJOR}-${ICEBERG_VERSION}.jar"; \
    curl -fsSL -o /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      "${APACHE}/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fsSL -o /opt/flink/lib/hadoop-client-api-${HADOOP_VERSION}.jar \
      "${APACHE}/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar"; \
    curl -fsSL -o /opt/flink/lib/hadoop-client-runtime-${HADOOP_VERSION}.jar \
      "${APACHE}/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar"; \
    curl -fsSL -o /opt/flink/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# SQL scripts written by the test harness land here via a bind mount, and the
# Hadoop-catalog test needs a writable warehouse directory.
RUN mkdir -p /work /tmp/hadoop-warehouse && chown -R flink:flink /work /tmp/hadoop-warehouse

USER flink
