import type { CompatibilityData, SupportEntry, Platform, Feature, Version } from "../types";
import featuresJson from "./features.json";

// --- AWS S3 buckets engines (order: athena, emr, glue, managed-flink, redshift-s3) ---
import bAthena from "./platforms/aws/s3buckets/athena/athena.json";
import bEmr from "./platforms/aws/s3buckets/emr/emr.json";
import bGlue from "./platforms/aws/s3buckets/glue/glue.json";
import bManagedFlink from "./platforms/aws/s3buckets/managed-flink/managed-flink.json";
import bRedshift from "./platforms/aws/s3buckets/redshift-s3/redshift-s3.json";
// firehose intentionally NOT imported (staged, excluded from app)

// --- AWS S3 tables engines (same order) ---
import tAthena from "./platforms/aws/s3tables/athena/athena.json";
import tEmr from "./platforms/aws/s3tables/emr/emr.json";
import tGlue from "./platforms/aws/s3tables/glue/glue.json";
import tManagedFlink from "./platforms/aws/s3tables/managed-flink/managed-flink.json";
import tRedshift from "./platforms/aws/s3tables/redshift-s3/redshift-s3.json";
// firehose intentionally NOT imported (staged, excluded from app)

// --- Non-AWS vendors (order: gcp, azure, databricks, snowflake, oss) ---
import bigquery from "./platforms/gcp/bigquery/bigquery.json";
import dataproc from "./platforms/gcp/dataproc/dataproc.json";
import synapse from "./platforms/azure/synapse/synapse.json";
import fabric from "./platforms/azure/fabric/fabric.json";
import databricks from "./platforms/databricks/databricks/databricks.json";
// Snowflake has two storage modes, mirroring the AWS s3buckets/s3tables split:
// "managed" (Snowflake-provided storage, EXTERNAL_VOLUME = SNOWFLAKE_MANAGED)
// and "external" (customer cloud storage through an external volume). Both
// files carry the same platform id ("snowflake") so filters survive the toggle.
import snowflakeManaged from "./platforms/snowflake/managed/snowflake/snowflake.json";
import snowflakeExternal from "./platforms/snowflake/external/snowflake/snowflake.json";
import duckdb from "./platforms/oss/duckdb/duckdb.json";
import clickhouse from "./platforms/oss/clickhouse/clickhouse.json";
import daft from "./platforms/oss/daft/daft.json";
import spark from "./platforms/oss/spark/spark.json";
import sparkGluten from "./platforms/oss/spark-gluten/spark-gluten.json";
import sparkComet from "./platforms/oss/spark-comet/spark-comet.json";
import flink from "./platforms/oss/flink/flink.json";
import pyiceberg from "./platforms/oss/pyiceberg/pyiceberg.json";
import doris from "./platforms/oss/doris/doris.json";
import databend from "./platforms/oss/databend/databend.json";
// kafka-connect intentionally NOT imported (staged, excluded from app)

export interface EngineFile {
  platforms: unknown[];
  support: Record<string, unknown>;
}

// Non-AWS engines before the Snowflake slot (gcp → azure → databricks).
const preSnowflakeEngines: EngineFile[] = [
  bigquery,
  dataproc,
  synapse,
  fabric,
  databricks,
];

// Non-AWS engines after the Snowflake slot (oss).
const postSnowflakeEngines: EngineFile[] = [
  duckdb,
  clickhouse,
  daft,
  spark,
  sparkGluten,
  sparkComet,
  flink,
  pyiceberg,
  doris,
  databend,
];

const awsBucketsEngines: EngineFile[] = [bAthena, bEmr, bGlue, bManagedFlink, bRedshift];
const awsTablesEngines: EngineFile[] = [tAthena, tEmr, tGlue, tManagedFlink, tRedshift];

// Pure merge: concatenate platforms in input order (AWS engines first, then
// non-AWS with the mode-selected Snowflake file in its fixed slot between
// databricks and the OSS block), and union support maps in input order.
export function mergeEngines(
  awsEngines: EngineFile[],
  snowflakeEngine: EngineFile,
): CompatibilityData {
  const platforms: Platform[] = [];
  const support: Record<string, SupportEntry> = {};

  for (const engine of [
    ...awsEngines,
    ...preSnowflakeEngines,
    snowflakeEngine,
    ...postSnowflakeEngines,
  ]) {
    platforms.push(...(engine.platforms as Platform[]));
    Object.assign(support, engine.support as Record<string, SupportEntry>);
  }

  return {
    platforms,
    features: featuresJson.features as Feature[],
    versions: featuresJson.versions as Version[],
    support,
  };
}

// All four AWS-mode x Snowflake-mode combinations are precomputed: the merge is
// a cheap concat and this keeps every dataset a stable object identity, so
// React memoization keyed on the dataset keeps working across toggles.
export const data: CompatibilityData = mergeEngines(awsBucketsEngines, snowflakeManaged);
export const dataS3Tables: CompatibilityData = mergeEngines(awsTablesEngines, snowflakeManaged);
export const dataSnowflakeExternal: CompatibilityData = mergeEngines(
  awsBucketsEngines,
  snowflakeExternal,
);
export const dataS3TablesSnowflakeExternal: CompatibilityData = mergeEngines(
  awsTablesEngines,
  snowflakeExternal,
);

/** Select the engines dataset for an (AWS mode, Snowflake mode) pair. */
export function getEngineData(
  awsS3Mode: "s3-buckets" | "s3-tables",
  snowflakeMode: "snowflake" | "external",
): CompatibilityData {
  if (awsS3Mode === "s3-tables") {
    return snowflakeMode === "external" ? dataS3TablesSnowflakeExternal : dataS3Tables;
  }
  return snowflakeMode === "external" ? dataSnowflakeExternal : data;
}
