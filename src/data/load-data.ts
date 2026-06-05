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
import snowflake from "./platforms/snowflake/snowflake/snowflake.json";
import duckdb from "./platforms/oss/duckdb/duckdb.json";
import clickhouse from "./platforms/oss/clickhouse/clickhouse.json";
import daft from "./platforms/oss/daft/daft.json";
import spark from "./platforms/oss/spark/spark.json";
import flink from "./platforms/oss/flink/flink.json";
import pyiceberg from "./platforms/oss/pyiceberg/pyiceberg.json";
import doris from "./platforms/oss/doris/doris.json";
import databend from "./platforms/oss/databend/databend.json";
// kafka-connect intentionally NOT imported (staged, excluded from app)

export interface EngineFile {
  platforms: unknown[];
  support: Record<string, unknown>;
}

// Non-AWS engines in the fixed merge order (gcp → azure → databricks → snowflake → oss).
const nonAwsEngines: EngineFile[] = [
  bigquery,
  dataproc,
  synapse,
  fabric,
  databricks,
  snowflake,
  duckdb,
  clickhouse,
  daft,
  spark,
  flink,
  pyiceberg,
  doris,
  databend,
];

const awsBucketsEngines: EngineFile[] = [bAthena, bEmr, bGlue, bManagedFlink, bRedshift];
const awsTablesEngines: EngineFile[] = [tAthena, tEmr, tGlue, tManagedFlink, tRedshift];

// Pure merge: concatenate platforms in input order (AWS engines first, then
// non-AWS), and union support maps in input order.
export function mergeEngines(awsEngines: EngineFile[]): CompatibilityData {
  const platforms: Platform[] = [];
  const support: Record<string, SupportEntry> = {};

  for (const engine of [...awsEngines, ...nonAwsEngines]) {
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

export const data: CompatibilityData = mergeEngines(awsBucketsEngines);
export const dataS3Tables: CompatibilityData = mergeEngines(awsTablesEngines);
