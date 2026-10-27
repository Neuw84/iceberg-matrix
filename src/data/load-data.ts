import type {
  AwsS3Mode,
  CompatibilityData,
  Feature,
  Platform,
  SnowflakeStorageMode,
  SupportEntry,
  Version,
} from "../types";
import featuresJson from "./features.json";
// Which features each engine feature suite tests (see ./test-coverage.ts).
import testCoverage from "virtual:test-coverage";

// --- AWS S3 buckets engines (order: athena, emr, glue, managed-flink, redshift-s3) ---
import bAthena from "./platforms/aws/s3buckets/athena/athena.json";
import bEmr from "./platforms/aws/s3buckets/emr/emr.json";
import bGlue from "./platforms/aws/s3buckets/glue/glue.json";
import bManagedFlink from "./platforms/aws/s3buckets/managed-flink/managed-flink.json";
import bRedshift from "./platforms/aws/s3buckets/redshift-s3/redshift-s3.json";
import bAurora from "./platforms/aws/s3buckets/aurora/aurora.json";


// --- Non-AWS vendors (order: gcp, azure, databricks, snowflake, oss) ---
import bigquery from "./platforms/gcp/bigquery/bigquery.json";
import dataproc from "./platforms/gcp/dataproc/dataproc.json";
// synapse removed (Microsoft has Synapse Spark in maintenance/LTS and directs
// workloads to Fabric Runtime 2.0, which the azure-fabric row covers)
import fabric from "./platforms/azure/fabric/fabric.json";
import databricks from "./platforms/databricks/databricks/databricks.json";
// Snowflake has two storage modes, mirroring the AWS s3buckets/s3tables split:
// "managed" (Snowflake-provided storage, EXTERNAL_VOLUME = SNOWFLAKE_MANAGED)
// and "external" (customer cloud storage through an external volume). Both
// files carry the same platform id ("snowflake") so filters survive the toggle.
// Only "managed" is imported here; "external" loads on demand.
import snowflakeManaged from "./platforms/snowflake/managed/snowflake/snowflake.json";
import dremio from "./platforms/dremio/dremio/dremio.json";
import starburst from "./platforms/starburst/starburst/starburst.json";
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
import trino from "./platforms/oss/trino/trino.json";
import oracle26ai from "./platforms/oracle/oracle-26ai/oracle-26ai.json";
// --- Alibaba Cloud engines (own group, between Snowflake and OSS/3rd Party) ---
import aliEmrSpark from "./platforms/alibaba/emr-spark/emr-spark.json";
import aliMaxCompute from "./platforms/alibaba/maxcompute/maxcompute.json";
import aliHologres from "./platforms/alibaba/hologres/hologres.json";
import aliRealtimeFlink from "./platforms/alibaba/realtime-flink/realtime-flink.json";

export interface EngineFile {
  platforms: unknown[];
  support: Record<string, unknown>;
}

// Non-AWS engines before the Snowflake slot (gcp → azure → databricks).
const preSnowflakeEngines: EngineFile[] = [
  bigquery,
  dataproc,
  fabric,
  databricks,
];

// Alibaba Cloud engines, rendered as their own group right after Snowflake.
const alibabaEngines: EngineFile[] = [
  aliEmrSpark,
  aliMaxCompute,
  aliHologres,
  aliRealtimeFlink,
];

// Non-AWS engines after the Alibaba Cloud block (oss / 3rd party).
const postSnowflakeEngines: EngineFile[] = [
  duckdb,
  clickhouse,
  daft,
  spark,
  sparkGluten,
  sparkComet,
  flink,
  starburst,
  trino,
  pyiceberg,
  doris,
  databend,
  oracle26ai,
];

const awsBucketsEngines: EngineFile[] = [bAthena, bEmr, bGlue, bManagedFlink, bRedshift, bAurora];

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
    dremio,
    ...alibabaEngines,
    ...postSnowflakeEngines,
  ]) {
    platforms.push(...(engine.platforms as Platform[]));
    Object.assign(support, engine.support as Record<string, SupportEntry>);
  }

  // Mark the cells a suite tests. Copies, so the imported engine JSON stays
  // untouched across merges.
  for (const suite of testCoverage) {
    for (const { feature, version } of suite.features) {
      for (const v of version ? [version] : suite.versions) {
        const key = `${suite.platform}:${feature}:${v}`;
        if (support[key]) support[key] = { ...support[key], testedBy: suite.file, testedOn: suite.runs };
      }
    }
  }

  return {
    platforms,
    features: featuresJson.features as Feature[],
    versions: featuresJson.versions as Version[],
    support,
  };
}

// The default dataset (S3 buckets, Snowflake managed) is built eagerly: it is
// what every visitor sees first, so it ships in the first-load bundle and the
// page renders without a loading step.
export const data: CompatibilityData = mergeEngines(awsBucketsEngines, snowflakeManaged);

// The other three (AWS mode x Snowflake mode) combinations need the S3 Tables
// files and/or the Snowflake external file. Those live in their own modules and
// are fetched on demand, so a visitor who never flips either toggle never
// downloads them. Merged datasets are cached, which keeps each one a stable
// object identity so React memoization keyed on the dataset survives toggles.
const engineCache = new Map<string, CompatibilityData>([["s3-buckets|snowflake", data]]);
const keyOf = (aws: AwsS3Mode, snowflake: SnowflakeStorageMode) => `${aws}|${snowflake}`;

let awsTablesLoad: Promise<EngineFile[]> | undefined;
function loadAwsTables(): Promise<EngineFile[]> {
  awsTablesLoad ??= import("./engine-variants/aws-s3tables")
    .then((m) => m.awsTablesEngines)
    .catch((err) => {
      awsTablesLoad = undefined; // allow a retry after a failed fetch
      throw err;
    });
  return awsTablesLoad;
}

let snowflakeExternalLoad: Promise<EngineFile> | undefined;
function loadSnowflakeExternal(): Promise<EngineFile> {
  snowflakeExternalLoad ??= import("./engine-variants/snowflake-external")
    .then((m) => m.snowflakeExternal)
    .catch((err) => {
      snowflakeExternalLoad = undefined;
      throw err;
    });
  return snowflakeExternalLoad;
}

/** The engines dataset for a mode pair if it is already loaded, else undefined. */
export function peekEngineData(
  awsS3Mode: AwsS3Mode,
  snowflakeMode: SnowflakeStorageMode,
): CompatibilityData | undefined {
  return engineCache.get(keyOf(awsS3Mode, snowflakeMode));
}

/** Load (once) and return the engines dataset for an (AWS mode, Snowflake mode) pair. */
export async function loadEngineData(
  awsS3Mode: AwsS3Mode,
  snowflakeMode: SnowflakeStorageMode,
): Promise<CompatibilityData> {
  const key = keyOf(awsS3Mode, snowflakeMode);
  const cached = engineCache.get(key);
  if (cached) return cached;
  const [awsEngines, snowflakeEngine] = await Promise.all([
    awsS3Mode === "s3-tables" ? loadAwsTables() : Promise.resolve(awsBucketsEngines),
    snowflakeMode === "external" ? loadSnowflakeExternal() : Promise.resolve(snowflakeManaged),
  ]);
  // Another caller may have finished the same merge while we awaited.
  const merged = engineCache.get(key) ?? mergeEngines(awsEngines, snowflakeEngine);
  engineCache.set(key, merged);
  return merged;
}
