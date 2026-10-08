import type { CompatibilityData, Feature, Platform, SupportEntry, Version } from "../types";
import ingestionFeaturesJson from "./ingestion/features.json";

// Explicit static imports in a fixed order, mirroring load-data.ts and
// load-catalogs.ts: the merged platform order is the matrix column order, and
// the group-header row groups *consecutive* columns, so each group's tools must
// stay contiguous (AWS first, then 3rd Party).
import awsFirehose from "./ingestion/aws-firehose/aws-firehose.json";
import kafkaConnect from "./ingestion/kafka-connect/kafka-connect.json";

export interface IngestionFile {
  platforms: unknown[];
  support: Record<string, unknown>;
}

// Adding an ingestion tool: import its file above and slot it into the right
// group here (see the extension recipe in ingestion.test.ts: the tests catch a
// file that exists on disk but is missing from this list).
const ingestionFiles: IngestionFile[] = [awsFirehose, kafkaConnect];

// Pure merge, same shape as the other loaders: concatenate platforms in input
// order and union the support maps.
export function mergeIngestion(files: IngestionFile[]): CompatibilityData {
  const platforms: Platform[] = [];
  const support: Record<string, SupportEntry> = {};
  for (const file of files) {
    platforms.push(...(file.platforms as Platform[]));
    Object.assign(support, file.support as Record<string, SupportEntry>);
  }
  return {
    platforms,
    features: ingestionFeaturesJson.features as Feature[],
    versions: ingestionFeaturesJson.versions as Version[],
    support,
  };
}

export const dataIngestion: CompatibilityData = mergeIngestion(ingestionFiles);
