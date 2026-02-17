import type { CompatibilityData, SupportEntry, Platform, Feature, Version } from "../types";
import featuresJson from "./features.json";
import aws from "./platforms/aws.json";
import awsTables from "./platforms/aws-tables.json";
import gcp from "./platforms/gcp.json";
import azure from "./platforms/azure.json";
import databricks from "./platforms/databricks.json";
import snowflake from "./platforms/snowflake.json";
import oss from "./platforms/oss.json";

interface VendorFile {
  platforms: unknown[];
  support: Record<string, unknown>;
}

const nonAwsVendors: VendorFile[] = [gcp, azure, databricks, snowflake, oss];

function mergeData(awsSource: VendorFile): CompatibilityData {
  const platforms: Platform[] = [];
  const support: Record<string, SupportEntry> = {};

  // AWS first, then the rest
  platforms.push(...(awsSource.platforms as Platform[]));
  Object.assign(support, awsSource.support as Record<string, SupportEntry>);

  for (const vendor of nonAwsVendors) {
    platforms.push(...(vendor.platforms as Platform[]));
    Object.assign(support, vendor.support as Record<string, SupportEntry>);
  }

  return {
    platforms,
    features: featuresJson.features as Feature[],
    versions: featuresJson.versions as Version[],
    support,
  };
}

export const data: CompatibilityData = mergeData(aws as VendorFile);
export const dataS3Tables: CompatibilityData = mergeData(awsTables as VendorFile);
