import type { CompatibilityData, SupportEntry, Platform, Feature, Version } from "../types";
import featuresJson from "./features.json";
import aws from "./platforms/aws.json";
import gcp from "./platforms/gcp.json";
import azure from "./platforms/azure.json";
import databricks from "./platforms/databricks.json";
import snowflake from "./platforms/snowflake.json";
import oss from "./platforms/oss.json";

interface VendorFile {
  platforms: unknown[];
  support: Record<string, unknown>;
}

const vendorFiles: VendorFile[] = [aws, gcp, azure, databricks, snowflake, oss];

function mergeData(): CompatibilityData {
  const platforms: Platform[] = [];
  const support: Record<string, SupportEntry> = {};

  for (const vendor of vendorFiles) {
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

export const data: CompatibilityData = mergeData();
