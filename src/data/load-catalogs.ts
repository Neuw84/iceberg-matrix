import type { CompatibilityData, Feature, Platform, SupportEntry, Version } from "../types";
import catalogFeaturesJson from "./catalogs/features.json";

// Explicit static imports in a fixed order, mirroring load-data.ts: the merged
// platform order is the matrix column order, and the group-header row groups
// *consecutive* columns, so each group's catalogs must stay contiguous.
//
// Order within each group is the source scorecard's overall score, descending.

// --- Proprietary / managed catalogs ---
import snowflakeHorizon from "./catalogs/snowflake-horizon/snowflake-horizon.json";
import awsGlueDataCatalog from "./catalogs/aws-glue-data-catalog/aws-glue-data-catalog.json";
import googleLakehouseRuntimeCatalog from "./catalogs/google-lakehouse-runtime-catalog/google-lakehouse-runtime-catalog.json";
import databricksUnity from "./catalogs/databricks-unity/databricks-unity.json";
import microsoftOnelake from "./catalogs/microsoft-onelake/microsoft-onelake.json";

// --- Open-source catalogs ---
import apachePolaris from "./catalogs/apache-polaris/apache-polaris.json";
import apacheGravitino from "./catalogs/apache-gravitino/apache-gravitino.json";
import lakekeeper from "./catalogs/lakekeeper/lakekeeper.json";
import projectNessie from "./catalogs/project-nessie/project-nessie.json";
import unityCatalogOss from "./catalogs/unity-catalog-oss/unity-catalog-oss.json";

export interface CatalogFile {
  platforms: unknown[];
  support: Record<string, unknown>;
}

// Adding a catalog: import its file above and slot it into the right group here
// (see the extension recipe in catalogs.test.ts — the tests catch a file that
// exists on disk but is missing from this list).
const catalogFiles: CatalogFile[] = [
  snowflakeHorizon,
  awsGlueDataCatalog,
  googleLakehouseRuntimeCatalog,
  databricksUnity,
  microsoftOnelake,
  apachePolaris,
  apacheGravitino,
  lakekeeper,
  projectNessie,
  unityCatalogOss,
];

// Pure merge, same shape as load-data.ts: concatenate platforms in input order
// and union the support maps.
export function mergeCatalogs(files: CatalogFile[]): CompatibilityData {
  const platforms: Platform[] = [];
  const support: Record<string, SupportEntry> = {};

  for (const file of files) {
    platforms.push(...(file.platforms as Platform[]));
    Object.assign(support, file.support as Record<string, SupportEntry>);
  }

  return {
    platforms,
    features: catalogFeaturesJson.features as Feature[],
    versions: catalogFeaturesJson.versions as Version[],
    support,
  };
}

export const dataCatalogs: CompatibilityData = mergeCatalogs(catalogFiles);
