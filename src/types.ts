// --- Data model types (match JSON schema) ---

export type PlatformGroup = "AWS" | "GCP" | "Azure" | "Databricks" | "Snowflake" | "3rd Party";

export interface Platform {
  id: string;
  name: string;
  vendor: string;
  category: "cloud" | "open-source";
  group: PlatformGroup;
  docUrl: string;
  /**
   * Optional grouping key. Platforms sharing the same `variantGroup` are
   * collapsed into a single matrix column with a toggle to switch between
   * them (e.g. OSS Spark: Vanilla / Gluten-Velox / Comet).
   */
  variantGroup?: string;
  /** Short label shown in the variant toggle (e.g. "Vanilla", "Gluten/Velox"). */
  variantLabel?: string;
}

export interface Feature {
  id: string;
  name: string;
  category: FeatureCategory;
  introducedIn: Version;
  description: string;
}

export type FeatureCategory =
  | "row-level-operations"
  | "partitioning"
  | "table-management"
  | "read-write"
  | "catalog-support"
  | "v3-data-types"
  | "v3-advanced";

export type Version = "v2" | "v3";

export type SupportLevel = "full" | "partial" | "none" | "unknown";

export interface SupportEntry {
  level: SupportLevel;
  notes: string;
  caveats: string[];
  links?: { label: string; url: string }[];
}

// Keyed as `${platformId}:${featureId}:${version}`
export interface CompatibilityData {
  platforms: Platform[];
  features: Feature[];
  versions: Version[];
  support: Record<string, SupportEntry>;
}

// --- UI state types ---

export interface FilterState {
  selectedVersions: Version[];
  selectedPlatforms: string[];
  selectedCategories: FeatureCategory[];
  selectedSupportLevels: SupportLevel[];
  searchQuery: string;
}

export type AwsS3Mode = "s3-buckets" | "s3-tables";

