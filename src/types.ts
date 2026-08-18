// --- Data model types (match JSON schema) ---

/**
 * Column-group header a platform renders under. The vendor groups belong to the
 * Engines view; "Proprietary" and "Open Source" are the two groups of the
 * Catalogs view. The matrix groups *consecutive* columns, so datasets must keep
 * each group's platforms contiguous.
 */
export type PlatformGroup =
  | "AWS"
  | "GCP"
  | "Azure"
  | "Databricks"
  | "Snowflake"
  | "3rd Party"
  | "Proprietary"
  | "Open Source";

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
  | "v3-advanced"
  // Catalogs view categories: spec-compliance facts (REST spec, format v2/v3)
  // and the openness rubric from "Iceberg: The State of Catalogs".
  | "spec-support"
  | "openness-rubric";

/**
 * "v2" and "v3" are the Iceberg spec versions of the Engines view. "current" is
 * the Catalogs view's single synthetic version: the openness rubric has no spec
 * dimension, but the support-key format is `${platformId}:${featureId}:${version}`
 * everywhere, so the catalogs dataset hangs off one version key.
 */
export type Version = "v2" | "v3" | "current";

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

/**
 * Which matrix the app is showing: query engines against Iceberg features
 * (the default), or Iceberg catalogs against the openness rubric.
 */
export type ViewMode = "engines" | "catalogs";

export interface FilterState {
  selectedVersions: Version[];
  selectedPlatforms: string[];
  selectedCategories: FeatureCategory[];
  selectedSupportLevels: SupportLevel[];
  searchQuery: string;
}

export type AwsS3Mode = "s3-buckets" | "s3-tables";

