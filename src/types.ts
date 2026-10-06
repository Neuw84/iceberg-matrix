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
  /**
   * The spec version a feature first appeared in. By default a feature is taken
   * to apply to this version and every later one in the dataset's `versions`
   * list, which is what drives the per-row version badge (e.g. a v2 feature
   * reads "V2/V3").
   */
  introducedIn: Version;
  /**
   * Optional: the spec versions a feature is available in, listed explicitly.
   * Overrides the "introducedIn onward" default. Use this for a feature that is
   * NOT carried forward unchanged — e.g. once V4 lands, equality deletes being
   * V2/V3-only would be `availableVersions: ["v2", "v3"]` with
   * `introducedIn: "v2"`, so the badge stays "V2/V3" instead of gaining "V4".
   * Values should be a subset of the dataset's `versions`, in spec order.
   */
  availableVersions?: Version[];
  /**
   * Optional: the first spec version in which a feature is NO LONGER available
   * (exclusive upper bound). A convenience alternative to `availableVersions`
   * for the common "dropped from version X on" case; ignored when
   * `availableVersions` is set. E.g. `introducedIn: "v2", removedIn: "v4"`
   * yields V2 and V3.
   */
  removedIn?: Version;
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
  /**
   * Whether the version comparison summary is shown. Independent of
   * `selectedVersions`: having both versions selected shows a column per
   * version in the grid, but the comparison summary panel appears only when
   * this is also true. Only meaningful with two or more versions selected;
   * it is cleared automatically when the selection drops to a single version.
   */
  compareMode: boolean;
  selectedPlatforms: string[];
  selectedCategories: FeatureCategory[];
  selectedSupportLevels: SupportLevel[];
  searchQuery: string;
}

export type AwsS3Mode = "s3-buckets" | "s3-tables";

/**
 * Snowflake Iceberg storage mode: "snowflake" is Snowflake-provided storage
 * (EXTERNAL_VOLUME = SNOWFLAKE_MANAGED, zero setup, Snowflake catalog only);
 * "external" keeps table files in customer cloud storage through an external
 * volume (required for external Iceberg catalogs).
 */
export type SnowflakeStorageMode = "snowflake" | "external";

