import type {
  AwsS3Mode,
  FeatureCategory,
  FilterState,
  SnowflakeStorageMode,
  SupportLevel,
  Version,
  ViewMode,
} from "../types";

/**
 * The shareable part of the app state, kept in the query string so a link
 * opens the same view: `?view=engines&p=duckdb,trino&v=v3&q=bloom`. Only
 * values that differ from the defaults are written, so the plain URL stays
 * clean. Filters belong to the view in the URL; the other views keep theirs.
 */
export interface UrlState {
  view: ViewMode;
  filters: Partial<FilterState>;
  awsS3Mode?: AwsS3Mode;
  snowflakeMode?: SnowflakeStorageMode;
}

const VIEWS: ViewMode[] = ["engines", "ingestion", "catalogs"];
const SPEC_VERSIONS: Version[] = ["v2", "v3"];
const LEVELS: SupportLevel[] = ["full", "partial", "none", "unknown"];

const list = (value: string | null) =>
  value ? value.split(",").map((s) => s.trim()).filter(Boolean) : [];

/**
 * Read the state a URL asks for. Values that cannot exist are dropped here;
 * platform and category ids depend on the view's dataset, so App drops
 * unknown ones once that data has loaded.
 */
export function parseUrlState(search: string): UrlState {
  const params = new URLSearchParams(search);
  const viewParam = params.get("view") as ViewMode | null;
  const view = viewParam && VIEWS.includes(viewParam) ? viewParam : "engines";
  const filters: Partial<FilterState> = {};

  const platforms = list(params.get("p"));
  if (platforms.length) filters.selectedPlatforms = platforms;
  const categories = list(params.get("cat"));
  if (categories.length) filters.selectedCategories = categories as FeatureCategory[];
  const levels = list(params.get("s")).filter((l): l is SupportLevel =>
    LEVELS.includes(l as SupportLevel),
  );
  if (levels.length) filters.selectedSupportLevels = levels;
  const query = params.get("q");
  if (query) filters.searchQuery = query;

  // Catalogs have the single synthetic version "current" and no Compare.
  if (view !== "catalogs") {
    const versions = list(params.get("v")).filter((v): v is Version =>
      SPEC_VERSIONS.includes(v as Version),
    );
    if (versions.length) filters.selectedVersions = versions;
    if (params.get("compare") === "1" && (filters.selectedVersions ?? SPEC_VERSIONS).length > 1) {
      filters.compareMode = true;
    }
  }

  const state: UrlState = { view, filters };
  // The storage switches are engines-view columns.
  if (view === "engines") {
    if (params.get("aws") === "s3-tables") state.awsS3Mode = "s3-tables";
    if (params.get("sf") === "external") state.snowflakeMode = "external";
  }
  return state;
}

/** The query string (with "?", or "" for all defaults) for the given state. */
export function toUrlSearch(
  view: ViewMode,
  filters: FilterState,
  defaults: FilterState,
  awsS3Mode: AwsS3Mode,
  snowflakeMode: SnowflakeStorageMode,
): string {
  const params = new URLSearchParams();
  if (view !== "engines") params.set("view", view);
  if (filters.selectedPlatforms.length) params.set("p", filters.selectedPlatforms.join(","));
  if (filters.selectedCategories.length) params.set("cat", filters.selectedCategories.join(","));
  if (filters.selectedSupportLevels.length) params.set("s", filters.selectedSupportLevels.join(","));
  if (filters.searchQuery.trim()) params.set("q", filters.searchQuery.trim());
  if (view !== "catalogs") {
    if (filters.selectedVersions.join(",") !== defaults.selectedVersions.join(",")) {
      params.set("v", filters.selectedVersions.join(","));
    }
    if (filters.compareMode) params.set("compare", "1");
  }
  if (view === "engines") {
    if (awsS3Mode === "s3-tables") params.set("aws", "s3-tables");
    if (snowflakeMode === "external") params.set("sf", "external");
  }
  // Ids are plain [a-z0-9-], so keep the commas readable instead of %2C.
  const search = params.toString().replace(/%2C/g, ",");
  return search ? `?${search}` : "";
}
