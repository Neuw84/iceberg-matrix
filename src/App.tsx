import { useState, useMemo, useEffect, lazy, Suspense } from "react";
import type { AwsS3Mode, FilterState, SnowflakeStorageMode, ViewMode } from "./types";
import { preloadView } from "./data/view-data";
import { useViewData } from "./hooks/useViewData";
import { FilterPanel } from "./components/FilterPanel";
import { ViewToggle } from "./components/ViewToggle";
import { CompatibilityMatrix } from "./components/CompatibilityMatrix";
import { applyFilters } from "./utils/filters";
import { parseUrlState, toUrlSearch } from "./utils/url-state";

// Code-split the comparison view: it is only rendered in compare mode, so keep
// it out of the initial bundle.
const ComparisonSummary = lazy(() =>
  import("./components/ComparisonSummary").then((m) => ({
    default: m.ComparisonSummary,
  })),
);

const initialEngineFilters: FilterState = {
  // Both spec versions are shown by default (a column per version per
  // platform); the comparison summary stays off until Compare is toggled on.
  selectedVersions: ["v2", "v3"],
  compareMode: false,
  selectedPlatforms: [],
  selectedCategories: [],
  selectedSupportLevels: [],
  searchQuery: "",
};

// The catalogs dataset has the single synthetic version "current" (the
// openness rubric carries no v2/v3 dimension), so its version selection never
// changes and no version tabs are rendered for it.
const initialCatalogFilters: FilterState = {
  selectedVersions: ["current"],
  compareMode: false,
  selectedPlatforms: [],
  selectedCategories: [],
  selectedSupportLevels: [],
  searchQuery: "",
};

// Ingestion tools (Kafka Connect, Firehose, OLake Go, Fivetran Managed Data Lake) have the same v2/v3 spec dimension
// as engines, so the version chips and Compare work the same way; only the
// platform set and the reduced, write-oriented feature list differ.
const initialIngestionFilters: FilterState = {
  selectedVersions: ["v2", "v3"],
  compareMode: false,
  selectedPlatforms: [],
  selectedCategories: [],
  selectedSupportLevels: [],
  searchQuery: "",
};

const initialFilters: Record<ViewMode, FilterState> = {
  engines: initialEngineFilters,
  catalogs: initialCatalogFilters,
  ingestion: initialIngestionFilters,
};

export default function App() {
  // A shared link opens the view and filters in its query string. Read once;
  // after that the URL follows the state (see the effect below).
  const [fromUrl] = useState(() => parseUrlState(window.location.search));
  const startFilters = (view: ViewMode) =>
    fromUrl.view === view ? { ...initialFilters[view], ...fromUrl.filters } : initialFilters[view];
  const [viewMode, setViewMode] = useState<ViewMode>(fromUrl.view);
  // One filter state per view: platform ids and versions are disjoint between
  // the two datasets, so sharing state would leak stale selections across the
  // toggle. Keeping both also restores your filters when you switch back.
  const [engineFilters, setEngineFilters] = useState<FilterState>(() => startFilters("engines"));
  const [catalogFilters, setCatalogFilters] = useState<FilterState>(() => startFilters("catalogs"));
  const [ingestionFilters, setIngestionFilters] = useState<FilterState>(() => startFilters("ingestion"));
  const [introOpen, setIntroOpen] = useState(false);
  const [awsS3Mode, setAwsS3Mode] = useState<AwsS3Mode>(fromUrl.awsS3Mode ?? "s3-buckets");
  // Snowflake defaults to its managed storage ("snowflake"), which the docs
  // recommend; "external" swaps in the external-volume (customer S3) data.
  const [snowflakeMode, setSnowflakeMode] = useState<SnowflakeStorageMode>(
    fromUrl.snowflakeMode ?? "snowflake",
  );

  const isCatalogsView = viewMode === "catalogs";
  const isIngestionView = viewMode === "ingestion";
  const isEnginesView = viewMode === "engines";
  // The default engines dataset is bundled with the app; Catalogs, Ingestion and
  // the S3 Tables / Snowflake external variants load on demand. While a variant
  // loads, the previous table of the same view stays on screen.
  const { data: activeData, loading: dataLoading, error: dataError, retry } = useViewData(
    viewMode,
    awsS3Mode,
    snowflakeMode,
  );
  const storedFilters = isCatalogsView
    ? catalogFilters
    : isIngestionView
      ? ingestionFilters
      : engineFilters;
  // A link can name platforms or categories this dataset does not have (a
  // renamed engine, a typo); ignore them rather than show an empty matrix. The
  // next filter change stores the cleaned selection.
  const filters = useMemo(() => {
    if (!activeData) return storedFilters;
    const platformIds = new Set(activeData.platforms.map((p) => p.id));
    const categories = new Set(activeData.features.map((f) => f.category));
    const selectedPlatforms = storedFilters.selectedPlatforms.filter((id) => platformIds.has(id));
    const selectedCategories = storedFilters.selectedCategories.filter((c) => categories.has(c));
    return selectedPlatforms.length === storedFilters.selectedPlatforms.length &&
      selectedCategories.length === storedFilters.selectedCategories.length
      ? storedFilters
      : { ...storedFilters, selectedPlatforms, selectedCategories };
  }, [activeData, storedFilters]);
  const setFilters = isCatalogsView
    ? setCatalogFilters
    : isIngestionView
      ? setIngestionFilters
      : setEngineFilters;

  const { platforms, features } = useMemo(
    () =>
      activeData
        ? applyFilters(activeData, filters)
        : { platforms: [], features: [] },
    [activeData, filters],
  );
  // The comparison summary is an explicit opt-in (the Compare toggle), not an
  // automatic consequence of selecting both versions. It still requires two
  // versions to compare, and never applies to the catalogs view.
  // Keep the address bar shareable. replaceState, not pushState: a filter
  // click is not a page the Back button should return to.
  useEffect(() => {
    const search = toUrlSearch(viewMode, filters, initialFilters[viewMode], awsS3Mode, snowflakeMode);
    if (search !== window.location.search) {
      window.history.replaceState(
        window.history.state,
        "",
        `${window.location.pathname}${search}${window.location.hash}`,
      );
    }
  }, [viewMode, filters, awsS3Mode, snowflakeMode]);

  const isCompareMode =
    !isCatalogsView && filters.compareMode && filters.selectedVersions.length > 1;

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-[1800px] mx-auto px-4 py-3 sm:px-6">
          <div className="flex items-center justify-between flex-wrap gap-3">
            <div className="flex items-center gap-3">
              <img
                src="/iceberg-logo.svg"
                alt="Apache Iceberg logo"
                width={36}
                height={36}
                // The one image that is always visible on first paint.
                fetchPriority="high"
                className="h-9 w-9"
              />
              <div>
                <h1 className="text-lg font-bold text-gray-900 leading-tight">
                  Apache Iceberg™ Compatibility Matrix
                </h1>
                <p className="text-xs text-gray-500 mt-0.5">
                  Community driven feature support across platforms, engines and catalogs
                </p>
              </div>
            </div>
            <div className="flex items-center gap-3 flex-wrap">
              {/* Version selection moved into the FilterPanel (multi-select
                  chips). The header keeps only the Engines/Catalogs toggle. */}
              <ViewToggle mode={viewMode} onChange={setViewMode} onPreload={preloadView} />
            </div>
          </div>
        </div>
      </header>

      <main className="w-full px-4 py-3 sm:px-6">
        {/* Text/controls stay at a readable width; the matrix below goes full-width */}
        <div className="max-w-[1800px] mx-auto">
        {/* Collapsible intro */}
        <div className="mb-3">
          <button
            type="button"
            onClick={() => setIntroOpen(!introOpen)}
            className="flex items-center gap-2 text-xs text-blue-600 hover:text-blue-800 cursor-pointer transition-colors"
          >
            <svg
              className={`w-3.5 h-3.5 transition-transform duration-200 ${introOpen ? "rotate-90" : ""}`}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
            </svg>
            {introOpen ? "Hide" : "About this matrix"}
          </button>
          {introOpen && (
            <div className="mt-2 bg-blue-50 border border-blue-200 rounded-lg p-3 text-xs text-blue-800 space-y-2 collapsible-content">
              {isIngestionView ? (
                <>
                  <p>
                    Ingestion tools write data into Iceberg tables from a source
                    such as a Kafka topic or a Kinesis stream, but are not built
                    for querying it. This view compares what they can write:
                    appends, upserts and deletes, schema handling, partitioning,
                    catalogs and V3 types. Read-oriented features (read support,
                    time travel, statistics) are left out because they do not
                    apply to a write-only tool.
                  </p>
                </>
              ) : isCatalogsView ? (
                <>
                  <p>
                    The table-format layer of Iceberg is an open specification, so
                    interoperability now lives or dies at the catalog: can
                    non-vendor engines read and write your tables over the Iceberg
                    REST Catalog (IRC) API, with short-lived vended credentials?
                    This view scores eleven catalogs against that openness rubric,
                    plus spec-compliance facts: Iceberg REST spec coverage and
                    table-format v2/v3 support.
                  </p>
                </>
              ) : (
                <>
                  <p>
                    One of Apache Iceberg's core promises is interoperability: because
                    the table format is an open specification, data written by one
                    engine can be read by any other engine that implements the spec.
                    A Spark job can write data that Trino, Flink, Athena, or Snowflake
                    can query without conversion or migration.
                  </p>
                  <p>
                    In practice, each engine implements the specification independently,
                    so feature support can vary. This matrix tracks the current state
                    of those implementations to help you make informed decisions.
                  </p>
                </>
              )}
            </div>
          )}
        </div>

        {activeData && (
          <div className="mb-3">
            <FilterPanel
              filters={filters}
              data={activeData}
              onFilterChange={setFilters}
              entityLabel={isCatalogsView ? "Catalogs" : isIngestionView ? "Tools" : "Platforms"}
            />
          </div>
        )}
        </div>
        {!activeData && (
          <div
            className="mb-3 rounded-lg border border-gray-200 bg-white p-6 text-center text-sm text-gray-500"
            role="status"
            aria-live="polite"
          >
            {dataError ? (
              <>
                <p className="text-red-600">Could not load this view's data.</p>
                <button
                  type="button"
                  onClick={retry}
                  className="mt-2 rounded-md bg-blue-600 px-3 py-1 text-xs font-semibold text-white hover:bg-blue-700 cursor-pointer"
                >
                  Retry
                </button>
              </>
            ) : (
              "Loading…"
            )}
          </div>
        )}

        {activeData && isCompareMode && (
          <Suspense fallback={null}>
            <ComparisonSummary
              data={activeData}
              platforms={platforms}
              features={features}
              versions={filters.selectedVersions}
            />
          </Suspense>
        )}

        {activeData && (
          <div aria-busy={dataLoading} className={dataLoading ? "opacity-70 transition-opacity" : undefined}>
            <CompatibilityMatrix
              data={activeData}
              filters={filters}
              // The per-vendor storage switches (AWS S3 Buckets/Tables, Snowflake
              // managed/external) are engines-view concerns; without these props
              // the matrix renders no toggles.
              awsS3Mode={isEnginesView ? awsS3Mode : undefined}
              onAwsS3ModeChange={isEnginesView ? setAwsS3Mode : undefined}
              snowflakeMode={isEnginesView ? snowflakeMode : undefined}
              onSnowflakeModeChange={isEnginesView ? setSnowflakeMode : undefined}
            />
          </div>
        )}
      </main>

      <footer className="border-t border-gray-200 bg-white px-4 py-3 sm:px-6 mt-6">
        <div className="max-w-[1800px] mx-auto text-xs text-gray-600 space-y-1">
          <p>
            All product names, logos, and brands mentioned on this site are the property of their respective owners.
            All company, product, and service names used are for identification purposes only.
            Use of these names, logos, and brands does not imply endorsement or affiliation.
          </p>
          <p>
            This is an open-source project — contributions are welcome!{" "}
            <a
              href="https://github.com/Neuw84/iceberg-matrix"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-700 hover:text-blue-900 underline"
            >
              GitHub repo
            </a>
          </p>
        </div>
      </footer>
    </div>
  );
}
