import type {
  CompatibilityData,
  FeatureCategory,
  FilterState,
  PlatformGroup,
  SupportLevel,
} from "../types";

const CATEGORY_LABELS: Record<FeatureCategory, string> = {
  "row-level-operations": "Row-Level Ops",
  partitioning: "Partitioning",
  "table-management": "Table Mgmt",
  "read-write": "Read / Write",
  "catalog-support": "Catalogs",
  "v3-data-types": "V3 Types",
  "v3-advanced": "V3 Advanced",
};

const SUPPORT_LEVELS: SupportLevel[] = ["full", "partial", "none", "unknown"];

const SUPPORT_COLORS: Record<SupportLevel, { active: string; inactive: string }> = {
  full: { active: "bg-green-600 text-white border-green-600", inactive: "bg-white text-green-700 border-green-300 hover:bg-green-50" },
  partial: { active: "bg-amber-500 text-white border-amber-500", inactive: "bg-white text-amber-700 border-amber-300 hover:bg-amber-50" },
  none: { active: "bg-red-500 text-white border-red-500", inactive: "bg-white text-red-600 border-red-300 hover:bg-red-50" },
  unknown: { active: "bg-gray-500 text-white border-gray-500", inactive: "bg-white text-gray-500 border-gray-300 hover:bg-gray-50" },
};

const GROUP_ORDER: PlatformGroup[] = [
  "AWS",
  "GCP",
  "Azure",
  "Snowflake",
  "Databricks",
  "3rd Party",
];

const GROUP_LABELS: Record<PlatformGroup, string> = {
  AWS: "AWS",
  GCP: "GCP",
  Azure: "Azure",
  Snowflake: "Snowflake",
  Databricks: "Databricks",
  "3rd Party": "OSS/3rd Party",
};

const GROUP_LOGOS: Partial<Record<PlatformGroup, string>> = {
  AWS: "/logos/Amazon_Web_Services_Logo.svg",
  Snowflake: "/logos/Snowflake_Logo.svg",
  Azure: "/logos/Microsoft_Azure.svg",
  GCP: "/logos/google.svg",
  Databricks: "/logos/databricks-color.svg",
};

const PLATFORM_LOGOS: Record<string, string> = {
  "aws-athena": "/logos/aws-athena.svg",
  "aws-emr-glue": "/logos/spark.svg",
  "aws-managed-flink": "/logos/flink.svg",
  databricks: "/logos/databricks.svg",
  snowflake: "/logos/snowflake.svg",
  duckdb: "/logos/duckdb.svg",
  clickhouse: "/logos/clickhouse.svg",
  spark: "/logos/spark.svg",
  flink: "/logos/flink.svg",
  daft: "/logos/daft.svg",
  "kafka-connect": "/logos/kafka-connect.svg",
  "google-bigquery": "/logos/bigquery.svg",
  "google-dataproc": "/logos/dataproc.svg",
  "azure-synapse": "/logos/synapse.png",
  "azure-fabric": "/logos/fabric.png",
};

interface FilterPanelProps {
  filters: FilterState;
  data: CompatibilityData;
  onFilterChange: (filters: FilterState) => void;
}

export function FilterPanel({
  filters,
  data,
  onFilterChange,
}: FilterPanelProps) {
  const groupMap = new Map<PlatformGroup, { id: string; name: string }[]>();
  for (const g of GROUP_ORDER) groupMap.set(g, []);
  for (const p of data.platforms) {
    groupMap.get(p.group)?.push({ id: p.id, name: p.name });
  }

  const selectedSet = new Set(filters.selectedPlatforms);

  const isGroupFullySelected = (group: PlatformGroup) => {
    const members = groupMap.get(group) ?? [];
    return members.length > 0 && members.every((m) => selectedSet.has(m.id));
  };

  const isGroupPartiallySelected = (group: PlatformGroup) => {
    const members = groupMap.get(group) ?? [];
    return members.some((m) => selectedSet.has(m.id)) && !isGroupFullySelected(group);
  };

  const toggleGroup = (group: PlatformGroup) => {
    const members = groupMap.get(group) ?? [];
    const memberIds = members.map((m) => m.id);
    if (isGroupFullySelected(group)) {
      const next = filters.selectedPlatforms.filter((id) => !memberIds.includes(id));
      onFilterChange({ ...filters, selectedPlatforms: next });
    } else {
      const current = new Set(filters.selectedPlatforms);
      for (const id of memberIds) current.add(id);
      onFilterChange({ ...filters, selectedPlatforms: [...current] });
    }
  };

  const togglePlatform = (id: string) => {
    const selected = selectedSet.has(id)
      ? filters.selectedPlatforms.filter((p) => p !== id)
      : [...filters.selectedPlatforms, id];
    onFilterChange({ ...filters, selectedPlatforms: selected });
  };

  const toggleCategory = (cat: FeatureCategory) => {
    const selected = filters.selectedCategories.includes(cat)
      ? filters.selectedCategories.filter((c) => c !== cat)
      : [...filters.selectedCategories, cat];
    onFilterChange({ ...filters, selectedCategories: selected });
  };

  const toggleSupportLevel = (level: SupportLevel) => {
    const selected = filters.selectedSupportLevels.includes(level)
      ? filters.selectedSupportLevels.filter((l) => l !== level)
      : [...filters.selectedSupportLevels, level];
    onFilterChange({ ...filters, selectedSupportLevels: selected });
  };

  const clearAll = () => {
    onFilterChange({
      selectedVersions: filters.selectedVersions,
      selectedPlatforms: [],
      selectedCategories: [],
      selectedSupportLevels: [],
      searchQuery: "",
    });
  };

  const allPlatformIds = data.platforms.map((p) => p.id);
  const allPlatformsSelected =
    allPlatformIds.length > 0 &&
    allPlatformIds.every((id) => selectedSet.has(id));

  const selectAllPlatforms = () => {
    onFilterChange({
      ...filters,
      selectedPlatforms: allPlatformsSelected ? [] : allPlatformIds,
    });
  };

  const hasFilters =
    filters.selectedPlatforms.length > 0 ||
    filters.selectedCategories.length > 0 ||
    filters.selectedSupportLevels.length > 0 ||
    filters.searchQuery.trim() !== "";

  const expandableGroups = GROUP_ORDER.filter(
    (g) => (groupMap.get(g)?.length ?? 0) > 1
  );
  const activeExpandable = expandableGroups.filter(
    (g) => isGroupFullySelected(g) || isGroupPartiallySelected(g)
  );

  return (
    <div
      className="bg-white border border-gray-200 rounded-lg p-3 shadow-sm"
      role="search"
      aria-label="Filter compatibility matrix"
    >
      <div className="flex flex-wrap items-start gap-x-6 gap-y-3">
        {/* Search */}
        <div className="w-full sm:w-auto sm:min-w-[220px]">
          <label
            htmlFor="feature-search"
            className="block text-[10px] font-semibold text-gray-500 uppercase tracking-wide mb-1"
          >
            Search
          </label>
          <input
            id="feature-search"
            type="text"
            value={filters.searchQuery}
            onChange={(e) =>
              onFilterChange({ ...filters, searchQuery: e.target.value })
            }
            placeholder="Filter features…"
            className="w-full border border-gray-300 rounded-md px-2.5 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-blue-400 focus:border-blue-400"
            aria-label="Search features by name"
          />
          <div className="flex gap-3 mt-1.5">
            <label className="flex items-center gap-1 cursor-pointer text-[10px] text-gray-500 hover:text-gray-700">
              <input
                type="checkbox"
                checked={allPlatformsSelected}
                onChange={selectAllPlatforms}
                className="w-3 h-3 rounded border-gray-300 text-blue-600 focus:ring-blue-400 cursor-pointer"
              />
              Select all
            </label>
            <label className="flex items-center gap-1 cursor-pointer text-[10px] text-gray-500 hover:text-gray-700">
              <input
                type="checkbox"
                checked={filters.selectedPlatforms.length === 0}
                onChange={clearAll}
                className="w-3 h-3 rounded border-gray-300 text-blue-600 focus:ring-blue-400 cursor-pointer"
              />
              Clear
            </label>
          </div>
        </div>

        {/* Platforms */}
        <div>
          <p className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide mb-1">Platforms</p>
          <div className="flex flex-wrap gap-1">
            {GROUP_ORDER.map((group) => {
              const full = isGroupFullySelected(group);
              const partial = isGroupPartiallySelected(group);
              return (
                <button
                  key={group}
                  type="button"
                  onClick={() => toggleGroup(group)}
                  className={`filter-chip ${
                    full
                      ? "bg-blue-600 text-white border-blue-600"
                      : partial
                        ? "bg-blue-100 text-blue-800 border-blue-300"
                        : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
                  }`}
                  aria-pressed={full}
                  aria-label={`Filter by ${GROUP_LABELS[group]}`}
                >
                  {GROUP_LOGOS[group] && (
                    <img src={GROUP_LOGOS[group]} alt="" className="w-3.5 h-3.5 inline-block mr-1 -mt-px" />
                  )}
                  {GROUP_LABELS[group]}
                </button>
              );
            })}
          </div>
          {activeExpandable.length > 0 && (
            <div className="mt-1.5 pl-2 border-l-2 border-blue-200 space-y-1.5">
              {activeExpandable.map((group) => (
                <div key={group} className="flex flex-wrap gap-1 items-center">
                  <span className="text-[9px] font-medium text-gray-400 uppercase mr-1">
                    {GROUP_LABELS[group]}:
                  </span>
                  {(groupMap.get(group) ?? []).map((p) => (
                    <button
                      key={p.id}
                      type="button"
                      onClick={() => togglePlatform(p.id)}
                      className={`px-2 py-0.5 rounded-full text-[10px] font-medium cursor-pointer transition-colors border ${
                        selectedSet.has(p.id)
                          ? "bg-blue-600 text-white border-blue-600"
                          : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
                      }`}
                      aria-pressed={selectedSet.has(p.id)}
                      aria-label={`Filter by ${p.name}`}
                    >
                      {PLATFORM_LOGOS[p.id] && (
                        <img src={PLATFORM_LOGOS[p.id]} alt="" className="w-3 h-3 inline-block mr-0.5 -mt-px" />
                      )}
                      {p.name}
                    </button>
                  ))}
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Categories */}
        <div>
          <p className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide mb-1">Categories</p>
          <div className="flex flex-wrap gap-1">
            {(Object.keys(CATEGORY_LABELS) as FeatureCategory[]).map((cat) => (
              <button
                key={cat}
                type="button"
                onClick={() => toggleCategory(cat)}
                className={`filter-chip ${
                  filters.selectedCategories.includes(cat)
                    ? "bg-blue-600 text-white border-blue-600"
                    : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
                }`}
                aria-pressed={filters.selectedCategories.includes(cat)}
                aria-label={`Filter by ${CATEGORY_LABELS[cat]}`}
              >
                {CATEGORY_LABELS[cat]}
              </button>
            ))}
          </div>
        </div>

        {/* Support Levels */}
        <div>
          <p className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide mb-1">Support</p>
          <div className="flex flex-wrap gap-1">
            {SUPPORT_LEVELS.map((level) => {
              const isActive = filters.selectedSupportLevels.includes(level);
              const colors = SUPPORT_COLORS[level];
              return (
                <button
                  key={level}
                  type="button"
                  onClick={() => toggleSupportLevel(level)}
                  className={`filter-chip capitalize ${isActive ? colors.active : colors.inactive}`}
                  aria-pressed={isActive}
                  aria-label={`Filter by ${level} support`}
                >
                  {level}
                </button>
              );
            })}
          </div>
        </div>

        {/* Clear */}
        {hasFilters && (
          <div className="flex items-end">
            <button
              type="button"
              onClick={clearAll}
              className="text-[10px] text-red-500 hover:text-red-700 cursor-pointer font-medium"
            >
              ✕ Clear all
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
