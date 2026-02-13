import { useState } from "react";
import type { FilterState } from "./types";
import { data } from "./data/load-data";
import { FilterPanel } from "./components/FilterPanel";
import { VersionTabs } from "./components/VersionTabs";
import { CompatibilityMatrix } from "./components/CompatibilityMatrix";
import { ComparisonSummary } from "./components/ComparisonSummary";
import { applyFilters } from "./utils/filters";

const initialFilters: FilterState = {
  selectedVersions: ["v2"],
  selectedPlatforms: [],
  selectedCategories: [],
  selectedSupportLevels: [],
  searchQuery: "",
};

export default function App() {
  const [filters, setFilters] = useState<FilterState>(initialFilters);
  const [introOpen, setIntroOpen] = useState(false);

  const handleVersionChange = (versions: typeof filters.selectedVersions) => {
    setFilters((prev) => ({ ...prev, selectedVersions: versions }));
  };

  const { platforms } = applyFilters(data, filters);
  const isCompareMode = filters.selectedVersions.length > 1;

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-[1800px] mx-auto px-4 py-3 sm:px-6">
          <div className="flex items-center justify-between flex-wrap gap-3">
            <div className="flex items-center gap-3">
              <img
                src="/iceberg-logo.svg"
                alt="Apache Iceberg logo"
                className="h-9 w-9"
              />
              <div>
                <h1 className="text-lg font-bold text-gray-900 leading-tight">
                  Apache Iceberg™ Compatibility Matrix
                </h1>
                <p className="text-xs text-gray-500 mt-0.5">
                  Feature support across platforms and engines
                </p>
              </div>
            </div>
            <VersionTabs
              versions={data.versions}
              selected={filters.selectedVersions}
              onChange={handleVersionChange}
            />
          </div>
        </div>
      </header>

      <main className="max-w-[1800px] mx-auto px-4 py-3 sm:px-6">
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
            </div>
          )}
        </div>

        <div className="mb-3">
          <FilterPanel
            filters={filters}
            data={data}
            onFilterChange={setFilters}
          />
        </div>

        {isCompareMode && (
          <ComparisonSummary
            data={data}
            platforms={platforms}
            versions={filters.selectedVersions}
          />
        )}

        <CompatibilityMatrix data={data} filters={filters} />
      </main>

      <footer className="border-t border-gray-200 bg-white px-4 py-3 sm:px-6 mt-6">
        <div className="max-w-[1800px] mx-auto text-[10px] text-gray-400 space-y-0.5">
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
              className="text-blue-500 hover:text-blue-700 underline"
            >
              GitHub repo
            </a>
          </p>
        </div>
      </footer>
    </div>
  );
}
