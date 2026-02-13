import { Fragment, useState } from "react";
import type {
  CompatibilityData,
  Feature,
  FeatureCategory,
  FilterState,
  Platform,
  PlatformGroup,
  SupportEntry,
  Version,
} from "../types";
import { applyFilters } from "../utils/filters";
import { getSupportEntry } from "../utils/support";
import { DetailPopover } from "./DetailPopover";
import { FeatureRow } from "./FeatureRow";

const CATEGORY_LABELS: Record<FeatureCategory, string> = {
  "row-level-operations": "Row-Level Operations",
  "schema-management": "Schema Management",
  partitioning: "Partitioning",
  "table-management": "Table Management",
  "read-write": "Read / Write",
  "catalog-support": "Catalog Support",
  "v3-data-types": "V3 Data Types",
  "v3-advanced": "V3 Advanced Features",
};

const CATEGORY_ORDER: FeatureCategory[] = [
  "row-level-operations",
  "schema-management",
  "partitioning",
  "table-management",
  "read-write",
  "v3-data-types",
  "v3-advanced",
  "catalog-support",
];

const CATEGORY_COLORS: Record<FeatureCategory, string> = {
  "row-level-operations": "border-l-orange-400 bg-orange-50",
  "schema-management": "border-l-blue-400 bg-blue-50",
  partitioning: "border-l-violet-400 bg-violet-50",
  "table-management": "border-l-emerald-400 bg-emerald-50",
  "read-write": "border-l-cyan-400 bg-cyan-50",
  "catalog-support": "border-l-amber-400 bg-amber-50",
  "v3-data-types": "border-l-pink-400 bg-pink-50",
  "v3-advanced": "border-l-rose-400 bg-rose-50",
};

/** Map platform IDs to logo filenames in /logos/ — only correct matches */
const PLATFORM_LOGOS: Record<string, string> = {
  "aws-athena": "/logos/aws-athena.svg",
  "aws-glue": "/logos/aws-glue.svg",
  "aws-emr": "/logos/spark.svg",
  "aws-managed-flink": "/logos/flink.svg",
  databricks: "/logos/databricks.svg",
  snowflake: "/logos/snowflake.svg",
  duckdb: "/logos/duckdb.svg",
  clickhouse: "/logos/clickhouse.svg",
  "spark": "/logos/spark.svg",
  "flink": "/logos/flink.svg",
  daft: "/logos/daft.svg",
  "kafka-connect": "/logos/kafka-connect.svg",
};

interface PopoverState {
  platform: Platform;
  feature: Feature;
  version: Version;
  entry: SupportEntry;
}

interface CompatibilityMatrixProps {
  data: CompatibilityData;
  filters: FilterState;
}

export function CompatibilityMatrix({
  data,
  filters,
}: CompatibilityMatrixProps) {
  const [popover, setPopover] = useState<PopoverState | null>(null);
  const [collapsedCategories, setCollapsedCategories] = useState<Set<FeatureCategory>>(new Set());
  const { platforms, features } = applyFilters(data, filters);
  const versions = filters.selectedVersions;

  if (platforms.length === 0 || features.length === 0) {
    return (
      <p className="text-gray-400 text-center py-12">
        No compatibility data available for the current filters.
      </p>
    );
  }

  const grouped = CATEGORY_ORDER.map((cat) => ({
    category: cat,
    label: CATEGORY_LABELS[cat],
    features: features.filter((f) => f.category === cat),
  })).filter((g) => g.features.length > 0);

  const colCount = platforms.length * versions.length;

  // Group platforms by their group for header rendering
  const platformGroups: { group: PlatformGroup; platforms: Platform[] }[] = [];
  for (const p of platforms) {
    const last = platformGroups[platformGroups.length - 1];
    if (last && last.group === p.group) {
      last.platforms.push(p);
    } else {
      platformGroups.push({ group: p.group, platforms: [p] });
    }
  }

  const GROUP_COLORS: Record<PlatformGroup, string> = {
    AWS: "bg-orange-100 text-orange-900 border-orange-200",
    GCP: "bg-blue-100 text-blue-900 border-blue-200",
    Azure: "bg-sky-100 text-sky-900 border-sky-200",
    Databricks: "bg-red-100 text-red-900 border-red-200",
    Snowflake: "bg-cyan-100 text-cyan-900 border-cyan-200",
    "3rd Party": "bg-gray-100 text-gray-700 border-gray-200",
  };

  const handleCellClick = (
    platform: Platform,
    feature: Feature,
    version: Version,
    entry: SupportEntry,
  ) => {
    setPopover({ platform, feature, version, entry });
  };

  const toggleCategory = (cat: FeatureCategory) => {
    setCollapsedCategories((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  };

  return (
    <div className="relative bg-white rounded-lg border border-gray-200 shadow-sm">
      {/* Scroll hint for narrow viewports */}
      <div className="sm:hidden text-center text-[10px] text-gray-400 py-1 bg-gray-50 border-b border-gray-100">
        ← Scroll horizontally to see all platforms →
      </div>
      <div className="overflow-x-auto matrix-wrapper">
        <table
          className="border-collapse text-sm"
          style={{ minWidth: 110 + colCount * 88, width: '100%' }}
          role="grid"
          aria-label="Iceberg compatibility matrix"
        >
          <thead>
            {/* Group header row */}
            <tr>
              <th
                className="sticky left-0 bg-white z-30 border-b border-gray-200"
                rowSpan={2}
                style={{ width: 110, minWidth: 110 }}
              />
              {platformGroups.map((pg) => (
                <th
                  key={pg.group}
                  colSpan={pg.platforms.length * versions.length}
                  className={`px-2 py-1.5 text-center text-[10px] font-bold uppercase tracking-wider border-b border-x ${GROUP_COLORS[pg.group]}`}
                >
                  {pg.group}
                </th>
              ))}
            </tr>
            {/* Platform header row */}
            <tr className="border-b border-gray-300">
              {platforms.map((p) =>
                versions.map((v) => (
                  <th
                    key={`${p.id}:${v}`}
                    className="px-1 py-1.5 text-center border-x border-gray-100 bg-gray-50/80"
                    scope="col"
                  >
                    <div className="flex flex-col items-center gap-0.5">
                      {PLATFORM_LOGOS[p.id] && (
                        <img
                          src={PLATFORM_LOGOS[p.id]}
                          alt=""
                          className="w-4 h-4 opacity-60"
                        />
                      )}
                      <span className="text-[10px] font-semibold text-gray-700 leading-tight">
                        {p.name}
                      </span>
                      {versions.length > 1 && (
                        <span className="text-[9px] text-gray-400 font-normal">
                          {v.toUpperCase()}
                        </span>
                      )}
                    </div>
                  </th>
                )),
              )}
            </tr>
          </thead>
          <tbody>
            {grouped.map((group) => {
              const isCollapsed = collapsedCategories.has(group.category);
              return (
                <Fragment key={group.category}>
                  <tr
                    className="category-row cursor-pointer select-none"
                    onClick={() => toggleCategory(group.category)}
                  >
                    <td
                      className={`sticky left-0 z-20 px-3 py-1.5 text-[10px] font-bold text-gray-600 uppercase tracking-wider border-l-4 ${CATEGORY_COLORS[group.category]}`}
                      style={{ minWidth: 110 }}
                    >
                      <span className="inline-flex items-center gap-1.5 whitespace-nowrap">
                        <svg
                          className={`w-3 h-3 transition-transform duration-200 ${isCollapsed ? "" : "rotate-90"}`}
                          fill="none"
                          viewBox="0 0 24 24"
                          stroke="currentColor"
                          strokeWidth={2.5}
                        >
                          <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
                        </svg>
                        {group.label}
                        <span className="text-[9px] font-normal text-gray-400 normal-case tracking-normal">
                          ({group.features.length})
                        </span>
                      </span>
                    </td>
                    <td
                      colSpan={colCount}
                      className={`border-l-0 ${CATEGORY_COLORS[group.category]}`}
                    />
                  </tr>
                  {!isCollapsed &&
                    group.features.map((feature) => (
                      <FeatureRow
                        key={feature.id}
                        feature={feature}
                        platforms={platforms}
                        versions={versions}
                        getSupportEntry={(pid, fid, ver) =>
                          getSupportEntry(data, pid, fid, ver)
                        }
                        onCellClick={handleCellClick}
                      />
                    ))}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>

      {popover && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/20">
          <DetailPopover
            entry={popover.entry}
            feature={popover.feature}
            platform={popover.platform}
            version={popover.version}
            onClose={() => setPopover(null)}
          />
        </div>
      )}
    </div>
  );
}
