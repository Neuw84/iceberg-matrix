import type { FeatureCategory } from "../types";

// Category headings shared by the matrix and the phone list.
export const CATEGORY_LABELS: Record<FeatureCategory, string> = {
  "row-level-operations": "Row-Level Operations",
  partitioning: "Partitioning",
  "table-management": "Table Management",
  "read-write": "Read / Write",
  "catalog-support": "Catalog Support",
  "v3-data-types": "V3 Data Types",
  "spec-support": "Spec Support",
  "openness-rubric": "Openness Rubric",
  "ingestion-write": "Write Path",
};

export const CATEGORY_ORDER: FeatureCategory[] = [
  // Catalogs view (its datasets contain only these two categories).
  "spec-support",
  "openness-rubric",
  // Ingestion view (its write-path category comes first; the rest are shared
  // with the engines view).
  "ingestion-write",
  // Engines view.
  "row-level-operations",
  "partitioning",
  "table-management",
  "read-write",
  "v3-data-types",
  "catalog-support",
];

export const CATEGORY_COLORS: Record<FeatureCategory, string> = {
  "row-level-operations": "border-l-orange-400 bg-orange-50",
  partitioning: "border-l-violet-400 bg-violet-50",
  "table-management": "border-l-emerald-400 bg-emerald-50",
  "read-write": "border-l-cyan-400 bg-cyan-50",
  "catalog-support": "border-l-amber-400 bg-amber-50",
  "v3-data-types": "border-l-pink-400 bg-pink-50",
  "spec-support": "border-l-sky-400 bg-sky-50",
  "openness-rubric": "border-l-indigo-400 bg-indigo-50",
  "ingestion-write": "border-l-teal-400 bg-teal-50",
};
