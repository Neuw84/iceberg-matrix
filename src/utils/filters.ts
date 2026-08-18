import type {
  CompatibilityData,
  Feature,
  FilterState,
  Platform,
} from "../types";
import { getSupportEntry } from "./support";

export function applyFilters(
  data: CompatibilityData,
  filters: FilterState
): { platforms: Platform[]; features: Feature[] } {
  let platforms = data.platforms;
  let features = data.features;

  // Filter platforms by selected IDs
  if (filters.selectedPlatforms.length > 0) {
    const ids = new Set(filters.selectedPlatforms);
    platforms = platforms.filter((p) => ids.has(p.id));
  }

  // Hide V3-only features when V3 is not among selected versions
  const hasV3 = filters.selectedVersions.includes("v3");
  if (!hasV3) {
    features = features.filter((f) => f.introducedIn !== "v3");
  }

  // Filter features by selected categories
  if (filters.selectedCategories.length > 0) {
    const cats = new Set(filters.selectedCategories);
    features = features.filter((f) => cats.has(f.category));
  }

  // Filter features by search query (case-insensitive name match)
  if (filters.searchQuery.trim() !== "") {
    const query = filters.searchQuery.trim().toLowerCase();
    features = features.filter((f) => f.name.toLowerCase().includes(query));
  }

  // Filter features by support level: keep a row when at least one of its
  // visible cells (kept platform × selected version) has a selected level.
  // Missing entries render as "unknown" in the grid, so they match here too.
  if (filters.selectedSupportLevels.length > 0) {
    const levels = new Set(filters.selectedSupportLevels);
    const keptPlatforms = platforms;
    features = features.filter((f) =>
      keptPlatforms.some((p) =>
        filters.selectedVersions.some((v) =>
          levels.has(getSupportEntry(data, p.id, f.id, v).level)
        )
      )
    );
  }

  return { platforms, features };
}
