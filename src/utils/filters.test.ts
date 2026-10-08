import { describe, expect, it } from "vitest";
import { data } from "../data/load-data";
import type { FilterState } from "../types";
import { applyFilters } from "./filters";

describe("support-level filtering", () => {
  it("ignores ratings for spec versions where a feature cannot be written", () => {
    const filters: FilterState = {
      selectedVersions: ["v2", "v3"],
      compareMode: false,
      selectedPlatforms: ["daft"],
      selectedCategories: [],
      selectedSupportLevels: ["unknown"],
      searchQuery: "",
    };

    // Daft's V3 position-delete entry is unknown, but V3 forbids writing
    // position delete files. Its applicable V2 entry is rated none.
    const ids = applyFilters(data, filters).features.map((feature) => feature.id);
    expect(ids).not.toContain("position-deletes");
    expect(ids).toContain("bloom-filters");
  });
});
