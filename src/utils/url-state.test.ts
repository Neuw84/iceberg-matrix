import { describe, it, expect } from "vitest";
import type { FilterState } from "../types";
import { parseUrlState, toUrlSearch } from "./url-state";

const engineDefaults: FilterState = {
  selectedVersions: ["v2", "v3"],
  compareMode: false,
  selectedPlatforms: [],
  selectedCategories: [],
  selectedSupportLevels: [],
  searchQuery: "",
};

describe("parseUrlState", () => {
  it("defaults to the engines view with no filters", () => {
    expect(parseUrlState("")).toEqual({ view: "engines", filters: {} });
  });

  it("reads view, filters and storage modes", () => {
    expect(
      parseUrlState("?p=duckdb,trino&v=v3&cat=partitioning&s=full,none&q=bloom&aws=s3-tables&sf=external"),
    ).toEqual({
      view: "engines",
      filters: {
        selectedPlatforms: ["duckdb", "trino"],
        selectedVersions: ["v3"],
        selectedCategories: ["partitioning"],
        selectedSupportLevels: ["full", "none"],
        searchQuery: "bloom",
      },
      awsS3Mode: "s3-tables",
      snowflakeMode: "external",
    });
  });

  it("drops values that cannot exist", () => {
    expect(parseUrlState("?view=nope&v=v9&s=great")).toEqual({ view: "engines", filters: {} });
  });

  it("only turns Compare on with two versions", () => {
    expect(parseUrlState("?compare=1").filters.compareMode).toBe(true);
    expect(parseUrlState("?compare=1&v=v3").filters.compareMode).toBeUndefined();
  });

  it("ignores versions, Compare and storage modes outside the views that have them", () => {
    expect(parseUrlState("?view=catalogs&v=v3&compare=1&aws=s3-tables")).toEqual({
      view: "catalogs",
      filters: {},
    });
    expect(parseUrlState("?view=ingestion&sf=external").snowflakeMode).toBeUndefined();
  });
});

describe("toUrlSearch", () => {
  it("writes nothing for the defaults", () => {
    expect(toUrlSearch("engines", engineDefaults, engineDefaults, "s3-buckets", "snowflake")).toBe("");
  });

  it("round-trips through parseUrlState", () => {
    const filters: FilterState = {
      ...engineDefaults,
      selectedPlatforms: ["duckdb", "aws-emr"],
      selectedSupportLevels: ["partial"],
      searchQuery: "deletion vectors",
      compareMode: true,
    };
    const search = toUrlSearch("engines", filters, engineDefaults, "s3-tables", "snowflake");
    expect(search).toBe("?p=duckdb,aws-emr&s=partial&q=deletion+vectors&compare=1&aws=s3-tables");
    const parsed = parseUrlState(search);
    expect({ ...engineDefaults, ...parsed.filters }).toEqual(filters);
    expect(parsed.awsS3Mode).toBe("s3-tables");
  });
});
