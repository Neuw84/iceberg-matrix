/**
 * Structural tests for the catalogs dataset (src/data/catalogs/).
 *
 * The catalog matrix is built from hand-maintained JSON, so these tests are the
 * schema: they catch a missing cell, a typoed support key, or a malformed link
 * at test time instead of as a blank cell in the UI.
 *
 * Extension recipe — adding a catalog:
 *   1. Create src/data/catalogs/<id>/<id>.json (folder and file named after the
 *      platform id) with one platform object and a support entry for every
 *      feature in src/data/catalogs/features.json.
 *   2. Add the id to EXPECTED_CATALOG_IDS below.
 *   3. Import and append it in src/data/load-catalogs.ts (groups must stay
 *      contiguous: Proprietary block, then Open Source block).
 * Adding a criterion: add it to catalogs/features.json and a support entry to
 * every catalog file; the coverage test enumerates the gaps for you.
 */
import { describe, it, expect } from "vitest";
import catalogFeaturesJson from "./catalogs/features.json";

// The one list to update when a catalog is added or removed.
const EXPECTED_CATALOG_IDS = [
  "snowflake-horizon",
  "aws-glue-data-catalog",
  "databricks-unity",
  "google-lakehouse-runtime-catalog",
  "microsoft-onelake",
  "apache-polaris",
  "apache-gravitino",
  "lakekeeper",
  "project-nessie",
  "unity-catalog-oss",
];

const VALID_LEVELS = ["full", "partial", "none", "unknown"];
const VALID_GROUPS = ["Proprietary", "Open Source"];
const VALID_PLATFORM_CATEGORIES = ["cloud", "open-source"];

interface RawEntry {
  level?: unknown;
  notes?: unknown;
  caveats?: unknown;
  links?: unknown;
}

interface RawCatalogFile {
  platforms?: Array<Record<string, unknown>>;
  support?: Record<string, RawEntry>;
}

// Eagerly glob every catalog file so a file that exists on disk but was never
// wired anywhere still gets validated (and a forgotten loader import is caught
// by the merged-dataset tests once the loader exists).
const modules = import.meta.glob<{ default: RawCatalogFile }>(
  "./catalogs/*/*.json",
  { eager: true },
);

const files = Object.entries(modules).map(([path, mod]) => ({
  path,
  content: mod.default,
}));

const featureIds = catalogFeaturesJson.features.map((f) => f.id);
const versions = catalogFeaturesJson.versions;

describe("catalogs/features.json", () => {
  it("declares a single synthetic version 'current'", () => {
    // The openness rubric has no v2/v3 dimension; the whole dataset hangs off
    // one version key so it can share the ${id}:${featureId}:${version} format.
    expect(versions).toEqual(["current"]);
  });

  it("defines well-formed catalog features", () => {
    expect(catalogFeaturesJson.features.length).toBeGreaterThan(0);
    for (const f of catalogFeaturesJson.features) {
      expect(f.id, "feature id").toMatch(/^[a-z0-9-]+$/);
      expect(f.name.trim()).not.toBe("");
      expect(f.description.trim()).not.toBe("");
      expect(["spec-support", "openness-rubric"]).toContain(f.category);
      expect(versions).toContain(f.introducedIn);
    }
  });

  it("has unique feature ids", () => {
    expect(new Set(featureIds).size).toBe(featureIds.length);
  });
});

describe("catalog files", () => {
  it("exactly the expected catalogs exist on disk", () => {
    const found = files
      .map((f) => f.content.platforms?.[0]?.id)
      .sort();
    expect(found).toEqual([...EXPECTED_CATALOG_IDS].sort());
  });

  it.each(files.map((f) => [f.path, f.content] as const))(
    "%s is a well-formed catalog file",
    (path, content) => {
      // Exactly one platform per file, like the engine files.
      expect(content.platforms, "platforms array").toHaveLength(1);
      const platform = content.platforms![0];

      for (const field of ["id", "name", "vendor", "category", "group", "docUrl"]) {
        expect(typeof platform[field], `platform.${field}`).toBe("string");
        expect((platform[field] as string).trim()).not.toBe("");
      }
      expect(VALID_GROUPS).toContain(platform.group);
      expect(VALID_PLATFORM_CATEGORIES).toContain(platform.category);
      expect(platform.docUrl as string).toMatch(/^https:\/\//);

      // Folder and file are named after the platform id, mirroring the
      // engine-file convention (aws/s3buckets/athena/athena.json).
      const id = platform.id as string;
      expect(path).toBe(`./catalogs/${id}/${id}.json`);
    },
  );

  it.each(files.map((f) => [f.content.platforms?.[0]?.id, f.content] as const))(
    "%s covers every rubric feature for every version",
    (id, content) => {
      const expectedKeys = featureIds
        .flatMap((fid) => versions.map((v) => `${id}:${fid}:${v}`))
        .sort();
      expect(Object.keys(content.support ?? {}).sort()).toEqual(expectedKeys);
    },
  );

  it.each(files.map((f) => [f.content.platforms?.[0]?.id, f.content] as const))(
    "%s has well-formed support entries",
    (_id, content) => {
      for (const [key, entry] of Object.entries(content.support ?? {})) {
        expect(VALID_LEVELS, `${key} level`).toContain(entry.level);
        expect(typeof entry.notes, `${key} notes`).toBe("string");
        expect((entry.notes as string).trim(), `${key} notes empty`).not.toBe("");
        expect(Array.isArray(entry.caveats), `${key} caveats`).toBe(true);
        for (const c of entry.caveats as unknown[]) {
          expect(typeof c, `${key} caveat`).toBe("string");
        }
        // links is optional, but when present it must be the typed
        // {label, url}[] shape — the engine data has already drifted once
        // (plain string arrays) because nothing checked this.
        if (entry.links !== undefined) {
          expect(Array.isArray(entry.links), `${key} links`).toBe(true);
          for (const link of entry.links as Array<Record<string, unknown>>) {
            expect(typeof link.label, `${key} link label`).toBe("string");
            expect(link.url as string, `${key} link url`).toMatch(/^https:\/\//);
          }
        }
      }
    },
  );
});

describe("merged catalogs dataset (load-catalogs.ts)", () => {
  it("includes every catalog file on disk, exactly once", async () => {
    const { dataCatalogs } = await import("./load-catalogs");
    const merged = dataCatalogs.platforms.map((p) => p.id).sort();
    // A file that exists but was never imported into the loader shows up here.
    expect(merged).toEqual([...EXPECTED_CATALOG_IDS].sort());
  });

  it("keeps each group's catalogs contiguous for the matrix group header", async () => {
    const { dataCatalogs } = await import("./load-catalogs");
    const groups = dataCatalogs.platforms.map((p) => p.group);
    const transitions = groups.filter((g, i) => i > 0 && g !== groups[i - 1]);
    // One transition = two contiguous blocks (Proprietary, then Open Source).
    expect(transitions).toEqual(["Open Source"]);
    expect(groups[0]).toBe("Proprietary");
  });

  it("carries the rubric features and the single 'current' version", async () => {
    const { dataCatalogs } = await import("./load-catalogs");
    expect(dataCatalogs.versions).toEqual(["current"]);
    expect(dataCatalogs.features.map((f) => f.id)).toEqual(featureIds);
    expect(Object.keys(dataCatalogs.support)).toHaveLength(
      EXPECTED_CATALOG_IDS.length * featureIds.length,
    );
  });
});
