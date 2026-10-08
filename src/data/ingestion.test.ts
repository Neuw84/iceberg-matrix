/**
 * Structural tests for the ingestion dataset (src/data/ingestion/).
 *
 * The ingestion matrix is built from hand-maintained JSON, so these tests are
 * the schema: they catch a missing cell, a typoed support key, or a malformed
 * link at test time instead of as a blank cell in the UI.
 *
 * Extension recipe, adding an ingestion tool:
 *   1. Create src/data/ingestion/<id>/<id>.json (folder and file named after the
 *      platform id) with one platform object and a support entry for every
 *      feature in src/data/ingestion/features.json, for every version.
 *   2. Add the id to EXPECTED_INGESTION_IDS below.
 *   3. Import and append it in src/data/load-ingestion.ts (groups must stay
 *      contiguous: AWS first, then 3rd Party).
 * Adding a feature: add it to ingestion/features.json and a support entry for
 * each version to every tool file; the coverage test lists the gaps for you.
 * Keep the set write-oriented: ingestion tools are not built for reading, so
 * read features (read support, time travel, statistics) do not belong here.
 */
import { describe, it, expect } from "vitest";
import ingestionFeaturesJson from "./ingestion/features.json";

// The one list to update when a tool is added or removed.
const EXPECTED_INGESTION_IDS = ["aws-firehose", "kafka-connect"];

const VALID_LEVELS = ["full", "partial", "none", "unknown"];
const VALID_GROUPS = ["AWS", "3rd Party"];
const VALID_PLATFORM_CATEGORIES = ["cloud", "open-source"];
const VALID_CATEGORIES = [
  "ingestion-write",
  "row-level-operations",
  "table-management",
  "partitioning",
  "catalog-support",
  "v3-data-types",
];
// Read-oriented features that must not appear in a write-only view.
const READ_FEATURE_IDS = ["read-support", "time-travel", "statistics", "bloom-filters"];

interface RawEntry {
  level?: unknown;
  notes?: unknown;
  caveats?: unknown;
  links?: unknown;
}
interface RawIngestionFile {
  platforms?: Array<Record<string, unknown>>;
  support?: Record<string, RawEntry>;
}

// Eagerly glob every ingestion file so one that exists on disk but was never
// wired into the loader still gets validated.
const modules = import.meta.glob<{ default: RawIngestionFile }>(
  "./ingestion/*/*.json",
  { eager: true },
);
const files = Object.entries(modules).map(([path, mod]) => ({
  path,
  content: mod.default,
}));
const featureIds = ingestionFeaturesJson.features.map((f) => f.id);
const versions = ingestionFeaturesJson.versions;

describe("ingestion/features.json", () => {
  it("uses the v2 and v3 spec versions", () => {
    expect(versions).toEqual(["v2", "v3"]);
  });

  it("defines well-formed ingestion features", () => {
    expect(ingestionFeaturesJson.features.length).toBeGreaterThan(0);
    for (const f of ingestionFeaturesJson.features) {
      expect(f.id, "feature id").toMatch(/^[a-z0-9-]+$/);
      expect(f.name.trim()).not.toBe("");
      expect(f.description.trim()).not.toBe("");
      expect(VALID_CATEGORIES).toContain(f.category);
      expect(versions).toContain(f.introducedIn);
    }
  });

  it("has unique feature ids", () => {
    expect(new Set(featureIds).size).toBe(featureIds.length);
  });

  it("contains no read-oriented features", () => {
    for (const id of READ_FEATURE_IDS) {
      expect(featureIds, `${id} is a read feature`).not.toContain(id);
    }
  });
});

describe("ingestion files", () => {
  it("exactly the expected tools exist on disk", () => {
    const found = files.map((f) => f.content.platforms?.[0]?.id).sort();
    expect(found).toEqual([...EXPECTED_INGESTION_IDS].sort());
  });

  it.each(files.map((f) => [f.path, f.content] as const))(
    "%s is a well-formed ingestion file",
    (path, content) => {
      expect(content.platforms, "platforms array").toHaveLength(1);
      const platform = content.platforms![0];
      for (const field of ["id", "name", "vendor", "category", "group", "docUrl"]) {
        expect(typeof platform[field], `platform.${field}`).toBe("string");
        expect((platform[field] as string).trim()).not.toBe("");
      }
      expect(VALID_GROUPS).toContain(platform.group);
      expect(VALID_PLATFORM_CATEGORIES).toContain(platform.category);
      expect(platform.docUrl as string).toMatch(/^https:\/\//);
      const id = platform.id as string;
      expect(path).toBe(`./ingestion/${id}/${id}.json`);
    },
  );

  it.each(files.map((f) => [f.content.platforms?.[0]?.id, f.content] as const))(
    "%s covers every ingestion feature for every version",
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

  it("rates every V3 cell of the V2-only Firehose as none", () => {
    // AWS: "Firehose only supports V2 table format". A non-none V3 cell here
    // would claim V3 support the service documents it does not have.
    const firehose = files.find((f) => f.content.platforms?.[0]?.id === "aws-firehose");
    for (const [key, entry] of Object.entries(firehose!.content.support ?? {})) {
      if (key.endsWith(":v3")) expect(entry.level, key).toBe("none");
    }
  });
});

describe("merged ingestion dataset (load-ingestion.ts)", () => {
  it("includes every ingestion file on disk, exactly once", async () => {
    const { dataIngestion } = await import("./load-ingestion");
    const merged = dataIngestion.platforms.map((p) => p.id).sort();
    expect(merged).toEqual([...EXPECTED_INGESTION_IDS].sort());
  });

  it("keeps each group's tools contiguous for the matrix group header", async () => {
    const { dataIngestion } = await import("./load-ingestion");
    const groups = dataIngestion.platforms.map((p) => p.group);
    const transitions = groups.filter((g, i) => i > 0 && g !== groups[i - 1]);
    expect(transitions).toEqual(["3rd Party"]);
    expect(groups[0]).toBe("AWS");
  });

  it("carries the ingestion features and both spec versions", async () => {
    const { dataIngestion } = await import("./load-ingestion");
    expect(dataIngestion.versions).toEqual(["v2", "v3"]);
    expect(dataIngestion.features.map((f) => f.id)).toEqual(featureIds);
    expect(Object.keys(dataIngestion.support)).toHaveLength(
      EXPECTED_INGESTION_IDS.length * featureIds.length * versions.length,
    );
  });
});
