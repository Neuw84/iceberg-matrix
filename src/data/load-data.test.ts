/**
 * Structural tests for the engines dataset (src/data/platforms/).
 *
 * The matrix is built from hand-maintained JSON spread across per-engine
 * files, so these tests are the schema: they catch a platform missing a
 * feature x version entry, a typoed support key, or a malformed link at test
 * time instead of as a blank "unknown" cell in the UI.
 *
 * Extension recipe — adding an engine: see agents.md ("Adding a New Platform
 * or Engine"). The coverage tests below enumerate any gaps for you.
 */
import { describe, it, expect } from "vitest";
import featuresJson from "./features.json";
import { data, loadEngineData, peekEngineData } from "./load-data";
import testCoverage from "virtual:test-coverage";
import { coveredFeatures } from "./test-coverage";
import type { CompatibilityData } from "../types";

const VALID_LEVELS = ["full", "partial", "none", "unknown"];

// Ingestion tools live in their own dataset (src/data/ingestion/, shown in the
// Ingestion view) and must not appear among the query engines.
const INGESTION_ONLY_IDS = ["arc", "aws-firehose", "fivetran-managed-data-lake", "kafka-connect", "olake-go", "pixeltable"];

interface RawEntry {
  level?: unknown;
  notes?: unknown;
  caveats?: unknown;
  links?: unknown;
}

interface RawEngineFile {
  platforms?: Array<Record<string, unknown>>;
  support?: Record<string, RawEntry>;
}

// Eagerly glob every engine file on disk so a malformed file is caught even
// before it is wired into the loader.
const modules = import.meta.glob<{ default: RawEngineFile }>(
  "./platforms/**/*.json",
  { eager: true },
);

const files = Object.entries(modules).map(([path, mod]) => ({
  path,
  content: mod.default,
}));

const featureIds = featuresJson.features.map((f) => f.id);
const versions = featuresJson.versions;

describe("engine files on disk", () => {
  it("found the nested per-engine files", () => {
    expect(files.length).toBeGreaterThan(0);
  });

  it.each(files.map((f) => [f.path, f.content] as const))(
    "%s is a well-formed engine file",
    (path, content) => {
      // Exactly one platform per file.
      expect(content.platforms, "platforms array").toHaveLength(1);
      const platform = content.platforms![0];

      for (const field of ["id", "name", "vendor", "category", "group", "docUrl"]) {
        expect(typeof platform[field], `platform.${field}`).toBe("string");
        expect((platform[field] as string).trim()).not.toBe("");
      }
      expect(platform.docUrl as string).toMatch(/^https:\/\//);

      // File is named after its folder, and the folder name is the platform id
      // with its vendor prefix dropped (aws-redshift-s3 -> redshift-s3/).
      const segments = path.split("/");
      const fileName = segments[segments.length - 1].replace(/\.json$/, "");
      const folderName = segments[segments.length - 2];
      expect(fileName, "file named after folder").toBe(folderName);
      const id = platform.id as string;
      expect(
        id === folderName || id.endsWith(`-${folderName}`),
        `platform id "${id}" should map to folder "${folderName}"`,
      ).toBe(true);
    },
  );

  it.each(files.map((f) => [f.path, f.content] as const))(
    "%s covers every feature for every version",
    (_path, content) => {
      const id = content.platforms?.[0]?.id as string;
      const expectedKeys = featureIds
        .flatMap((fid) => versions.map((v) => `${id}:${fid}:${v}`))
        .sort();
      expect(Object.keys(content.support ?? {}).sort()).toEqual(expectedKeys);
    },
  );

  it.each(files.map((f) => [f.path, f.content] as const))(
    "%s has well-formed support entries",
    (_path, content) => {
      for (const [key, entry] of Object.entries(content.support ?? {})) {
        expect(VALID_LEVELS, `${key} level`).toContain(entry.level);
        expect(typeof entry.notes, `${key} notes`).toBe("string");
        expect(Array.isArray(entry.caveats), `${key} caveats`).toBe(true);
        for (const c of entry.caveats as unknown[]) {
          expect(typeof c, `${key} caveat`).toBe("string");
        }
        // links is optional, but when present it must be the typed
        // {label, url}[] shape DetailPopover renders — the Redshift files
        // drifted to bare URL strings once because nothing checked this.
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

// Only the default dataset is bundled; the other three are loaded on demand,
// exactly as the app loads them, so these tests also exercise the lazy path.
const dataS3Tables = await loadEngineData("s3-tables", "snowflake");
const dataSnowflakeExternal = await loadEngineData("s3-buckets", "external");
const dataS3TablesSnowflakeExternal = await loadEngineData("s3-tables", "external");

describe("lazy engine datasets (loadEngineData)", () => {
  it("has the default dataset ready without loading anything", () => {
    // Synchronous: this is what lets the first render skip a loading state.
    expect(peekEngineData("s3-buckets", "snowflake")).toBe(data);
  });

  it("returns the same object for repeated loads, so memoization survives toggles", async () => {
    expect(await loadEngineData("s3-tables", "snowflake")).toBe(dataS3Tables);
    expect(await loadEngineData("s3-buckets", "snowflake")).toBe(data);
    expect(peekEngineData("s3-tables", "external")).toBe(dataS3TablesSnowflakeExternal);
  });

  it("builds each variant from its own files", async () => {
    // Same platforms in the same order in every mode: only the cell data differs.
    const order = data.platforms.map((p) => p.id);
    for (const variant of [dataS3Tables, dataSnowflakeExternal, dataS3TablesSnowflakeExternal]) {
      expect(variant.platforms.map((p) => p.id)).toEqual(order);
    }
    // S3 Tables carries its own AWS cells (the Athena note differs by mode).
    expect(dataS3Tables.support["aws-athena:position-deletes:v2"]).not.toEqual(
      data.support["aws-athena:position-deletes:v2"],
    );
    // Snowflake external is built from the external file. Its cells may equal the
    // managed ones today, so compare against the file itself, not against `data`.
    const { snowflakeExternal } = await import("./engine-variants/snowflake-external");
    // toMatchObject: the loader adds test-coverage fields on top of the file's.
    expect(dataSnowflakeExternal.support["snowflake:position-deletes:v2"]).toMatchObject(
      (snowflakeExternal.support as Record<string, object>)["snowflake:position-deletes:v2"],
    );
    // And a combined variant takes S3 Tables AWS cells together with external Snowflake.
    expect(dataS3TablesSnowflakeExternal.support["aws-athena:position-deletes:v2"]).toEqual(
      dataS3Tables.support["aws-athena:position-deletes:v2"],
    );
  });
});

describe.each([
  ["data (S3 buckets, Snowflake managed)", data],
  ["dataS3Tables (S3 Tables, Snowflake managed)", dataS3Tables],
  ["dataSnowflakeExternal (S3 buckets, Snowflake external)", dataSnowflakeExternal],
  ["dataS3TablesSnowflakeExternal (S3 Tables, Snowflake external)", dataS3TablesSnowflakeExternal],
] as const)("merged engines dataset: %s", (_name, dataset: CompatibilityData) => {
  it("gives every platform an entry for every feature x version", () => {
    const missing: string[] = [];
    for (const p of dataset.platforms) {
      for (const f of dataset.features) {
        for (const v of dataset.versions) {
          const key = `${p.id}:${f.id}:${v}`;
          if (!(key in dataset.support)) missing.push(key);
        }
      }
    }
    // Enumerate the gaps so a failure names the exact cells to fill.
    expect(missing).toEqual([]);
  });

  it("has no orphan support keys", () => {
    const platformIds = new Set(dataset.platforms.map((p) => p.id));
    const ids = new Set(featureIds);
    const orphans = Object.keys(dataset.support).filter((key) => {
      const [pid, fid, ver] = key.split(":");
      return !platformIds.has(pid) || !ids.has(fid) || !dataset.versions.includes(ver as (typeof dataset.versions)[number]);
    });
    expect(orphans).toEqual([]);
  });

  it("has unique platform ids", () => {
    const ids = dataset.platforms.map((p) => p.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("keeps ingestion-only tools out of the engines dataset", () => {
    const ids = new Set(dataset.platforms.map((p) => p.id));
    for (const tool of INGESTION_ONLY_IDS) {
      expect(ids.has(tool), `${tool} belongs to the ingestion dataset`).toBe(false);
    }
  });

  it("keeps each platform group's columns contiguous for the group header", () => {
    const groups = dataset.platforms.map((p) => p.group);
    const seen = new Set<string>();
    let previous: string | null = null;
    for (const g of groups) {
      if (g !== previous) {
        expect(seen.has(g), `group "${g}" appears in two separate blocks`).toBe(false);
        seen.add(g);
        previous = g;
      }
    }
  });
});

describe("test coverage", () => {
  const featureIds = new Set(featuresJson.features.map((f) => f.id));

  for (const suite of testCoverage) {
    describe(suite.file, () => {
      it("tests at least one feature", () => {
        expect(suite.features.length).toBeGreaterThan(0);
      });

      // A typoed feature id in the Python would otherwise mark nothing.
      it("names only features that exist, on a platform that exists", () => {
        expect(data.platforms.map((p) => p.id)).toContain(suite.platform);
        for (const { feature } of suite.features) expect(featureIds, feature).toContain(feature);
      });

      // The S3 Tables / Snowflake external datasets carry the same platform
      // ids, so the default dataset is enough to see the marks land.
      it("marks its cells as tested", () => {
        const marked = Object.entries(data.support).filter(
          ([key, e]) => key.startsWith(`${suite.platform}:`) && e.testedBy === suite.file,
        );
        expect(marked.length).toBeGreaterThan(0);
      });
    });
  }
});

describe("coveredFeatures", () => {
  const suite = (tests: string, registry: string) => `
def test_a() -> TestResult:
    r = TestResult("feature-a", "A")
    r.result = "pass"
    return r


def test_v3_arg() -> TestResult:
    r = TestResult("feature-v3", "V3 only", "v3")
    return _catalog_test(r, body)


def test_with_diagnostic(version: str) -> list:
    r = TestResult("feature-main", "Main", version)
    d = TestResult("feature-main-read", "Main: read half",
                   version, diagnostic=True)
    return [r, d]


def test_v3_attr() -> TestResult:
    r = TestResult("feature-v3-attr", "V3 only")
    r.version_tested = "v3"
    return _catalog_test(r, body)


def test_always_skips() -> TestResult:
    r = TestResult("feature-skip", "Skip")
    r.result = "skip"
    return r


def test_sometimes_skips() -> TestResult:
    r = TestResult("feature-maybe", "Maybe")
    if not available():
        r.result = "skip"
        return r
    return _catalog_test(r, body)


def test_unregistered() -> TestResult:
    r = TestResult("feature-unregistered", "Unregistered")
    r.result = "pass"
    return r
${tests}
ALL_TESTS = [
${registry}
]
`;

  it("counts registered tests that can measure, and skips the rest", () => {
    const src = suite("", "    test_a,\n    test_always_skips,\n    test_sometimes_skips,");
    expect(coveredFeatures(src)).toEqual([{ feature: "feature-a" }, { feature: "feature-maybe" }]);
  });

  it("keeps the version a result names", () => {
    const src = suite("", "    test_v3_arg,\n    test_v3_attr,\n    test_with_diagnostic,");
    expect(coveredFeatures(src)).toEqual([
      { feature: "feature-main" },
      { feature: "feature-v3", version: "v3" },
      { feature: "feature-v3-attr", version: "v3" },
    ]);
  });

  it("throws without an ALL_TESTS registry", () => {
    expect(() => coveredFeatures("def test_a():\n    pass\n")).toThrow(/ALL_TESTS/);
  });
});
