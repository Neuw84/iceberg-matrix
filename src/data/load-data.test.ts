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
import { data, dataS3Tables } from "./load-data";
import type { CompatibilityData } from "../types";

const VALID_LEVELS = ["full", "partial", "none", "unknown"];

// Engines stored in the structure but deliberately not imported by the loader.
const STAGED_EXCLUDED_IDS = ["aws-firehose", "kafka-connect"];

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

// Eagerly glob every engine file on disk — including the staged ones the
// loader skips — so a malformed file is caught even before it is wired in.
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
    (path, content) => {
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

describe.each([
  ["data (S3 buckets)", data],
  ["dataS3Tables (S3 Tables)", dataS3Tables],
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

  it("excludes the staged engines", () => {
    const ids = new Set(dataset.platforms.map((p) => p.id));
    for (const staged of STAGED_EXCLUDED_IDS) {
      expect(ids.has(staged), `${staged} should stay excluded`).toBe(false);
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
