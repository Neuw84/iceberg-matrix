/**
 * Which matrix cells an engine feature suite (tests/<engine>_feature_tests.py)
 * exercises. Nothing is written by the suites: the Vite plugin in
 * vite.config.ts reads each suite's source at build time with
 * `coveredFeatures` and serves the result as `virtual:test-coverage`.
 *
 * A covered cell counts as tested because a suite fails CI whenever a measured
 * result disagrees with the JSON rating.
 *
 * Pure (no Node or DOM APIs): vite.config.ts and the app both import it.
 */
import type { Version } from "../types";

export interface SuiteConfig {
  /** Repo-relative path of the suite. */
  file: string;
  /** Platform id of the engine JSON the suite compares against. */
  platform: string;
  /** Spec versions the suite runs its tests on. */
  versions: Version[];
}

export interface SuiteCoverage extends SuiteConfig {
  /** Feature ids with at least one registered test that can measure. */
  features: string[];
}

/**
 * Suites whose results mark cells as tested. Add a suite here only once it
 * runs in CI, since being listed is what claims the cells are checked.
 */
export const SUITES: SuiteConfig[] = [
  // Runs every version-agnostic test once on V2 and once on V3 tables.
  { file: "tests/duckdb_feature_tests.py", platform: "duckdb", versions: ["v2", "v3"] },
];

/**
 * Feature ids a suite tests: the `TestResult("<feature-id>", ...)` of each
 * top-level `test_*` function listed in its `ALL_TESTS` registry. A test that
 * always skips (e.g. a catalog the suite cannot reach) measures nothing and is
 * left out.
 */
export function coveredFeatures(source: string): string[] {
  const registry = source.match(/^ALL_TESTS\s*=\s*\[([\s\S]*?)\]/m);
  if (!registry) throw new Error("suite has no ALL_TESTS registry");
  const registered = new Set(registry[1].match(/\btest_\w+/g) ?? []);

  const features = new Set<string>();
  // A function body is every following line that is indented or blank.
  for (const [, name, body] of source.matchAll(
    /^def (test_\w+)\(.*\n((?:[ \t]+.*\n|[ \t]*\n)*)/gm,
  )) {
    if (!registered.has(name)) continue;
    const feature = body.match(/TestResult\(\s*"([a-z0-9-]+)"/)?.[1];
    if (!feature) continue;
    // Unconditional skip: assigned at the function's own indentation, not
    // under an `if` (a missing optional dependency still leaves a real test).
    if (/^ {4}\w+\.result\s*=\s*"skip"/m.test(body)) continue;
    features.add(feature);
  }
  return [...features].sort();
}
