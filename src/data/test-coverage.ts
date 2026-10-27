/**
 * Which matrix cells an engine feature suite (tests/<engine>_feature_tests.py)
 * exercises. Nothing is written by the suites: the Vite plugin in
 * vite.config.ts reads each suite's source at build time with
 * `coveredFeatures` and serves the result as `virtual:test-coverage`.
 *
 * A covered cell counts as tested because a suite fails whenever a measured
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
  /**
   * Spec versions a test covers when its TestResult names none: both for a
   * suite that reruns every test per version, else the TestResult default.
   */
  versions: Version[];
  /** "pr": runs on every pull request. "manual": started by hand on a paid account. */
  runs: "pr" | "manual";
}

/** One tested cell; `version` is unset when the suite's `versions` apply. */
export interface CoveredFeature {
  feature: string;
  version?: Version;
}

export interface SuiteCoverage extends SuiteConfig {
  features: CoveredFeature[];
}

// Suites that rerun every test once per version (a `for version in VERSIONS`
// loop in main()).
const BOTH: Version[] = ["v2", "v3"];
// Single-pass suites: a test measures V2 unless its TestResult says "v3".
const V2: Version[] = ["v2"];

/**
 * Suites whose results mark cells as tested. The AWS suites run in both
 * storage modes (S3 buckets and S3 Tables) and Snowflake's in both storage
 * modes, each against that mode's data file, so one entry covers both.
 */
export const SUITES: SuiteConfig[] = [
  // OSS engines, run on every pull request.
  { file: "tests/duckdb_feature_tests.py", platform: "duckdb", versions: BOTH, runs: "pr" },
  { file: "tests/iceberg_feature_tests.py", platform: "spark", versions: BOTH, runs: "pr" },
  { file: "tests/flink_feature_tests.py", platform: "flink", versions: BOTH, runs: "pr" },
  { file: "tests/trino_feature_tests.py", platform: "trino", versions: V2, runs: "pr" },
  { file: "tests/pyiceberg_feature_tests.py", platform: "pyiceberg", versions: V2, runs: "pr" },
  { file: "tests/daft_feature_tests.py", platform: "daft", versions: V2, runs: "pr" },
  { file: "tests/clickhouse_feature_tests.py", platform: "clickhouse", versions: V2, runs: "pr" },
  // Cloud platforms, started by hand. EMR and Glue run the Spark suite on the
  // cluster through tests/aws/platform_entrypoint.py.
  { file: "tests/iceberg_feature_tests.py", platform: "aws-emr", versions: BOTH, runs: "manual" },
  { file: "tests/iceberg_feature_tests.py", platform: "aws-glue", versions: BOTH, runs: "manual" },
  { file: "tests/redshift_feature_tests.py", platform: "aws-redshift-s3", versions: BOTH, runs: "manual" },
  { file: "tests/aurora_feature_tests.py", platform: "aws-aurora", versions: V2, runs: "manual" },
  { file: "tests/databricks_feature_tests.py", platform: "databricks", versions: V2, runs: "manual" },
  { file: "tests/snowflake_feature_tests.py", platform: "snowflake", versions: V2, runs: "manual" },
];

/**
 * Cells a suite tests: every `TestResult("<feature-id>", ...)` in the
 * top-level `test_*` functions listed in its `ALL_TESTS` registry, with the
 * version the result names (`TestResult(..., "v3")` or `r.version_tested =
 * "v3"`). Diagnostic results (`diagnostic=True`) are not compared with the
 * matrix and are ignored. A test that always skips (e.g. a catalog the suite cannot reach)
 * measures nothing and is left out.
 */
export function coveredFeatures(source: string): CoveredFeature[] {
  const registry = source.match(/^ALL_TESTS\s*=\s*\[([\s\S]*?)\]/m);
  if (!registry) throw new Error("suite has no ALL_TESTS registry");
  const registered = new Set(registry[1].match(/\btest_\w+/g) ?? []);

  const found = new Map<string, CoveredFeature>();
  // A function body is every following line that is indented or blank.
  for (const [, name, body] of source.matchAll(
    /^def (test_\w+)\(.*\n((?:[ \t]+.*\n|[ \t]*\n)*)/gm,
  )) {
    if (!registered.has(name)) continue;
    // Unconditional skip: assigned at the function's own indentation, not
    // under an `if` (a missing optional dependency still leaves a real test).
    if (/^ {4}\w+\.result\s*=\s*"skip"/m.test(body)) continue;
    const bodyVersion = body.match(/\.version_tested\s*=\s*"(v[23])"/)?.[1];
    for (const [, feature, args] of body.matchAll(/TestResult\(\s*"([a-z0-9-]+)"([^)]*)\)/g)) {
      // Diagnostic rows are reported next to a cell but never compared to it.
      if (/diagnostic\s*=\s*True/.test(args)) continue;
      const version = (args.match(/"(v[23])"/)?.[1] ?? bodyVersion) as Version | undefined;
      found.set(`${feature}:${version ?? ""}`, version ? { feature, version } : { feature });
    }
  }
  return [...found.values()].sort(
    (a, b) => a.feature.localeCompare(b.feature) || (a.version ?? "").localeCompare(b.version ?? ""),
  );
}
