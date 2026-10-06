# Iceberg Compatibility Matrix — Project Guide

## Project Overview

A React single-page application that displays an interactive compatibility matrix for Apache Iceberg features across cloud platforms and open-source engines. A top-level Engines/Catalogs toggle switches between two datasets: the engines matrix (default, one column per engine with V2/V3 version chips, transition cells where versions differ, an AWS S3-mode toggle, and a Compare differences table) and a catalogs openness-rubric matrix (10 Iceberg catalogs scored against 6 openness criteria). Built with Vite, TypeScript, React 19.2, and Tailwind CSS. Deployed to GitHub Pages via GitHub Actions.

## Project Structure

```
├── .github/workflows/       # CI (PR validation) and Deploy (GitHub Pages) pipelines
├── .kiro/specs/              # Kiro spec documents (requirements, design, tasks)
├── public/
│   ├── logos/                # SVG logos for each platform/engine
│   └── iceberg-logo.svg
├── src/
│   ├── components/           # React UI components
│   │   ├── CompatibilityMatrix.tsx   # Main matrix grid
│   │   ├── ComparisonSummary.tsx     # Compare mode: version differences-only table
│   │   ├── DetailPopover.tsx         # Popover with support details
│   │   ├── FeatureRow.tsx            # Single feature row in the matrix
│   │   ├── FilterPanel.tsx           # Sidebar filters (version, platform, category, etc.)
│   │   ├── SupportCell.tsx           # Individual cell showing support level
│   │   └── ViewToggle.tsx            # Top-level Engines/Catalogs view switcher
│   ├── data/                 # JSON data files
│   │   ├── features.json             # Engine feature definitions and categories
│   │   ├── load-data.ts              # Merges nested per-engine files into CompatibilityData at import time
│   │   ├── load-catalogs.ts          # Merges per-catalog files into the catalogs CompatibilityData
│   │   ├── catalogs.test.ts          # Structural tests for the catalogs dataset (extension point: EXPECTED_CATALOG_IDS)
│   │   ├── catalogs/                 # Catalogs openness-rubric dataset
│   │   │   ├── features.json         # 3 spec-support features + 6 rubric criteria
│   │   │   └── <catalog-id>/<catalog-id>.json   # One folder+file per catalog (10 today)
│   │   └── platforms/                # Nested per-vendor / per-engine platform + support data
│   │       ├── aws/                  # split first by S3 mode, then by engine
│   │       │   ├── s3buckets/        # AWS in S3-buckets mode (exported as `data`)
│   │       │   │   ├── athena/athena.json
│   │       │   │   ├── emr/emr.json
│   │       │   │   ├── glue/glue.json
│   │       │   │   ├── managed-flink/managed-flink.json
│   │       │   │   ├── redshift-s3/redshift-s3.json
│   │       │   │   └── firehose/firehose.json        # staged, NOT imported
│   │       │   └── s3tables/         # AWS in S3-Tables mode (exported as `dataS3Tables`)
│   │       │       ├── athena/athena.json
│   │       │       ├── emr/emr.json
│   │       │       ├── glue/glue.json
│   │       │       ├── managed-flink/managed-flink.json
│   │       │       ├── redshift-s3/redshift-s3.json
│   │       │       └── firehose/firehose.json        # staged, NOT imported
│   │       ├── gcp/                  # bigquery/, dataproc/
│   │       ├── azure/                # fabric/ (synapse removed: Microsoft directs Synapse Spark to Fabric)
│   │       ├── databricks/           # databricks/
│   │       ├── snowflake/            # split by Iceberg storage mode:
│   │       │   ├── managed/snowflake/snowflake.json   # Snowflake-provided storage
│   │       │   └── external/snowflake/snowflake.json  # external volume (customer S3)
│   │       └── oss/                  # duckdb/, clickhouse/, daft/, spark/,
│   │                                 # spark-gluten/, spark-comet/, flink/,
│   │                                 # trino/, pyiceberg/, doris/, databend/,
│   │                                 # kafka-connect/ (staged, NOT imported)
│   ├── utils/                # Pure utility functions
│   │   ├── comparison.ts             # Comparison logic between platforms
│   │   ├── filters.ts                # Filter/search logic
│   │   └── support.ts                # Support level helpers
│   ├── types.ts              # TypeScript type definitions
│   ├── App.tsx               # Root app component
│   ├── App.test.tsx          # App-level tests
│   ├── main.tsx              # Entry point
│   ├── index.css             # Tailwind directives + global styles
│   └── test-setup.ts         # Vitest setup (jest-dom matchers)
├── tests/                    # Engine feature-test suites (measure the matrix cells for real)
│   ├── spark_fixture.py              # Shared helper: Spark/Java-API/DuckDB fixtures in the REST catalog
│   ├── iceberg_feature_tests.py      # Spark (OSS + AWS EMR/Glue modes)
│   ├── duckdb_feature_tests.py, pyiceberg_feature_tests.py, daft_feature_tests.py,
│   ├── clickhouse_feature_tests.py, flink_feature_tests.py, trino_feature_tests.py   # other OSS engines
│   ├── databricks_/snowflake_/redshift_feature_tests.py     # cloud engines (need creds)
│   └── docker/                       # Polaris + RustFS stack (docker-compose.polaris.yml,
│                                     # start/stop-polaris.sh, env.sh) and Flink cluster
├── index.html                # Vite HTML entry
├── vite.config.ts            # Vite + Vitest config
├── tailwind.config.js        # Tailwind configuration
├── tsconfig.json             # TypeScript project references
├── eslint.config.js          # ESLint flat config
└── package.json
```

## Tech Stack

- React 19.2 + TypeScript 5.9
- Vite 8 (build + dev server)
- Tailwind CSS 3
- Vitest 4 + Testing Library + fast-check (property-based tests)
- ESLint 9 (flat config)
- GitHub Actions (CI on PRs, deploy on push to main)

## Available Scripts

| Command          | Description                              |
|------------------|------------------------------------------|
| `npm run dev`    | Start Vite dev server                    |
| `npm run build`  | Type-check + production build            |
| `npm run lint`   | Run ESLint                               |
| `npm test`       | Run Vitest (single run)                  |
| `npm run preview`| Preview production build locally         |

## Data Model

Support data follows the key pattern `{platformId}:{featureId}:{version}` (e.g. `aws-athena:read-support:v2`).

Each entry has:
- `level`: `"full"` | `"partial"` | `"none"` | `"unknown"`
- `notes`: Human-readable explanation
- `caveats`: Array of strings with limitations
- `links`: official references if available

Engine platforms are grouped by: AWS, GCP, Azure, Databricks, Snowflake, 3rd Party. Catalogs are grouped by: Proprietary, Open Source.

Engine features are categorized into: row-level-operations, schema-management, partitioning, table-management, read-write, catalog-support, v3-data-types, v3-advanced. Catalog features all use the single category: openness-rubric.

Versions: engine support entries use `v2`/`v3`. The catalogs dataset has no version dimension, so all its entries use the single synthetic version `current` (e.g. `snowflake-horizon:managed-offering:current`). The UI hides version chrome when the version is `current` (no V2/V3 version chips, no per-row version badge, no version line in the popover).

## Data Architecture

Platform data lives under `src/data/platforms/` in a nested per-vendor / per-engine hierarchy. There are exactly six vendor folders: `aws`, `gcp`, `azure`, `databricks`, `snowflake`, and `oss` (all lowercase).

### Per-engine layout

Each vendor folder holds one subfolder per engine, and each engine subfolder contains a single JSON file named after the engine (e.g. `gcp/bigquery/bigquery.json`). An engine file mirrors the vendor-file shape but holds exactly one engine's data:

```jsonc
{
  "platforms": [ /* exactly one Platform object */ ],
  "support":    { /* only this engine's "{id}:{featureId}:{version}" entries */ }
}
```

### Engine subfolder naming (vendor-prefix omission)

The engine subfolder name is derived from the platform `id` by dropping the leading vendor prefix and its trailing hyphen:

- `google-bigquery` → `bigquery`, `aws-redshift-s3` → `redshift-s3`, `aws-managed-flink` → `managed-flink`.
- An id without a vendor prefix keeps its name: `duckdb` → `duckdb`, `snowflake` → `snowflake`, `kafka-connect` → `kafka-connect`.

Two engines within the same vendor must never derive to the same subfolder name.

### AWS dual-mode layout

AWS is split first by S3 mode and then by engine. `aws/s3buckets/` and `aws/s3tables/` each contain the same per-engine subfolders — `athena`, `emr`, `glue`, `managed-flink`, `redshift-s3` (plus the staged `firehose`). The two modes hold separate compatibility data for S3 buckets vs S3 Tables.

### How the loader merges the data

`src/data/load-data.ts` uses explicit static imports of each engine's JSON file (no `import.meta.glob`) and concatenates them in a fixed order:

- AWS engines first, in the order `athena`, `emr`, `glue`, `managed-flink`, `redshift-s3`.
- Then the non-AWS vendors in the order `gcp`, `azure`, `databricks`, `snowflake`, `oss`.

It exports four `CompatibilityData` datasets, one per (AWS mode × Snowflake mode) combination: `data` (S3 buckets + Snowflake managed), `dataS3Tables` (S3 Tables + Snowflake managed), `dataSnowflakeExternal` (S3 buckets + Snowflake external) and `dataS3TablesSnowflakeExternal` (S3 Tables + Snowflake external), plus a `getEngineData(awsS3Mode, snowflakeMode)` selector the app uses. `features` and `versions` come unchanged from `features.json`. The fixed import order makes the merged platform order and support map deterministic, independent of filesystem enumeration.

### Snowflake dual-storage layout

Snowflake mirrors the AWS dual-mode pattern, split by Iceberg storage option rather than by engine: `snowflake/managed/snowflake/snowflake.json` (Snowflake-provided storage, `EXTERNAL_VOLUME = SNOWFLAKE_MANAGED`) and `snowflake/external/snowflake/snowflake.json` (customer cloud storage through an external volume). Both files carry the same platform id `snowflake`, so filters and the matrix column survive the toggle; the mode is expressed by which file the merge includes in the Snowflake slot (between `databricks` and the OSS block). The UI switch is a pill in the Snowflake group header (Snowflake / External), state owned by `App.tsx` as `snowflakeMode` (default `"snowflake"`), gated exactly like the AWS toggle (engines view only, hidden when Snowflake platforms are filtered out).

### Staged-but-excluded engines

Firehose (`aws/s3buckets/firehose` and `aws/s3tables/firehose`) and Kafka Connect (`oss/kafka-connect`) are stored in the structure but deliberately excluded from both datasets — the loader simply does not import their files. As a result, neither `aws-firehose` nor `kafka-connect` appears in `data` or `dataS3Tables`, and neither is rendered by the app.

> Note: the original flat per-vendor files (`aws.json`, `aws-tables.json`, `gcp.json`, etc.) have been removed. The nested structure is the only source of truth that `load-data.ts` reads from.

### Catalogs dataset

The catalogs view has its own dataset under `src/data/catalogs/`, independent from the engines data:

- `catalogs/features.json` defines 9 features under the single version `current`: 3 spec-compliance features with category `spec-support` (`rest-spec-support` — Iceberg REST Catalog spec coverage, `v2-spec-support`, `v3-spec-support` — table-format version support) followed by the 6 openness-rubric criteria with category `openness-rubric` (`managed-offering`, `serves-own-irc`, `rw-foreign-irc`, `federation-broker`, `multi-engine-multi-cloud`, `governance-cred-vending`).
- Each catalog lives in `catalogs/<catalog-id>/<catalog-id>.json` with exactly one platform object and that catalog's 9 support entries keyed `<catalog-id>:<feature-id>:current`. Every cell carries a `links` entry pointing at its public source.
- Catalog ids today — Proprietary: `snowflake-horizon`, `aws-glue-data-catalog`, `databricks-unity`, `google-lakehouse-runtime-catalog`, `microsoft-onelake`. Open Source: `apache-polaris`, `apache-gravitino`, `lakekeeper`, `project-nessie`, `unity-catalog-oss`.
- `src/data/load-catalogs.ts` mirrors `load-data.ts`: explicit static imports concatenated in a fixed order (Proprietary block first, then Open Source; within each group, source-scorecard score descending) and exports `dataCatalogs: CompatibilityData`. Groups must stay contiguous — the matrix group-header row spans consecutive columns.
- Ratings and notes are based on [Iceberg: The State of Catalogs](https://gitlab.com/hunt.dan/iceberg-state-of-catalogs); the source rubric's `no_engine` scores map to level `none` with an explanatory caveat.
- `src/data/catalogs.test.ts` structurally validates the dataset (naming convention, full feature coverage, valid levels, `{label, url}` links) and that the merged `dataCatalogs` matches the files on disk. Its `EXPECTED_CATALOG_IDS` array is the extension point.

### View wiring

`App.tsx` holds a `viewMode` state (`"engines" | "catalogs"`, default engines) switched by `ViewToggle` in the header (the only control in the header now). Each view keeps its own independent `FilterState` (engines starts at `selectedVersions: ["v2","v3"]`, catalogs at `["current"]`), so filters survive toggling and never leak across views. The catalogs view reuses `CompatibilityMatrix`, `FilterPanel` (with `entityLabel="Catalogs"`), and `DetailPopover`, but renders no AWS S3-mode toggle. The Iceberg version is a **multi-select chip group inside `FilterPanel`** ("VERSION", styled like the Platform/Category/Support chips), not a header switcher. By default **both V2 and V3 are selected** (`initialEngineFilters.selectedVersions: ["v2","v3"]`). The last selected version cannot be deselected, so the grid never loses its version dimension, and the chips are hidden in the catalogs view (whose single synthetic `current` version has no v2/v3 choice).

**One column per engine (not per version).** The grid renders a single column per platform; the version dimension lives *inside* each cell. `FeatureRow` restricts a cell's versions to `selectedVersions ∩ applicableVersions(feature, allVersions)` (via `src/utils/versions.ts`), then hands that version→entry list to `SupportCell`. `SupportCell` renders a **solid** cell when every version agrees (the common case) and a **transition cell** only when they differ: one segment per version laid out left to right (`V2 ✓ Full → V3 ✗ None`), each tinted with its own status and labelled with its version, with an arrow badge on every boundary and a purple frame. Segments share the cell width equally and use short labels (`Part.`, `Unk.`; full words stay in the aria-label/tooltip) so a transition cell is exactly as wide as a solid one. **All engine columns have the same fixed width**: the table uses `table-layout: fixed` with widths from `src/components/matrixLayout.ts` (`NAME_COL_WIDTH` 176, `ENGINE_COL_WIDTH` 110, the minimum that fits a two-segment cell), so a difference never widens its column. Because of the `applicableVersions` intersection, a V3-only feature (Deletion Vectors, Lineage Tracking, Variant Type) shows a single solid V3 cell even with both versions selected, never a bogus "V2 None / V3 Full" split. The frame, arrow and per-segment labels mean a difference is identifiable without relying on colour, and the row extends to 3+ segments when V4 lands.

Comparison is a **separate opt-in**, decoupled from version selection: a `compareMode` boolean on `FilterState` (default `false`) toggled by a **Compare** button next to the version chips. When on, `ComparisonSummary` renders a **differences-only table** (columns `Feature | Engine | <each selected version> | Change`) listing every (feature, engine) whose support level is not identical across the versions the feature applies to; agreeing pairs and single-version (e.g. V3-only) features are omitted. The rows come from `computeDifferences` in `src/utils/comparison.ts`, which intersects each feature with `applicableVersions`, marks non-applicable version columns `null` (rendered "—"), and classifies the move as Gained / Lost / Changed — a transition touching `unknown` is neutral "Changed", not an over-claimed gain/loss. It renders only when `compareMode && selectedVersions.length > 1`; the Compare button is disabled+greyed below two versions, and `compareMode` is cleared automatically when the selection drops to one. So "both versions selected" shows the single-column grid (with split cells) and no table until Compare is pressed.

Each feature row also shows a version badge next to its name ("Positional Deletes V2/V3", "Deletion Vectors V3") computed by `src/utils/versions.ts` from the feature's `introducedIn` plus the optional `availableVersions`/`removedIn` bounds — so a future v4 (e.g. equality deletes being V2/V3-only) is a data-only change.

## Best Practices

### Adding a New Platform or Engine

1. Create the engine subfolder under its vendor folder using the prefix-less name (e.g. platform `google-bigquery` → `src/data/platforms/gcp/bigquery/`). For AWS, create it under both `aws/s3buckets/<engine>/` and `aws/s3tables/<engine>/`.
2. Add the engine's JSON file named after the subfolder (e.g. `bigquery/bigquery.json`) containing a single platform object under `"platforms"` and a support entry for every feature × version combination under `"support"`.
3. Wire the new engine into `src/data/load-data.ts`: add an explicit static `import` for the engine JSON file and append it to the correct ordered array — `awsBucketsEngines` and `awsTablesEngines` for an AWS engine (both S3 modes), or `nonAwsEngines` for the other vendors — at the right position in the merge order (AWS engines first in the order athena, emr, glue, managed-flink, redshift-s3; then gcp, azure, databricks, snowflake, oss).
4. If the platform needs a logo, add an SVG to `public/logos/`.
5. Run `npm run build` and `npm test` to verify nothing breaks.

### Adding a New Feature

1. Add the feature definition to `src/data/features.json`.
2. Add support entries for the new feature across all existing platforms in each engine's JSON file under `src/data/platforms/`.
3. Remember about adding caveats or links where relevant ( official docs).

### Adding a New Catalog

1. Create `src/data/catalogs/<catalog-id>/<catalog-id>.json` with one platform object (`group` either `"Proprietary"` or `"Open Source"`; `category` `"cloud"` for proprietary, `"open-source"` for OSS) and one support entry per rubric feature, keyed `<catalog-id>:<feature-id>:current`. Give every cell a `Source` link.
2. Add the catalog id to `EXPECTED_CATALOG_IDS` in `src/data/catalogs.test.ts`.
3. Wire it into `src/data/load-catalogs.ts`: add a static `import` and slot it into `catalogFiles` inside its group's block (groups must stay contiguous; order within a group is score descending).
4. Optionally register a logo in the `PLATFORM_LOGOS` maps in `CompatibilityMatrix.tsx` and `FilterPanel.tsx` if a suitable file exists in `public/logos/`.
5. Run `npm test` — the structural suite catches missing coverage, bad keys, or a file that exists on disk but is missing from the loader.

### Adding a New Rubric Criterion

1. Add the feature to `src/data/catalogs/features.json` (category `"openness-rubric"`, `introducedIn`/`versions` `"current"`).
2. Add a support entry for it to every catalog file. The tests enforce full feature × catalog coverage.

### Code Conventions

- Components are functional React with hooks, no class components.
- All data types live in `src/types.ts`.
- Utility functions are pure (no side effects) and live in `src/utils/`.
- Tailwind for styling — no CSS modules or styled-components.
- Tests use Vitest + Testing Library. Property-based tests use fast-check.

### CI/CD

- PRs trigger lint → test → build validation.
- Pushes to `main` trigger test → build → deploy to GitHub Pages.
- The deploy workflow sets a `BASE_URL` env var, but it has no effect: `vite.config.ts` hard-codes `base: '/'`, which is what the custom domain needs. The site is served from the apex, not a repo subpath.
- Node 24 has to be kept in two independent places, which is easy to get wrong:
  1. `node-version: 24` on `actions/setup-node` — the Node that runs *our* build and tests.
  2. The action versions themselves — each action declares its own `runs.using`, and older majors are pinned to the deprecated `node20`. Setting `node-version` does nothing for these.
  Both are on Node 24 today. When bumping, check the runtime rather than assuming a version number implies it: `curl -s https://raw.githubusercontent.com/<owner>/<repo>/<tag>/action.yml | grep 'using:'`. Do not fall back to `ACTIONS_ALLOW_USE_UNSECURE_NODE_VERSION`.

### Data Integrity

- Data is split into nested per-vendor / per-engine files under `src/data/platforms/` and merged at import time by `src/data/load-data.ts`. The nested structure is the single source of truth that `load-data.ts` reads from; there is no aggregated JSON file to keep in sync.
- Each engine file holds exactly one platform object plus only that platform's support entries. The loader concatenates the engine files in a fixed order, so the merged platform order and support map are deterministic regardless of filesystem enumeration.
- Firehose and Kafka Connect are staged in the structure but excluded from both datasets (their files are never imported), so they are not rendered.
- Feature definitions live in `src/data/features.json` (single source of truth for engine features and versions). The catalogs view has its own `src/data/catalogs/features.json`; keep them separate — external tooling counts every feature in the engines file for coverage.
- The catalogs dataset follows the same rules via `src/data/load-catalogs.ts`: one file per catalog, fixed merge order, groups contiguous. `src/data/catalogs.test.ts` enforces its structure.
- Every platform must have entries for all features × all versions. Missing entries will show as blank cells in the matrix.
- Use `"unknown"` level when a platform hasn't announced support for a feature yet, rather than omitting the entry.

### Engine Feature Tests (`tests/`)

- Every OSS suite runs against one shared Iceberg REST catalog: Apache Polaris 1.7 + RustFS from `tests/docker/docker-compose.polaris.yml` (`./tests/docker/start-polaris.sh` / `stop-polaris.sh`; `source tests/docker/env.sh` for the env vars). Catalog `demo`, REST base `http://127.0.0.1:8181/api/catalog`, OAuth2 client credentials `root:s3cr3t` with scope `PRINCIPAL_ROLE:ALL`, RustFS `rustfs:rustfs12345` on `:9000`. Polaris (unlike the old Lakekeeper stack) accepts V3 GEOMETRY schemas; `DROP_WITH_PURGE_ENABLED` is on because Flink's DROP TABLE purges.
- `tests/spark_fixture.py` is the shared producer: a cached local PySpark session on that catalog (`create_fixture` for merge-on-read / copy-on-write tables, `write_equality_delete` / `create_equality_delete_fixture` using the Iceberg Java API through the Spark JVM, `create_column_default_fixture`, storage-level inspectors) plus DuckDB-backed producers (`create_geometry_fixture`, `create_timestamp_ns_fixture`) for the V3 columns Spark's connector cannot write. Suites measure the half they can exercise (e.g. an engine without DML measures whether it *reads* an equality delete) instead of hardcoding `skip`/`fail`; a test may only report `skip` when the dependency is genuinely absent (no Spark, no AWS creds).
- Run suites one at a time; Spark + Flink Docker concurrently OOM-kills the Flink containers (exit 137) and produces an invalid run. Compile with Python 3.11 (`uv run --python 3.11 python -m py_compile tests/<suite>.py`) — 3.11 rejects backslashes in f-string expressions.
- Each suite exits non-zero on `discrepancies > 0 || errors > 0`. A discrepancy is a measured result that disagrees with the matrix JSON; treat it as a proposed matrix edit to confirm with the user, never as a test bug to silence.
- Workflows `iceberg-`, `duckdb-`, `pyiceberg-`, `daft-`, `flink-`, `clickhouse-`, `trino-tests.yml` start Polaris, install Java 17 + PySpark + the Iceberg 1.11 jars where the fixture is needed, and post the report as a PR comment.
- Trino (`trino_feature_tests.py`) runs in Docker (`tests/docker/start-trino.sh`, `docker-compose.trino.yml`, `trinodb/trino` image) with an `iceberg` catalog set to `iceberg.catalog.type=rest` against the shared Polaris; it is driven over HTTP with the `trino` Python client. Its connector speaks REST natively, so most cells are exercised in Trino SQL and only the V3 columns / equality deletes it cannot write come from the Spark/DuckDB fixtures. Compared against `src/data/platforms/oss/trino/trino.json` (Trino 483); the loader tolerates that file being absent, in which case every cell reads as `unknown`.
