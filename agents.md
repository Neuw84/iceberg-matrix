# Iceberg Compatibility Matrix — Project Guide

## Project Overview

A React single-page application that displays an interactive compatibility matrix for Apache Iceberg features across cloud platforms and open-source engines. Built with Vite, TypeScript, React 19, and Tailwind CSS. Deployed to GitHub Pages via GitHub Actions.

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
│   │   ├── ComparisonSummary.tsx     # Side-by-side comparison view
│   │   ├── DetailPopover.tsx         # Popover with support details
│   │   ├── FeatureRow.tsx            # Single feature row in the matrix
│   │   ├── FilterPanel.tsx           # Sidebar filters (version, platform, category, etc.)
│   │   ├── SupportCell.tsx           # Individual cell showing support level
│   │   └── VersionTabs.tsx           # V2/V3 version tab switcher
│   ├── data/                 # JSON data files
│   │   ├── features.json             # Feature definitions and categories
│   │   ├── load-data.ts              # Merges nested per-engine files into CompatibilityData at import time
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
│   │       ├── azure/                # synapse/, fabric/
│   │       ├── databricks/           # databricks/
│   │       ├── snowflake/            # snowflake/
│   │       └── oss/                  # duckdb/, clickhouse/, daft/, spark/, flink/,
│   │                                 # pyiceberg/, doris/, databend/,
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
├── index.html                # Vite HTML entry
├── vite.config.ts            # Vite + Vitest config
├── tailwind.config.js        # Tailwind configuration
├── tsconfig.json             # TypeScript project references
├── eslint.config.js          # ESLint flat config
└── package.json
```

## Tech Stack

- React 19 + TypeScript 5.9
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

Platforms are grouped by: AWS, GCP, Azure, Databricks, Snowflake, 3rd Party.

Features are categorized into: row-level-operations, schema-management, partitioning, table-management, read-write, catalog-support, v3-data-types, v3-advanced.

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

It exports two `CompatibilityData` datasets: `data` (built from `aws/s3buckets` + the non-AWS vendors) and `dataS3Tables` (built from `aws/s3tables` + the non-AWS vendors). `features` and `versions` come unchanged from `features.json`. The fixed import order makes the merged platform order and support map deterministic, independent of filesystem enumeration.

### Staged-but-excluded engines

Firehose (`aws/s3buckets/firehose` and `aws/s3tables/firehose`) and Kafka Connect (`oss/kafka-connect`) are stored in the structure but deliberately excluded from both datasets — the loader simply does not import their files. As a result, neither `aws-firehose` nor `kafka-connect` appears in `data` or `dataS3Tables`, and neither is rendered by the app.

> Note: the original flat per-vendor files (`aws.json`, `aws-tables.json`, `gcp.json`, etc.) are retained temporarily for verification and will be removed later. The nested structure is the source of truth that `load-data.ts` reads from.

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

### Code Conventions

- Components are functional React with hooks, no class components.
- All data types live in `src/types.ts`.
- Utility functions are pure (no side effects) and live in `src/utils/`.
- Tailwind for styling — no CSS modules or styled-components.
- Tests use Vitest + Testing Library. Property-based tests use fast-check.

### CI/CD

- PRs trigger lint → test → build validation.
- Pushes to `main` trigger test → build → deploy to GitHub Pages.
- The `BASE_URL` env var is set during deploy to handle the repo subpath.
- CI and Deploy workflows run on Node 24 (`actions/setup-node@v4` with `node-version: 24`). Node 20 is deprecated on GitHub Actions runners — keep the build and deploy pipelines on Node 24 and do not fall back to `ACTIONS_ALLOW_USE_UNSECURE_NODE_VERSION`.

### Data Integrity

- Data is split into nested per-vendor / per-engine files under `src/data/platforms/` and merged at import time by `src/data/load-data.ts`. The nested structure is the single source of truth that `load-data.ts` reads from; there is no aggregated JSON file to keep in sync.
- Each engine file holds exactly one platform object plus only that platform's support entries. The loader concatenates the engine files in a fixed order, so the merged platform order and support map are deterministic regardless of filesystem enumeration.
- The original flat per-vendor files are retained temporarily for verification and will be removed later; do not add new data to them.
- Firehose and Kafka Connect are staged in the structure but excluded from both datasets (their files are never imported), so they are not rendered.
- Feature definitions live in `src/data/features.json` (single source of truth for features and versions).
- Every platform must have entries for all features × all versions. Missing entries will show as blank cells in the matrix.
- Use `"unknown"` level when a platform hasn't announced support for a feature yet, rather than omitting the entry.
