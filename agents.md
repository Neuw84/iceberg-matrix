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
│   │   ├── load-data.ts              # Merges vendor files into CompatibilityData at import time
│   │   └── platforms/                # Per-vendor platform + support data
│   │       ├── aws.json              # AWS platforms and support entries
│   │       ├── gcp.json              # GCP platforms and support entries
│   │       ├── azure.json            # Azure platforms and support entries
│   │       ├── databricks.json       # Databricks platforms and support entries
│   │       ├── snowflake.json        # Snowflake platforms and support entries
│   │       └── oss.json              # Open-source engines (DuckDB, Spark, Flink, etc.)
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
- Vite 7 (build + dev server)
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

Platforms are grouped by: AWS, GCP, Azure, Databricks, Snowflake, 3rd Party.

Features are categorized into: row-level-operations, schema-management, partitioning, table-management, read-write, catalog-support, v3-data-types, v3-advanced.

## Best Practices

### Adding a New Platform

1. Add the platform object to the relevant `src/data/platforms/{vendor}.json` file under `"platforms"`.
2. Add support entries for every feature × version combination in the same file under `"support"`.
3. If the platform needs a logo, add an SVG to `public/logos/`.
4. Run `npm test` and `npm run build` to verify nothing breaks.

### Adding a New Feature

1. Add the feature definition to `src/data/features.json`.
2. Add support entries for the new feature across all existing platforms in each `src/data/platforms/{vendor}.json` file.

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

### Data Integrity

- Data is split into per-vendor files under `src/data/platforms/` and merged at import time by `src/data/load-data.ts`. There is no single aggregated JSON file to keep in sync.
- Feature definitions live in `src/data/features.json` (single source of truth for features and versions).
- Every platform must have entries for all features × all versions. Missing entries will show as blank cells in the matrix.
- Use `"unknown"` level when a platform hasn't announced support for a feature yet, rather than omitting the entry.
