# Iceberg Compatibility Matrix

An interactive compatibility matrix for [Apache Iceberg](https://iceberg.apache.org/) features across cloud platforms and open-source engines. Filter by platform, feature category, Iceberg version (V2/V3), and support level to quickly find what's supported where.

**Live site →** deployed via GitHub Pages on every push to `main`.

## Quick Start

```bash
# Install dependencies
npm install

# Start the dev server
npm run dev

# Run tests
npm test

# Lint
npm run lint

# Production build
npm run build
```

## Tech Stack

- React 19.2, TypeScript, Vite 8
- Tailwind CSS 3
- Vitest + Testing Library
- GitHub Actions (CI + deploy)

## Project Structure

```
src/
├── components/       # React UI components (matrix, filters, popovers, etc.)
├── data/             # JSON data files (platforms, features, support entries)
├── utils/            # Pure utility functions (filters, comparison, support helpers)
├── types.ts          # TypeScript type definitions
└── App.tsx           # Root component
public/logos/         # SVG logos for platforms and engines
```

Data lives under `src/data/platforms/` in a nested per-vendor / per-engine structure — each engine has its own file (e.g. `oss/duckdb/duckdb.json`, `gcp/bigquery/bigquery.json`). AWS is split further by S3 mode (`aws/s3buckets/<engine>/` and `aws/s3tables/<engine>/`). `src/data/load-data.ts` imports the engine files and merges them, at import time, into two datasets (`data` for S3-buckets, `dataS3Tables` for S3 Tables). Feature and version definitions live in `src/data/features.json`.

## Contributing

Contributions are welcome! Please open a pull request against `main`.

### Adding a New Engine or Platform

1. Create the engine's folder under its vendor (using the prefix-less engine name, e.g. platform `google-bigquery` → `src/data/platforms/gcp/bigquery/`). For AWS, add it under both `aws/s3buckets/<engine>/` and `aws/s3tables/<engine>/`.
2. Add the engine JSON file named after the folder (e.g. `bigquery/bigquery.json`) with a single platform object under `"platforms"` and a support entry for **every feature × version** combination under `"support"`. Use `"unknown"` for features the platform hasn't announced yet — don't omit entries.
3. Wire the engine into `src/data/load-data.ts`: add a static `import` for the JSON file and append it to the correct ordered array (AWS engines, or the non-AWS vendors).
4. Drop an SVG logo into `public/logos/` and wire it up in the `PLATFORM_LOGOS` map in `CompatibilityMatrix.tsx` and `FilterPanel.tsx`.
5. Run `npm test && npm run build` to verify nothing breaks.

### Adding a New Feature

1. Add the feature definition to `src/data/features.json`.
2. Add support entries for the new feature across **all** existing engines, in each engine's JSON file under `src/data/platforms/`.

### Updating Existing Data

If a platform's support level changes (e.g. a feature goes from `"partial"` to `"full"`), update that engine's JSON file under `src/data/platforms/`. Include a link to the announcement or docs in the `"notes"` field.

```
"aws-athena:read-support:v2": {
  "level": "full",
  "notes": "Full read support via Athena engine v3",
  "caveats": [],
  "links": [
    { "label": "Athena Iceberg docs", "url": "https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html" }
  ]
}
```


### Pull Request Guidelines

- Fork the repo and create a feature branch (`git checkout -b add-my-engine`).
- Keep PRs focused — one engine or one feature per PR is ideal.
- Make sure CI passes (lint → test → build) before requesting review.
- Include a brief description of what changed and why.

### Data Model Reference

Support entries are keyed as `{platformId}:{featureId}:{version}` (e.g. `aws-athena:read-support:v2`).

Each entry has:
- `level` — `"full"` | `"partial"` | `"none"` | `"unknown"`
- `notes` — human-readable explanation
- `caveats` — array of strings with limitations
- `links` - Array of public links with docs

## Running the Iceberg Feature Tests

The repo includes a PySpark-based test suite that validates Iceberg features against an actual local Spark+Iceberg environment and compares results with the JSON support data.

### Prerequisites

- Java 17
- Python 3.11+
- PySpark 4.1.2 (installed via `tests/requirements.txt`)

> The suite exercises **both** Iceberg format-version 2 and version 3, so it
> requires an Iceberg runtime that supports V3 (VARIANT, geometry, deletion
> vectors, row lineage). Use Iceberg **1.11.0+** with Spark 4.1 / Scala 2.13.

### Local Execution

```bash
# Install dependencies
pip install -r tests/requirements.txt

# Download the Iceberg Spark 4.1 (Scala 2.13) runtime JAR
ICEBERG_VERSION=1.11.0
curl -fSL -o "iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar"

# Run the tests (auto-detects the JAR, or falls back to Maven coordinates).
# Override the version with ICEBERG_VERSION=... if needed.
python tests/iceberg_feature_tests.py
```

The suite also enforces **matrix coverage**: every feature defined in
`src/data/features.json` must have a corresponding `test_*` function registered
in `ALL_TESTS`. If a feature is added to the matrix without a test, the run
fails with a list of uncovered features.

Reports are written to `test-reports/` as both JSON and Markdown.

### CI

Tests run automatically on pull requests via the **Iceberg Feature Tests** GitHub Actions workflow. The workflow:
- Sets up Java 17, Python 3.11, PySpark, and the Iceberg runtime JAR
- Runs all feature tests in Spark local mode
- Uploads the report as a build artifact
- Posts a summary comment on PRs

You can also trigger the workflow manually via `workflow_dispatch`.

## License

This project is licensed under the [MIT License](./LICENSE).
