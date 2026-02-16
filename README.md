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

- React 19, TypeScript, Vite 7
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

Data is split into per-vendor files under `src/data/platforms/` (aws.json, gcp.json, azure.json, databricks.json, snowflake.json, oss.json) and merged at import time by `src/data/load-data.ts`. Feature definitions live in `src/data/features.json`.

## Contributing

Contributions are welcome! Please open a pull request against `main`.

### Adding a New Engine or Platform

1. Add the platform object to the relevant `src/data/platforms/{vendor}.json` file under `"platforms"`.
2. Add support entries for **every feature × version** combination in the same file under `"support"`. Use `"unknown"` for features the platform hasn't announced yet — don't omit entries.
3. Drop an SVG logo into `public/logos/` and wire it up in the `PLATFORM_LOGOS` map in `CompatibilityMatrix.tsx` and `FilterPanel.tsx`.
4. Run `npm test && npm run build` to verify nothing breaks.

### Adding a New Feature

1. Add the feature definition to `src/data/features.json`.
2. Add it to the `"features"` array in `src/data/compatibility-data.json`.
3. Add support entries for the new feature across **all** existing platforms in each vendor JSON and in `compatibility-data.json`.

### Updating Existing Data

If a platform's support level changes (e.g. a feature goes from `"partial"` to `"full"`), update both the vendor-specific JSON and `compatibility-data.json`. Include a link to the announcement or docs in the `"notes"` field.

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

- Java 11 or 17
- Python 3.11+

### Local Execution

```bash
# Install dependencies
pip install -r tests/requirements.txt

# Download the Iceberg Spark runtime JAR
ICEBERG_VERSION=1.7.1
curl -fSL -o "iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"

# Run the tests
python tests/iceberg_feature_tests.py
```

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
