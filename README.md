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

The runtime data source is `src/data/compatibility-data.json`. Vendor-specific files (`aws.json`, `gcp.json`, etc.) exist for organizational clarity and must be kept in sync with the aggregated file.

## Contributing

Contributions are welcome! Please open a pull request against `main`.

### Adding a New Engine or Platform

1. Add the platform object to the relevant `src/data/{vendor}.json` file under `"platforms"`.
2. Add support entries for **every feature × version** combination in the same file under `"support"`. Use `"unknown"` for features the platform hasn't announced yet — don't omit entries.
3. Add the same platform and support entries to `src/data/compatibility-data.json`.
4. Drop an SVG logo into `public/logos/` and wire it up in the `PLATFORM_LOGOS` map in `CompatibilityMatrix.tsx` and `FilterPanel.tsx`.
5. Run `npm test && npm run build` to verify nothing breaks.

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

## License

This project is licensed under the [MIT License](./LICENSE).
