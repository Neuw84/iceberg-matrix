import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import type { Plugin } from 'vite'
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import { SUITES, coveredFeatures } from './src/data/test-coverage'

// Reads the engine feature suites at build time and serves which features each
// one tests as `virtual:test-coverage`, so the Python source never reaches the
// bundle. Edits to a suite reload the dev server.
function testCoverage(): Plugin {
  const id = 'virtual:test-coverage'
  const resolved = '\0' + id
  return {
    name: 'test-coverage',
    resolveId: (source) => (source === id ? resolved : undefined),
    load(loadId) {
      if (loadId !== resolved) return
      const coverage = SUITES.map((suite) => {
        const path = fileURLToPath(new URL(suite.file, import.meta.url))
        this.addWatchFile(path)
        return { ...suite, features: coveredFeatures(readFileSync(path, 'utf8')) }
      })
      return `export default ${JSON.stringify(coverage)}`
    },
  }
}

// https://vite.dev/config/
export default defineConfig({
  base: '/',
  plugins: [react(), testCoverage()],
  build: {
    // The matrix data (all per-engine JSON) is intentionally isolated into its
    // own chunk; ~540 kB raw compresses to ~59 kB gzip and is cached separately
    // from app code. Set the warning limit above it to keep build output clean.
    chunkSizeWarningLimit: 700,
    rollupOptions: {
      output: {
        // Split the large static matrix data (all per-engine JSON) and the
        // React runtime into their own long-term-cacheable chunks so the main
        // app chunk stays small and app-code changes don't bust the data cache.
        manualChunks(id) {
          // Datasets that are fetched on demand each get their own chunk. They
          // must be matched BEFORE the catch-all 'matrix-data' rule below:
          // lumping them into it would make the first load download them all.
          if (id.includes('/src/data/catalogs/') || id.includes('/src/data/load-catalogs')) {
            return 'data-catalogs';
          }
          if (id.includes('/src/data/ingestion/') || id.includes('/src/data/load-ingestion')) {
            return 'data-ingestion';
          }
          if (
            id.includes('/src/data/engine-variants/aws-s3tables') ||
            id.includes('/platforms/aws/s3tables/')
          ) {
            return 'data-aws-s3tables';
          }
          if (
            id.includes('/src/data/engine-variants/snowflake-external') ||
            id.includes('/platforms/snowflake/external/')
          ) {
            return 'data-snowflake-external';
          }
          if (id.includes('/src/data/')) return 'matrix-data';
          if (id.includes('node_modules/react')) return 'react-vendor';
        },
      },
    },
  },
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: ['./src/test-setup.ts'],
    css: true,
    // The App tests render the whole matrix (30+ platforms, ~100 rows) and some
    // click through it, so a single test uses ~2s on a laptop. The 5s default
    // left too little headroom for a slower CI runner: the same tests timed out
    // there. This is only an upper bound, so fast runs are unaffected.
    testTimeout: 30000,

  },
})
