// Served by the testCoverage plugin in vite.config.ts.
declare module "virtual:test-coverage" {
  import type { SuiteCoverage } from "./data/test-coverage";
  const coverage: SuiteCoverage[];
  export default coverage;
}
