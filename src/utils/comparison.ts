import type { CompatibilityData, Version } from "../types";
import { getSupportEntry } from "./support";

export function computeComparison(
  data: CompatibilityData,
  platformId: string,
  versionA: Version,
  versionB: Version
): { gained: number; lost: number; changed: number } {
  let gained = 0;
  let lost = 0;
  let changed = 0;

  for (const feature of data.features) {
    const a = getSupportEntry(data, platformId, feature.id, versionA).level;
    const b = getSupportEntry(data, platformId, feature.id, versionB).level;

    if (a === b) continue;

    if (a === "none" && b !== "none") {
      gained++;
    } else if (a !== "none" && b === "none") {
      lost++;
    } else {
      changed++;
    }
  }

  return { gained, lost, changed };
}
