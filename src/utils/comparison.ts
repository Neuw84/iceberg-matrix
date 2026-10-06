import type {
  CompatibilityData,
  Feature,
  Platform,
  SupportLevel,
  Version,
} from "../types";
import { getSupportEntry } from "./support";
import { applicableVersions } from "./versions";

/**
 * The support level of a (feature, platform) across each of the given versions,
 * positional against the `versions` argument the table renders. A `null` entry
 * means the feature does not exist in that version (e.g. the V2 column of a
 * V3-only feature), so the table renders it blank rather than as "None".
 */
export interface DifferenceRow {
  feature: Feature;
  platform: Platform;
  /** Support level per version, in `versions` order; null where N/A. */
  levels: (SupportLevel | null)[];
}

/** Whether two or more of the present (non-null) levels disagree. */
function levelsDiffer(levels: (SupportLevel | null)[]): boolean {
  const present = levels.filter((l): l is SupportLevel => l !== null);
  return present.length >= 2 && present.some((l) => l !== present[0]);
}

/**
 * Every (feature, platform) pair whose support level is NOT identical across
 * the versions the feature applies to — the rows that populate the Compare
 * differences table. Rows are produced feature-major, then platform, matching
 * the matrix's visual order. Pairs that agree (the common case) are omitted, as
 * are features that exist in only one of the selected versions (a V3-only
 * feature has nothing to compare, so its absent-in-V2 column is not treated as
 * a difference).
 */
export function computeDifferences(
  data: CompatibilityData,
  platforms: Platform[],
  features: Feature[],
  versions: Version[],
): DifferenceRow[] {
  if (versions.length < 2) return [];
  const rows: DifferenceRow[] = [];
  for (const feature of features) {
    // Only the versions this feature actually exists in count toward a
    // difference; the rest render as a blank (null) column.
    const applicable = new Set(applicableVersions(feature, data.versions));
    for (const platform of platforms) {
      const levels = versions.map((v) =>
        applicable.has(v)
          ? getSupportEntry(data, platform.id, feature.id, v).level
          : null,
      );
      if (levelsDiffer(levels)) {
        rows.push({ feature, platform, levels });
      }
    }
  }
  return rows;
}
