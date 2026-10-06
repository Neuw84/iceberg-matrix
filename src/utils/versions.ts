import type { Feature, Version } from "../types";

/**
 * The spec versions a feature applies to, in the order given by the dataset's
 * version list.
 *
 * Resolution order:
 *   1. If the feature lists `availableVersions` explicitly, use those (filtered
 *      to the dataset's versions, kept in dataset order). This is the escape
 *      hatch for a feature that is not carried forward unchanged — e.g. once v4
 *      lands, equality deletes being V2/V3-only is `availableVersions:
 *      ["v2","v3"]`, so it reads "V2/V3" rather than gaining "V4".
 *   2. Else every version from `introducedIn` up to (but not including)
 *      `removedIn`, if a `removedIn` bound is given.
 *   3. Else every version from `introducedIn` onward — the default, so a v2
 *      feature reads "V2/V3" today and would read "V2/V3/V4" automatically when
 *      v4 is added to the dataset's `versions`.
 *
 * The catalogs view's synthetic "current" version is handled by callers (they
 * pass a single-version list and suppress the badge), so it never reaches here
 * in a way that would mislabel a rubric row.
 */
export function applicableVersions(
  feature: Feature,
  allVersions: Version[],
): Version[] {
  // 1. Explicit list wins, intersected with (and ordered by) the dataset.
  if (feature.availableVersions && feature.availableVersions.length > 0) {
    const explicit = new Set(feature.availableVersions);
    return allVersions.filter((v) => explicit.has(v));
  }

  const start = allVersions.indexOf(feature.introducedIn);
  if (start === -1) {
    // introducedIn is not in this dataset's version list (e.g. the catalogs
    // view): the feature applies to whatever versions the dataset has.
    return allVersions;
  }

  // 2. Bounded above by removedIn (exclusive), when present and known.
  if (feature.removedIn) {
    const end = allVersions.indexOf(feature.removedIn);
    if (end !== -1) return allVersions.slice(start, Math.max(start, end));
  }

  // 3. Default: introducedIn onward.
  return allVersions.slice(start);
}

/**
 * A compact badge for a feature's applicable versions, e.g. "V2/V3" or "V3".
 * Returns an empty string when there is nothing meaningful to show (a single
 * synthetic version such as the catalogs "current", where a badge would be
 * noise rather than information).
 */
export function versionBadge(feature: Feature, allVersions: Version[]): string {
  // The catalogs view has one synthetic version ("current"); a badge there is
  // meaningless, so suppress it.
  if (allVersions.length <= 1 || allVersions.includes("current")) return "";
  const versions = applicableVersions(feature, allVersions);
  if (versions.length === 0) return "";
  return versions.map((v) => v.toUpperCase()).join("/");
}
