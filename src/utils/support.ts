import type { CompatibilityData, SupportEntry, Version } from "../types";

const DEFAULT_ENTRY: SupportEntry = {
  level: "unknown",
  notes: "",
  caveats: [],
};

export function supportKey(
  platformId: string,
  featureId: string,
  version: Version
): string {
  return `${platformId}:${featureId}:${version}`;
}

export function getSupportEntry(
  data: CompatibilityData,
  platformId: string,
  featureId: string,
  version: Version
): SupportEntry {
  const key = supportKey(platformId, featureId, version);
  return data.support[key] ?? { ...DEFAULT_ENTRY };
}
