import type { Feature, Platform, SupportEntry, Version } from "../types";
import { SupportCell } from "./SupportCell";

interface FeatureRowProps {
  feature: Feature;
  platforms: Platform[];
  versions: Version[];
  getSupportEntry: (platformId: string, featureId: string, version: Version) => SupportEntry;
  onCellClick: (platform: Platform, feature: Feature, version: Version, entry: SupportEntry) => void;
}

export function FeatureRow({
  feature,
  platforms,
  versions,
  getSupportEntry,
  onCellClick,
}: FeatureRowProps) {
  return (
    <tr className="border-b border-gray-100 hover:bg-blue-50/30 transition-colors">
      <td className="sticky left-0 bg-white px-3 py-1.5 text-xs text-gray-800 font-medium z-10 border-r border-gray-100" style={{ width: 180, minWidth: 180 }}>
        {feature.name}
      </td>
      {platforms.map((platform) =>
        versions.map((version) => {
          const entry = getSupportEntry(platform.id, feature.id, version);
          return (
            <td
              key={`${platform.id}:${version}`}
              className="px-1 py-1 text-center border-x border-gray-50"
              role="gridcell"
            >
              <SupportCell
                entry={entry}
                onClick={() => onCellClick(platform, feature, version, entry)}
              />
            </td>
          );
        })
      )}
    </tr>
  );
}
