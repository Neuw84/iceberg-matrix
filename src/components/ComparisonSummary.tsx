import type { CompatibilityData, Feature, Platform, Version } from "../types";
import { computeDifferences } from "../utils/comparison";

const LEVEL_STYLE: Record<string, string> = {
  full: "text-green-700",
  partial: "text-amber-700",
  none: "text-red-600",
  unknown: "text-gray-400",
};

const LEVEL_LABEL: Record<string, string> = {
  full: "Full",
  partial: "Partial",
  none: "None",
  unknown: "Unknown",
};

interface ComparisonSummaryProps {
  data: CompatibilityData;
  platforms: Platform[];
  features: Feature[];
  versions: Version[];
}

/**
 * The version-comparison panel: a differences-only table listing every
 * (feature, engine) whose support level is not identical across the selected
 * versions. One row per difference, one column per selected version, plus a
 * plain-language "Change" summary. Pairs that agree across versions are
 * omitted, so the table surfaces exactly what changed.
 */
export function ComparisonSummary({
  data,
  platforms,
  features,
  versions,
}: ComparisonSummaryProps) {
  if (versions.length < 2) return null;

  const rows = computeDifferences(data, platforms, features, versions);

  return (
    <div className="bg-purple-50 border border-purple-200 rounded-lg p-4 mb-4">
      <h3 className="text-sm font-semibold text-purple-800 mb-3">
        Comparison: {versions.map((v) => v.toUpperCase()).join(" → ")}
      </h3>

      {rows.length === 0 ? (
        <p className="text-xs text-gray-500">
          No differences between the selected versions for the current filters.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="text-left text-purple-800 border-b border-purple-200">
                <th className="py-1.5 pr-3 font-semibold">Feature</th>
                <th className="py-1.5 pr-3 font-semibold">Engine</th>
                {versions.map((v) => (
                  <th key={v} className="py-1.5 pr-3 font-semibold text-center">
                    {v.toUpperCase()}
                  </th>
                ))}
                <th className="py-1.5 font-semibold">Change</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(({ feature, platform, levels }) => {
                // Compare only the versions the feature exists in (non-null).
                const present = levels.filter(
                  (l): l is NonNullable<typeof l> => l !== null,
                );
                const first = present[0];
                const last = present[present.length - 1];
                // "Gained"/"Lost" only describe a real move into or out of
                // support. A transition that touches "unknown" (undocumented,
                // not absent) is neither, so it reads as a neutral "Changed"
                // rather than overclaiming a gain or loss.
                const touchesUnknown = first === "unknown" || last === "unknown";
                let change: string;
                let changeClass: string;
                if (!touchesUnknown && first === "none" && last !== "none") {
                  change = "Gained";
                  changeClass = "text-green-600";
                } else if (!touchesUnknown && first !== "none" && last === "none") {
                  change = "Lost";
                  changeClass = "text-red-600";
                } else {
                  change = "Changed";
                  changeClass = "text-amber-600";
                }
                return (
                  <tr
                    key={`${feature.id}:${platform.id}`}
                    className="border-b border-purple-100 last:border-0"
                  >
                    <td className="py-1 pr-3 text-gray-800 font-medium">
                      {feature.name}
                    </td>
                    <td className="py-1 pr-3 text-gray-600">{platform.name}</td>
                    {levels.map((level, i) => (
                      <td
                        key={versions[i]}
                        className={`py-1 pr-3 text-center font-medium ${
                          level === null
                            ? "text-gray-300"
                            : LEVEL_STYLE[level] ?? LEVEL_STYLE.unknown
                        }`}
                      >
                        {level === null ? "—" : LEVEL_LABEL[level] ?? LEVEL_LABEL.unknown}
                      </td>
                    ))}
                    <td className={`py-1 font-semibold ${changeClass}`}>
                      {change}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
