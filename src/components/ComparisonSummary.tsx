import type { CompatibilityData, Platform, Version } from "../types";
import { computeComparison } from "../utils/comparison";

interface ComparisonSummaryProps {
  data: CompatibilityData;
  platforms: Platform[];
  versions: Version[];
}

export function ComparisonSummary({
  data,
  platforms,
  versions,
}: ComparisonSummaryProps) {
  if (versions.length < 2) return null;

  const [vA, vB] = versions;

  return (
    <div className="bg-purple-50 border border-purple-200 rounded-lg p-4 mb-4">
      <h3 className="text-sm font-semibold text-purple-800 mb-3">
        Comparison: {vA.toUpperCase()} → {vB.toUpperCase()}
      </h3>
      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-3">
        {platforms.map((p) => {
          const { gained, lost, changed } = computeComparison(
            data,
            p.id,
            vA,
            vB
          );
          return (
            <div
              key={p.id}
              className="bg-white rounded p-2 text-center border border-purple-100"
            >
              <p className="text-xs font-medium text-gray-700 mb-1 truncate">
                {p.name}
              </p>
              <div className="flex justify-center gap-2 text-xs">
                {gained > 0 && (
                  <span className="text-green-600">+{gained}</span>
                )}
                {lost > 0 && <span className="text-red-600">-{lost}</span>}
                {changed > 0 && (
                  <span className="text-yellow-600">~{changed}</span>
                )}
                {gained === 0 && lost === 0 && changed === 0 && (
                  <span className="text-gray-400">No changes</span>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
