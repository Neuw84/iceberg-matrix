import { memo, useRef, useState } from "react";
import type { Feature, Platform, SupportEntry, Version } from "../types";
import { createPortal } from "react-dom";
import { SupportCell } from "./SupportCell";
import { applicableVersions, versionBadge } from "../utils/versions";
import { NAME_COL_WIDTH } from "./matrixLayout";

interface FeatureRowProps {
  feature: Feature;
  platforms: Platform[];
  /** The selected versions, in display order. One column per platform; when a
   *  platform's levels differ across these versions the cell splits. */
  versions: Version[];
  /** Full version list of the dataset, for the per-row version badge (distinct
   *  from `versions`, which is the user's current selection). */
  allVersions: Version[];
  getSupportEntry: (platformId: string, featureId: string, version: Version) => SupportEntry;
  onCellClick: (platform: Platform, feature: Feature, version: Version, entry: SupportEntry) => void;
}

function FeatureRowImpl({
  feature,
  platforms,
  versions,
  allVersions,
  getSupportEntry,
  onCellClick,
}: FeatureRowProps) {
  const badge = versionBadge(feature, allVersions);
  // A cell should only show the versions the feature actually exists in. For a
  // V3-only feature (e.g. Deletion Vectors, Lineage Tracking) this drops the
  // meaningless V2 band — otherwise, with both versions selected, the cell
  // would split into a bogus "V2 None / V3 Full". Intersect the feature's
  // applicable versions with the user's current selection, preserving order.
  const applicable = new Set(applicableVersions(feature, allVersions));
  const cellVersions = versions.filter((v) => applicable.has(v));
  const [tooltip, setTooltip] = useState<{ x: number; y: number } | null>(null);
  const spanRef = useRef<HTMLSpanElement>(null);

  const showTooltip = () => {
    if (!spanRef.current) return;
    const rect = spanRef.current.getBoundingClientRect();
    setTooltip({ x: rect.left, y: rect.bottom + 4 });
  };

  return (
    <tr className="border-b border-gray-100 hover:bg-blue-50/30 transition-colors">
      <td className="sticky left-0 bg-white px-2 py-1.5 text-xs text-gray-800 font-medium z-20 border-r border-gray-100 [overflow-wrap:anywhere]" style={{ width: NAME_COL_WIDTH, minWidth: NAME_COL_WIDTH }}>
        <span
          ref={spanRef}
          className="cursor-help border-b border-dotted border-gray-300"
          onMouseEnter={showTooltip}
          onMouseLeave={() => setTooltip(null)}
        >
          {feature.name}
        </span>
        {badge && (
          <span
            className="ml-1 align-middle rounded bg-gray-100 px-1 py-px text-[9px] font-semibold uppercase tracking-wide text-gray-500"
            aria-label={`Applies to Iceberg ${badge}`}
          >
            {badge}
          </span>
        )}
        {tooltip && createPortal(
          <div
            role="tooltip"
            className="fixed z-[9999] max-w-xs rounded bg-white border border-gray-200 px-2.5 py-1.5 text-[11px] font-normal leading-snug text-gray-700 shadow-lg pointer-events-none"
            style={{ left: tooltip.x, top: tooltip.y }}
          >
            {feature.description}
          </div>,
          document.body
        )}
      </td>
      {platforms.map((platform) => {
        // One cell per platform. Gather an entry per version the feature
        // applies to (within the current selection); the cell renders solid
        // when they agree and splits when they differ.
        const entries = cellVersions.map((version) => ({
          version,
          entry: getSupportEntry(platform.id, feature.id, version),
        }));
        return (
          <td
            key={platform.id}
            className="px-1 py-1 text-center border-x border-gray-50"
            role="gridcell"
          >
            <SupportCell
              entries={entries}
              onClick={(version) => {
                const hit = entries.find((e) => e.version === version) ?? entries[0];
                onCellClick(platform, feature, hit.version, hit.entry);
              }}
            />
          </td>
        );
      })}
    </tr>
  );
}

export const FeatureRow = memo(FeatureRowImpl);
