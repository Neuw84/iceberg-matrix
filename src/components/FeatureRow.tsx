import { useRef, useState } from "react";
import type { Feature, Platform, SupportEntry, Version } from "../types";
import { createPortal } from "react-dom";
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
  const [tooltip, setTooltip] = useState<{ x: number; y: number } | null>(null);
  const spanRef = useRef<HTMLSpanElement>(null);

  const showTooltip = () => {
    if (!spanRef.current) return;
    const rect = spanRef.current.getBoundingClientRect();
    setTooltip({ x: rect.left, y: rect.bottom + 4 });
  };

  return (
    <tr className="border-b border-gray-100 hover:bg-blue-50/30 transition-colors">
      <td className="sticky left-0 bg-white px-2 py-1.5 text-xs text-gray-800 font-medium z-20 border-r border-gray-100" style={{ width: 110, minWidth: 110 }}>
        <span
          ref={spanRef}
          className="cursor-help border-b border-dotted border-gray-300"
          onMouseEnter={showTooltip}
          onMouseLeave={() => setTooltip(null)}
        >
          {feature.name}
        </span>
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
