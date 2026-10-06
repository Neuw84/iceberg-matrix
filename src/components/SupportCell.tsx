import { memo } from "react";
import type { SupportEntry, SupportLevel, Version } from "../types";

const CONFIG: Record<
  string,
  { icon: string; label: string; color: string; bg: string; border: string }
> = {
  full: {
    icon: "✓",
    label: "Full",
    color: "text-green-700",
    bg: "bg-green-50 hover:bg-green-100",
    border: "border-green-200",
  },
  partial: {
    icon: "◐",
    label: "Partial",
    color: "text-amber-700",
    bg: "bg-amber-50 hover:bg-amber-100",
    border: "border-amber-200",
  },
  none: {
    icon: "✗",
    label: "None",
    color: "text-red-600",
    bg: "bg-red-50 hover:bg-red-100",
    border: "border-red-200",
  },
  unknown: {
    icon: "?",
    label: "Unknown",
    color: "text-gray-400",
    bg: "bg-gray-50 hover:bg-gray-100",
    border: "border-gray-200",
  },
};

/** Compact labels for the half-width segments of a transition cell. The full
 *  word stays in each segment's aria-label and tooltip. */
const SHORT_LABEL: Record<string, string> = {
  full: "Full",
  partial: "Part.",
  none: "None",
  unknown: "Unk.",
};

function cfgFor(level: SupportLevel) {
  return CONFIG[level] ?? CONFIG.unknown;
}

interface SupportCellProps {
  /**
   * Support entries by version, in the order the versions should stack (top to
   * bottom) when they differ. A single-version map renders a plain solid cell;
   * a multi-version map renders a solid cell when every version shares the same
   * support level, and a horizontally-split cell (one labelled band per
   * version, plus a corner marker) when they differ.
   */
  entries: { version: Version; entry: SupportEntry }[];
  /** Click a version's cell/band; passes which version was clicked. */
  onClick?: (version: Version) => void;
}

/** A plain, solid support button — the single-value cell. */
function SolidCell({
  level,
  notes,
  onClick,
}: {
  level: SupportLevel;
  notes: string;
  onClick?: () => void;
}) {
  const cfg = cfgFor(level);
  return (
    <button
      type="button"
      onClick={onClick}
      className={`inline-flex items-center justify-center gap-1 w-full px-1.5 py-0.5 rounded border text-xs font-medium cursor-pointer transition-colors ${cfg.bg} ${cfg.color} ${cfg.border}`}
      aria-label={`Support level: ${cfg.label}`}
      title={notes || cfg.label}
    >
      <span aria-hidden="true" className="text-[11px]">{cfg.icon}</span>
      <span className="text-[10px]">{cfg.label}</span>
    </button>
  );
}

export const SupportCell = memo(function SupportCell({
  entries,
  onClick,
}: SupportCellProps) {
  // Defensive: an empty map renders as a single unknown cell.
  if (entries.length === 0) {
    return <SolidCell level="unknown" notes="" />;
  }

  const allSame = entries.every((e) => e.entry.level === entries[0].entry.level);

  // Common case: one version, or every selected version agrees — a solid cell.
  if (entries.length === 1 || allSame) {
    const { version, entry } = entries[0];
    return (
      <SolidCell
        level={entry.level}
        notes={entry.notes}
        onClick={onClick ? () => onClick(version) : undefined}
      />
    );
  }

  // Difference: a transition cell. One segment per version laid out left to
  // right (V2 → V3 → …), each tinted with its own status and labelled with its
  // version, with an arrow badge on every boundary and a purple frame so a
  // differing cell is identifiable without relying on colour. The segments
  // share the column's width equally (flex-1, min-w-0), so the cell is exactly
  // as wide as a solid one: the matrix columns stay a uniform width. Each
  // segment is independently clickable and opens that version's detail.
  const summary = entries
    .map(({ version, entry }) => `${version.toUpperCase()}: ${cfgFor(entry.level).label}`)
    .join(" → ");

  return (
    <div
      className="relative flex items-stretch w-full rounded border border-purple-500 overflow-hidden"
      role="group"
      aria-label={`Differs by version — ${summary}`}
      title={summary}
    >
      {entries.map(({ version, entry }, i) => {
        const cfg = cfgFor(entry.level);
        return (
          <div key={version} className="relative flex flex-1 min-w-0">
            {/* Arrow badge sitting on the boundary with the previous segment. */}
            {i > 0 && (
              <span
                aria-hidden="true"
                className="pointer-events-none absolute left-0 top-1/2 z-10 flex h-3.5 w-3.5 -translate-x-1/2 -translate-y-1/2 items-center justify-center rounded-full bg-purple-600 text-[9px] leading-none text-white"
              >
                →
              </span>
            )}
            <button
              type="button"
              onClick={onClick ? () => onClick(version) : undefined}
              className={`flex w-full min-w-0 flex-col items-center justify-center px-0.5 py-0.5 font-medium cursor-pointer transition-colors ${cfg.bg} ${cfg.color}`}
              aria-label={`${version.toUpperCase()} support level: ${cfg.label}`}
              title={entry.notes || `${version.toUpperCase()}: ${cfg.label}`}
            >
              <span className="text-[8px] font-bold uppercase leading-none opacity-70">
                {version}
              </span>
              <span className="inline-flex items-center gap-0.5 whitespace-nowrap leading-tight">
                <span aria-hidden="true" className="text-[10px]">{cfg.icon}</span>
                <span className="text-[9px]">{SHORT_LABEL[entry.level] ?? cfg.label}</span>
              </span>
            </button>
          </div>
        );
      })}
    </div>
  );
});
