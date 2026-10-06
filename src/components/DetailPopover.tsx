import { useEffect, useRef } from "react";
import type { Feature, Platform, SupportEntry, Version } from "../types";

const LEVEL_BADGE: Record<string, string> = {
  full: "bg-green-100 text-green-800",
  partial: "bg-amber-100 text-amber-800",
  none: "bg-red-100 text-red-700",
  unknown: "bg-gray-100 text-gray-600",
};

/** One version's support entry, as handed over by the cell that was clicked. */
export interface VersionEntry {
  version: Version;
  entry: SupportEntry;
}

interface DetailPopoverProps {
  /** Every version the clicked cell covers, in display order. A cell that spans
   *  V2 and V3 opens with both, so V3's notes, caveats and links are visible
   *  even when V2 and V3 agree and the cell renders as a single solid one. */
  entries: VersionEntry[];
  feature: Feature;
  platform: Platform;
  onClose: () => void;
}

interface Section {
  versions: Version[];
  entry: SupportEntry;
}

/**
 * Group the versions whose entries are identical (same level, notes, caveats
 * and links) into one section, so content that is the same on V2 and V3 is
 * shown once under "V2 · V3" instead of being repeated. Differing entries keep
 * their own section. Order follows the first version of each group.
 */
function toSections(entries: VersionEntry[]): Section[] {
  const sections: Section[] = [];
  const index = new Map<string, number>();
  for (const { version, entry } of entries) {
    const key = JSON.stringify([entry.level, entry.notes, entry.caveats, entry.links ?? []]);
    const at = index.get(key);
    if (at === undefined) {
      index.set(key, sections.length);
      sections.push({ versions: [version], entry });
    } else {
      sections[at].versions.push(version);
    }
  }
  return sections;
}

function EntryBody({ entry }: { entry: SupportEntry }) {
  const hasDetails = entry.notes || entry.caveats.length > 0 || (entry.links?.length ?? 0) > 0;
  if (!hasDetails) {
    return (
      <p className="text-gray-400 italic text-xs">No additional details available.</p>
    );
  }
  return (
    <>
      {entry.notes && <p className="mb-2 leading-relaxed">{entry.notes}</p>}
      {entry.caveats.length > 0 && (
        <div>
          <p className="font-medium text-[10px] text-gray-500 uppercase tracking-wide mb-1">
            Caveats
          </p>
          <ul className="list-disc list-inside space-y-1 text-xs text-gray-600">
            {entry.caveats.map((c, i) => (
              <li key={i}>{c}</li>
            ))}
          </ul>
        </div>
      )}
      {entry.links && entry.links.length > 0 && (
        <div className="mt-2">
          <p className="font-medium text-[10px] text-gray-500 uppercase tracking-wide mb-1">
            References
          </p>
          <ul className="space-y-1 text-xs">
            {entry.links.map((link, i) => (
              <li key={i}>
                <a
                  href={link.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-blue-600 hover:text-blue-800 underline"
                >
                  {link.label}
                </a>
              </li>
            ))}
          </ul>
        </div>
      )}
    </>
  );
}

export function DetailPopover({
  entries,
  feature,
  platform,
  onClose,
}: DetailPopoverProps) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    function handleClickOutside(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    }
    document.addEventListener("keydown", handleKey);
    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      document.removeEventListener("keydown", handleKey);
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, [onClose]);

  const sections = toSections(entries);
  const versions = entries.map((e) => e.version);
  // "current" is the catalogs dataset's single synthetic version; echoing
  // "CURRENT · Since CURRENT" would be noise, and so would a version label on
  // its only section.
  const isCurrent = versions.length === 1 && versions[0] === "current";
  const showVersionLabels = !isCurrent;
  const subtitle = isCurrent
    ? platform.name
    : `${platform.name} · ${versions.map((v) => v.toUpperCase()).join(" · ")} · Since ${feature.introducedIn.toUpperCase()}`;

  return (
    <div
      ref={ref}
      role="dialog"
      aria-label={`Details for ${feature.name} on ${platform.name}`}
      // Phone: the original 24rem, never wider than the screen. Tablet and up
      // get more room so long notes and caveat lists don't wrap into a tall,
      // narrow column.
      className="w-96 max-w-[calc(100vw-2rem)] md:w-[40rem] lg:w-[48rem] bg-white border border-gray-200 rounded-xl shadow-2xl overflow-hidden"
    >
      {/* Header */}
      <div className="bg-gray-50 border-b border-gray-200 px-4 py-3 flex justify-between items-start">
        <div>
          <h3 className="font-semibold text-sm text-gray-900">{feature.name}</h3>
          <p className="text-[11px] text-gray-500 mt-0.5">{subtitle}</p>
        </div>
        <button
          type="button"
          onClick={onClose}
          className="text-gray-400 hover:text-gray-600 text-lg leading-none cursor-pointer ml-2 mt-0.5"
          aria-label="Close"
        >
          ×
        </button>
      </div>

      {/* Body: one section per distinct entry; identical V2/V3 content is merged. */}
      <div className="max-h-[75vh] overflow-y-auto px-4 py-3 text-sm text-gray-700 space-y-4">
        {sections.map((section) => {
          const label = section.versions.map((v) => v.toUpperCase()).join(" · ");
          return (
            <section
              key={section.versions.join("+")}
              aria-label={showVersionLabels ? `${label} details` : undefined}
            >
              <div className="mb-2 flex items-center gap-2">
                {showVersionLabels && (
                  <span className="text-[10px] font-bold uppercase tracking-wide text-gray-500">
                    {label}
                  </span>
                )}
                <span
                  className={`inline-block px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wide ${
                    LEVEL_BADGE[section.entry.level] ?? LEVEL_BADGE.unknown
                  }`}
                >
                  {section.entry.level}
                </span>
              </div>
              <EntryBody entry={section.entry} />
            </section>
          );
        })}
      </div>
    </div>
  );
}
