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

const REPO_URL = "https://github.com/Neuw84/iceberg-matrix";

/** A new GitHub issue prefilled with the cell, so a report names what it means. */
function correctionUrl(feature: Feature, platform: Platform, entries: VersionEntry[]): string {
  const rating = entries
    .map(({ version, entry }) => (version === "current" ? entry.level : `${version.toUpperCase()}: ${entry.level}`))
    .join(", ");
  const body = [
    `**Feature:** ${feature.name} (\`${feature.id}\`)`,
    `**Platform:** ${platform.name} (\`${platform.id}\`)`,
    `**Current rating:** ${rating}`,
    "",
    "**What should it be, and why?**",
    "",
    "",
    "**Source** (docs, release notes, or a test that shows it):",
    "",
  ].join("\n");
  const params = new URLSearchParams({
    title: `Correction: ${feature.name} on ${platform.name}`,
    body,
  });
  return `${REPO_URL}/issues/new?${params}`;
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
    const key = JSON.stringify([
      entry.level,
      entry.notes,
      entry.caveats,
      entry.links ?? [],
      entry.testedBy ?? null,
      entry.testedOn ?? null,
      entry.lastReviewed ?? null,
    ]);
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

/** Where the rating comes from: a CI-checked suite, or the docs. */
function SourceLine({ entry }: { entry: SupportEntry }) {
  const suite = entry.testedBy;
  if (suite) {
    return (
      <p className="mb-2 rounded-md bg-indigo-50 px-2 py-1 text-[11px] text-indigo-900">
        <span className="font-semibold">Tested</span> by{" "}
        <a
          href={`https://github.com/Neuw84/iceberg-matrix/blob/main/${suite}`}
          target="_blank"
          rel="noopener noreferrer"
          className="font-mono underline hover:text-indigo-700"
        >
          {suite.split("/").pop()}
        </a>
        {entry.testedOn === "manual"
          ? ", run by hand on the vendor's platform"
          : ", checked on every pull request"}
      </p>
    );
  }
  return (
    <p className="mb-2 rounded-md bg-gray-50 px-2 py-1 text-[11px] text-gray-600">
      <span className="font-semibold">From documentation</span>
      {entry.lastReviewed ? ` · reviewed ${entry.lastReviewed}` : " · not yet verified by a test suite"}
    </p>
  );
}

function EntryBody({ entry }: { entry: SupportEntry }) {
  const hasDetails = entry.notes || entry.caveats.length > 0 || (entry.links?.length ?? 0) > 0;
  if (!hasDetails) {
    return (
      <>
        <SourceLine entry={entry} />
        <p className="text-gray-400 italic text-xs">No additional details available.</p>
      </>
    );
  }
  return (
    <>
      <SourceLine entry={entry} />
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
  const closeRef = useRef<HTMLButtonElement>(null);

  // Modal focus: move into the dialog on open, back to the cell that opened it
  // on close. Separate from the listeners below, whose effect reruns whenever
  // the parent passes a new onClose.
  useEffect(() => {
    const opener = document.activeElement as HTMLElement | null;
    closeRef.current?.focus();
    return () => opener?.focus();
  }, []);

  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
      // Keep Tab inside the dialog: the backdrop makes the page behind inert.
      if (e.key === "Tab" && ref.current) {
        const focusable = ref.current.querySelectorAll<HTMLElement>("a[href], button");
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (e.shiftKey && document.activeElement === first) {
          e.preventDefault();
          last.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
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
      aria-modal="true"
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
          ref={closeRef}
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

      <div className="border-t border-gray-200 bg-gray-50 px-4 py-2 text-right text-xs">
        <a
          href={correctionUrl(feature, platform, entries)}
          target="_blank"
          rel="noopener noreferrer"
          className="text-blue-700 hover:text-blue-900 underline"
        >
          Suggest a correction
        </a>
      </div>
    </div>
  );
}
