import { useEffect, useRef } from "react";
import type { Feature, Platform, SupportEntry, Version } from "../types";

const LEVEL_BADGE: Record<string, string> = {
  full: "bg-green-100 text-green-800",
  partial: "bg-amber-100 text-amber-800",
  none: "bg-red-100 text-red-700",
  unknown: "bg-gray-100 text-gray-600",
};

interface DetailPopoverProps {
  entry: SupportEntry;
  feature: Feature;
  platform: Platform;
  version: Version;
  onClose: () => void;
}

export function DetailPopover({
  entry,
  feature,
  platform,
  version,
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

  const hasDetails = entry.notes || entry.caveats.length > 0;
  const badgeClass = LEVEL_BADGE[entry.level] ?? LEVEL_BADGE.unknown;

  return (
    <div
      ref={ref}
      role="dialog"
      aria-label={`Details for ${feature.name} on ${platform.name}`}
      className="w-96 bg-white border border-gray-200 rounded-xl shadow-2xl overflow-hidden"
    >
      {/* Header */}
      <div className="bg-gray-50 border-b border-gray-200 px-4 py-3 flex justify-between items-start">
        <div>
          <h3 className="font-semibold text-sm text-gray-900">{feature.name}</h3>
          <p className="text-[11px] text-gray-500 mt-0.5">
            {platform.name} · {version.toUpperCase()} · Since{" "}
            {feature.introducedIn.toUpperCase()}
          </p>
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

      {/* Body */}
      <div className="px-4 py-3">
        <span className={`inline-block px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wide ${badgeClass}`}>
          {entry.level}
        </span>

        <div className="mt-3 max-h-48 overflow-y-auto text-sm text-gray-700">
          {hasDetails ? (
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
          ) : (
            <p className="text-gray-400 italic text-xs">
              No additional details available.
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
