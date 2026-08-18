import type { Version } from "../types";

interface VersionTabsProps {
  versions: Version[];
  selected: Version[];
  onChange: (selected: Version[]) => void;
  /** Grays out and deactivates the whole control (catalogs view: the rubric
   *  has no v2/v3 dimension, but keeping the control visible keeps the header
   *  layout consistent across the two views). */
  disabled?: boolean;
}

export function VersionTabs({ versions, selected, onChange, disabled = false }: VersionTabsProps) {
  const isCompareMode = selected.length > 1;

  const toggle = (v: Version) => {
    if (selected.includes(v)) {
      if (selected.length <= 1) return;
      onChange(selected.filter((s) => s !== v));
    } else {
      if (isCompareMode) {
        onChange([...selected, v]);
      } else {
        onChange([v]);
      }
    }
  };

  const toggleCompare = () => {
    if (isCompareMode) {
      onChange([selected[0]]);
    } else {
      onChange([...versions]);
    }
  };

  return (
    <div className="flex items-center gap-1.5" role="tablist" aria-label="Iceberg version selector">
      {versions.map((v) => (
        <button
          key={v}
          type="button"
          role="tab"
          aria-selected={!disabled && selected.includes(v)}
          disabled={disabled}
          onClick={() => toggle(v)}
          className={`px-3 py-1 rounded-md text-xs font-semibold transition-colors ${
            disabled
              ? "bg-gray-100 text-gray-300 cursor-not-allowed"
              : selected.includes(v)
                ? "bg-blue-600 text-white shadow-sm cursor-pointer"
                : "bg-gray-100 text-gray-600 hover:bg-gray-200 cursor-pointer"
          }`}
        >
          {v.toUpperCase()}
        </button>
      ))}
      <div className="w-px h-5 bg-gray-300 mx-1" />
      <button
        type="button"
        disabled={disabled}
        onClick={toggleCompare}
        className={`px-3 py-1 rounded-md text-xs font-medium transition-colors ${
          disabled
            ? "bg-gray-100 text-gray-300 cursor-not-allowed"
            : isCompareMode
              ? "bg-purple-600 text-white shadow-sm cursor-pointer"
              : "bg-gray-100 text-gray-500 hover:bg-gray-200 cursor-pointer"
        }`}
        aria-label={!disabled && isCompareMode ? "Exit comparison mode" : "Compare versions"}
      >
        {!disabled && isCompareMode ? "Exit Compare" : "Compare"}
      </button>
    </div>
  );
}
