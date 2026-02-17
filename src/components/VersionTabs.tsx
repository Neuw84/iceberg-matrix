import type { Version } from "../types";

interface VersionTabsProps {
  versions: Version[];
  selected: Version[];
  onChange: (selected: Version[]) => void;
}

export function VersionTabs({ versions, selected, onChange }: VersionTabsProps) {
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
          aria-selected={selected.includes(v)}
          onClick={() => toggle(v)}
          className={`px-3 py-1 rounded-md text-xs font-semibold cursor-pointer transition-colors ${
            selected.includes(v)
              ? "bg-blue-600 text-white shadow-sm"
              : "bg-gray-100 text-gray-600 hover:bg-gray-200"
          }`}
        >
          {v.toUpperCase()}
        </button>
      ))}
      <div className="w-px h-5 bg-gray-300 mx-1" />
      <button
        type="button"
        onClick={toggleCompare}
        className={`px-3 py-1 rounded-md text-xs font-medium cursor-pointer transition-colors ${
          isCompareMode
            ? "bg-purple-600 text-white shadow-sm"
            : "bg-gray-100 text-gray-500 hover:bg-gray-200"
        }`}
        aria-label={isCompareMode ? "Exit comparison mode" : "Compare versions"}
      >
        {isCompareMode ? "Exit Compare" : "Compare"}
      </button>
    </div>
  );
}
