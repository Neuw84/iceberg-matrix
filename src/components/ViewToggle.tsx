import type { ViewMode } from "../types";

interface ViewToggleProps {
  mode: ViewMode;
  onChange: (mode: ViewMode) => void;
}

const OPTIONS: { mode: ViewMode; label: string }[] = [
  { mode: "engines", label: "Engines" },
  { mode: "catalogs", label: "Catalogs" },
];

/**
 * The top-level view switcher: engines × Iceberg features (default) versus
 * catalogs × the openness rubric. A contained segmented control so it reads as
 * a view change, distinct from the flat filter pills next to it.
 */
export function ViewToggle({ mode, onChange }: ViewToggleProps) {
  return (
    <div
      className="inline-flex items-center rounded-lg bg-gray-100 p-0.5"
      role="tablist"
      aria-label="Matrix view"
    >
      {OPTIONS.map((opt) => (
        <button
          key={opt.mode}
          type="button"
          role="tab"
          aria-selected={mode === opt.mode}
          onClick={() => onChange(opt.mode)}
          className={`px-3 py-1 rounded-md text-xs font-semibold cursor-pointer transition-colors ${
            mode === opt.mode
              ? "bg-white text-gray-900 shadow-sm"
              : "text-gray-500 hover:text-gray-700"
          }`}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}
