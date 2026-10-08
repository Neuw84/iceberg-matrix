import type { ViewMode } from "../types";

interface ViewToggleProps {
  mode: ViewMode;
  onChange: (mode: ViewMode) => void;
}

const OPTIONS: { mode: ViewMode; label: string }[] = [
  { mode: "engines", label: "Engines" },
  { mode: "ingestion", label: "Ingestion" },
  { mode: "catalogs", label: "Catalogs" },
];

/**
 * The top-level view switcher: engines × Iceberg features (default), ingestion
 * tools × a reduced write-oriented feature set, or catalogs × the openness rubric. A contained segmented control so it reads as
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
          className={`px-4 py-1.5 rounded-md text-sm font-semibold cursor-pointer transition-colors ${
            mode === opt.mode
              ? "bg-blue-600 text-white shadow-sm"
              : "text-gray-700 hover:text-gray-900 hover:bg-gray-200"
          }`}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}
