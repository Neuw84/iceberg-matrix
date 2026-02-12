import type { SupportEntry } from "../types";

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

interface SupportCellProps {
  entry: SupportEntry;
  onClick?: () => void;
}

export function SupportCell({ entry, onClick }: SupportCellProps) {
  const cfg = CONFIG[entry.level] ?? CONFIG.unknown;

  return (
    <button
      type="button"
      onClick={onClick}
      className={`inline-flex items-center justify-center gap-1 w-full px-1.5 py-0.5 rounded border text-xs font-medium cursor-pointer transition-colors ${cfg.bg} ${cfg.color} ${cfg.border}`}
      aria-label={`Support level: ${cfg.label}`}
      title={entry.notes || cfg.label}
    >
      <span aria-hidden="true" className="text-[11px]">{cfg.icon}</span>
      <span className="text-[10px]">{cfg.label}</span>
    </button>
  );
}
