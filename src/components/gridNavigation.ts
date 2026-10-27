/**
 * Arrow-key movement between the matrix's cell buttons, which `role="grid"`
 * promises to keyboard and screen-reader users. Left/Right step through the
 * buttons of a row (a split V2→V3 cell has one per version), Home/End jump to
 * its ends, Up/Down keep the column and skip rows without cells (category
 * headers, collapsed groups). Pure DOM, so it needs no per-cell state.
 */
export const GRID_KEYS = ["ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight", "Home", "End"];

/** Cell buttons of a row; the first cell is the feature name. */
const rowButtons = (row: HTMLTableRowElement) =>
  Array.from(row.querySelectorAll<HTMLButtonElement>("td:not(:first-child) button"));

/** The button to focus for `key` from the focused `from`, or null to stay. */
export function nextGridButton(
  table: HTMLTableElement,
  from: HTMLElement,
  key: string,
): HTMLButtonElement | null {
  const cell = from.closest("td");
  const row = cell?.parentElement as HTMLTableRowElement | null;
  if (!cell || !row || !(from instanceof HTMLButtonElement)) return null;

  const inRow = rowButtons(row);
  const i = inRow.indexOf(from);
  if (i === -1) return null;
  if (key === "ArrowLeft") return inRow[i - 1] ?? null;
  if (key === "ArrowRight") return inRow[i + 1] ?? null;
  if (key === "Home") return inRow[0] ?? null;
  if (key === "End") return inRow[inRow.length - 1] ?? null;

  const rows = Array.from(table.querySelectorAll<HTMLTableRowElement>("tbody tr")).filter(
    (r) => rowButtons(r).length > 0,
  );
  const target = rows[rows.indexOf(row) + (key === "ArrowDown" ? 1 : -1)];
  return target?.cells[cell.cellIndex]?.querySelector("button") ?? null;
}
