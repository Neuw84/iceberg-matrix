import { describe, it, expect } from "vitest";
import { nextGridButton } from "./gridNavigation";

// Two feature rows around a category row; the second row has a split cell
// (two buttons) in the first engine column.
function table() {
  const t = document.createElement("table");
  t.innerHTML = `<tbody>
    <tr><td>Feature A</td><td><button id="a1"></button></td><td><button id="a2"></button></td></tr>
    <tr class="category-row"><td><button id="collapse"></button></td></tr>
    <tr><td>Feature B</td><td><button id="b1v2"></button><button id="b1v3"></button></td><td><button id="b2"></button></td></tr>
  </tbody>`;
  return t;
}
const move = (t: HTMLTableElement, id: string, key: string) =>
  nextGridButton(t, t.querySelector<HTMLElement>(`#${id}`)!, key)?.id ?? null;

describe("nextGridButton", () => {
  it("steps through a row's buttons, including both halves of a split cell", () => {
    const t = table();
    expect(move(t, "b1v2", "ArrowRight")).toBe("b1v3");
    expect(move(t, "b1v3", "ArrowRight")).toBe("b2");
    expect(move(t, "b2", "ArrowRight")).toBeNull();
    expect(move(t, "b2", "Home")).toBe("b1v2");
    expect(move(t, "a1", "End")).toBe("a2");
  });

  it("keeps the column and skips category rows going up and down", () => {
    const t = table();
    expect(move(t, "a2", "ArrowDown")).toBe("b2");
    expect(move(t, "b1v3", "ArrowUp")).toBe("a1");
    expect(move(t, "a1", "ArrowUp")).toBeNull();
  });

  it("ignores focus outside the cell buttons", () => {
    expect(move(table(), "collapse", "ArrowDown")).toBeNull();
  });
});
