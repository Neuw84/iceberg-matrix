import { useSyncExternalStore } from "react";

/**
 * Whether a CSS media query matches, updating when it changes (rotation,
 * window resize). False where matchMedia is missing (jsdom), so tests render
 * the desktop layout.
 */
export function useMediaQuery(query: string): boolean {
  return useSyncExternalStore(
    (onChange) => {
      if (typeof window.matchMedia !== "function") return () => {};
      const list = window.matchMedia(query);
      list.addEventListener("change", onChange);
      return () => list.removeEventListener("change", onChange);
    },
    () => typeof window.matchMedia === "function" && window.matchMedia(query).matches,
    () => false,
  );
}
