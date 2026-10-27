import defaultColors from "tailwindcss/colors";
import plugin from "tailwindcss/plugin";

/**
 * Dark mode by flipping the palette instead of writing a `dark:` variant on
 * every class. Each color is a CSS variable; in dark mode every scale is
 * mirrored (50↔950, 100↔900, …, 500 stays) and white becomes gray-900. Light
 * backgrounds turn dark, dark text turns light, and pairs such as
 * `bg-green-50 text-green-700` keep their contrast. Black is left alone (it
 * is only used for the popover backdrop).
 *
 * Follows the system setting; `dark:` variants still work for the few things
 * a flip cannot fix (e.g. logos drawn for a white background).
 */
const PALETTES = [
  "slate", "gray", "zinc", "neutral", "stone", "red", "orange", "amber", "yellow", "lime",
  "green", "emerald", "teal", "cyan", "sky", "blue", "indigo", "violet", "purple", "fuchsia",
  "pink", "rose",
];
const SHADES = ["50", "100", "200", "300", "400", "500", "600", "700", "800", "900", "950"];

const channels = (hex) => [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));
const rgb = (hex) => channels(hex).join(" ");
/** `hex` moved `amount` of the way toward `toward`, as an "r g b" triplet. */
const mix = (hex, toward, amount) => {
  const to = channels(toward);
  return channels(hex).map((c, i) => Math.round(c + (to[i] - c) * amount)).join(" ");
};
// The two lightest shades become tinted backgrounds when flipped (950, 900).
// At full strength they glare on a dark page, so pull them toward the
// neutral background. Grays are left as they are: the page/card contrast
// depends on them.
const DARK_TONE_DOWN = { "50": 0.55, "100": 0.4 };
const NEUTRALS = ["slate", "gray", "zinc", "neutral", "stone"];

const light = { "--c-white": rgb("#ffffff") };
const dark = { "--c-white": rgb(defaultColors.gray[900]) };
const colors = {
  transparent: "transparent",
  current: "currentColor",
  inherit: "inherit",
  black: "#000",
  white: "rgb(var(--c-white) / <alpha-value>)",
};
for (const name of PALETTES) {
  colors[name] = {};
  SHADES.forEach((shade, i) => {
    const v = `--c-${name}-${shade}`;
    light[v] = rgb(defaultColors[name][shade]);
    const flipped = defaultColors[name][SHADES[SHADES.length - 1 - i]];
    dark[v] = DARK_TONE_DOWN[shade] && !NEUTRALS.includes(name)
      ? mix(flipped, defaultColors.gray[900], DARK_TONE_DOWN[shade])
      : rgb(flipped);
    colors[name][shade] = `rgb(var(${v}) / <alpha-value>)`;
  });
}

/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  darkMode: "media",
  theme: {
    colors,
    extend: {},
  },
  plugins: [
    plugin(({ addBase }) => {
      addBase({
        // Native controls (checkboxes, scrollbars) follow the theme too.
        ":root": { ...light, colorScheme: "light dark" },
        "@media (prefers-color-scheme: dark)": { ":root": dark },
      });
    }),
  ],
};
