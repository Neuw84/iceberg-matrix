/** Width of the sticky feature-name column. */
export const NAME_COL_WIDTH = 176;

/**
 * Width of every engine column. The table uses a fixed layout so all engine
 * columns are exactly this wide regardless of content: a version-transition
 * cell never widens its column. 110px is the minimum that fits a two-segment
 * transition cell (V2 ✓ Full → V3 ✗ None) without clipping.
 */
export const ENGINE_COL_WIDTH = 110;
