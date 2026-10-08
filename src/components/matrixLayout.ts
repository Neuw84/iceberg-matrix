/** Width of the sticky feature-name column. */
export const NAME_COL_WIDTH = 176;

/**
 * Width of every engine column. The table uses a fixed layout so all engine
 * columns are exactly this wide regardless of content: a version-transition
 * cell never widens its column. 110px is the minimum that fits a two-segment
 * transition cell (V2 ✓ Full → V3 ✗ None) without clipping.
 */
export const ENGINE_COL_WIDTH = 110;

/**
 * Upper bound on an engine column's width. Columns share any spare width
 * equally, which is right for a wide matrix but makes a view with only a couple
 * of columns (Ingestion) stretch each one across half the screen. Capping the
 * table at NAME_COL_WIDTH + columns × this keeps a small dataset compact, and
 * is large enough that the 10-column catalogs view and the 21-column engines
 * view still fill the page as before.
 */
export const ENGINE_COL_MAX_WIDTH = 240;
