import type {
  AwsS3Mode,
  BigQueryTableMode,
  CompatibilityData,
  SnowflakeStorageMode,
  ViewMode,
} from "../types";
import { loadEngineData, peekEngineData } from "./load-data";

// One entry point for "the dataset behind a view". The default engines dataset
// is bundled with the app and is always ready. Everything else (Catalogs,
// Ingestion, the S3 Tables, Snowflake external and BigQuery Lakehouse engine
// variants) is a separate
// chunk fetched on first use, so a visitor who never opens it never downloads it.

interface ViewSelection {
  view: ViewMode;
  awsS3Mode: AwsS3Mode;
  snowflakeMode: SnowflakeStorageMode;
  bigqueryMode: BigQueryTableMode;
}

let catalogs: CompatibilityData | undefined;
let catalogsLoad: Promise<CompatibilityData> | undefined;
let ingestion: CompatibilityData | undefined;
let ingestionLoad: Promise<CompatibilityData> | undefined;

// The most recent dataset shown for each view. While a variant is loading (for
// example after flipping the AWS S3 Tables toggle) the UI keeps showing this
// instead of going blank, then swaps when the new dataset arrives.
const latest = new Map<ViewMode, CompatibilityData>();

function loadCatalogs(): Promise<CompatibilityData> {
  catalogsLoad ??= import("./load-catalogs")
    .then((m) => (catalogs = m.dataCatalogs))
    .catch((err) => {
      catalogsLoad = undefined; // allow a retry after a failed fetch
      throw err;
    });
  return catalogsLoad;
}

function loadIngestion(): Promise<CompatibilityData> {
  ingestionLoad ??= import("./load-ingestion")
    .then((m) => (ingestion = m.dataIngestion))
    .catch((err) => {
      ingestionLoad = undefined;
      throw err;
    });
  return ingestionLoad;
}

/** The dataset for a selection if it is already loaded, else undefined. */
export function peekViewData({ view, awsS3Mode, snowflakeMode, bigqueryMode }: ViewSelection):
  | CompatibilityData
  | undefined {
  const found =
    view === "catalogs"
      ? catalogs
      : view === "ingestion"
        ? ingestion
        : peekEngineData(awsS3Mode, snowflakeMode, bigqueryMode);
  if (found) latest.set(view, found);
  return found;
}

/** The last dataset shown for a view, for stale-while-loading display. */
export function lastViewData(view: ViewMode): CompatibilityData | undefined {
  return latest.get(view);
}

/** Load (once) and return the dataset for a selection. */
export function loadViewData({
  view,
  awsS3Mode,
  snowflakeMode,
  bigqueryMode,
}: ViewSelection): Promise<CompatibilityData> {
  const pending =
    view === "catalogs"
      ? loadCatalogs()
      : view === "ingestion"
        ? loadIngestion()
        : loadEngineData(awsS3Mode, snowflakeMode, bigqueryMode);
  return pending.then((d) => {
    latest.set(view, d);
    return d;
  });
}

/**
 * Start fetching a view's data ahead of the click (called when its tab is
 * hovered or focused). Failures are ignored here: a real load on click reports
 * them and offers a retry.
 */
export function preloadView(view: ViewMode): void {
  if (view === "engines") return;
  void loadViewData({
    view,
    awsS3Mode: "s3-buckets",
    snowflakeMode: "snowflake",
    bigqueryMode: "managed",
  }).catch(() => {});
}
