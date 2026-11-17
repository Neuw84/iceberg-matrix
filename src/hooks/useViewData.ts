import { useCallback, useEffect, useReducer, useState } from "react";
import type {
  AwsS3Mode,
  BigQueryTableMode,
  CompatibilityData,
  SnowflakeStorageMode,
  ViewMode,
} from "../types";
import { lastViewData, loadViewData, peekViewData } from "../data/view-data";

export interface ViewDataState {
  /** The dataset to render: the requested one if loaded, else the previous one
   *  for the same view (so toggling a variant does not blank the table). */
  data: CompatibilityData | undefined;
  /** True while the requested dataset is not loaded yet. */
  loading: boolean;
  error: Error | undefined;
  retry: () => void;
}

/**
 * Resolve the dataset behind the active view, loading its chunk on demand. The
 * default engines dataset is bundled, so the first render is synchronous; other
 * views and variants resolve after their chunk arrives.
 */
export function useViewData(
  view: ViewMode,
  awsS3Mode: AwsS3Mode,
  snowflakeMode: SnowflakeStorageMode,
  bigqueryMode: BigQueryTableMode,
): ViewDataState {
  const [, rerender] = useReducer((n: number) => n + 1, 0);
  const [attempt, setAttempt] = useState(0);
  const [failure, setFailure] = useState<{ key: string; error: Error } | undefined>();

  // Only engines depend on the two toggles; the other views have one dataset.
  const key =
    view === "engines" ? `engines|${awsS3Mode}|${snowflakeMode}|${bigqueryMode}` : view;
  const selection = { view, awsS3Mode, snowflakeMode, bigqueryMode };
  const ready = peekViewData(selection);

  useEffect(() => {
    if (ready) return;
    let live = true;
    loadViewData({ view, awsS3Mode, snowflakeMode, bigqueryMode }).then(
      () => {
        if (live) rerender();
      },
      (err: unknown) => {
        if (live) setFailure({ key, error: err instanceof Error ? err : new Error(String(err)) });
      },
    );
    return () => {
      live = false;
    };
  }, [ready, key, attempt, view, awsS3Mode, snowflakeMode, bigqueryMode]);

  const retry = useCallback(() => {
    setFailure(undefined);
    setAttempt((n) => n + 1);
  }, []);

  return {
    data: ready ?? lastViewData(view),
    loading: !ready,
    error: failure?.key === key ? failure.error : undefined,
    retry,
  };
}
