import { useMemo, useState, lazy, Suspense } from "react";
import type {
  AwsS3Mode,
  CompatibilityData,
  Feature,
  FilterState,
  Platform,
  SnowflakeStorageMode,
} from "../types";
import { applyFilters } from "../utils/filters";
import { getSupportEntry } from "../utils/support";
import { applicableVersions, versionBadge } from "../utils/versions";
import { SupportCell } from "./SupportCell";
import type { VersionEntry } from "./DetailPopover";
import { CATEGORY_COLORS, CATEGORY_LABELS, CATEGORY_ORDER } from "./categories";

const DetailPopover = lazy(() =>
  import("./DetailPopover").then((m) => ({ default: m.DetailPopover })),
);

interface MobileMatrixProps {
  data: CompatibilityData;
  filters: FilterState;
  /** Singular noun for a column: "Engine", "Tool" or "Catalog". */
  entityNoun: string;
  awsS3Mode?: AwsS3Mode;
  onAwsS3ModeChange?: (mode: AwsS3Mode) => void;
  snowflakeMode?: SnowflakeStorageMode;
  onSnowflakeModeChange?: (mode: SnowflakeStorageMode) => void;
}

interface PopoverState {
  platform: Platform;
  feature: Feature;
  entries: VersionEntry[];
}

const label = (p: Platform) => (p.variantLabel ? `${p.name} ${p.variantLabel}` : p.name);

/**
 * The phone layout. Thirty-odd engine columns cannot fit a phone, so instead
 * of the grid the reader picks one engine (and optionally a second to compare)
 * and reads its features as a list, grouped by category. Same filters, same
 * cells and the same detail popover as the grid.
 */
export function MobileMatrix({
  data,
  filters,
  entityNoun,
  awsS3Mode,
  onAwsS3ModeChange,
  snowflakeMode,
  onSnowflakeModeChange,
}: MobileMatrixProps) {
  const { platforms, features } = useMemo(() => applyFilters(data, filters), [data, filters]);
  // Default to the first two platforms when the filters (or a shared link)
  // picked several, else to the first one alone.
  const [primaryId, setPrimaryId] = useState<string>();
  const [secondaryId, setSecondaryId] = useState<string>();
  const [popover, setPopover] = useState<PopoverState | null>(null);

  const primary = platforms.find((p) => p.id === primaryId) ?? platforms[0];
  const secondaryDefault = filters.selectedPlatforms.length > 1 ? platforms[1]?.id : "";
  const secondary = platforms.find(
    (p) => p.id === (secondaryId ?? secondaryDefault) && p.id !== primary?.id,
  );
  const shown = secondary ? [primary, secondary] : primary ? [primary] : [];

  const groups = CATEGORY_ORDER.map((category) => ({
    category,
    features: features.filter((f) => f.category === category),
  })).filter((g) => g.features.length > 0);

  const shownGroups = new Set(shown.map((p) => p.group));
  const selectClass =
    "w-full rounded-md border border-gray-300 bg-white px-2 py-1.5 text-sm text-gray-900";

  if (platforms.length === 0) {
    return (
      <p className="rounded-lg border border-gray-200 bg-white p-4 text-center text-sm text-gray-500">
        No {entityNoun.toLowerCase()}s match the filters.
      </p>
    );
  }

  return (
    <div className="rounded-lg border border-gray-200 bg-white shadow-sm">
      <div className="grid grid-cols-2 gap-2 border-b border-gray-200 p-3">
        <label className="text-[10px] font-semibold uppercase tracking-wide text-gray-500">
          {entityNoun}
          <select
            className={`${selectClass} mt-1 normal-case tracking-normal`}
            value={primary.id}
            onChange={(e) => setPrimaryId(e.target.value)}
          >
            {platforms.map((p) => (
              <option key={p.id} value={p.id}>{label(p)}</option>
            ))}
          </select>
        </label>
        <label className="text-[10px] font-semibold uppercase tracking-wide text-gray-500">
          Compare with
          <select
            className={`${selectClass} mt-1 normal-case tracking-normal`}
            value={secondary?.id ?? ""}
            onChange={(e) => setSecondaryId(e.target.value)}
          >
            <option value="">None</option>
            {platforms
              .filter((p) => p.id !== primary.id)
              .map((p) => (
                <option key={p.id} value={p.id}>{label(p)}</option>
              ))}
          </select>
        </label>

        {/* Storage switches, only when a shown engine has them. */}
        {shownGroups.has("AWS") && awsS3Mode && onAwsS3ModeChange && (
          <StorageSwitch
            label="AWS storage"
            options={[["s3-buckets", "S3 Buckets"], ["s3-tables", "S3 Tables"]]}
            value={awsS3Mode}
            onChange={onAwsS3ModeChange}
          />
        )}
        {shownGroups.has("Snowflake") && snowflakeMode && onSnowflakeModeChange && (
          <StorageSwitch
            label="Snowflake storage"
            options={[["snowflake", "Managed"], ["external", "External"]]}
            value={snowflakeMode}
            onChange={onSnowflakeModeChange}
          />
        )}
      </div>

      {/* Column names, so a two-engine comparison reads left to right. */}
      {shown.length > 1 && (
        <div className="flex gap-2 border-b border-gray-200 px-3 py-1.5 text-[10px] font-semibold text-gray-600">
          <span className="flex-1" />
          {shown.map((p) => (
            <span key={p.id} className="w-24 shrink-0 truncate text-center">{label(p)}</span>
          ))}
        </div>
      )}

      {groups.map((group) => (
        <section key={group.category} aria-label={CATEGORY_LABELS[group.category]}>
          <h2
            className={`border-l-4 px-3 py-1.5 text-[10px] font-bold uppercase tracking-wider text-gray-600 ${CATEGORY_COLORS[group.category]}`}
          >
            {CATEGORY_LABELS[group.category]}
          </h2>
          <ul>
            {group.features.map((feature) => {
              const applicable = new Set(applicableVersions(feature, data.versions));
              const versions = filters.selectedVersions.filter((v) => applicable.has(v));
              const badge = versionBadge(feature, data.versions);
              return (
                <li
                  key={feature.id}
                  className="flex items-center gap-2 border-b border-gray-100 px-3 py-2"
                >
                  <div className="min-w-0 flex-1 text-sm text-gray-800">
                    {feature.name}
                    {badge && (
                      <span className="ml-1 rounded bg-gray-100 px-1 py-px align-middle text-[9px] font-semibold uppercase text-gray-500">
                        {badge}
                      </span>
                    )}
                  </div>
                  {shown.map((platform) => {
                    const entries = versions.map((version) => ({
                      version,
                      entry: getSupportEntry(data, platform.id, feature.id, version),
                    }));
                    return (
                      <div key={platform.id} className="w-24 shrink-0">
                        <SupportCell
                          entries={entries}
                          onClick={() => setPopover({ platform, feature, entries })}
                        />
                      </div>
                    );
                  })}
                </li>
              );
            })}
          </ul>
        </section>
      ))}

      {popover && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/20 p-4">
          <Suspense fallback={null}>
            <DetailPopover
              entries={popover.entries}
              feature={popover.feature}
              platform={popover.platform}
              onClose={() => setPopover(null)}
            />
          </Suspense>
        </div>
      )}
    </div>
  );
}

function StorageSwitch<T extends string>({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: [T, string][];
  value: T;
  onChange: (value: T) => void;
}) {
  return (
    <div role="group" aria-label={label} className="col-span-2 flex items-center gap-2 text-xs">
      <span className="text-[10px] font-semibold uppercase tracking-wide text-gray-500">{label}</span>
      {options.map(([v, text]) => (
        <button
          key={v}
          type="button"
          aria-pressed={value === v}
          onClick={() => onChange(v)}
          className={`rounded-full border px-2 py-0.5 ${
            value === v
              ? "border-blue-600 bg-blue-600 text-white"
              : "border-gray-300 bg-white text-gray-700"
          }`}
        >
          {text}
        </button>
      ))}
    </div>
  );
}
