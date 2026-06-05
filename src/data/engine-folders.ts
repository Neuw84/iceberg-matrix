// Test/dev utility helpers that formalize the engine folder-naming rule used by
// the nested `src/data/platforms/` structure. These functions are pure and
// side-effect-free; they are exercised by the structural/property tests only and
// are NOT imported by the app runtime (`load-data.ts` uses explicit static
// imports, so the folder names are encoded directly in import paths).

/**
 * Derive the engine subfolder name for a platform id.
 *
 * The rule: if the platform has a vendor prefix (the leading hyphen-delimited
 * segment that names its vendor) and the id begins with `vendorPrefix + "-"`,
 * the folder name is the id with that prefix and its trailing hyphen removed.
 * Otherwise the folder name equals the id unchanged.
 *
 * @param platformId   The platform identifier (e.g. `"aws-redshift-s3"`, `"duckdb"`).
 * @param vendorPrefix The lowercase vendor segment (e.g. `"aws"`, `"google"`),
 *                     or `null` when the platform has no vendor prefix.
 * @returns The engine folder name.
 *
 * @example
 * deriveEngineFolderName("google-bigquery", "google"); // "bigquery"
 * deriveEngineFolderName("aws-redshift-s3", "aws");     // "redshift-s3"
 * deriveEngineFolderName("aws-managed-flink", "aws");   // "managed-flink"
 * deriveEngineFolderName("duckdb", null);               // "duckdb"
 * deriveEngineFolderName("kafka-connect", null);        // "kafka-connect"
 */
export function deriveEngineFolderName(platformId: string, vendorPrefix: string | null): string {
  if (vendorPrefix && platformId.startsWith(vendorPrefix + "-")) {
    return platformId.slice(vendorPrefix.length + 1);
  }
  return platformId;
}

/**
 * The set of platform ids that live under a single vendor folder, together with
 * the vendor prefix that applies to those ids (shared per vendor; `null` when the
 * vendor's ids carry no prefix, e.g. the `oss` engines).
 */
export interface VendorEngineGroup {
  /** The vendor prefix applied to every id in this group, or `null` for none. */
  vendorPrefix: string | null;
  /** The platform ids stored under this vendor folder. */
  platformIds: string[];
}

/**
 * A single folder-name collision within one vendor: two or more platform ids in
 * that vendor derived to the same engine folder name.
 */
export interface FolderNameCollision {
  /** The vendor folder key whose engines collide. */
  vendor: string;
  /** The duplicated engine folder name that two or more ids resolved to. */
  folderName: string;
  /** The platform ids (2 or more) that resolved to `folderName`, in input order. */
  platformIds: string[];
}

/**
 * Detect engine folder-name collisions within each vendor.
 *
 * For each vendor group, every platform id is reduced to its engine folder name
 * via {@link deriveEngineFolderName} using that group's `vendorPrefix`. A collision
 * is reported for any derived folder name shared by two or more ids in the same
 * vendor, naming the duplicated folder name and the platform ids involved. When
 * all derived names within a vendor are distinct, that vendor produces no
 * collision entries; an input with no collisions anywhere yields an empty array.
 *
 * Pure and side-effect-free.
 *
 * @param idsByVendor Map keyed by vendor folder name to that vendor's engine group.
 * @returns A flat list of collisions across all vendors (empty when there are none).
 *
 * @example
 * detectFolderNameCollisions({
 *   aws: { vendorPrefix: "aws", platformIds: ["aws-athena", "aws-emr"] },
 * }); // [] (no collisions)
 *
 * detectFolderNameCollisions({
 *   demo: { vendorPrefix: "demo", platformIds: ["demo-a", "a"] },
 * }); // [{ vendor: "demo", folderName: "a", platformIds: ["demo-a", "a"] }]
 */
export function detectFolderNameCollisions(
  idsByVendor: Record<string, VendorEngineGroup>,
): FolderNameCollision[] {
  const collisions: FolderNameCollision[] = [];

  for (const [vendor, group] of Object.entries(idsByVendor)) {
    // Group platform ids by their derived folder name, preserving input order.
    const idsByFolderName = new Map<string, string[]>();
    for (const platformId of group.platformIds) {
      const folderName = deriveEngineFolderName(platformId, group.vendorPrefix);
      const existing = idsByFolderName.get(folderName);
      if (existing) {
        existing.push(platformId);
      } else {
        idsByFolderName.set(folderName, [platformId]);
      }
    }

    for (const [folderName, platformIds] of idsByFolderName) {
      if (platformIds.length >= 2) {
        collisions.push({ vendor, folderName, platformIds });
      }
    }
  }

  return collisions;
}
