"""Shared plumbing for the AWS platform test drivers.

EMR Serverless and Glue differ only in how a job is created and run. Everything
around that is identical: the same repo bundle, the same S3 layout, the same
report download, the same summary. That common part lives here so the two
drivers cannot drift apart, which matters most for the report format -- the whole
point is that every engine publishes the same shape.

S3 layout, one prefix per engine so engines never collide:

    s3://<bucket>/<engine>/scripts/<run>/     entry point + repo bundle
    s3://<bucket>/<engine>/warehouse/<run>/   table data (s3buckets mode)
    s3://<bucket>/<engine>/logs/<run>/        job logs
    s3://<bucket>/<engine>/reports/<run>/     JSON + markdown reports

Environment (shared by all drivers):
    AWS_REGION            (required)
    AWS_DATA_BUCKET       (required) bucket for scripts/warehouse/logs/reports
    AWS_TABLE_BUCKET_ARN  (required for the s3tables mode)
    RUN_TAG               (required) unique per run, e.g. icebergmatrix-<run_id>
    MODES                 both|s3buckets|s3tables      (default: both)
    RESOURCE_PREFIX       (default: icebergmatrix) must match the IAM scope
    JOB_TIMEOUT_MINUTES   (default: 30)
"""

import os
import time
import zipfile
from pathlib import Path

import boto3

REGION = os.environ["AWS_REGION"]
DATA_BUCKET = os.environ["AWS_DATA_BUCKET"]
TABLE_BUCKET_ARN = os.environ.get("AWS_TABLE_BUCKET_ARN", "")
RUN_TAG = os.environ["RUN_TAG"]
MODES = os.environ.get("MODES", "both")
RESOURCE_PREFIX = os.environ.get("RESOURCE_PREFIX", "icebergmatrix")
JOB_TIMEOUT_MINUTES = int(os.environ.get("JOB_TIMEOUT_MINUTES", "30"))

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
LOCAL_REPORT_DIR = REPO_ROOT / "test-reports"

# Catalog implementation used by both engines and both storage modes. For
# s3buckets it addresses the default Glue catalog; for s3tables it addresses the
# federated s3tablescatalog through glue.id. See infra/aws/README.MD section 7.
GLUE_CATALOG_IMPL = "org.apache.iceberg.aws.glue.GlueCatalog"

# Engines whose matrix data must travel in the bundle. The suite compares its
# results against these files on the cluster, so a missing one turns every test
# into a discrepancy.
BUNDLED_ENGINES = ("emr", "glue")
STORAGE_MODES = ("s3buckets", "s3tables")

s3 = boto3.client("s3", region_name=REGION)


def s3_uri(*parts: str) -> str:
    return f"s3://{DATA_BUCKET}/" + "/".join(p.strip("/") for p in parts)


def modes_to_run() -> list:
    return list(STORAGE_MODES) if MODES == "both" else [MODES]


def s3tables_glue_id() -> str:
    """The federated catalog id that identifies the table bucket to Glue.

    Format <account>:s3tablescatalog/<table-bucket-name>, derived from the table
    bucket ARN so there is one source of truth.
    """
    account = TABLE_BUCKET_ARN.split(":")[4]
    bucket = TABLE_BUCKET_ARN.rsplit("/", 1)[-1]
    return f"{account}:s3tablescatalog/{bucket}"


def s3tables_catalog_props() -> str:
    """Properties addressing the federated S3 Tables catalog through Glue."""
    return f"glue.id={s3tables_glue_id()},client.region={REGION}"


def build_bundle(dest: Path) -> Path:
    """Zip the suite and the matrix data the suite compares against.

    Engine-agnostic on purpose: every engine's platform JSON is included, so one
    bundle serves any driver and adding an engine does not mean remembering to
    extend this. The whole thing is tens of KiB.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    includes = [REPO_ROOT / "tests" / "iceberg_feature_tests.py",
                REPO_ROOT / "src" / "data" / "features.json"]
    for engine in BUNDLED_ENGINES:
        for mode in STORAGE_MODES:
            includes.append(REPO_ROOT / "src" / "data" / "platforms" / "aws"
                            / mode / engine / f"{engine}.json")

    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in includes:
            if not path.is_file():
                raise FileNotFoundError(f"bundle input missing: {path}")
            zf.write(path, path.relative_to(REPO_ROOT).as_posix())
    print(f"[driver] bundle: {dest} ({dest.stat().st_size // 1024} KiB)")
    return dest


def upload(local: Path, key: str) -> str:
    s3.upload_file(str(local), DATA_BUCKET, key)
    uri = f"s3://{DATA_BUCKET}/{key}"
    print(f"[driver] uploaded {uri}")
    return uri


def wait_for(get_state, want: set, bad: set, what: str, timeout_s: int = 600) -> str:
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        state = get_state()
        if state != last:
            print(f"[driver] {what}: {state}")
            last = state
        if state in want:
            return state
        if state in bad:
            raise RuntimeError(f"{what} entered {state}")
        time.sleep(10)
    raise TimeoutError(f"{what} still {last} after {timeout_s}s")


def download_reports(engine: str, mode: str) -> Path | None:
    """Pull the report this mode produced into test-reports/, OSS-style names."""
    prefix = f"{engine}/reports/{RUN_TAG}/{mode}/"
    listing = s3.list_objects_v2(Bucket=DATA_BUCKET, Prefix=prefix).get("Contents", [])
    if not listing:
        print(f"[driver] no report objects under s3://{DATA_BUCKET}/{prefix}")
        return None

    LOCAL_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = None
    for obj in listing:
        name = obj["Key"].rsplit("/", 1)[-1]
        # <engine>-<mode>-iceberg-test-report.json / .md
        local = LOCAL_REPORT_DIR / f"{engine}-{mode}-{name}"
        s3.download_file(DATA_BUCKET, obj["Key"], str(local))
        print(f"[driver] report: {local.relative_to(REPO_ROOT)}")
        if local.suffix == ".json":
            json_path = local
    return json_path


def local_report_md(engine: str, mode: str) -> Path | None:
    """The markdown report download_reports() put next to the JSON, if it exists."""
    path = LOCAL_REPORT_DIR / f"{engine}-{mode}-iceberg-test-report.md"
    return path if path.is_file() else None


def demote_headings(md: str, by: int = 2) -> str:
    """Shift markdown heading levels so a report nests under a per-mode heading.

    The suite writes a standalone document starting at '#'. Embedding that as-is
    would produce several competing top-level headings in one job summary, so
    each level is pushed down. Fenced blocks are left alone: a '#' at the start
    of a line inside one is content, not a heading.
    """
    out, in_fence = [], False
    for line in md.splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
        if not in_fence and line.startswith("#"):
            depth = len(line) - len(line.lstrip("#"))
            line = "#" * min(depth + by, 6) + line[depth:]
        out.append(line)
    return "\n".join(out)


def summarise(engine: str, title: str, header: list, results: list, reports: dict) -> int:
    """Build the job summary: a combined verdict, then each mode's full report.

    The per-mode markdown is embedded verbatim rather than re-rendered here, so
    the feature-by-feature matrix is identical to the one the OSS engine suites
    publish and cannot drift from it. Two reports are around 20 KB together,
    well inside the 1 MiB job-summary limit.

    Returns 1 if any mode failed, produced no report, or reported errors or
    discrepancies, so the caller can exit non-zero.
    """
    lines = [f"# {title}", ""] + header + [""]
    worst = 0

    # Lead with every mode side by side so the outcome is visible without
    # scrolling past two full matrices.
    verdict = ["| Mode | Total | Passed | Failed | Skipped | Errors | Discrepancies |",
               "|------|-------|--------|--------|---------|--------|---------------|"]
    for r in results:
        rep = reports.get(r["mode"])
        if r["state"] != "SUCCESS" or not rep:
            state = r["state"] if r["state"] != "SUCCESS" else "NO REPORT"
            verdict.append(f"| {r['mode']} | {state} | | | | | |")
            worst = max(worst, 1)
            continue
        s = rep["summary"]
        verdict.append(f"| {r['mode']} | {s['total']} | {s['passed']} | {s['failed']} | "
                       f"{s['skipped']} | {s['errors']} | {s['discrepancies']} |")
        if s["discrepancies"] or s["errors"]:
            worst = max(worst, 1)
    lines += verdict + [""]

    for r in results:
        rep = reports.get(r["mode"])
        lines.append(f"## {r['mode']}")
        lines.append("")
        if r["state"] != "SUCCESS":
            lines.append(f"Job run {r['state']}: {r['state_details']}")
            lines.append("")
            continue
        if not rep:
            lines.append("Job succeeded but produced no report.")
            lines.append("")
            continue

        discs = [t for t in rep["tests"] if not t["match"]]
        if discs:
            # Called out ahead of the matrix: a discrepancy is the one thing
            # that needs acting on, and it is easy to miss among 70 rows.
            lines.append("### Discrepancies")
            lines.append("")
            for t in discs:
                lines.append(f"- **{t['feature_name']}** ({t['version']}): "
                             f"test={t['result']}, json={t['json_level']} — {t['details'][:160]}")
            lines.append("")

        md = local_report_md(engine, r["mode"])
        if md:
            lines.append(demote_headings(md.read_text()))
            lines.append("")
        else:
            # Fall back to the counts rather than showing nothing.
            lines += [f"Report markdown missing; counts only: {rep['summary']}", ""]

    text = "\n".join(lines)
    print("\n" + text)
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(text + "\n")
    return worst
