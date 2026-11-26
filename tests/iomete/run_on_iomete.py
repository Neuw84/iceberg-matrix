"""Run the Spark suite (tests/iceberg_feature_tests.py) as an IOMETE Spark job and collect its report.

The job is created through IOMETE's Spark jobs API in a domain the token's user can run jobs in.
The suite, its spark_fixture.py sibling, features.json and the IOMETE engine file are mounted into
the job with configMaps, so nothing is uploaded elsewhere. tests/iomete/iomete_entrypoint.py runs
the suite against the platform's spark_catalog (IOMETE's Iceberg REST catalog) and prints the JSON
report into the driver log, which this script decodes into test-reports/iomete/. The job is deleted
afterwards; the suite drops the namespaces it creates.

Usage (image = a full Spark image reference your IOMETE install can pull):
  IOMETE_BASE_URL=https://<host> IOMETE_TOKEN=<api token> IOMETE_DOMAIN=<domain> \
    python3 tests/iomete/run_on_iomete.py suite spark4 <registry>/spark-py:4.2.1-v1
  ... suite spark3 <registry>/spark-py:3.5.9-v1
  ... dv <spark4 image> <spark3 image>   deletion vectors written on the first, read on the second
  ... shred <spark4 image>               variant shredding footer check, per table property
Optional: IOMETE_TOKEN_FILE instead of IOMETE_TOKEN; IOMETE_DRIVER_TYPE / IOMETE_EXECUTOR_TYPE;
MATRIX_CATALOG (default spark_catalog). The token is only sent as a request header, never printed.
"""
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
REPORTS = os.path.join(REPO, "test-reports", "iomete")
BEGIN, END = "@@ICEBERG_MATRIX_REPORT_BEGIN@@", "@@ICEBERG_MATRIX_REPORT_END@@"
TERMINAL = {"COMPLETED", "FAILED", "ABORTED", "SUBMISSION_FAILED"}

BASE = os.environ["IOMETE_BASE_URL"].rstrip("/")
DOMAIN = os.environ["IOMETE_DOMAIN"]
TOKEN = os.environ.get("IOMETE_TOKEN") or open(os.environ["IOMETE_TOKEN_FILE"]).read().strip()
JOBS = f"api/v2/domains/{DOMAIN}/spark/jobs"


def api(method, path, body=None, raw=False, ok=(200, 201, 204)):
    req = urllib.request.Request(f"{BASE}/{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None)
    req.add_header("X-API-Token", TOKEN)
    req.add_header("Authorization", f"Bearer {TOKEN}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            data = r.read()
            status = r.status
    except urllib.error.HTTPError as e:
        data, status = e.read(), e.code
    if status not in ok:
        raise RuntimeError(f"{method} {path} -> {status}: {data[:400]!r}")
    if raw:
        return data
    return json.loads(data) if data else None


def pick_node_type(types, component, env_var):
    if os.environ.get(env_var):
        return os.environ[env_var]
    fit = [t for t in types if component in t.get("components", []) and t.get("name") != "karate-jupyter"]
    # The suite runs ~70 small Spark jobs: take the smallest type with >= 4 GiB, else the largest.
    big = [t for t in fit if t.get("memory", 0) >= 4096]
    choice = min(big, key=lambda t: (t["cpu"], t["memory"])) if big else max(fit, key=lambda t: t["memory"])
    return choice["name"]


def setup():
    namespaces = api("GET", f"api/v3/domains/{DOMAIN}/namespaces")
    local = [n["namespace"] for n in namespaces if n.get("dataPlane") == "default" and n["namespace"] != "iomete-system"]
    namespace = (local or [n["namespace"] for n in namespaces if n["namespace"] != "iomete-system"])[0]
    bundle = api("GET", f"api/v1/bundles?scope=OWNED&domain={DOMAIN}")["items"][0]["bundleId"]
    user = api("GET", "api/v1/user")["id"]
    try:
        types = api("GET", "api/v1/admin/node-types")
    except RuntimeError:
        types = api("GET", f"api/v1/domains/{DOMAIN}/node-types")
    driver = pick_node_type(types, "DRIVER", "IOMETE_DRIVER_TYPE")
    executor = pick_node_type(types, "EXECUTOR", "IOMETE_EXECUTOR_TYPE")
    print(f"namespace={namespace} driver={driver} executor={executor}")
    return namespace, bundle, user, driver, executor


def config_maps(folder):
    def cm(path, mount):
        return {"key": os.path.basename(path), "content": open(path).read(), "mountPath": mount}
    maps = [cm(os.path.join(HERE, "iomete_entrypoint.py"), "/etc/configs/run")]
    if folder:
        maps += [
            cm(os.path.join(REPO, "tests", "iceberg_feature_tests.py"), "/etc/configs/matrix/tests"),
            cm(os.path.join(REPO, "tests", "spark_fixture.py"), "/etc/configs/matrix/fixture"),
            cm(os.path.join(REPO, "src", "data", "features.json"), "/etc/configs/matrix/src/data"),
            cm(os.path.join(REPO, "src", "data", "platforms", "iomete", folder, f"{folder}.json"),
               "/etc/configs/matrix/platform"),
        ]
    return maps


def run_job(ctx, image, mode, folder=None, label=""):
    namespace, bundle, user, driver, executor = ctx
    name = f"icebergmatrix-{mode}-{(folder or 'dv')}-{uuid.uuid4().hex[:6]}"
    env = {"PLATFORM_LABEL": label, "MATRIX_CATALOG": os.environ.get("MATRIX_CATALOG", "spark_catalog")}
    if folder:
        env.update({"MATRIX_PLATFORM_ID": f"iomete-{folder}", "MATRIX_DATA_PATH": f"platform/{folder}.json"})
    body = {
        "name": name, "namespace": namespace, "jobType": "MANUAL", "jobUser": user, "bundleId": bundle,
        "description": "iceberg-matrix feature suite (icebergmatrix.org), safe to delete",
        "template": {
            "image": image,
            "mainApplicationFile": "local:///etc/configs/run/iomete_entrypoint.py",
            "applicationType": "python",
            "arguments": [mode],
            "envVars": env,
            "configMaps": config_maps(folder),
            "restartPolicy": {"type": "Never"},
            "instanceConfig": {"driverType": driver, "executorType": executor, "executorCount": 1,
                               "singleNodeDeployment": False},
            "maxExecutionDurationSeconds": "5400",
        },
    }
    api("POST", JOBS, body)
    jobs = api("GET", f"{JOBS}?search={name}&page=0&size=20")
    job_id = next(j["id"] for j in jobs["items"] if j["name"] == name)
    print(f"job {name} ({job_id}) on {image}")
    try:
        run_id = api("POST", f"{JOBS}/{job_id}/runs", {})["id"]
        status, started = None, time.time()
        while time.time() - started < 6000:
            time.sleep(20)
            try:
                status = api("GET", f"{JOBS}/{job_id}/runs/{run_id}").get("driverStatus")
            except RuntimeError as e:
                print(f"  poll: {e}")
                continue
            print(f"  {int(time.time() - started)}s {status}")
            if status in TERMINAL:
                break
        log = api("GET", f"{JOBS}/{job_id}/runs/{run_id}/logs/download", raw=True).decode(errors="replace")
        os.makedirs(REPORTS, exist_ok=True)
        log_path = os.path.join(REPORTS, f"{name}.log")
        open(log_path, "w").write(log)
        print(f"  status={status} log={log_path} ({len(log)} chars)")
        return status, log
    finally:
        api("DELETE", f"{JOBS}/{job_id}", ok=(200, 204, 404))
        print(f"  deleted job {name}")


def extract_report(log):
    lines = log.splitlines()
    try:
        i = next(n for n, l in enumerate(lines) if BEGIN in l)
        j = next(n for n, l in enumerate(lines) if END in l and n > i)
    except StopIteration:
        return None
    blob = "".join(l.strip().split()[-1] for l in lines[i + 1:j] if l.strip())
    return json.loads(base64.b64decode(blob))


def main():
    cmd = sys.argv[1]
    ctx = setup()
    if cmd == "suite":
        folder, image = sys.argv[2], sys.argv[3]
        status, log = run_job(ctx, image, "suite", folder, label=f"IOMETE {image.rsplit('/', 1)[-1]}")
        report = extract_report(log)
        if not report:
            print("no report in the driver log; see the saved log")
            return 1
        out = os.path.join(REPORTS, f"report-{folder}-{image.replace(':', '_').replace('/', '_')}.json")
        json.dump(report, open(out, "w"), indent=2)
        s = report["summary"]
        print(f"spark {report['spark_version']} iceberg {report['iceberg_version']} catalog {report['catalog_mode']}")
        print(f"summary {s}  -> {out}")
        for t in report["tests"]:
            flag = "" if t["match"] else "  <-- DISCREPANCY"
            print(f"  {t['feature_id']:28} {t['version']} {t['result']:5} json={t['json_level']:8} "
                  f"{(t['details'] or '')[:90]}{flag}")
    elif cmd == "shred":
        _, log = run_job(ctx, sys.argv[2], "shred-probe")
        print("  " + "\n  ".join(l for l in log.splitlines() if "@@SHRED@@" in l or "Error" in l[:200]))
    elif cmd == "dv":
        write_image, read_image = sys.argv[2], sys.argv[3]
        for image, mode in ((write_image, "dv-write"), (read_image, "dv-read")):
            _, log = run_job(ctx, image, mode)
            print("  " + "\n  ".join(l for l in log.splitlines() if "@@DV_" in l))
    return 0


if __name__ == "__main__":
    sys.exit(main())
