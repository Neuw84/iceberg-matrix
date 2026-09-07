"""Create or delete a small single-node Databricks cluster pinned to a specific
Databricks Runtime, so the feature suite can measure a runtime other than the
SQL warehouse's (e.g. DBR 16.4 LTS vs the latest DBSQL).

The cluster is Unity-Catalog-capable (SINGLE_USER security mode) because the
suite creates managed Iceberg tables, which require UC. It carries an
aggressive autotermination so a crashed run cannot bill for long, and teardown
permanently deletes it regardless.

Usage:
    python tests/databricks_cluster.py create --spark-version 16.4.x-scala2.12 \
        [--node-type m5d.large] [--name icebergmatrix-<tag>]
        -> prints CLUSTER_ID=<id> and HTTP_PATH=sql/protocolv1/o/0/<id> on stdout
    python tests/databricks_cluster.py delete --cluster-id <id>

Environment: DATABRICKS_HOST, DATABRICKS_TOKEN.
Exit codes: 0 ok, 1 failure (create timeout / API error), 2 bad usage.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

CREATE_TIMEOUT_S = 900  # cold single-node clusters can take ~5-10 min
POLL_S = 20


def _api(path: str, payload: dict | None = None) -> dict:
    url = f"{HOST}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data, method="POST" if data is not None else "GET",
        headers={"Authorization": f"Bearer {TOKEN}",
                 "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:500]
        raise SystemExit(f"[cluster] API {path} failed: HTTP {e.code}: {body}")


def create(spark_version: str, node_type: str, name: str) -> int:
    spec = {
        "cluster_name": name,
        "spark_version": spark_version,
        "node_type_id": node_type,
        "num_workers": 0,
        "autotermination_minutes": 30,
        # Single-node profile: one driver doing the work, cheapest shape that
        # can run the suite's small DDL/DML.
        "spark_conf": {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
        "custom_tags": {"ResourceClass": "SingleNode"},
        # Unity Catalog access (managed Iceberg tables live in UC).
        "data_security_mode": "SINGLE_USER",
    }
    out = _api("/api/2.1/clusters/create", spec)
    cluster_id = out.get("cluster_id")
    if not cluster_id:
        print(f"[cluster] create returned no cluster_id: {out}", file=sys.stderr)
        return 1
    print(f"[cluster] created {cluster_id} ({spark_version}); waiting for RUNNING",
          file=sys.stderr)

    deadline = time.time() + CREATE_TIMEOUT_S
    while time.time() < deadline:
        info = _api(f"/api/2.1/clusters/get?cluster_id={cluster_id}")
        state = info.get("state")
        if state == "RUNNING":
            print(f"[cluster] RUNNING", file=sys.stderr)
            # Machine-readable outputs for the workflow.
            print(f"CLUSTER_ID={cluster_id}")
            print(f"HTTP_PATH=sql/protocolv1/o/0/{cluster_id}")
            return 0
        if state in ("TERMINATED", "ERROR", "UNKNOWN"):
            reason = info.get("state_message") or info.get("termination_reason")
            print(f"[cluster] failed to start: state={state} {reason}",
                  file=sys.stderr)
            _api("/api/2.1/clusters/permanent-delete", {"cluster_id": cluster_id})
            return 1
        print(f"[cluster] state={state} ...", file=sys.stderr)
        time.sleep(POLL_S)

    print(f"[cluster] timed out waiting for RUNNING; deleting {cluster_id}",
          file=sys.stderr)
    _api("/api/2.1/clusters/permanent-delete", {"cluster_id": cluster_id})
    return 1


def delete(cluster_id: str) -> int:
    # Best-effort, never fails the workflow teardown.
    try:
        _api("/api/2.1/clusters/permanent-delete", {"cluster_id": cluster_id})
        print(f"[cluster] permanently deleted {cluster_id}", file=sys.stderr)
    except SystemExit as e:  # noqa: PERF203
        print(f"[cluster] delete failed (may already be gone): {e}",
              file=sys.stderr)
    return 0


def main() -> int:
    if not (HOST and TOKEN):
        print("[cluster] DATABRICKS_HOST / DATABRICKS_TOKEN not set", file=sys.stderr)
        return 2
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("create")
    c.add_argument("--spark-version", required=True)
    c.add_argument("--node-type", default="m5d.large")
    c.add_argument("--name", default=f"icebergmatrix-runtime-{int(time.time())}")
    d = sub.add_parser("delete")
    d.add_argument("--cluster-id", required=True)
    args = p.parse_args()
    if args.cmd == "create":
        return create(args.spark_version, args.node_type, args.name)
    return delete(args.cluster_id)


if __name__ == "__main__":
    sys.exit(main())
