#!/usr/bin/env python3
"""Remove what interrupted BigQuery suite runs left behind.

A completed run cleans up after itself; a killed one does not. This deletes:
  - Lakehouse namespaces named bq_* (BigQuery suite) or ms_* (Managed Spark),
    their tables first, in GCP_LAKEHOUSE_CATALOG
  - tables in GCP_DATASET whose names match the suite's prefixes
  - objects under gs://GCP_DATA_BUCKET/managed/ and /lakehouse/bq_*

It only touches names the suite itself generates. Pass --dry-run to list
without deleting.

Usage: source tests/gcp/env.sh && uv run --python 3.11 $GCP_UV_WITH \
           python tests/gcp/teardown.py [--dry-run]
"""
import os
import re
import sys

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery, storage

PROJECT = os.environ["GCP_PROJECT"]
REGION = os.environ.get("GCP_REGION", "us-central1")
DATASET = os.environ.get("GCP_DATASET", "icebergmatrix_tests")
BUCKET = os.environ.get("GCP_DATA_BUCKET", "")
CATALOG = os.environ.get("GCP_LAKEHOUSE_CATALOG", "")
DRY = "--dry-run" in sys.argv
PREFIX = (f"https://biglake.googleapis.com/iceberg/v1/restcatalog/v1/"
          f"projects/{PROJECT}/catalogs/{CATALOG}")
SUITE_TABLE = re.compile(r"^(fx|create|read|ins|dml|cat|rowops|schema|promo|hp|hpb|pe|tt|maint|"
                         r"ref|stats|bloom|ty|v3probe)_[0-9a-f]+$|^fx_\w+$")


def act(what: str, fn) -> None:
    print(("would delete " if DRY else "deleting ") + what)
    if not DRY:
        fn()


def lakehouse() -> None:
    if not CATALOG:
        return
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    s = AuthorizedSession(creds)
    s.headers.update({"x-goog-user-project": PROJECT,
                      "X-Iceberg-Access-Delegation": "vended-credentials"})
    for ns in s.get(f"{PREFIX}/namespaces").json().get("namespaces", []):
        name = ns[0]
        if not name.startswith(("bq_", "ms_")):
            continue
        for ident in s.get(f"{PREFIX}/namespaces/{name}/tables").json().get("identifiers", []):
            t = ident["name"]
            act(f"table {name}.{t}", lambda t=t: s.delete(
                f"{PREFIX}/namespaces/{name}/tables/{t}", params={"purgeRequested": "true"}))
        act(f"namespace {name}", lambda: s.delete(f"{PREFIX}/namespaces/{name}"))


def managed() -> None:
    bq = bigquery.Client(project=PROJECT, location=REGION)
    for t in bq.list_tables(f"{PROJECT}.{DATASET}"):
        if SUITE_TABLE.match(t.table_id):
            act(f"table {DATASET}.{t.table_id}",
                lambda t=t: bq.delete_table(t.reference, not_found_ok=True))


def objects() -> None:
    if not BUCKET:
        return
    bucket = storage.Client(project=PROJECT).bucket(BUCKET)
    blobs = list(bucket.list_blobs(prefix="managed/"))
    blobs += [b for b in bucket.list_blobs(prefix="lakehouse/bq_")]
    if blobs:
        act(f"{len(blobs)} object(s) under gs://{BUCKET}/managed/ and lakehouse/bq_*",
            lambda: [b.delete() for b in blobs])


if __name__ == "__main__":
    lakehouse()
    managed()
    objects()
