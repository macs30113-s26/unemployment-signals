"""
Lambda worker for Wikipedia pageview ingest.

Invoked once per hourly pageview dump. Streams the .gz file from
dumps.wikimedia.org, decompresses on the fly, filters to a curated set of
article titles (only en/en.m projects), and writes a small Parquet file to S3.

This is the "fan-out" leaf of the pipeline — the orchestrator generates ~65k
event payloads (one per hour from 2019-01-01 to 2026-05-01) and async-invokes
this function concurrently. With Lambda concurrency ~50, the full ingest of
~65k files completes in roughly an hour rather than the ~50 hours a single
worker would need.

Memory: 1024 MB is generous; peak in-memory size is one decompressed hourly
file (~300 MB) when we read it all at once. Streaming the gunzip output keeps
the working set well under the limit.

Timeout: 5 minutes. A typical file takes 2-4 seconds end-to-end (download
dominates at ~50-80 MB compressed); 5 min covers network blips and retries.

Environment variables expected (set on the Lambda function):
    BUCKET                S3 bucket name for staged output
    STAGING_PREFIX        S3 key prefix, e.g. "staging" -> staging/yyyy/mm/dd/hh.parquet
    ARTICLES_KEY          S3 key for articles.yaml (uploaded once by orchestrator)

Event payload shape (from orchestrator):
    {
        "year": 2024,
        "month": 1,
        "day": 15,
        "hour": 12
    }
"""

import gzip
import io
import json
import os
import time
import urllib.request
from datetime import datetime

import boto3
import yaml

# NOTE on dependencies: this Lambda intentionally uses *only* PyYAML + boto3
# (boto3 is provided by the Lambda runtime). We avoid pyarrow because the
# manylinux wheel is ~130 MB, which would push the deployment package over
# Lambda's 50 MB direct-upload limit and force S3-staged uploads.
#
# Instead, we write gzipped JSON Lines at the staging layer. The aggregation
# job (Spark) handles the Parquet conversion when it does the rollups. This
# is also semantically cleaner: Parquet's columnar advantage exists for
# wide-fan-out analytical queries, not for the one-hour-of-30-articles files
# we produce here. Per-file Parquet metadata overhead would exceed payload.

# Globals are reused across warm invocations — important for performance.
# Lambda containers stay warm for ~5-15 min, so these initializations amortize
# across many events.
_S3 = boto3.client("s3")
_TITLE_INDEX: dict[str, tuple[str, str]] | None = None  # title -> (canonical, group)
_BUCKET = os.environ["BUCKET"]
_STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "staging").rstrip("/")
_ARTICLES_KEY = os.environ.get("ARTICLES_KEY", "config/articles.yaml")

# Wikimedia uses this URL pattern. Date in filename is the END of the capture
# hour per Wikimedia convention.
_URL_FMT = (
    "https://dumps.wikimedia.org/other/pageviews/"
    "{y:04d}/{y:04d}-{m:02d}/pageviews-{y:04d}{m:02d}{d:02d}-{h:02d}0000.gz"
)


def _load_article_index() -> dict[str, tuple[str, str]]:
    """Build a flat lookup: title (canonical or alias) -> (canonical_title, group_name).

    Cached at module scope so warm Lambda invocations skip the S3 GET.
    """
    global _TITLE_INDEX
    if _TITLE_INDEX is not None:
        return _TITLE_INDEX

    obj = _S3.get_object(Bucket=_BUCKET, Key=_ARTICLES_KEY)
    config = yaml.safe_load(obj["Body"].read())

    index: dict[str, tuple[str, str]] = {}
    for group_name, group_def in config["groups"].items():
        for entry in group_def["articles"]:
            canonical = entry["canonical"]
            index[canonical] = (canonical, group_name)
            for alias in entry.get("aliases", []) or []:
                index[alias] = (canonical, group_name)

    _TITLE_INDEX = index
    return index


def _stream_and_filter(url: str, index: dict[str, tuple[str, str]]) -> list[dict]:
    """Stream-download a gzipped pageview dump and return only matching rows.

    Memory-conscious: never materializes the full decompressed file. We read
    the gzip stream line by line and emit only matches, so peak heap is
    bounded by the (tiny) match set.
    """
    req = urllib.request.Request(url, headers={"User-Agent": "lsc4ss-final-project/1.0"})
    rows: list[dict] = []

    with urllib.request.urlopen(req, timeout=120) as response:
        # GzipFile wraps the HTTP stream — no temp file, no full read.
        with gzip.GzipFile(fileobj=response) as gz:
            # Decode as latin-1 to avoid surprises from non-UTF-8 bytes in
            # exotic article titles. We never write the title out, we only
            # match it against our index, so latin-1 is safe.
            for raw_line in gz:
                try:
                    line = raw_line.decode("latin-1")
                except Exception:
                    continue

                # Cheap pre-filter: skip everything that isn't English Wikipedia.
                # The "en " or "en.m " prefix check avoids splitting most lines.
                if not (line.startswith("en ") or line.startswith("en.m ")):
                    continue

                parts = line.split(" ")
                if len(parts) < 3:
                    continue

                domain, title, count_str = parts[0], parts[1], parts[2]
                hit = index.get(title)
                if hit is None:
                    continue

                canonical, group = hit
                try:
                    views = int(count_str)
                except ValueError:
                    continue

                rows.append(
                    {
                        "domain": domain,        # "en" or "en.m"
                        "raw_title": title,      # what appeared in the dump
                        "canonical": canonical,  # post-redirect-resolution title
                        "group": group,          # thematic group
                        "views": views,
                    }
                )

    return rows


def _write_jsonl_gz(rows: list[dict], y: int, m: int, d: int, h: int) -> str:
    """Write filtered rows to S3 as gzipped JSON Lines. Returns the S3 key.

    Format: one JSON object per line, gzip-compressed.
      {"domain": "en", "raw_title": "...", "canonical": "...",
       "group": "...", "views": 123, "capture_end_utc": "2024-01-15T12:00:00"}

    Why JSONL instead of CSV: handles titles with commas/quotes safely with
    no escaping logic. Why gzip: ~5x smaller on this data, and Spark reads
    *.json.gz transparently with no extra config.

    We write an empty file (just the gzip header) when no rows match — this
    is important because the orchestrator's "already_done" check looks for
    the key's existence, not its size. An empty file marks an hour as
    processed-and-empty, preventing wasteful re-invocations.
    """
    # Add timestamp column. The hour is the END of the capture window
    # (Wikimedia convention) — we keep this exact and document it.
    capture_end = datetime(y, m, d, h, 0, 0).isoformat()

    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for r in rows:
            r["capture_end_utc"] = capture_end
            gz.write((json.dumps(r) + "\n").encode("utf-8"))
    buf.seek(0)

    # Hive-style partition key layout — lets Spark/Athena prune by date.
    key = f"{_STAGING_PREFIX}/year={y:04d}/month={m:02d}/day={d:02d}/hour={h:02d}.jsonl.gz"
    _S3.put_object(Bucket=_BUCKET, Key=key, Body=buf.getvalue())
    return key


def lambda_handler(event, context):
    """Entry point: process one (year, month, day, hour) tuple."""
    start = time.time()
    y, m, d, h = int(event["year"]), int(event["month"]), int(event["day"]), int(event["hour"])

    url = _URL_FMT.format(y=y, m=m, d=d, h=h)
    index = _load_article_index()

    try:
        rows = _stream_and_filter(url, index)
    except Exception as e:
        # Surface failures in CloudWatch but don't crash the orchestrator —
        # it'll see the error from the response and can retry just this hour.
        return {
            "ok": False,
            "url": url,
            "error": f"{type(e).__name__}: {e}",
            "elapsed_s": round(time.time() - start, 2),
        }

    key = _write_jsonl_gz(rows, y, m, d, h)

    return {
        "ok": True,
        "url": url,
        "s3_key": key,
        "match_count": len(rows),
        "elapsed_s": round(time.time() - start, 2),
    }


if __name__ == "__main__":
    # Local smoke test — bypass the actual Wikimedia download and just exercise
    # the article-index load. Run with: BUCKET=... python lambda_function.py
    print(json.dumps(_load_article_index(), indent=2, default=str))
