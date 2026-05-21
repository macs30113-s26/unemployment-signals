"""
REST API ingest — Layer 1 primary path.

Wikimedia provides a public REST API that returns pre-aggregated per-article
daily pageview counts. For our 31 curated articles across the 2019-2026
window, this is ~30 HTTP requests total — each returning ~2700 days of data.
Total wall-clock: 1-3 minutes, parallelised with a small multiprocessing pool.

This is the fast path used for the actual research. The slower dump-pull
path in ingest_pageviews.py is retained for cross-validation against a
subset of the data (see aggregate/validate_dump_vs_rest.py) — together
they form a two-tier ingest strategy that gives both speed and
audit-against-source verification.

Output format: writes one JSONL file per article into REST_DIR with one
record per day. This is consumed by aggregate_pageviews.py.

Usage:
    python ingest/ingest_pageviews_rest.py \\
        --start 2019-01-01 --end 2026-05-01 \\
        --output /scratch/midway3/$USER/wiki-attention/staging_rest \\
        --articles config/articles.yaml \\
        --workers 6
"""

import argparse
import gzip
import io
import json
import logging
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path

import yaml

REST_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    "en.wikipedia/all-access/user/"
    "{title}/daily/{start}/{end}"
)


def load_articles(yaml_path: Path):
    """Return list of (canonical_title, group_name) for all 31 articles."""
    config = yaml.safe_load(yaml_path.read_text())
    articles = []
    for group_name, group_def in config["groups"].items():
        for entry in group_def["articles"]:
            articles.append((entry["canonical"], group_name))
    return articles


def _do_request(url: str, max_retries: int = 5) -> dict:
    last_error = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "lsc4ss-final-project/1.0 (yinyuanc@uchicago.edu)",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_error = f"HTTP {e.code}"
            if e.code == 404:
                return {"items": []}
            wait = 2 * (2 ** attempt) + random.uniform(0, 2)
            time.sleep(wait)
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            wait = 2 * (2 ** attempt) + random.uniform(0, 2)
            time.sleep(wait)
    raise RuntimeError(f"failed after {max_retries} retries: {last_error}")


def fetch_article(args):
    title, group, start_yyyymmdd, end_yyyymmdd, out_dir = args
    t0 = time.time()
    out_path = Path(out_dir) / f"{title}.jsonl.gz"
    if out_path.exists() and out_path.stat().st_size > 0:
        return {"ok": True, "skipped": True, "title": title}

    time.sleep(random.uniform(0, 1.0))

    url = REST_URL.format(title=urllib.parse.quote(title, safe=""), start=start_yyyymmdd, end=end_yyyymmdd)
    try:
        data = _do_request(url)
        items = data.get("items", [])
        rows = []
        for it in items:
            day = it["timestamp"][:8]
            rows.append({
                "canonical": title,
                "group": group,
                "date": f"{day[:4]}-{day[4:6]}-{day[6:8]}",
                "views": int(it["views"]),
            })
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            for r in rows:
                gz.write((json.dumps(r) + "\n").encode("utf-8"))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(buf.getvalue())
        return {
            "ok": True,
            "skipped": False,
            "title": title,
            "n_days": len(rows),
            "elapsed_s": round(time.time() - t0, 2),
        }
    except Exception as e:
        return {
            "ok": False,
            "title": title,
            "error": str(e),
            "elapsed_s": round(time.time() - t0, 2),
        }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--articles", default="config/articles.yaml")
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("rest")

    start = datetime.strptime(args.start, "%Y-%m-%d").strftime("%Y%m%d")
    end = datetime.strptime(args.end, "%Y-%m-%d").strftime("%Y%m%d")

    articles = load_articles(Path(args.articles))
    log.info("%d articles, REST window %s -> %s, %d workers",
             len(articles), start, end, args.workers)

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    tasks = [(t, g, start, end, str(out_dir)) for t, g in articles]

    t_start = time.time()
    results = []
    with Pool(processes=args.workers) as pool:
        for r in pool.imap_unordered(fetch_article, tasks):
            results.append(r)
            status = "OK" if r["ok"] else "FAIL"
            if r.get("skipped"):
                log.info("SKIP %s (already exists)", r["title"])
            elif r["ok"]:
                log.info("OK   %s  %d days  %.1fs", r["title"],
                         r.get("n_days", 0), r.get("elapsed_s", 0))
            else:
                log.warning("FAIL %s: %s", r["title"], r.get("error", "?"))

    elapsed = time.time() - t_start
    n_ok = sum(1 for r in results if r["ok"] and not r.get("skipped"))
    n_skip = sum(1 for r in results if r.get("skipped"))
    n_fail = sum(1 for r in results if not r["ok"])
    total_days = sum(r.get("n_days", 0) for r in results)
    log.info("DONE in %.1fs: %d ok, %d skipped, %d failed, %d total day-records",
             elapsed, n_ok, n_skip, n_fail, total_days)

    if n_fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
