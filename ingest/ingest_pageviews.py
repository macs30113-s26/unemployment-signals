import argparse
import gzip
import io
import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
from pathlib import Path

import yaml

_URL_FMT = (
    "https://dumps.wikimedia.org/other/pageviews/"
    "{y:04d}/{y:04d}-{m:02d}/pageviews-{y:04d}{m:02d}{d:02d}-{h:02d}0000.gz"
)

_TITLE_INDEX: dict[str, tuple[str, str]] | None = None
_STAGING_DIR: Path | None = None


def _build_index(articles_yaml: Path) -> dict[str, tuple[str, str]]:
    config = yaml.safe_load(articles_yaml.read_text())
    index: dict[str, tuple[str, str]] = {}
    for group_name, group_def in config["groups"].items():
        for entry in group_def["articles"]:
            canonical = entry["canonical"]
            index[canonical] = (canonical, group_name)
            for alias in entry.get("aliases", []) or []:
                index[alias] = (canonical, group_name)
    return index


def _init_worker(articles_yaml: str, staging_dir: str):
    global _TITLE_INDEX, _STAGING_DIR
    _TITLE_INDEX = _build_index(Path(articles_yaml))
    _STAGING_DIR = Path(staging_dir)


def _output_path(staging_dir: Path, y: int, m: int, d: int, h: int) -> Path:
    return (
        staging_dir
        / f"year={y:04d}"
        / f"month={m:02d}"
        / f"day={d:02d}"
        / f"hour={h:02d}.jsonl.gz"
    )


def process_hour(target: tuple[int, int, int, int]) -> dict:
    y, m, d, h = target
    out_path = _output_path(_STAGING_DIR, y, m, d, h)

    if out_path.exists() and out_path.stat().st_size > 0:
        return {"ok": True, "skipped": True, "target": target}

    url = _URL_FMT.format(y=y, m=m, d=d, h=h)
    t0 = time.time()
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "lsc4ss-final-project/1.0"}
        )
        rows: list[dict] = []
        capture_end = datetime(y, m, d, h, 0, 0).isoformat()

        with urllib.request.urlopen(req, timeout=120) as response:
            with gzip.GzipFile(fileobj=response) as gz:
                for raw_line in gz:
                    try:
                        line = raw_line.decode("latin-1")
                    except Exception:
                        continue
                    if not (line.startswith("en ") or line.startswith("en.m ")):
                        continue
                    parts = line.split(" ")
                    if len(parts) < 3:
                        continue
                    domain, title, count_str = parts[0], parts[1], parts[2]
                    hit = _TITLE_INDEX.get(title)
                    if hit is None:
                        continue
                    canonical, group = hit
                    try:
                        views = int(count_str)
                    except ValueError:
                        continue
                    rows.append({
                        "domain": domain,
                        "raw_title": title,
                        "canonical": canonical,
                        "group": group,
                        "views": views,
                        "capture_end_utc": capture_end,
                    })

        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = out_path.with_suffix(".jsonl.gz.tmp")
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz_out:
            for r in rows:
                gz_out.write((json.dumps(r) + "\n").encode("utf-8"))
        tmp_path.write_bytes(buf.getvalue())
        tmp_path.rename(out_path)

        return {
            "ok": True,
            "skipped": False,
            "target": target,
            "match_count": len(rows),
            "elapsed_s": round(time.time() - t0, 2),
        }

    except Exception as e:
        return {
            "ok": False,
            "target": target,
            "url": url,
            "error": f"{type(e).__name__}: {e}",
            "elapsed_s": round(time.time() - t0, 2),
        }


def hour_iter(start: datetime, end: datetime):
    cur = start
    while cur < end:
        yield cur.year, cur.month, cur.day, cur.hour
        cur += timedelta(hours=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--staging", required=True)
    ap.add_argument("--articles", default="config/articles.yaml")
    ap.add_argument("--workers", type=int, default=cpu_count())
    ap.add_argument("--max-targets", type=int, default=0)
    ap.add_argument("--log-interval", type=int, default=200)
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("ingest")

    start = datetime.strptime(args.start, "%Y-%m-%d")
    end = datetime.strptime(args.end, "%Y-%m-%d")

    targets = list(hour_iter(start, end))
    if args.max_targets:
        targets = targets[: args.max_targets]
    log.info(
        "window: %s -> %s, %d hourly targets, %d workers",
        start, end, len(targets), args.workers,
    )

    staging = Path(args.staging)
    staging.mkdir(parents=True, exist_ok=True)
    log.info("staging dir: %s", staging)

    t_start = time.time()
    n_done = 0
    n_skipped = 0
    n_failed = 0
    total_matches = 0

    with Pool(
        processes=args.workers,
        initializer=_init_worker,
        initargs=(str(Path(args.articles).resolve()), str(staging.resolve())),
    ) as pool:
        for result in pool.imap_unordered(process_hour, targets, chunksize=8):
            n_done += 1
            if not result["ok"]:
                n_failed += 1
                if n_failed <= 10:
                    log.warning(
                        "FAIL %s: %s",
                        result["target"],
                        result.get("error", "unknown"),
                    )
            elif result.get("skipped"):
                n_skipped += 1
            else:
                total_matches += result.get("match_count", 0)

            if n_done % args.log_interval == 0:
                elapsed = time.time() - t_start
                rate = n_done / elapsed if elapsed > 0 else 0
                eta_s = (len(targets) - n_done) / rate if rate > 0 else 0
                log.info(
                    "progress %d/%d (%.1f%%), %d skipped, %d failed, "
                    "%d total matches, %.1f/s, ETA %.0fmin",
                    n_done, len(targets), 100 * n_done / len(targets),
                    n_skipped, n_failed, total_matches, rate, eta_s / 60,
                )

    elapsed = time.time() - t_start
    log.info(
        "DONE in %.1fmin: %d done, %d skipped, %d failed, %d total matches",
        elapsed / 60, n_done, n_skipped, n_failed, total_matches,
    )

    if n_failed > max(10, len(targets) // 100):
        sys.exit(1)


if __name__ == "__main__":
    main()
