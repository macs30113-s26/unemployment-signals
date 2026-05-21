#!/usr/bin/env python
"""
Cross-validate the Dask dump aggregation against the Dask REST aggregation
at the DAILY level, restricted to days with >=20 hours of dump coverage.

Why daily: big1's 7-day pull windows do not align with ISO Monday-Sunday week
boundaries, so dump weekly sums are partial-week and not directly comparable
to REST weekly sums. Daily is the right comparison grain.

Why filter: 49 days in dump staging have <12 hours of data (scatter samples
from earlier exploratory pulls). Those days' daily sums under-count REST by
1-3 orders of magnitude purely because of missing hours, which is a sampling
artifact, not a data-source disagreement. The complete-days subset (>=20
hours) is the fair comparison set.

Output: analyze/figures/dask_dump_vs_rest_validation.csv with per-row
relative diffs for the complete-day subset.
"""
import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Defaults are computed from $USER if not provided on the command line, so
# anyone with a Midway3 account can reproduce after running aggregate_dask.py
# with both --source rest and --source dump.
USER = os.environ.get("USER", "yinyuanc")
DEFAULT_DUMP = f"/scratch/midway3/{USER}/wiki-attention/curated_dask_dump_slurm/daily_dask.csv"
DEFAULT_REST = f"/scratch/midway3/{USER}/wiki-attention/curated_dask_slurm/daily_dask.csv"
DEFAULT_COMPLETE_DAYS = "analyze/figures/dump_complete_days.json"
DEFAULT_OUT = "analyze/figures/dask_dump_vs_rest_validation.csv"

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--dump-csv", default=DEFAULT_DUMP,
                    help="Path to Dask dump daily aggregation CSV")
parser.add_argument("--rest-csv", default=DEFAULT_REST,
                    help="Path to Dask REST daily aggregation CSV")
parser.add_argument("--complete-days-json", default=DEFAULT_COMPLETE_DAYS,
                    help="Path to JSON list of dump days with >=20h coverage")
parser.add_argument("--out-csv", default=DEFAULT_OUT,
                    help="Output CSV path")
args = parser.parse_args()

DUMP_CSV = args.dump_csv
REST_CSV = args.rest_csv
COMPLETE_DAYS_JSON = args.complete_days_json
OUT_CSV = args.out_csv


def main():
    dump = pd.read_csv(DUMP_CSV, parse_dates=["day"])
    rest = pd.read_csv(REST_CSV, parse_dates=["day"])
    print(f"[xval] dump: {len(dump):,} rows, {dump['canonical'].nunique()} articles")
    print(f"[xval] rest: {len(rest):,} rows, {rest['canonical'].nunique()} articles")
    print(f"[xval] dump day range: {dump['day'].min().date()} .. {dump['day'].max().date()}")
    print(f"[xval] rest day range: {rest['day'].min().date()} .. {rest['day'].max().date()}")

    # Load complete-day set (>=20 hours of dump coverage).
    with open(COMPLETE_DAYS_JSON) as f:
        cd_info = json.load(f)
    complete_days = pd.to_datetime(cd_info["days"])
    print(f"[xval] complete-day filter: {cd_info['n_days']} days "
          f"with >={cd_info['threshold_hours']} hours of dump coverage")

    # Inner-join on (canonical, group, day), then restrict to complete days.
    merged = dump.merge(
        rest,
        on=["canonical", "group", "day"],
        how="inner",
        suffixes=("_dump", "_rest"),
    )
    print(f"[xval] pairs before day filter: {len(merged):,}")
    merged = merged[merged["day"].isin(complete_days)].copy()
    print(f"[xval] pairs after  day filter: {len(merged):,}")

    if len(merged) == 0:
        print("[xval] ERROR: no overlap on complete days")
        sys.exit(1)

    merged["rel_diff"] = np.where(
        merged["views_dump"] > 0,
        (merged["views_rest"] - merged["views_dump"]) / merged["views_dump"],
        np.nan,
    )
    merged["abs_rel_diff"] = merged["rel_diff"].abs()

    # Per-group stats
    print(f"\n[xval] per-group |rel_diff| stats (complete days only):")
    print(f"{'group':<24s} {'n':>6s} {'median':>9s} {'mean':>9s} {'p95':>9s} {'max':>9s}")
    for g, sub in merged.groupby("group"):
        d = sub["abs_rel_diff"].dropna()
        if len(d) == 0:
            continue
        print(f"{g:<24s} {len(sub):>6d} "
              f"{d.median()*100:>7.2f}% "
              f"{d.mean()*100:>7.2f}% "
              f"{d.quantile(0.95)*100:>7.2f}% "
              f"{d.max()*100:>7.2f}%")

    d_all = merged["abs_rel_diff"].dropna()
    print(f"\n[xval] OVERALL (complete days):")
    print(f"  n pairs:    {len(d_all):,}")
    print(f"  median |rel_diff|: {d_all.median()*100:.2f}%")
    print(f"  mean   |rel_diff|: {d_all.mean()*100:.2f}%")
    print(f"  p95    |rel_diff|: {d_all.quantile(0.95)*100:.2f}%")
    print(f"  max    |rel_diff|: {d_all.max()*100:.2f}%")

    signed = merged["rel_diff"].dropna()
    print(f"  signed mean (REST - dump)/dump: {signed.mean()*100:+.2f}%")
    print(f"  pairs where REST > dump: {(signed > 0).sum()} ({100*(signed > 0).mean():.1f}%)")

    out_path = Path(OUT_CSV)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["canonical", "group", "day", "views_dump", "views_rest",
            "rel_diff", "abs_rel_diff"]
    merged.sort_values(["canonical", "day"])[cols].to_csv(out_path, index=False)
    print(f"\n[xval] wrote {out_path} ({len(merged):,} rows)")
    print("[xval] OK")


if __name__ == "__main__":
    main()
