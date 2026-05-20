#!/usr/bin/env python
"""
Day 1 Dask smoke test.

Purpose: verify Dask works on Midway3 against the existing 31 REST jsonl.gz
files before committing to the full ~2000-file dump-pull pipeline.

NOTE: 31 files / 75k rows is too small to be the §1 scope solution. This
script's job is plumbing validation: read, partition, distribute, aggregate,
write, and report. Day 2 swaps the input glob to the dump corpus and the
LocalCluster to a SLURMCluster (dask-jobqueue) -- same script structure.
"""
import argparse
import glob
import json
import os
import time
from pathlib import Path

import dask
dask.config.set({"dataframe.convert-string": False})
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client, LocalCluster


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--input-glob",
        default="/scratch/midway3/yinyuanc/wiki-attention/staging_rest/*.jsonl.gz",
    )
    p.add_argument(
        "--output-dir",
        default="/scratch/midway3/yinyuanc/wiki-attention/curated_dask/",
    )
    p.add_argument("--n-workers", type=int, default=4)
    p.add_argument("--threads-per-worker", type=int, default=2)
    return p.parse_args()


def main():
    args = parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    files = sorted(glob.glob(args.input_glob))
    print(f"[smoke] input files matched: {len(files)}")
    assert len(files) > 0, f"no files matched {args.input_glob}"

    # LocalCluster: uses the 8 cores of this interactive node.
    # Day 2 will replace this block with dask_jobqueue.SLURMCluster.
    cluster = LocalCluster(
        n_workers=args.n_workers,
        threads_per_worker=args.threads_per_worker,
        memory_limit="3GB",
        processes=True,
    )
    client = Client(cluster)
    print(f"[smoke] dashboard: {client.dashboard_link}")
    print(f"[smoke] workers: {len(client.scheduler_info()['workers'])}")
    print(f"[smoke] dask version: {dask.__version__}")

    t0 = time.time()

    # Read one partition per file => 31 partitions across 4 workers.
    # read_json with lines=True handles gzip transparently via the .gz extension.
    ddf = dd.read_json(
        files,
        lines=True,
        blocksize=None,           # one partition per file (correct for small jsonl.gz)
        compression="gzip",
        meta={
            "canonical": "object",
            "group": "object",
            "date": "datetime64[ns]",
            "views": "int64",
        },
    )
    print(f"[smoke] npartitions: {ddf.npartitions}")

    # Force date parsing on the cluster, not the client.
    ddf["views"] = ddf["views"].astype("int64")

    # Distributed aggregation #1: weekly per (canonical, group)
    ddf["week"] = ddf["date"].dt.to_period("W").dt.start_time
    weekly = (
        ddf.groupby(["canonical", "group", "week"])["views"]
        .sum()
        .reset_index()
    )

    # Distributed aggregation #2: monthly per (canonical, group)
    ddf["month"] = ddf["date"].dt.to_period("M").dt.start_time
    monthly = (
        ddf.groupby(["canonical", "group", "month"])["views"]
        .sum()
        .reset_index()
    )

    # Task graph size BEFORE compute (this is the §1 scope evidence)
    print(f"[smoke] weekly  task graph size: {len(weekly.__dask_graph__())}")
    print(f"[smoke] monthly task graph size: {len(monthly.__dask_graph__())}")

    # Trigger compute. .compute() pulls results back to client as pandas.
    weekly_pdf, monthly_pdf = dask.compute(weekly, monthly)
    t1 = time.time()
    print(f"[smoke] compute wall time: {t1 - t0:.2f}s")
    print(f"[smoke] weekly rows:  {len(weekly_pdf):,}")
    print(f"[smoke] monthly rows: {len(monthly_pdf):,}")

    # Write outputs alongside (not on top of) the pandas curated/ dir so we can
    # cross-validate dask-vs-pandas in Day 2 (parallel to dump-vs-REST in §6).
    weekly_out = out / "weekly_dask.csv"
    monthly_out = out / "monthly_dask.csv"
    weekly_pdf.sort_values(["canonical", "week"]).to_csv(weekly_out, index=False)
    monthly_pdf.sort_values(["canonical", "month"]).to_csv(monthly_out, index=False)
    print(f"[smoke] wrote {weekly_out}")
    print(f"[smoke] wrote {monthly_out}")

    # Per-group sanity totals -- should roughly match the pandas curated/ run.
    group_totals = (
        weekly_pdf.groupby("group")["views"].sum().sort_values(ascending=False)
    )
    print("[smoke] per-group total views (weekly aggregation):")
    for g, v in group_totals.items():
        print(f"  {g:<24s} {v:>12,}")

    client.close()
    cluster.close()
    print("[smoke] OK")


if __name__ == "__main__":
    main()
