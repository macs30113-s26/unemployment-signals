import argparse
import gzip
import json
from collections import defaultdict
from pathlib import Path

import pandas as pd


def load_dump_daily(dump_dir: Path) -> pd.DataFrame:
    """Walk staging/ and aggregate hourly JSONL counts to daily."""
    counts = defaultdict(int)
    n_files = 0
    for f in dump_dir.rglob("*.jsonl.gz"):
        n_files += 1
        with gzip.open(f, "rb") as fh:
            for line in fh:
                rec = json.loads(line)
                # capture_end_utc is end-of-hour; attribute to start-of-hour date
                cap_end = pd.to_datetime(rec["capture_end_utc"])
                day = (cap_end - pd.Timedelta(hours=1)).date()
                key = (rec["canonical"], day)
                counts[key] += rec["views"]
    rows = [
        {"canonical": c, "date": d, "dump_views": v}
        for (c, d), v in counts.items()
    ]
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    print(f"loaded {n_files} dump files -> {len(df)} (canonical,date) records")
    return df


def load_rest_daily(rest_dir: Path) -> pd.DataFrame:
    """Concatenate all REST-derived per-article JSONLs into a flat frame."""
    rows = []
    for f in rest_dir.glob("*.jsonl.gz"):
        with gzip.open(f, "rb") as fh:
            for line in fh:
                rec = json.loads(line)
                rows.append({
                    "canonical": rec["canonical"],
                    "date": rec["date"],
                    "rest_views": rec["views"],
                })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    print(f"loaded {len(df)} REST (canonical,date) records "
          f"across {df['canonical'].nunique()} articles")
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", required=True, type=Path)
    ap.add_argument("--rest", required=True, type=Path)
    ap.add_argument("--out",  required=True, type=Path)
    args = ap.parse_args()

    dump = load_dump_daily(args.dump)
    rest = load_rest_daily(args.rest)

    # Only keep dump-derived days where we have all 24 hours of coverage.
    # (Partial days would systematically under-count.)
    hours = defaultdict(set)
    for f in args.dump.rglob("hour=*.jsonl.gz"):
        # path looks like .../year=2019/month=01/day=15/hour=12.jsonl.gz
        parts = f.parts
        y = int(parts[-4].split("=")[1])
        m = int(parts[-3].split("=")[1])
        d = int(parts[-2].split("=")[1])
        h = int(parts[-1].split("=")[1].split(".")[0])
        hours[(y, m, d)].add(h)
    full_days = {pd.Timestamp(y, m, d) for (y, m, d), hs in hours.items()
                 if len(hs) == 24}
    print(f"{len(full_days)} dump days have full 24-hour coverage")

    dump_full = dump[dump["date"].isin(full_days)]
    merged = dump_full.merge(rest, on=["canonical", "date"], how="inner")
    if merged.empty:
        print("\nNo overlapping (canonical, date) records to validate.")
        print("This is expected if you haven't run any full-day dump batches yet.")
        merged.to_csv(args.out, index=False)
        return

    merged["abs_diff"] = (merged["dump_views"] - merged["rest_views"]).abs()
    merged["rel_diff"] = merged["abs_diff"] / merged["rest_views"].clip(lower=1)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.out, index=False)

    print(f"\n=== Cross-Validation Summary ===")
    print(f"matched records         : {len(merged)}")
    print(f"records w/ rel_diff < 5%: {(merged['rel_diff'] < 0.05).sum()} "
          f"({100*(merged['rel_diff'] < 0.05).mean():.1f}%)")
    print(f"records w/ rel_diff <10%: {(merged['rel_diff'] < 0.10).sum()} "
          f"({100*(merged['rel_diff'] < 0.10).mean():.1f}%)")
    print(f"mean rel_diff           : {merged['rel_diff'].mean():.3f}")
    print(f"median rel_diff         : {merged['rel_diff'].median():.3f}")
    print(f"\nReport written to {args.out}")


if __name__ == "__main__":
    main()
