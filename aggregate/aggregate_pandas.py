import argparse
import gzip
import json
from pathlib import Path

import pandas as pd


def load_rest(staging_dir: Path) -> pd.DataFrame:
    rows = []
    for f in sorted(staging_dir.glob("*.jsonl.gz")):
        with gzip.open(f, "rb") as fh:
            for line in fh:
                rows.append(json.loads(line))
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["views"] = df["views"].astype("int64")
    df["year"] = df["date"].dt.year
    df["week_start"] = (df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="d"))
    df["month"] = df["date"].values.astype("datetime64[M]")
    return df


def write_grain(df, period_col, out_dir):
    agg = (
        df.groupby([period_col, "canonical", "group", "year"], as_index=False)["views"]
        .sum()
        .rename(columns={period_col: "period"})
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    for year, sub in agg.groupby("year"):
        path = out_dir / f"year={year}"
        path.mkdir(parents=True, exist_ok=True)
        sub.drop(columns=["year"]).to_csv(path / "part.csv", index=False)
    print(f"  wrote {len(agg):,} rows to {out_dir}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--staging", required=True, type=Path)
    ap.add_argument("--curated", required=True, type=Path)
    args = ap.parse_args()

    print(f"Loading from {args.staging}")
    df = load_rest(args.staging)
    print(f"  {len(df):,} records, {df['canonical'].nunique()} articles, "
          f"{df['date'].min().date()} to {df['date'].max().date()}")

    print("\nPer-group totals:")
    print(df.groupby("group")["views"].sum().sort_values(ascending=False).to_string())

    print("\nWriting daily ...")
    write_grain(df, "date", args.curated / "daily")
    print("Writing weekly ...")
    write_grain(df, "week_start", args.curated / "weekly")
    print("Writing monthly ...")
    write_grain(df, "month", args.curated / "monthly")

    print("\nDone.")


if __name__ == "__main__":
    main()
