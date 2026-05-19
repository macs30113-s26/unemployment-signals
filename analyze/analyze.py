import argparse
import time
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import ruptures as rpt
from statsmodels.tsa.stattools import grangercausalitytests

plt.rcParams.update({
    "figure.dpi": 120,
    "savefig.dpi": 160,
    "font.size": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.titlesize": 11,
    "axes.titleweight": "bold",
})

GROUPS = [
    "core_unemployment",
    "labour_market_stress",
    "macro_insecurity",
    "tech_displacement",
]

SHOCKS = [
    ("2020-03-15", "COVID layoffs"),
    ("2022-06-15", "Inflation peak"),
    ("2022-11-30", "ChatGPT launch"),
    ("2023-01-20", "Tech layoff wave"),
]

FRED_SERIES = {
    "UNRATE": "Unemployment rate (%)",
    "ICSA":   "Initial jobless claims",
    "PAYEMS": "Total nonfarm payrolls (thousands)",
}


def load_pageviews(curated_dir: Path, grain: str = "weekly") -> pd.DataFrame:
    import glob; df = pd.concat([pd.read_csv(p) for p in glob.glob(str(curated_dir / grain / "year=*/part.csv"))], ignore_index=True)
    df["week_start"] = pd.to_datetime(df["period"])
    df = df.rename(columns={"group": "grp"})
    return df


def load_fred(series_id: str) -> pd.DataFrame:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    df = pd.read_csv(url)
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna().reset_index(drop=True)


def group_weekly(df_views: pd.DataFrame) -> pd.DataFrame:
    return (
        df_views.groupby(["week_start", "grp"], as_index=False)["views"]
        .sum()
        .pivot(index="week_start", columns="grp", values="views")
        .reindex(columns=GROUPS)
        .fillna(0)
        .astype(float)
    )


def fig_attention_timeseries(panel: pd.DataFrame, out_path: Path):
    fig, axes = plt.subplots(4, 1, figsize=(9, 10), sharex=True)
    for ax, grp in zip(axes, GROUPS):
        s = panel[grp]
        ax.plot(s.index, s.values, lw=1.2, color="#2a4d8f")
        ax.set_yscale("log")
        ax.set_title(grp.replace("_", " ").title())
        ax.set_ylabel("Weekly pageviews")
        for date_str, label in SHOCKS:
            d = pd.to_datetime(date_str)
            if s.index.min() <= d <= s.index.max():
                ax.axvline(d, color="#cc4040", lw=0.7, alpha=0.7)
                ax.text(d, ax.get_ylim()[1] * 0.6, label, rotation=90,
                        fontsize=7, color="#cc4040", va="top")
    axes[-1].xaxis.set_major_locator(mdates.YearLocator())
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    axes[-1].set_xlabel("Week")
    fig.suptitle("Public attention to economic insecurity (English Wikipedia)",
                 y=1.00)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def fig_attention_vs_unrate(panel: pd.DataFrame, unrate: pd.DataFrame, out_path: Path):
    unrate_w = (
        unrate.set_index("date")["value"]
        .resample("W-MON").ffill()
        .reindex(panel.index, method="ffill")
    )
    fig, axes = plt.subplots(2, 2, figsize=(11, 7), sharex=True)
    for ax, grp in zip(axes.flat, GROUPS):
        att = (panel[grp] - panel[grp].mean()) / panel[grp].std()
        une = (unrate_w - unrate_w.mean()) / unrate_w.std()
        ax.plot(att.index, att.values, lw=1.0, color="#2a4d8f", label="Attention (z)")
        ax.plot(une.index, une.values, lw=1.0, color="#d96b1a", label="UNRATE (z)")
        ax.set_title(grp.replace("_", " ").title())
        ax.axhline(0, color="grey", lw=0.5)
        ax.legend(loc="upper left", fontsize=8)
    fig.suptitle("Attention vs Unemployment Rate, by thematic group  (z-scored)",
                 y=1.00)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def granger_table(panel: pd.DataFrame, target: pd.Series, max_lag: int = 8) -> pd.DataFrame:
    # Granger test on first-differenced series for stationarity
    rows = []
    for grp in GROUPS:
        df = pd.concat([panel[grp].diff(), target.diff()], axis=1).dropna()
        df.columns = ["attention", "target"]
        if len(df) < max_lag + 5:
            rows.append({"group": grp, "best_lag_weeks": None, "p_value": None,
                         "error": "too few observations"})
            continue
        try:
            res = grangercausalitytests(df[["target", "attention"]],
                                        maxlag=max_lag, verbose=False)
            pvals = {lag: res[lag][0]["ssr_ftest"][1]
                     for lag in range(1, max_lag + 1)}
            best_lag = min(pvals, key=pvals.get)
            rows.append({"group": grp, "best_lag_weeks": best_lag,
                         "p_value": pvals[best_lag]})
        except Exception as e:
            rows.append({"group": grp, "best_lag_weeks": None, "p_value": None,
                         "error": str(e)})
    return pd.DataFrame(rows)


def changepoint_table(panel: pd.DataFrame, n_bkps: int = 5) -> pd.DataFrame:
    rows = []
    for grp in GROUPS:
        s = np.log1p(panel[grp].values)
        algo = rpt.Pelt(model="rbf").fit(s)
        try:
            bkps = algo.predict(pen=10)[:-1]
        except Exception:
            bkps = []
        for bkp_idx in bkps[:n_bkps]:
            if bkp_idx >= len(panel.index):
                continue
            dt = panel.index[bkp_idx]
            best = min(SHOCKS,
                       key=lambda s: abs(pd.to_datetime(s[0]) - dt))
            offset_d = (pd.to_datetime(best[0]) - dt).days
            rows.append({
                "group": grp,
                "break_date": dt.date(),
                "nearest_shock": best[1],
                "shock_offset_days": offset_d,
            })
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--curated", required=True, type=Path)
    ap.add_argument("--out-dir", default="analyze/figures", type=Path)
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading curated weekly panel from {args.curated}/weekly ...")
    t0 = time.time()
    df = load_pageviews(args.curated, grain="weekly")
    print(f"  {len(df):,} rows in {time.time()-t0:.1f}s")
    panel = group_weekly(df)
    print(f"  panel shape {panel.shape}, range "
          f"{panel.index.min().date()} → {panel.index.max().date()}")

    print("\nLoading FRED series...")
    fred = {sid: load_fred(sid) for sid in FRED_SERIES}

    print("\nGenerating figures...")
    fig_attention_timeseries(panel, args.out_dir / "01_attention_by_group.png")
    fig_attention_vs_unrate(panel, fred["UNRATE"],
                            args.out_dir / "02_attention_vs_unrate.png")

    print("\nRunning Granger causality (attention -> initial jobless claims)...")
    icsa_weekly = (
        fred["ICSA"].set_index("date")["value"]
        .resample("W-MON").mean()
        .reindex(panel.index, method="ffill")
    )
    granger = granger_table(panel, icsa_weekly)
    print(granger.to_string(index=False))
    granger.to_csv(args.out_dir / "granger_results.csv", index=False)

    print("\nRunning changepoint detection...")
    cps = changepoint_table(panel)
    print(cps.to_string(index=False))
    cps.to_csv(args.out_dir / "changepoints.csv", index=False)

    print(f"\nDone. Outputs in {args.out_dir.resolve()}")


if __name__ == "__main__":
    main()
