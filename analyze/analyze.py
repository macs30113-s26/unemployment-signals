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


def granger_table(panel: pd.DataFrame, target: pd.Series, max_lag: int = 8,
                  log_transform: bool = False) -> pd.DataFrame:
    """Granger test on first-differenced series for stationarity.

    If log_transform=True, applies np.log1p to both series before differencing.
    This addresses §9.5: raw .diff() on pageview/claims series is dominated by
    the COVID 2020 spike (counts span ~10x baseline), which may inflate the
    apparent significance for groups that co-move with the COVID shock.
    """
    rows = []
    for grp in GROUPS:
        x = panel[grp].astype(float)
        y = target.astype(float)
        if log_transform:
            x = np.log1p(x)
            y = np.log1p(y)
        df = pd.concat([x.diff(), y.diff()], axis=1).dropna()
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


def cross_correlation_table(panel: pd.DataFrame, target: pd.Series,
                            max_lag_weeks: int = 26,
                            log_transform: bool = False) -> pd.DataFrame:
    """Cross-correlation between attention and target over a window of lags.

    For each group and each lag k in [-max_lag_weeks, +max_lag_weeks], compute
    the Pearson correlation between attention.shift(k).diff() and target.diff().
    Positive k means attention leads target by k weeks.

    First-differencing matches the section 5.3 Granger specification for
    stationarity. If log_transform=True, np.log1p() is applied before
    differencing; this suppresses the multiplicative COVID 2020 spike that
    dominates raw .diff() variance (see section 9.5).

    Returns a long-form DataFrame with columns:
        group, lag_weeks, correlation, n_obs
    """
    rows = []
    for grp in GROUPS:
        x_raw = panel[grp].astype(float)
        y_raw = target.astype(float)
        if log_transform:
            x_raw = np.log1p(x_raw)
            y_raw = np.log1p(y_raw)
        for k in range(-max_lag_weeks, max_lag_weeks + 1):
            x_shift = x_raw.shift(k).diff()
            y_diff = y_raw.diff()
            both = pd.concat([x_shift, y_diff], axis=1).dropna()
            both.columns = ["x", "y"]
            if len(both) < 10:
                rows.append({"group": grp, "lag_weeks": k,
                             "correlation": np.nan, "n_obs": len(both)})
                continue
            r = both["x"].corr(both["y"])
            rows.append({"group": grp, "lag_weeks": k,
                         "correlation": r, "n_obs": len(both)})
    return pd.DataFrame(rows)


def cross_correlation_best_lag(xcorr_df: pd.DataFrame) -> pd.DataFrame:
    """Extract the best (max |correlation|) lag per group from a long-form
    cross-correlation table."""
    rows = []
    for grp, sub in xcorr_df.groupby("group"):
        sub = sub.dropna(subset=["correlation"])
        if len(sub) == 0:
            rows.append({"group": grp, "best_lag_weeks": None,
                         "correlation_at_best": None, "abs_correlation": None})
            continue
        idx = sub["correlation"].abs().idxmax()
        best = sub.loc[idx]
        rows.append({"group": grp,
                     "best_lag_weeks": int(best["lag_weeks"]),
                     "correlation_at_best": float(best["correlation"]),
                     "abs_correlation": float(abs(best["correlation"]))})
    return pd.DataFrame(rows)


def fig_cross_correlation(panel: pd.DataFrame, target: pd.Series,
                          out_path: Path, max_lag_weeks: int = 26):
    """Plot cross-correlation function vs lag, for each group, both raw .diff()
    and log .diff() side by side. Positive lag = attention leads target.
    """
    xcorr_raw = cross_correlation_table(panel, target, max_lag_weeks, False)
    xcorr_log = cross_correlation_table(panel, target, max_lag_weeks, True)
    fig, axes = plt.subplots(2, 2, figsize=(11, 7), sharex=True, sharey=True)
    for ax, grp in zip(axes.flat, GROUPS):
        sub_raw = xcorr_raw[xcorr_raw["group"] == grp]
        sub_log = xcorr_log[xcorr_log["group"] == grp]
        ax.plot(sub_raw["lag_weeks"], sub_raw["correlation"],
                lw=1.2, color="#2a4d8f", label="raw .diff()")
        ax.plot(sub_log["lag_weeks"], sub_log["correlation"],
                lw=1.2, color="#d96b1a", label="log1p .diff()")
        ax.axhline(0, color="grey", lw=0.5)
        ax.axvline(0, color="grey", lw=0.5, ls="--")
        sub_r = sub_raw.dropna(subset=["correlation"])
        if len(sub_r):
            best_idx = sub_r["correlation"].abs().idxmax()
            best_lag = sub_r.loc[best_idx, "lag_weeks"]
            best_r = sub_r.loc[best_idx, "correlation"]
            ax.scatter([best_lag], [best_r], color="#2a4d8f", s=30, zorder=5)
            ax.annotate(f"lag={int(best_lag)}\nr={best_r:.2f}",
                        xy=(best_lag, best_r), fontsize=8,
                        xytext=(5, 5), textcoords="offset points")
        ax.set_title(grp.replace("_", " ").title())
        ax.set_xlabel("lag (weeks; positive = attention leads)")
        ax.set_ylabel("Pearson r")
        ax.legend(loc="lower right", fontsize=8)
    fig.suptitle("Cross-correlation: attention lead/lag vs initial jobless claims",
                 y=1.00)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


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
    granger = granger_table(panel, icsa_weekly, log_transform=False)
    print("  raw .diff() specification:")
    print(granger.to_string(index=False))
    granger.to_csv(args.out_dir / "granger_results.csv", index=False)

    granger_log = granger_table(panel, icsa_weekly, log_transform=True)
    print("  log1p .diff() specification (addresses §9.5 COVID-spike concern):")
    print(granger_log.to_string(index=False))
    granger_log.to_csv(args.out_dir / "granger_results_log.csv", index=False)

    # Side-by-side comparison
    granger_cmp = granger[["group", "best_lag_weeks", "p_value"]].rename(
        columns={"best_lag_weeks": "best_lag_raw", "p_value": "p_value_raw"}
    ).merge(
        granger_log[["group", "best_lag_weeks", "p_value"]].rename(
            columns={"best_lag_weeks": "best_lag_log", "p_value": "p_value_log"}
        ),
        on="group",
    )
    granger_cmp.to_csv(args.out_dir / "granger_results_comparison.csv", index=False)
    print("  raw vs log side-by-side:")
    print(granger_cmp.to_string(index=False))

    print("\nRunning cross-correlation lead-lag analysis...")
    xcorr_raw = cross_correlation_table(panel, icsa_weekly, max_lag_weeks=26,
                                        log_transform=False)
    xcorr_log = cross_correlation_table(panel, icsa_weekly, max_lag_weeks=26,
                                        log_transform=True)
    xcorr_raw.to_csv(args.out_dir / "cross_correlation_lead_lag_raw.csv",
                     index=False)
    xcorr_log.to_csv(args.out_dir / "cross_correlation_lead_lag_log.csv",
                     index=False)
    best_raw = cross_correlation_best_lag(xcorr_raw)
    best_log = cross_correlation_best_lag(xcorr_log)
    best_raw["spec"] = "raw_diff"
    best_log["spec"] = "log_diff"
    best_all = pd.concat([best_raw, best_log], ignore_index=True)
    best_all.to_csv(args.out_dir / "cross_correlation_best_lag.csv", index=False)
    print("  raw .diff() best lag per group:")
    print(best_raw.to_string(index=False))
    print("  log1p .diff() best lag per group:")
    print(best_log.to_string(index=False))
    fig_cross_correlation(panel, icsa_weekly,
                          args.out_dir / "04_cross_correlation_lead_lag.png",
                          max_lag_weeks=26)
    print(f"  wrote 04_cross_correlation_lead_lag.png + 3 CSVs")

    print("\nRunning changepoint detection...")
    cps = changepoint_table(panel)
    print(cps.to_string(index=False))
    cps.to_csv(args.out_dir / "changepoints.csv", index=False)

    print(f"\nDone. Outputs in {args.out_dir.resolve()}")


if __name__ == "__main__":
    main()
