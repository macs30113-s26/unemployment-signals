"""
Analysis script — Layer 3.

Pulls the curated pageview panel from Athena, joins it with BLS / FRED
labour-market series, and produces the figures and statistical tests that
populate the README.

Three analyses:
  1. Time-series plots of group-level attention vs unemployment / layoffs,
     with the three macro shocks marked.
  2. Granger causality tests: do pageviews lead or lag BLS data?
  3. Bayesian changepoint detection: are detected structural breaks in
     attention temporally aligned with the known shocks?

Run on a small EC2 (t3.medium is fine — these models are not CPU-bound) with
the analysis venv (see requirements_analysis.txt). Athena queries cost
~$0.005/GB scanned; the full curated panel is <100 MB so each run is
essentially free.

Outputs go to ./figures/ as PNG. You'll commit these to GitHub for the README.
"""

import argparse
import os
import time
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import ruptures as rpt
from pyathena import connect
from statsmodels.tsa.stattools import grangercausalitytests

# Plot style: clean, paper-ready.
plt.rcParams.update({
    "figure.dpi": 120,
    "savefig.dpi": 160,
    "font.size": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.titlesize": 11,
    "axes.titleweight": "bold",
})

GROUPS = ["core_unemployment", "labour_market_stress", "macro_insecurity", "tech_displacement"]

# Macro shock annotations — used on every plot for context.
SHOCKS = [
    ("2020-03-15", "COVID layoffs"),
    ("2022-06-15", "Inflation peak"),
    ("2023-01-20", "Tech layoff wave"),
    ("2022-11-30", "ChatGPT launch"),
]

# FRED series IDs.  All free, no API key needed if we use the CSV endpoint.
FRED_SERIES = {
    "UNRATE":        "Unemployment rate (%)",
    "ICSA":          "Initial jobless claims",
    "PAYEMS":        "Total nonfarm payrolls (thousands)",
}


# ---------- data loading ----------

def load_pageviews(athena_db: str, output_loc: str, grain: str = "weekly") -> pd.DataFrame:
    """Pull the curated panel from Athena into a pandas DataFrame.

    We use the *weekly* grain for the main analysis because (a) BLS Initial
    Claims is reported weekly so we get a clean alignment, and (b) weekly
    smooths out the day-of-week pageview cycle (Wikipedia traffic dips on
    weekends).
    """
    conn = connect(s3_staging_dir=output_loc, region_name="us-east-1")
    q = f"""
        SELECT period AS week_start,
               canonical,
               "group" AS grp,
               views
        FROM {athena_db}.{grain}
        ORDER BY week_start, "group", canonical
    """
    df = pd.read_sql(q, conn)
    df["week_start"] = pd.to_datetime(df["week_start"])
    return df


def load_fred(series_id: str) -> pd.DataFrame:
    """Fetch one FRED series via the public CSV endpoint."""
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    df = pd.read_csv(url)
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"])
    # FRED uses "." for missing observations in some series — coerce.
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna().reset_index(drop=True)


def group_weekly(df_views: pd.DataFrame) -> pd.DataFrame:
    """Roll the article-level panel up to (week, group) totals."""
    return (
        df_views.groupby(["week_start", "grp"], as_index=False)["views"].sum()
        .pivot(index="week_start", columns="grp", values="views")
        .reindex(columns=GROUPS)
        .fillna(0)
        .astype(float)
    )


# ---------- figures ----------

def fig_attention_timeseries(group_panel: pd.DataFrame, out_path: Path):
    """Four-panel timeseries: one subplot per group, log scale, shocks marked."""
    fig, axes = plt.subplots(4, 1, figsize=(9, 10), sharex=True)
    for ax, grp in zip(axes, GROUPS):
        s = group_panel[grp]
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
    fig.suptitle("Public attention to economic insecurity (English Wikipedia)", y=1.00)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def fig_attention_vs_unrate(group_panel: pd.DataFrame, unrate: pd.DataFrame, out_path: Path):
    """Overlay each group's attention against the unemployment rate."""
    # Resample UNRATE (monthly) to weekly via forward-fill so the two indices align.
    unrate_w = (
        unrate.set_index("date")["value"]
        .resample("W-MON").ffill()
        .reindex(group_panel.index, method="ffill")
    )

    fig, axes = plt.subplots(2, 2, figsize=(11, 7), sharex=True)
    for ax, grp in zip(axes.flat, GROUPS):
        # Z-score both series so they can share an axis.
        att = (group_panel[grp] - group_panel[grp].mean()) / group_panel[grp].std()
        une = (unrate_w - unrate_w.mean()) / unrate_w.std()
        ax.plot(att.index, att.values, lw=1.0, color="#2a4d8f", label="Attention (z)")
        ax.plot(une.index, une.values, lw=1.0, color="#d96b1a", label="UNRATE (z)")
        ax.set_title(grp.replace("_", " ").title())
        ax.axhline(0, color="grey", lw=0.5)
        ax.legend(loc="upper left", fontsize=8)
    fig.suptitle("Attention vs Unemployment Rate, by thematic group  (z-scored)", y=1.00)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


# ---------- statistical tests ----------

def granger_table(group_panel: pd.DataFrame, target: pd.Series, max_lag: int = 8) -> pd.DataFrame:
    """Run Granger causality: does each group's attention Granger-cause `target`?

    target should be a weekly series indexed by the same dates as group_panel.
    Reports min p-value across F-tests for lags 1..max_lag, and the lag at
    which it occurred. We z-score both series — Granger tests don't require
    it, but it makes coefficients comparable across groups.

    Caveat written into the README: Granger causality is *predictive*, not
    causal in the structural sense. We're testing "is attention a leading
    indicator", not "does attention cause unemployment".
    """
    rows = []
    for grp in GROUPS:
        s = group_panel[grp].copy()
        # First-difference to handle the obvious non-stationarity; statsmodels'
        # test assumes stationary inputs.
        df = pd.concat([s.diff(), target.diff()], axis=1).dropna()
        df.columns = ["attention", "target"]
        try:
            res = grangercausalitytests(df[["target", "attention"]], maxlag=max_lag, verbose=False)
            pvals = {lag: res[lag][0]["ssr_ftest"][1] for lag in range(1, max_lag + 1)}
            best_lag = min(pvals, key=pvals.get)
            rows.append({"group": grp, "best_lag_weeks": best_lag, "p_value": pvals[best_lag]})
        except Exception as e:
            rows.append({"group": grp, "best_lag_weeks": None, "p_value": None,
                         "error": str(e)})
    return pd.DataFrame(rows)


def changepoint_table(group_panel: pd.DataFrame, n_bkps: int = 5) -> pd.DataFrame:
    """For each group, detect the top-n structural breaks via PELT.

    Compares detected dates against the four known SHOCKS — for each detected
    break, report the closest known shock and how many days off.
    """
    rows = []
    for grp in GROUPS:
        s = np.log1p(group_panel[grp].values)
        algo = rpt.Pelt(model="rbf").fit(s)
        try:
            bkps = algo.predict(pen=10)[:-1]  # drop trailing endpoint
        except Exception:
            bkps = []
        for bkp_idx in bkps[:n_bkps]:
            if bkp_idx >= len(group_panel.index):
                continue
            dt = group_panel.index[bkp_idx]
            # Nearest known shock
            best = min(SHOCKS, key=lambda s: abs(pd.to_datetime(s[0]) - dt))
            offset_d = (pd.to_datetime(best[0]) - dt).days
            rows.append({"group": grp, "break_date": dt.date(),
                         "nearest_shock": best[1],
                         "shock_offset_days": offset_d})
    return pd.DataFrame(rows)


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--athena-db", default="pageviews")
    ap.add_argument("--athena-output", required=True,
                    help="s3://bucket/athena-results/ — Athena's query result location")
    ap.add_argument("--out-dir", default="figures")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(exist_ok=True)

    print("Loading curated weekly panel from Athena...")
    t0 = time.time()
    df = load_pageviews(args.athena_db, args.athena_output, grain="weekly")
    print(f"  {len(df):,} rows in {time.time()-t0:.1f}s")
    group_panel = group_weekly(df)
    print(f"  panel shape {group_panel.shape}, range "
          f"{group_panel.index.min().date()} → {group_panel.index.max().date()}")

    print("Loading FRED series...")
    fred = {sid: load_fred(sid) for sid in FRED_SERIES}

    print("Generating figures...")
    fig_attention_timeseries(group_panel, out / "01_attention_by_group.png")
    fig_attention_vs_unrate(group_panel, fred["UNRATE"], out / "02_attention_vs_unrate.png")

    print("\nRunning Granger causality (attention -> initial jobless claims)...")
    icsa_weekly = (
        fred["ICSA"].set_index("date")["value"]
        .resample("W-MON").mean()
        .reindex(group_panel.index, method="ffill")
    )
    granger = granger_table(group_panel, icsa_weekly)
    print(granger.to_string(index=False))
    granger.to_csv(out / "granger_results.csv", index=False)

    print("\nRunning changepoint detection...")
    cps = changepoint_table(group_panel)
    print(cps.to_string(index=False))
    cps.to_csv(out / "changepoints.csv", index=False)

    print(f"\nDone. Figures + tables written to {out.resolve()}")


if __name__ == "__main__":
    main()
