import argparse
import glob
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import ruptures as rpt

plt.rcParams.update({
    "figure.dpi": 120,
    "savefig.dpi": 160,
    "font.size": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
})

TECH_ALL = [
    "Artificial_intelligence",
    "Automation",
    "ChatGPT",
    "Future_of_work",
    "Generative_artificial_intelligence",
    "Large_language_model",
    "Technological_unemployment",
    "Universal_basic_income",
]

TECH_PRE2019 = [
    "Artificial_intelligence",
    "Automation",
    "Future_of_work",
    "Technological_unemployment",
    "Universal_basic_income",
]

TECH_LABOR_NARROW = [
    "Technological_unemployment",
    "Automation",
    "Universal_basic_income",
]

SHOCKS = [
    ("2020-03-15", "COVID layoffs"),
    ("2022-06-15", "Inflation peak"),
    ("2022-11-30", "ChatGPT launch"),
    ("2023-01-20", "Tech layoff wave"),
]


def load_weekly(curated_dir):
    files = glob.glob(str(curated_dir / "weekly" / "year=*/part.csv"))
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    df["period"] = pd.to_datetime(df["period"])
    return df


def article_panel(df, articles):
    sub = df[df["canonical"].isin(articles)]
    pivot = (
        sub.pivot_table(index="period", columns="canonical",
                        values="views", aggfunc="sum")
        .reindex(columns=articles)
        .fillna(0)
    )
    return pivot


def pelt_breakpoints(series, pen=10):
    if (series <= 0).all() or len(series) < 10:
        return []
    algo = rpt.Pelt(model="rbf").fit(np.log1p(series))
    try:
        return algo.predict(pen=pen)[:-1]
    except Exception:
        return []


def fig_article_breakdown(panel, out_path):
    n = panel.shape[1]
    fig, axes = plt.subplots(n, 1, figsize=(11, 1.8 * n), sharex=True)
    if n == 1:
        axes = [axes]
    for ax, art in zip(axes, panel.columns):
        s = panel[art]
        non_zero = (s > 0).sum()
        ax.plot(s.index, s.values, lw=1.0, color="#2a4d8f")
        if (s > 0).any():
            ax.set_yscale("log")
        ax.set_title(f"{art}  (non-zero weeks: {non_zero}/{len(s)})",
                     fontsize=9, loc="left", fontweight="bold")
        ax.set_ylabel("weekly views")
        for date_str, label in SHOCKS:
            d = pd.to_datetime(date_str)
            if s.index.min() <= d <= s.index.max():
                ax.axvline(d, color="#cc4040", lw=0.6, alpha=0.6)
    axes[-1].xaxis.set_major_locator(mdates.YearLocator())
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    axes[-1].set_xlabel("Week")
    fig.suptitle("Tech-displacement attention by article (robustness)",
                 y=1.00, fontsize=11, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def changepoint_for_group(panel, label):
    if panel.empty:
        return {"label": label, "break_dates": [], "n_articles": 0, "n_weeks": 0}
    summed = panel.sum(axis=1)
    bkps = pelt_breakpoints(summed.values, pen=10)
    break_dates = [summed.index[i].date() for i in bkps if i < len(summed)]
    return {
        "label": label,
        "n_articles": panel.shape[1],
        "n_weeks": len(summed),
        "break_dates": [str(d) for d in break_dates],
    }


def coverage_table(df, articles):
    rows = []
    for art in articles:
        sub = df[df["canonical"] == art]
        if sub.empty:
            rows.append({"canonical": art, "n_weeks_present": 0,
                         "first_week": None, "last_week": None,
                         "first_nonzero_week": None})
            continue
        first = sub["period"].min()
        last  = sub["period"].max()
        nz    = sub[sub["views"] > 0]["period"].min() if (sub["views"] > 0).any() else None
        rows.append({
            "canonical": art,
            "n_weeks_present": len(sub),
            "first_week": str(first.date()),
            "last_week":  str(last.date()),
            "first_nonzero_week": str(nz.date()) if nz is not None else None,
        })
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--curated", required=True, type=Path)
    ap.add_argument("--out-dir", default=Path("analyze/figures"), type=Path)
    args = ap.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading weekly panel from {args.curated}/weekly")
    df = load_weekly(args.curated)
    print(f"  {len(df):,} rows, {df['period'].min().date()} -> {df['period'].max().date()}")

    print("\n=== Coverage of tech_displacement articles ===")
    cov = coverage_table(df, TECH_ALL)
    print(cov.to_string(index=False))
    cov.to_csv(args.out_dir / "tech_article_coverage.csv", index=False)

    print("\nGenerating article-level breakdown plot...")
    panel = article_panel(df, TECH_ALL)
    fig_article_breakdown(panel, args.out_dir / "03_tech_articles_breakdown.png")
    print(f"  wrote {args.out_dir / '03_tech_articles_breakdown.png'}")

    print("\n=== Changepoint detection under three article subsets ===")
    results = []
    for label, subset in [
        ("ALL tech_displacement (8 articles)",        TECH_ALL),
        ("EXCLUDE post-window-created (ChatGPT, GenAI, LLM)", TECH_PRE2019),
        ("LABOR-NARROW (Tech_unempl, Autom, UBI)",   TECH_LABOR_NARROW),
    ]:
        p = article_panel(df, subset)
        r = changepoint_for_group(p, label)
        results.append(r)
        print(f"\n  Subset: {label}")
        print(f"    {r['n_articles']} articles, {r['n_weeks']} weeks")
        print(f"    PELT break_dates: {r['break_dates']}")

    pd.DataFrame(results).to_csv(args.out_dir / "robustness_changepoints.csv",
                                 index=False)

    print("\n=== ChatGPT individual contribution ===")
    chatgpt = df[df["canonical"] == "ChatGPT"]
    if not chatgpt.empty:
        total_views = df[df["canonical"].isin(TECH_ALL)].groupby("period")["views"].sum()
        chatgpt_views = chatgpt.groupby("period")["views"].sum().reindex(total_views.index, fill_value=0)
        share_2023plus = (
            chatgpt_views[chatgpt_views.index >= "2023-01-01"].sum()
            / total_views[total_views.index >= "2023-01-01"].sum()
        )
        print(f"  ChatGPT share of tech_displacement attention since 2023-01-01: "
              f"{share_2023plus:.1%}")

    print(f"\nDone. Outputs in {args.out_dir}")


if __name__ == "__main__":
    main()
