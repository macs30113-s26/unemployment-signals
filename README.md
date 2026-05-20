# Public Attention to Unemployment and Economic Insecurity: A Wikipedia-Pageviews Analysis (2019–2026)

**LSC4SS Final Project — Solo project.**
**Author:** Yinyuan Chen
**Course:** Large-Scale Computing for the Social Sciences, Spring 2026
**Platform:** UChicago RCC Midway3 (Slurm, caslake partition)
**Repo:** https://github.com/macs30113-s26/final-project-unemployment-signals

---

## 1. Research Question

How did public attention to unemployment and economic insecurity change between 2019 and 2026, and how did that attention respond to four macro-economic shocks: the March-2020 pandemic layoff wave, the 2022 inflation surge, the November-2022 ChatGPT launch, and the 2023 tech-sector layoff cycle?

Wikipedia pageviews capture an *anxiety* channel — what people search when they are personally uncertain — that complements survey- and admin-data measures of unemployment (which capture realised behaviour with a publication lag). Three concrete hypotheses are tested:

1. **H1 — Predictive lead.** Wikipedia attention on classical-unemployment articles temporally precedes the BLS initial-jobless-claims series.
2. **H2 — Separation of channels.** AI / automation-themed articles do not show the same temporal precedence relative to jobless claims that classical articles do.
3. **H3 (original) — AI-era structural break.** The 2022-11-30 ChatGPT launch produced a structural break in technological-displacement attention. *This hypothesis is revisited in §5.5 and rejected after a robustness check.*

## 2. Repository Layout

Every file below is a working component of the final pipeline (or archived as failed-attempt documentation). Click any link to jump to the file on GitHub.

```
final-project-unemployment-signals/
│
├── README.md                                              ← this file
├── requirements.txt                                       Python deps
├── .gitignore
│
├── config/
│   └── articles.yaml                                      31 canonical articles + aliases, 4 groups
│
├── ingest/                                                Layer 1: parallel HTTP ingest
│   ├── ingest_pageviews_rest.py                           PRIMARY — REST API, user-only, 6-worker pool
│   ├── ingest_pageviews.py                                VALIDATION — raw dump pull, 4-worker pool, backoff
│   └── verify_articles.py                                 Wikipedia title/redirect smoke check
│
├── aggregate/                                             Layer 2: daily → weekly/monthly rollup
│   ├── aggregate_pandas.py                                WORKING aggregator (~5 s on 75k rows)
│   ├── aggregate_pageviews.py                             PySpark version, attempted but failed (§7.3)
│   └── validate_dump_vs_rest.py                           cross-source consistency check
│
├── analyze/                                               Layer 3: Granger + PELT + figures
│   ├── analyze.py                                         main analysis
│   ├── analyze_robustness.py                              article-level + subset checks (§5.5)
│   └── figures/                                           all committed outputs
│       ├── 01_attention_by_group.png                      group time series
│       ├── 02_attention_vs_unrate.png                     attention vs UNRATE
│       ├── 03_tech_articles_breakdown.png                 per-article tech_displacement
│       ├── granger_results.csv                            §5.3 numbers
│       ├── changepoints.csv                               §5.4 numbers
│       ├── robustness_changepoints.csv                    §5.5 subset breaks
│       ├── tech_article_coverage.csv                      §5.5 coverage diagnostic
│       ├── user_vs_allagents_comparison.csv               §10.1 bot-share evidence
│       └── dump_rest_validation.csv                       §5.5 / §10.2 cross-source check
│
├── slurm/
│   ├── ingest_rest.sbatch                                 final REST ingest job
│   ├── ingest.sbatch                                      diagnostic dump-pull job
│   ├── aggregate.sbatch                                   Spark sbatch (job 49794175 — failed)
│   └── logs/                                              13 committed stdout/stderr files
│
└── aws/                                                   ARCHIVED FAILED-ATTEMPT (§7.1)
    ├── README.md                                          incident archive
    ├── ingest/, aggregate/, analyze/                      Lambda + EC2 + Athena source code
    └── requirements_analysis.txt
```

GitHub direct links to the four most-cited files: [README.md](README.md) · [config/articles.yaml](config/articles.yaml) · [ingest/ingest_pageviews_rest.py](ingest/ingest_pageviews_rest.py) · [analyze/analyze.py](analyze/analyze.py) · [analyze/analyze_robustness.py](analyze/analyze_robustness.py) · [analyze/figures/](analyze/figures/) · [slurm/logs/](slurm/logs/) · [aws/README.md](aws/README.md).

## 3. Scope and Data Volume (Honest)

The pipeline operates in two ingest tiers and one analysis tier. What was *actually* processed:

| Tier | Source | Records pulled |
|------|--------|----------------|
| Layer 1A — REST API (primary)       | Wikimedia REST `all-access/user` daily aggregates, 31 articles × 7 years | **75,346 day-records** |
| Layer 1B — raw dumps (validation)   | Wikimedia hourly dumps, sampled                                       | 353 hourly files (~10 days) |
| Layer 2 — pandas aggregation        | Daily → weekly → monthly rollup, year-partitioned CSV                 | 75k → 10,954 weekly rows |
| Layer 3 — analysis                  | Granger lag-VAR + PELT changepoint + matplotlib                       | 383-week panel × 4 groups |

The Wikimedia hourly dump corpus is ~65,000 files (~3 TB compressed) for this window; the dump-pull code ([ingest/ingest_pageviews.py](ingest/ingest_pageviews.py)) is structurally capable of pulling all of them, but we **did not** do so — Wikimedia rate-limited us aggressively (§7.2), and the REST API delivers the same per-article daily totals for the curated 31-article list in 70 seconds (job 49826767). The primary analysis is built on the REST data; the dump sample is used only for cross-source validation (§5.5).

We do not claim TB-scale realised processing. The scalable-computing patterns demonstrated (§4) are real, but the dataset is small.

## 4. Architecture

```
   Wikimedia REST       Wikimedia dumps      FRED public CSV
        │                     │                     │
        │ 31 articles         │ 353 hourly files    │
        ▼                     ▼                     │
   Layer 1A PRIMARY      Layer 1B VALIDATION        │
   multiprocessing       multiprocessing            │
   Pool(6), Slurm        Pool(4), Slurm, backoff    │
        │                     │                     │
        ▼                     ▼                     │
   staging_rest/         staging/                   │
   31 jsonl.gz           353 jsonl.gz               │
        │                     │                     │
        ▼                     │                     │
   Layer 2 AGG               │                     │
   aggregate_pandas.py        │                     │
        │                     ▼                     │
        ▼              validate_dump_vs_rest.py     │
   curated/{daily,             │                    │
   weekly,monthly}/            ▼                    │
        │             dump_rest_validation.csv      │
        ▼                                           │
   Layer 3 ANALYZE  ◄──────────────────────────────┘
   analyze.py + analyze_robustness.py
        │
        ▼
   analyze/figures/  (3 PNG + 6 CSV)
```

### 4.1 Scalable-computing patterns demonstrated

**Multiprocessing-based parallel HTTP ingest, two paths.** Both [ingest_pageviews_rest.py](ingest/ingest_pageviews_rest.py) (REST, 6 workers) and [ingest_pageviews.py](ingest/ingest_pageviews.py) (raw dump, 4 workers with exponential backoff) use `multiprocessing.Pool` to fan out independent downloads. The REST path completes in 70 s; the dump path is the leaf-parallel pattern that would scale to the full corpus given an unrate-limited mirror — the bottleneck is upstream (§7.2), not parallelism.

**Atomic-write idempotency.** Each worker writes to a `.tmp` file and renames after gzip flush; cancelled Slurm jobs (49791029, 49791901) left no partial-write poison. Subsequent submissions correctly skip completed files via `path.exists() + stat().st_size > 0`, making restarts cumulative.

**Two-tier strategy under upstream rate-limiting.** When the dump server's per-IP cap made a full pull untenable (~80 h wall-clock at the only concurrency that did not trigger HTTP 429), we shifted the primary path to the REST API and kept the dump-pull as a validation sample. This is itself a scalable-computing decision: when the upstream resource is rate-limited rather than parallelism-limited, vertical workload reorganisation beats throwing more workers at the leaf.

**HPC scheduling under Slurm with full job-id audit trail.** Every ingest run is an `sbatch` submission. Seven Slurm job IDs (§9) span the project's life; all `stdout`/`stderr` are committed under [slurm/logs/](slurm/logs/). The Spark aggregation was *attempted* (job 49794175) but **failed** at startup (JAVA_HOME unset on caslake); §7.3 explains why this was replaced with a pandas implementation rather than re-attempted with manual Spark setup.

## 5. Findings

All numbers come from the `all-access/user` REST data (excludes spider/bot traffic; see §10.1). Three figures and six CSV tables are committed under [analyze/figures/](analyze/figures/).

### 5.1 Group-level attention time series

![Attention by group](analyze/figures/01_attention_by_group.png)

Core_unemployment and labour_market_stress show their largest spike at the March-2020 COVID layoff wave and no visible response to the 2022 inflation surge. Macro_insecurity peaks instead at the 2022 inflation period. Tech_displacement shows a ~10× step change after late-2022, the source of which is investigated in §5.5.

### 5.2 Attention vs UNRATE

![Attention vs UNRATE](analyze/figures/02_attention_vs_unrate.png)

Z-scored attention overlaid on the BLS unemployment rate. Around the 2020 shock, classical attention and UNRATE move together with attention appearing to lead UNRATE by a few weeks visually; we do not compute a formal lead-lag cross-correlation in this version. For tech_displacement, attention rises in 2023–2025 while UNRATE is falling — consistent with a news-cycle-driven attention pattern decoupled from realised labour-market movements.

### 5.3 Granger predictive lead (not structural causation)

We run lag-VAR Granger tests on first-differenced raw weekly attention vs. first-differenced raw initial-jobless-claims (no log transform applied — see §10.5), scanning lags 1–8 and reporting the best (smallest p) lag per group. The "best lag" reporting is exploratory and not corrected for the multiple-testing implied by lag selection; the p-values below should be read as descriptive evidence of *temporal precedence* under linear forecasting, not as confirmatory tests.

| group                  | best lag (weeks) | p-value     |
|------------------------|------------------|-------------|
| core_unemployment      | 1                | 3.5 × 10⁻¹⁵ |
| labour_market_stress   | 8                | 2.4 × 10⁻⁷⁵ |
| macro_insecurity       | 7                | 2.1 × 10⁻⁴  |
| tech_displacement      | 1                | 0.970       |

Full table: [analyze/figures/granger_results.csv](analyze/figures/granger_results.csv). The three classical groups all predictively precede jobless claims; tech_displacement does not. Consistent with H1 and H2; not a structural-causation claim.

### 5.4 PELT changepoint detection (not Bayesian)

Algorithm: `ruptures.Pelt(model="rbf")`, a frequentist exact-search algorithm with an RBF cost function. No posterior probabilities are computed. Offset convention: `offset_days = shock_date − break_date`; a negative offset means the break is *later* than the named shock. Full table: [analyze/figures/changepoints.csv](analyze/figures/changepoints.csv).

| group                | break date  | nearest shock              | offset (days) |
|----------------------|-------------|----------------------------|---------------|
| tech_displacement    | 2022-12-05  | ChatGPT launch (2022-11-30) | −5            |
| tech_displacement    | 2023-07-03  | Tech layoff wave            | −164          |
| core_unemployment    | 2020-06-08  | COVID layoffs               | −85           |
| labour_market_stress | 2020-06-08  | COVID layoffs               | −85           |
| macro_insecurity     | 2023-03-20  | Tech layoff wave            | −59           |

Honest reading: only the `tech_displacement` 2022-12-05 break is within two weeks of its named shock. The classical-group breaks are detected ~3 months *after* COVID hit — PELT is picking the post-spike plateau, not the spike date itself. Calling these breaks "tightly aligned" with the named shocks would be overstating; they are temporally *associated* but not coincident.

### 5.5 Robustness: the ChatGPT-aligned break is an article-creation artifact

Coverage check ([tech_article_coverage.csv](analyze/figures/tech_article_coverage.csv)): three of the eight `tech_displacement` articles — `ChatGPT`, `Generative_artificial_intelligence`, `Large_language_model` — did not exist on Wikipedia before late 2022, and `Future_of_work` has only one non-zero week in the panel and should be considered missing data (see config note in [config/articles.yaml](config/articles.yaml)). Re-running PELT on three article subsets ([robustness_changepoints.csv](analyze/figures/robustness_changepoints.csv)):

| Subset                                     | Articles | PELT break dates |
|--------------------------------------------|----------|------------------|
| ALL 8 articles                             | 8        | **2022-12-05**, 2023-07-03 |
| EXCLUDE post-window-created                | 5        | 2020-02-24, 2021-06-28, 2022-08-22, 2024-05-13 |
| LABOR-NARROW (Tech_unempl, Autom, UBI)     | 3        | 2020-06-08, 2020-09-21, 2021-05-24, 2024-05-13, 2025-04-28 |

When the three mid-window-created articles are removed, **no ChatGPT-aligned break is detected**. The ChatGPT article alone accounts for **82.9% of tech_displacement attention since 2023-01-01**.

![Article-level breakdown](analyze/figures/03_tech_articles_breakdown.png)

**H3 is therefore rejected**: the −5-day "ChatGPT break" on the all-article series is mostly an artifact of three AI articles entering the panel in late 2022 / early 2023. The robust signal in `tech_displacement` reflects *AI-product attention* (driven by the ChatGPT article itself), not labour-displacement anxiety.

### 5.6 Summary of hypotheses

| Hypothesis | Verdict |
|------------|---------|
| H1 — classical attention predictively leads jobless claims | Consistent with the data (p < 10⁻³, all three classical groups) |
| H2 — tech_displacement attention is decoupled from realised jobless claims | Consistent with the data (p = 0.97) |
| H3 — ChatGPT launch caused a structural break in *labour-displacement anxiety* | **Rejected.** The break disappears once mid-window-created AI articles are removed, and ChatGPT alone contributes 83% of group attention post-2023. |

## 6. Reproducibility

```bash
ssh CNETID@midway3.rcc.uchicago.edu
git clone https://github.com/macs30113-s26/final-project-unemployment-signals.git wiki-attention
cd wiki-attention
module load python/anaconda-2022.05
python -m pip install --user statsmodels ruptures
sbatch slurm/ingest_rest.sbatch                  # ~70 s, REST primary
python aggregate/aggregate_pandas.py \
    --staging /scratch/midway3/$USER/wiki-attention/staging_rest \
    --curated /scratch/midway3/$USER/wiki-attention/curated
# sinteractive node (needed for FRED HTTP access):
sinteractive --account=macs30113 --partition=caslake \
    --ntasks=1 --cpus-per-task=4 --mem=8G --time=00:30:00
module load python/anaconda-2022.05
python analyze/analyze.py            --curated <...> --out-dir analyze/figures
python analyze/analyze_robustness.py --curated <...> --out-dir analyze/figures
```

## 7. Incidents and Engineering Pivots

### 7.1 Failed AWS Lambda pipeline

The original cloud-fan-out architecture (AWS Lambda + S3 + Spark + Athena) is archived under [aws/](aws/) and **never completed an end-to-end run**. Two attempts triggered AWS Academy Learner Lab account deactivations:

- Concurrency = 40: deactivated at ~12 minutes, $20.30 / $50 spent. Post-deactivation IAM included an `s3:ListBucket` explicit deny that prevented standard resume logic.
- Concurrency = 8 (guardrail at 10): deactivated at ~29 minutes, $0 spent, 2,000 / 21,000 target hours pulled. The lab's per-account Lambda concurrency cap is 10; async retries appear to have transiently exceeded it.

The migration to Midway3 was the direct response. See [aws/README.md](aws/README.md) for the incident archive.

### 7.2 Wikimedia dumps rate limiting

Three dump-tier jobs (49788236 at 28 workers; 49791029 at 8; 49791901 at 4) all hit HTTP 429 or plateaued at ~13 files/min. The full 7-year pull would have required ~80 hours and 20+ sequential Slurm submissions. The two-tier design (§4.1) was the response.

### 7.3 PySpark failed; pandas was sufficient

Job 49794175 (Spark aggregation) failed at startup: caslake nodes do not export `JAVA_HOME` and no `spark/*` modulefile is available. Rather than building Spark from source, we replaced the aggregator with pandas ([aggregate_pandas.py](aggregate/aggregate_pandas.py)), which completes in ~5 seconds on 75k rows. Spark would have been correct for the full dump corpus; for this dataset it was overkill. The PySpark code ([aggregate_pageviews.py](aggregate/aggregate_pageviews.py)) is retained for documentation but is not part of the working pipeline.

## 8. Slurm Job IDs

### Final pipeline (the run whose output produces the figures and CSVs in this repo)

| Stage              | Job ID       | Wall-clock | Status         | Output            |
|--------------------|--------------|------------|----------------|-------------------|
| REST ingest (user) | **49826767** | 70 s       | **COMPLETED**  | `staging_rest/` → 75,346 day-records that feed everything downstream |

Post-ingest steps ([aggregate_pandas.py](aggregate/aggregate_pandas.py), [analyze.py](analyze/analyze.py), [analyze_robustness.py](analyze/analyze_robustness.py)) ran interactively on a sinteractive node rather than via `sbatch`: the post-ingest data volume is 75k rows / ~3 MB in memory, so pandas finishes the aggregation in ~5 seconds and the analysis in ~30 seconds, and a separate Slurm submission for each would be ceremonial. The decision is described honestly in §7.3 and §4.1 rather than papered over with cosmetic sbatch wrappers.

### Diagnostic / failed runs (architecture-evolution evidence)

These are not part of the final pipeline; they document the engineering pivots in §7.

| Stage                       | Job ID   | Wall-clock | Status                       | Notes |
|-----------------------------|----------|------------|------------------------------|-------|
| Dump ingest (28-worker)     | 49788236 | 4.5 min    | COMPLETED, 99% HTTP-failed   | First dump attempt — §7.2 |
| Dump ingest (8-worker)      | 49791029 | ~13 min    | CANCELLED                    | 429s persisted at lower concurrency — §7.2 |
| Dump ingest (4-worker)      | 49791901 | ~7 min     | CANCELLED                    | Throughput plateau at 13/min — §7.2 |
| REST ingest (all-agents)    | 49793806 | 69 s       | COMPLETED                    | Superseded by 49826767 (agent filter change) |
| REST ingest (all-agents v2) | 49793986 | ~10 s      | COMPLETED                    | Superseded by 49826767 |
| Spark aggregate             | 49794175 | 56 s       | **FAILED**                   | JAVA_HOME unset — §7.3 |

All stdout/stderr from these jobs are committed under [slurm/logs/](slurm/logs/).

## 9. Known Limitations

**9.1 REST `user` filter.** The final dataset uses `all-access/user`, which excludes Wikimedia's heuristic bot/spider classification. The earlier `all-agents` run inflated total counts by 19.5% on a weighted-mean basis, with per-group bot share ranging 16.2%–26.6% (core_unemployment had the highest bot contamination). Evidence: [analyze/figures/user_vs_allagents_comparison.csv](analyze/figures/user_vs_allagents_comparison.csv), computed from the `staging_rest_allagents/` backup vs. the final `staging_rest/`. The user/agent classifier is itself heuristic.

**9.2 REST vs dump access-type mismatch.** [dump_rest_validation.csv](analyze/figures/dump_rest_validation.csv) covers 288 (article, date) pairs across 11 days where both sources have full coverage. Median relative difference is 11.8%, but the **mean is ~102%** with a long right tail — REST counts tend to be ~2× dump counts, plausibly because REST's `all-access` includes mobile-app traffic that the `en`+`en.m` dumps exclude, but the tail is wide enough that this is a **partial shape-check, not a full validation**. We do not claim the two sources reconcile at the level of individual records.

**9.3 No alias merging in the REST primary path.** [ingest/ingest_pageviews_rest.py](ingest/ingest_pageviews_rest.py) fetches each canonical title's REST aggregates but does **not** merge aliases listed in [config/articles.yaml](config/articles.yaml). The aliases ARE consumed by the dump-pull code, but since the final analysis is built on REST data, no alias merging is in effect. The config-file header comment was updated to reflect this. If an article was renamed mid-window, the post-rename canonical may under-count compared to the redirect-resolved truth.

**9.4 Construct validity of `tech_displacement`.** §5.5 shows that the group is dominated by AI-product attention (ChatGPT alone is 83% post-2023) rather than labour-displacement anxiety. A cleaner future operationalisation would require either a narrower article list (e.g., `Technological_unemployment` only) or a sentiment-weighted measure.

**9.5 Granger predictability ≠ causation.** §5.3 is a forecast-improvement test on first-differenced series. We select the best-of-8 lags per group and do not correct for multiple comparisons; treat the p-values as descriptive evidence of temporal precedence, not as confirmatory tests of causal structure. The 2020 COVID shock plausibly drives both Wikipedia attention *and* jobless claims, so a common-shock confound cannot be ruled out without an instrument. Additionally, the code applies `.diff()` to raw counts rather than to log-counts. Because pageview series are right-skewed and span several orders of magnitude (COVID spikes are ~10× baseline), the COVID 2020 segment dominates the first-difference variance and may inflate the apparent significance. A log-transformed first difference would be the more defensible specification and is left for future work.

**9.6 No formal lead-lag analysis in figures.** §5.2 reports a *visual* lead of attention over UNRATE in the COVID period; we did not compute a formal cross-correlation or event-study window. A future version should.

**9.7 Dump pull is small.** §5.5 / §9.2 cross-validation is conditional on the 11 full-coverage days within the 353-file dump sample. Extrapolating to the full 7-year window would require completing the dump-tier pull (~80 h at the only safe concurrency).

**9.8 Scratch storage.** Staging and curated data live on `/scratch/midway3/$USER`, purged after 60 days of inactivity. Outputs and CSVs are committed; raw staging is regenerable from public endpoints.

## References

- Choi, H., & Varian, H. (2012). Predicting the present with Google Trends. *Economic Record*, 88, 2–9.
- Killick, R., Fearnhead, P., & Eckley, I. A. (2012). Optimal detection of changepoints with a linear computational cost. *Journal of the American Statistical Association*, 107(500), 1590–1598. (PELT)
- Wikimedia Foundation. *Pageviews data dictionary*. https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews
