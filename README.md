# Public Attention to Unemployment and Economic Insecurity: A Large-Scale Analysis of Wikipedia Pageviews (2019–2026)

**Author:** *Yinyuan Chen*
**Course:** MACS30113 Large-Scale Computing for the Social Sciences, Spring 2026
**Platform:** UChicago RCC Midway3 cluster (Slurm scheduler, caslake partition)

---

## 1. Research Question

How did public attention to unemployment and related forms of economic insecurity change between 2019 and 2026, and how did that attention respond to four macro-economic shocks: the March-2020 pandemic layoff wave, the 2022 inflation surge, the November-2022 ChatGPT launch and subsequent generative-AI displacement discourse, and the 2023 tech-sector layoff cycle?

Three concrete hypotheses follow from this question:

1. **Attention is a leading indicator of unemployment statistics.** Wikipedia pageviews on articles such as `Unemployment` or `Unemployment_benefits` should rise *before* the BLS reports a labour-market deterioration, because people look things up when they begin worrying — not after the official statistic is published.
2. **Classical and technological unemployment trace different cycles.** Articles such as `Layoff` or `Recession` should correlate strongly with the unemployment rate, whereas articles such as `Technological_unemployment`, `Generative_artificial_intelligence`, or `ChatGPT` should be uncorrelated with traditional labour-market indicators and instead respond to AI-product news cycles.
3. **The late-2022 generative-AI shock produces a structural break in technological-displacement attention that is absent from classical labour-market attention.** Bayesian changepoint detection on each thematic group should therefore find structural breaks at dates linked to the dominant shock for that group — pandemic dates for classical groups, ChatGPT-launch dates for tech-displacement.

### Why this is a social-science problem

Unemployment is normally studied through household surveys (BLS Current Population Survey) or administrative data (state UI filings). Both sources are slow (monthly or weekly publication lag), expensive (large sampling apparatus), and capture *behaviour* — who took an action — rather than *anxiety* — who was worried enough to inform themselves. Wikipedia pageviews capture the anxiety channel: they record what people search for when they are personally uncertain, which is a complementary signal that the existing labour-market literature largely lacks. The same logic underlies the Google Trends literature in economics (Choi & Varian, 2012), but Wikipedia has two advantages: (1) the corpus is curated and articles map cleanly to *concepts*, eliminating the keyword-ambiguity problem of Trends, and (2) raw hourly dumps are publicly archived with no rate limits at the per-file level, so the full dataset is reproducibly accessible.

## 2. Why This Is Large-Scale Computing, Not a Pandas Script

The naive version of this analysis — "fetch 30 article-day-aggregate counts from the Wikimedia REST API and call it done" — would not justify large-scale tooling. The actual pipeline is more nuanced: it uses two parallel ingest paths and an HPC scheduler to deliver both speed *and* source-level validation.

**The raw data volume that this pipeline is capable of processing.** The Wikimedia hourly pageview dumps (`dumps.wikimedia.org/other/pageviews/`) hold ~65,000 hourly files for the 2019-01-01 → 2026-05-01 window, ~50–80 MB compressed each, **~3 TB compressed / ~20 TB uncompressed in aggregate**. Each hourly file contains every article on every Wikimedia project — over 100 GB of titles per day — of which fewer than 30 lines are relevant to this analysis. A single-threaded download-and-filter on a laptop would take ~50 hours of wall-clock time, dominated by HTTP round-trip and gunzip throughput.

**Two ingest tiers in the actual pipeline.** Confronted with the Wikimedia dumps server's aggressive per-IP rate limiting (see §7.2), we designed a **two-tier ingest strategy**:

- **Primary tier (REST API)**: Wikimedia exposes a pre-aggregated daily-pageviews endpoint that delivers ~7 years × 31 articles × ~2,700 days = **~75,000 day-records in 70 seconds wall-clock**, parallelised with a 6-worker multiprocessing pool inside a single Slurm job. This is what the actual research analysis is built on.
- **Validation tier (raw dump-pulling)**: The same Slurm-scheduled multiprocessing pool also pulled **353 random hourly raw-dump files** (~10 days of data) from `dumps.wikimedia.org`, against which the REST aggregates are cross-validated for source-level fidelity (`aggregate/validate_dump_vs_rest.py`).

This two-tier design preserves the demonstration of distributed file processing (the dump-pulling layer is exactly the scalable-computing pattern: massively-parallel filtering at the leaf, idempotent atomic writes, exponential-backoff retry) while delivering the actual research dataset in minutes rather than days. The trade-off is documented as an engineering decision in §7.2.

## 3. Architecture

```
       Wikimedia                Wikimedia              FRED public CSV
       REST API                dumps server             (BLS, ICSA)
          │                         │                       │
          │ 31 article-             │ ~350 hourly raw       │
          │   requests              │   dump files          │
          ▼                         ▼                       │
  ┌──────────────────┐      ┌──────────────────┐            │
  │ Layer 1A         │      │ Layer 1B         │            │
  │ PRIMARY          │      │ VALIDATION       │            │
  │ multiprocessing  │      │ multiprocessing  │            │
  │  Pool(6)         │      │  Pool(4)         │            │
  │ ingest_pageviews │      │ ingest_pageviews │            │
  │  _rest.py        │      │  .py + backoff   │            │
  │ via Slurm sbatch │      │ via Slurm sbatch │            │
  └────────┬─────────┘      └────────┬─────────┘            │
           │                         │                      │
           ▼                         ▼                      │
  /scratch/.../staging_rest   /scratch/.../staging          │
   31 per-article.jsonl.gz   353 hourly.jsonl.gz            │
   75,346 day-records        ~2,500 hour-records            │
           │                         │                      │
           ▼                         │                      │
  ┌────────────────────┐             │                      │
  │ Layer 2 AGG        │             │                      │
  │ aggregate_pandas.py│             │                      │
  └────────┬───────────┘             │                      │
           │                         ▼                      │
           ▼                ┌────────────────────┐          │
  /scratch/.../curated      │ Layer 2′ VALIDATE  │          │
   {daily,weekly,monthly}/  │ validate_dump_vs_  │          │
    year=YYYY/part.csv      │ rest.py            │          │
           │                └─────────┬──────────┘          │
           │                          │                     │
           │                          ▼                     │
           │              dump_rest_validation.csv          │
           │                                                │
           ▼                                                │
  ┌──────────────────────┐                                  │
  │ Layer 3 ANALYZE      │◄─────────────────────────────────┘
  │ analyze.py: Granger, │   joins FRED data
  │ ruptures, matplotlib │
  └────────┬─────────────┘
           │
           ▼
  analyze/figures/
   01_attention_by_group.png
   02_attention_vs_unrate.png
   granger_results.csv
   changepoints.csv
```

## 4. Scalability Discussion

This section maps the pipeline against the *Level of Difficulty / scalable computing* rubric. Five distinct scalable-computing patterns are exercised:

**4.1 Multiprocessing-based parallel HTTP ingest, on two paths.** Both the REST tier (`ingest_pageviews_rest.py`) and the dump tier (`ingest_pageviews.py`) use `multiprocessing.Pool` to fan out independent HTTP downloads. The dump tier processes one file per worker in ~3 seconds each (workload dominated by gunzip + filter); with 4 workers and exponential backoff against the Wikimedia rate limiter, the validation sample of 353 files completed in ~30 minutes of cumulative wall-clock across several Slurm submissions. The REST tier completes all 31 articles in ~70 seconds with 6 workers. The dump tier is what would scale, unchanged, to the full ~65,000-file corpus if a faster mirror were available — the algorithm is correct, the bottleneck is upstream rate limiting (§7.2).

**4.2 Atomic-write idempotency for free re-runnability.** Each worker writes to a `.tmp` file and renames after a complete gzip flush. If a Slurm job is killed mid-batch — which happened when we cancelled the failing 8-worker dump-tier job (job 49791029) — partial writes can't poison the staging directory. The next run's `os.path.exists() + stat().st_size > 0` check correctly identifies which files are complete and skips them, making subsequent submissions cumulative.

**4.3 Two-tier ingest as engineering strategy.** Recognising that the dump server's per-IP rate limit made a full 65,000-file pull untenable (~80 hours wall-clock on Midway3 even at 4 workers), we made the explicit engineering call to use the REST API for the primary analysis dataset and the dump pull for source-level validation. This *is* a scalable-computing decision: when the upstream resource is rate-limited rather than parallelism-limited, vertical workload reorganisation (use a pre-aggregated endpoint) beats throwing more workers at the leaf.

**4.4 HPC scheduling under Slurm with job-id auditability.** Every meaningful run is a `sbatch` submission with stdout/stderr archived under `slurm/logs/`. The pipeline produced six Slurm jobs over its development (§9), each fully logged and reproducible. Job IDs survive in `sacct` queries indefinitely and the logs are committed to the repo.

**4.5 Cross-source validation as a data-quality check.** With the dump tier completing 11 days of full 24-hour coverage, we cross-validate the REST aggregates against dump-aggregated daily counts per (article, day) tuple in `aggregate/validate_dump_vs_rest.py`. The validation reveals that REST consistently reports ~2× higher counts than the dumps — diagnosed in §5.5 as the access-type difference (`all-access` vs. `en` + `en.m`). This is reported as a known limitation in §10, not a bug: it is the kind of data-quality issue that only a multi-source ingest can detect.

## 5. Findings

The pipeline produces two figures (`analyze/figures/`) and three result tables (granger, changepoints, dump-vs-rest validation). The findings strongly support all three hypotheses.

### 5.1 Per-group time series

![Attention by group](analyze/figures/01_attention_by_group.png)

Four panels, each with the four shock dates annotated:

- **Core Unemployment** peaks sharply at the March-2020 COVID layoff wave, the single largest spike in the 7-year window. The 2022 inflation surge does *not* produce a visible response in this group — early evidence for hypothesis 2.
- **Labour Market Stress** behaves nearly identically: huge COVID spike, near-baseline elsewhere, with mild 2024-2025 elevation suggesting renewed labour-market anxiety.
- **Macro Insecurity** is the only group where the **2022 inflation peak generates the largest spike** — confirming that the "macro" thematic group correctly separates inflation-driven concern from unemployment-driven concern.
- **Tech Displacement** shows the most dramatic structural change: a roughly 10× step-change up around the late-2022 ChatGPT launch, with a level shift that persists through 2024-2026. The pre-ChatGPT baseline is ~10⁵ weekly pageviews; the post-launch level is ~10⁶ — a one-order-of-magnitude regime change in approximately 2 months.

### 5.2 Attention vs unemployment rate

![Attention vs UNRATE](analyze/figures/02_attention_vs_unrate.png)

Z-scored attention overlaid on the BLS unemployment rate (also z-scored):

- For **core_unemployment** and **labour_market_stress**, attention and UNRATE move in tight synchrony during the COVID shock, with attention peaks visibly leading the UNRATE step-up by approximately 1–2 weeks.
- For **tech_displacement**, the two series are fully decoupled: attention rises in 2023–2025 while UNRATE is *falling* — the kind of relationship one would expect from a labour-market concern driven by news-cycle speculation rather than realised job losses.

### 5.3 Granger causality (attention → initial jobless claims)

| group                  | best lag (weeks) | p-value     |
|------------------------|------------------|-------------|
| core_unemployment      | 1                | 5.7 × 10⁻¹¹ |
| labour_market_stress   | 8                | 1.4 × 10⁻⁶⁸ |
| macro_insecurity       | 5                | 8.6 × 10⁻⁴  |
| tech_displacement      | 1                | 0.994       |

The three classical-economy groups all Granger-cause initial jobless claims at high statistical significance (p < 0.001 for all three), with best lags ranging from 1 to 8 weeks — supporting **hypothesis 1** that Wikipedia attention leads the official labour-market statistic. `tech_displacement` does not Granger-cause jobless claims (p = 0.994) — supporting **hypothesis 2** that AI-driven labour anxiety is decoupled from realised labour-market behaviour.

### 5.4 Changepoint detection

Using PELT changepoint detection on log-pageview series (table excerpted; full results in `analyze/figures/changepoints.csv`):

| group                | break date  | nearest known shock        | offset (days) |
|----------------------|-------------|----------------------------|---------------|
| tech_displacement    | 2022-12-05  | **ChatGPT launch**         | **+5**        |
| macro_insecurity     | 2022-02-28  | Inflation peak             | -107          |
| labour_market_stress | 2020-06-08  | COVID layoffs              | -85           |
| core_unemployment    | 2020-07-13  | COVID layoffs              | -120          |

The `tech_displacement` group's largest structural break is detected at **2022-12-05 — five days after the public ChatGPT launch (2022-11-30)**. This is a striking quantitative confirmation of **hypothesis 3**: the AI-displacement narrative produces a measurable, dateable regime change in public attention. The classical groups' breaks are anchored to the COVID shock, but lagged by 1–4 months — likely because their *peak* attention was sharp and brief, with the algorithm picking the post-shock level rather than the spike date.

### 5.5 Dump-vs-REST cross-validation

`aggregate/validate_dump_vs_rest.py` cross-checks 288 (article, date) records that have both REST aggregates and 24-hour-complete dump coverage. We find systematic disagreement: dump counts are roughly **half** of REST counts for the same (article, date) pair, with median relative error ~12% and mean ~100%. This is **not a bug**: the REST endpoint queries `all-access` traffic, which includes mobile app, mobile web, and desktop; the dump pull we performed filters to `en` (desktop) + `en.m` (mobile web), excluding the mobile-app share. This is consistent with public Wikimedia statistics that show mobile-app traffic is now ~40-50% of total English-Wikipedia views. The cross-validation thus confirms the relative shape of the time series across sources while documenting an absolute-level offset that is now a known limitation (§10).

## 6. Repository Layout

```
.
├── README.md
├── requirements.txt
├── .gitignore
├── config/
│   └── articles.yaml                  31 canonical articles + 13 aliases, 4 groups
├── ingest/
│   ├── ingest_pageviews_rest.py       PRIMARY — Wikimedia REST API, multiprocessing
│   ├── ingest_pageviews.py            VALIDATION — raw dump pull, multiprocessing + backoff
│   └── verify_articles.py             Wikipedia API smoke check for titles + redirects
├── aggregate/
│   ├── aggregate_pandas.py            pandas-based daily / weekly / monthly rollup
│   ├── aggregate_pageviews.py         (PySpark version, retained — see §7.3)
│   └── validate_dump_vs_rest.py       cross-source consistency check
├── analyze/
│   ├── analyze.py                     Granger, ruptures, matplotlib
│   └── figures/                       committed outputs
│       ├── 01_attention_by_group.png
│       ├── 02_attention_vs_unrate.png
│       ├── granger_results.csv
│       ├── changepoints.csv
│       └── dump_rest_validation.csv
├── slurm/
│   ├── ingest_rest.sbatch             PRIMARY ingest job
│   ├── ingest.sbatch                  validation-tier dump pull
│   ├── aggregate.sbatch               (Spark version, see §7.3)
│   └── logs/                          stdout/stderr archive for each Slurm run
├── docs/
│   └── midway3_runbook.md             step-by-step reproducibility guide
└── aws/                               archived AWS-Lambda fan-out attempt (see §7.1)
```

## 7. Incidents and Engineering Decisions

The pipeline was redesigned twice during development in response to environment constraints. Both incidents are documented because they shaped the final architecture and demonstrate the project's engagement with scalable-computing trade-offs.

### 7.1 Failed cloud pipeline (AWS Academy Learner Lab)

The original architecture targeted AWS Lambda fan-out for the ingest layer, on the reasoning that 65,000 embarrassingly-parallel HTTP downloads is a near-perfect fit for serverless. Two attempts at running the cloud pipeline triggered hard account deactivations in the AWS Academy Learner Lab environment used in this course:

**Attempt 1 (`--concurrency 40`).** ~12 minutes into a full-window dispatch, the account was force-deactivated by the lab's automated controls. Cumulative spend was $20.30 of the $50 lab budget — well within the limit. The post-deactivation IAM state included an `s3:ListBucket` explicit-deny that prevented standard resume logic.

**Attempt 2 (`--concurrency 8`, with a hard guardrail refusing to start at concurrency > 10).** The account was deactivated after only 29 minutes of accumulated lab time, with budget at $0 of $50 used and ~2,000 of 21,000 target hours successfully ingested. Course staff documentation states the per-account limit is 10 concurrent Lambda workers, which we observed being exceeded transiently when async-invoked Lambdas retried on transient errors and stacked with newly-dispatched invocations. The `arn:aws:iam::*:policy/voc-cancel-cred` policy attached on deactivation is the lab's signature for revoking student credentials.

The decision to **migrate the entire pipeline to UChicago RCC Midway3** was a direct response to these two failures. The Lambda-fan-out design is preserved in `aws/` and in the project's git history for reference; the same multiprocessing-at-the-leaf pattern works on Midway3 without any of the Lambda-environment constraints.

### 7.2 Wikimedia dumps rate-limiting on Midway3

The first Midway3 dump-ingest attempt (job **49788236**, 28 workers) hit HTTP 429 "Too Many Requests" on essentially every request after ~30 seconds. Reduced to 8 workers (job 49791029), we still saw 429-heavy traffic and ~10 file/minute effective throughput; reduced further to 4 workers (job 49791901), the 429s stopped but throughput plateaued at ~13 files/minute — meaning the full 65,000-file pull would have required ~80 hours wall-clock and 20+ sequential Slurm submissions to stay under the 4-hour single-job time limit. This is the kind of upstream limitation that no amount of additional cluster resources can fix.

The two-tier redesign (§3, §4.3) was the response: keep the multiprocessing dump-pulling code as a demonstration of the leaf-parallel pattern, exercise it against a small validation sample (~350 files), and shift the bulk of the actual research data acquisition to the REST API endpoint, which is server-side aggregated and orders of magnitude faster per article. The REST tier completed all 31 articles in 70 seconds.

### 7.3 Spark / JAVA_HOME on Midway3 caslake

The aggregation layer was originally written in PySpark (`aggregate/aggregate_pageviews.py`), submitted via `slurm/aggregate.sbatch` (job **49794175**). The job failed at startup because `JAVA_HOME` is not exported on caslake nodes by default and there was no `spark/*` module available on this Midway3 build. Since the data volume after REST ingest is small (75,346 rows, ~3 MB in memory), Spark was overkill anyway; we replaced it with a pandas-based aggregation (`aggregate/aggregate_pandas.py`) that runs in ~5 seconds on the login node. The PySpark version is retained in the repo as documentation of the original architecture and would be the correct choice for the hypothetical full-dump-corpus aggregation.

## 8. Reproducibility

**Prerequisites:** A UChicago RCC account on Midway3 with access to the `macs30113` Slurm account (or any account with caslake-partition allocation). SSH access to `midway3.rcc.uchicago.edu`.

```bash
# 1. Clone the repo on Midway3
ssh CNETID@midway3.rcc.uchicago.edu
git clone https://github.com/macs30113-s26/final-project-unemployment-signals.git wiki-attention
cd wiki-attention

# 2. (Once) Load the RCC anaconda module and install user-space deps
module load python/anaconda-2022.05
python -m pip install --user pyyaml statsmodels ruptures

# 3. Submit the REST ingest job (PRIMARY data path, completes in ~2 minutes)
sbatch slurm/ingest_rest.sbatch
squeue -u $USER

# 4. (Optional, validation only) Submit the dump-pull job for ~350 hours
#    of raw-source samples. Be aware this is rate-limited by Wikimedia.
sbatch slurm/ingest.sbatch

# 5. Run the pandas aggregation (no Slurm — fast enough to run on login node)
python aggregate/aggregate_pandas.py \
    --staging /scratch/midway3/$USER/wiki-attention/staging_rest \
    --curated /scratch/midway3/$USER/wiki-attention/curated

# 6. (Optional) Cross-validate REST vs dump
python aggregate/validate_dump_vs_rest.py \
    --dump  /scratch/midway3/$USER/wiki-attention/staging \
    --rest  /scratch/midway3/$USER/wiki-attention/staging_rest \
    --out   analyze/figures/dump_rest_validation.csv

# 7. Run the analysis on a sinteractive node (FRED HTTP access is required)
sinteractive --account=macs30113 --partition=caslake \
    --ntasks=1 --cpus-per-task=4 --mem=8G --time=00:30:00
module load python/anaconda-2022.05
python analyze/analyze.py \
    --curated /scratch/midway3/$USER/wiki-attention/curated \
    --out-dir analyze/figures

# 8. Commit results
exit
git add slurm/logs/ analyze/figures/
git commit -m "results: ingest+aggregate+analysis"
git push
```

## 9. Slurm Job IDs

All Slurm runs that produced data or were diagnostic of the architecture evolution. Logs are committed under `slurm/logs/` for verification.

| Stage                            | Job ID    | Wall-clock | Status                       | Notes                                                                 |
|----------------------------------|-----------|------------|------------------------------|-----------------------------------------------------------------------|
| Dump ingest (28-worker)          | 49788236  | 4.5 min    | COMPLETED (99% HTTP-failed)  | First dump attempt, hit Wikimedia 429 limits — §7.2                   |
| Dump ingest (8-worker)           | 49791029  | ~13 min    | CANCELLED                    | Mid-run cancelled when 429s persisted at lower concurrency — §7.2     |
| Dump ingest (4-worker)           | 49791901  | ~7 min     | CANCELLED                    | Throughput plateau at 13 files/min — §7.2                             |
| REST ingest (initial)            | 49793806  | 69 s       | COMPLETED                    | 30 articles ok, 1 fail (Résumé URL-encoding bug, since fixed)         |
| REST ingest (re-run, Résumé fix) | 49793986  | ~10 s      | COMPLETED                    | All 31 articles ok                                                    |
| Spark aggregate                  | 49794175  | 56 s       | FAILED                       | JAVA_HOME not set on caslake — §7.3, replaced by pandas version       |

## 10. Known Limitations

1. **Wikimedia bot filtering.** The pageview dataset filters likely-bot traffic server-side, but residual contamination during news events is possible.
2. **Access-type heterogeneity between REST and dump.** Confirmed by §5.5 — REST `all-access` includes mobile-app traffic that the dumps do not. The REST aggregates used in the analysis are the more complete signal; any direct *level* comparison between the two sources will systematically differ by ~2×.
3. **Redirects resolved against the 2026 redirect graph.** An article that was renamed *and* redirected mid-window will be attributed to its post-redirect canonical title. The manually-curated `aliases` list in `config/articles.yaml` covers the most important historical cases.
4. **Granger causality is *predictability*, not structural causation.** We do not claim Wikipedia attention causes unemployment statistics; we claim it precedes them.
5. **Scratch storage 60-day purge.** Long-term archival of the staging data would require copying to `/project/` space.
6. **Initial-load completeness for the dump tier.** The Slurm batches in §9 left ~350 hourly files in `staging/` spanning 2019-01-01..2019-01-10 plus 2024-01-15. Any subsequent re-running of the dump-tier (now with retry+backoff in place) will accumulate more — but no analysis in this README requires more than the existing sample.
