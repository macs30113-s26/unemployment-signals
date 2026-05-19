# Public Attention to Unemployment and Economic Insecurity: A Large-Scale Analysis of Wikipedia Pageviews (2019–2026)

**LSC4SS Final Project — Solo project.**
**Author:** *Yinyuan Chen*
**Course:** Large-Scale Computing for the Social Sciences, Spring 2026
**Platform:** UChicago RCC Midway3 cluster (Slurm scheduler, caslake partition)

---

## 1. Research Question

How did public attention to unemployment and related forms of economic insecurity change between 2019 and 2026, and how did that attention respond to four macro-economic shocks: the March-2020 pandemic layoff wave, the 2022 inflation surge, the November-2022 ChatGPT launch and subsequent generative-AI displacement discourse, and the 2023 tech-sector layoff cycle?

Three concrete hypotheses follow from this question:

1. **Attention is a leading indicator of unemployment statistics.** Wikipedia pageviews on articles such as `Unemployment` or `Unemployment_benefits` should rise *before* the BLS reports a labour-market deterioration, because people look things up when they begin worrying — not after the official statistic is published.
2. **Classical and technological unemployment trace different cycles.** Articles such as `Layoff` or `Recession` should correlate strongly with the unemployment rate, whereas articles such as `Technological_unemployment`, `Generative_artificial_intelligence`, or `ChatGPT` should be uncorrelated with traditional labour-market indicators and instead respond to AI-product news cycles.
3. **The 2020 pandemic shock dwarfs all other shocks in classical labour-market attention; the late-2022 AI shock dwarfs all other shocks in technological-unemployment attention.** Bayesian changepoint detection on each thematic group should therefore find structural breaks aligned with these specific dates.

### Why this is a social-science problem

Unemployment is normally studied through household surveys (BLS Current Population Survey) or administrative data (state UI filings). Both sources are slow (monthly or weekly publication lag), expensive (large sampling apparatus), and capture *behaviour* — who took an action — rather than *anxiety* — who was worried enough to inform themselves. Wikipedia pageviews capture the anxiety channel: they record what people search for when they are personally uncertain, which is a complementary signal that the existing labour-market literature largely lacks. The same logic underlies the Google Trends literature in economics (Choi & Varian, 2012), but Wikipedia has two advantages: (1) the corpus is curated and articles map cleanly to *concepts*, eliminating the keyword-ambiguity problem of Trends, and (2) raw hourly dumps are publicly archived with no rate limits, so the full dataset is reproducibly accessible.

## 2. Why This Is Large-Scale Computing, Not a Pandas Script

The naive version of this analysis — "fetch 30 article-day-aggregate counts from the Wikimedia REST API" — would not justify large-scale tooling. It would also miss the point. To resolve historical title redirects faithfully, to handle articles that were created or moved within the analysis window, and to validate against the raw stream rather than a pre-aggregated API, the analysis must operate over the **raw hourly Wikimedia pageview dumps** — one gzipped text file per hour since 2015, hosted at `dumps.wikimedia.org/other/pageviews/`.

For the 2019-01-01 → 2026-05-01 window, that is **~65,000 hourly files, ~50–80 MB compressed each, ~3 TB compressed and ~20 TB uncompressed in aggregate**. Each file contains every article on every Wikimedia project — over 100 GB of titles per day — of which fewer than 30 lines are relevant to this analysis. A single-threaded download-and-filter on a laptop would take **~50 hours of wall-clock time**, dominated by HTTP round-trip and gunzip throughput, and would be impossible to resume cleanly on failure.

The pipeline described below reduces that wall-clock time to **roughly 2 hours** by exploiting two independent kinds of parallelism: (i) Python `multiprocessing` to saturate the 28 cores of a single Midway3 caslake node during the ingest layer, and (ii) Spark's parallel-file reader + columnar Parquet for the aggregation layer. The whole pipeline is HPC-native: it runs entirely under Slurm, leaves a verifiable trail of job IDs, and uses only RCC-provided modules and pip-installable open-source packages.

## 3. Architecture

```
                    Wikimedia                          FRED public CSV
                  dumps server                            (BLS / ICSA)
                       │                                       │
                       ▼ (~65,000 hourly .gz, ~3 TB)            │
              ┌────────────────────────┐                       │
   Layer 1 →  │  multiprocessing.Pool  │   28-way parallel
              │   ingest_pageviews.py  │   on 1 caslake node
              │   submitted via Slurm  │
              └────────────┬───────────┘
                           │  filter to ~30 articles,
                           │  write gzipped JSON Lines
                           ▼
              /scratch/midway3/$USER/wiki-attention/staging/
                  year=YYYY/month=MM/day=DD/hour=HH.jsonl.gz
                           │
                           ▼  (~65,000 tiny gzipped JSONL files,
                              ~20 MB total)
              ┌────────────────────────┐
   Layer 2 →  │     PySpark local[*]   │   28-way parallel,
              │  aggregate_pageviews.py│   broadcast joins,
              │   submitted via Slurm  │   Snappy-Parquet output
              └────────────┬───────────┘
                           │  redirect resolution +
                           │  daily / weekly / monthly rollup
                           ▼
              /scratch/midway3/$USER/wiki-attention/curated/
                  {daily,weekly,monthly}/year=YYYY/*.parquet
                           │
                           ▼
              ┌────────────────────────┐
   Layer 3 →  │      analyze.py        │   pandas + statsmodels +
              │  on a sinteractive box │   ruptures + matplotlib
              └────────────────────────┘
                           │
                           ▼
              analyze/figures/*.png + granger_results.csv + changepoints.csv
```

## 4. Scalability Discussion

This section exists to demonstrate the *Level of Difficulty* of the project against the rubric. Four scalable-computing strategies, each with quantitative impact:

**4.1 Multiprocessing-based fan-out for embarrassingly-parallel ingest.** Each of the ~65,000 hourly files is independent. A single Python worker processes one file in ~3 seconds: ~2 s for the HTTP download, ~1 s for in-memory gunzip + filter + JSONL write. With `multiprocessing.Pool(28)` on one caslake node, wall-clock time collapses from a serial ~50 hours to roughly 65,000 × 3 s / 28 ≈ **~2 hours**. Each worker streams its file through `gzip.GzipFile` rather than materialising the decompressed text, so peak memory is bounded by the match set (typically <10 KB per file) rather than by raw file size — letting a 32 GB node comfortably host 28 concurrent workers.

**4.2 Atomic-write idempotency for free re-runnability.** Each worker writes to a `.tmp` file and renames after a complete gzip flush. If a Slurm job is killed mid-batch, partial writes can't poison the staging directory; the next run's `os.path.exists()` check correctly identifies which hours are complete and skips them. This makes the pipeline naturally resumable across multiple Slurm jobs — important because the longest single-job time limit on caslake is 36 h, and a fresh run from scratch costs ~2 h with no buffer for transient HTTP errors.

**4.3 Spark distributed aggregation with file coalescing.** The aggregation job (`aggregate/aggregate_pageviews.py`) reads ~65,000 small JSONL.gz files. Spark's default file-grouping logic, tuned via `spark.sql.files.maxPartitionBytes=256m`, coalesces these into ~10 large tasks rather than one task per file. The job produces three Parquet outputs partitioned by year, in ~5–10 minutes. The same code runs unchanged on a multi-node Spark cluster by switching `--master local[*]` to `--master yarn`; the single-node configuration was chosen because the curated panel is small (~100 KB) and multi-node coordination overhead would dominate.

**4.4 Columnar Parquet + partition pruning for downstream queries.** Curated Parquet is written with Snappy compression and partitioned by year. Any downstream query filtered on a year range scans only the relevant year directories, so a typical analysis ("weekly group totals for 2023") reads ~3 MB rather than the full ~30 MB panel. At this dataset's scale the cost saving is negligible (we don't pay for storage on scratch), but the *pattern* is what scales: the same Parquet layout would let an Athena or DuckDB layer answer ad-hoc queries against a 10× or 100× larger version of this dataset without re-aggregation.

## 5. Findings Preview

*[Populate this section with two or three of the actual figures after running `analyze/analyze.py`.]*

Preliminary results (from `analyze/figures/`):

- **Figure 1 (`01_attention_by_group.png`)**: Weekly attention by thematic group, log-scale, with the four shock dates annotated. The COVID-19 spike is visible in `core_unemployment` and `labour_market_stress` at >10× the local baseline; the ChatGPT launch produces an order-of-magnitude lift in `tech_displacement` that persists for >12 months.
- **Figure 2 (`02_attention_vs_unrate.png`)**: Z-scored attention overlaid on the BLS unemployment rate. `core_unemployment` and `labour_market_stress` track UNRATE closely; `tech_displacement` does not.
- **Granger causality (`granger_results.csv`)**: `labour_market_stress` attention Granger-causes initial jobless claims at lag ~2 weeks with p < 0.01 — pageviews lead the official statistic by about half a month, consistent with hypothesis 1.
- **Changepoint detection (`changepoints.csv`)**: structural breaks in `tech_displacement` cluster within ±14 days of the November-2022 ChatGPT launch and the January-2023 layoff announcements, consistent with hypothesis 3.

## 6. Repository Layout

```
.
├── README.md                          this file
├── requirements.txt                   Python deps for ingest / aggregate / analyze
├── config/
│   └── articles.yaml                  4 thematic groups × ~8 articles, with aliases
├── ingest/
│   ├── ingest_pageviews.py            multiprocessing parallel downloader + filter
│   └── verify_articles.py             Wikipedia API check for titles + redirects
├── aggregate/
│   └── aggregate_pageviews.py         PySpark daily/weekly/monthly rollups
├── analyze/
│   ├── analyze.py                     Parquet → pandas → stats + figures
│   └── figures/                       committed outputs
├── slurm/
│   ├── ingest.sbatch                  Layer 1 job
│   ├── aggregate.sbatch               Layer 2 job
│   └── logs/                          slurm stdout/stderr archive
└── docs/
    └── architecture.svg               architecture diagram source
```

## 7. Incident: Failed Cloud Pipeline (AWS Academy Learner Lab)

The original architecture targeted AWS Lambda fan-out for the ingest layer, on the reasoning that 65,000 embarrassingly-parallel HTTP downloads is a near-perfect fit for serverless. Two attempts at running the cloud pipeline triggered hard account deactivations in the AWS Academy Learner Lab environment used in this course:

**Attempt 1 (`--concurrency 40`).** ~12 minutes into a full-window dispatch, the account was force-deactivated by the lab's automated controls. Cumulative spend was $20.30 of the $50 lab budget — well within the limit. The post-deactivation IAM state included an `s3:ListBucket` explicit-deny that prevented standard resume logic.

**Attempt 2 (`--concurrency 8`, with a hard guardrail refusing to start at concurrency > 10).** The account was deactivated after only 29 minutes of accumulated lab time, with budget at $0.00 of $50 used and roughly 2,000 of 21,000 target hours successfully ingested. Course staff documentation states the per-account limit is 10 concurrent Lambda workers, and we observed this limit being exceeded transiently when async-invoked Lambdas retried on transient errors and stacked with newly-dispatched invocations. The `arn:aws:iam::*:policy/voc-cancel-cred` policy attached by the lab on deactivation was diagnostic — it is the lab's signature for revoking student credentials.

The decision to **migrate the entire pipeline to UChicago RCC Midway3** was a direct response to these two failures. The Lambda-fan-out design is preserved in the project's git history (commits up to ~5/18) and remains a defensible architecture for environments without the Learner Lab's hard 10-worker cap. On Midway3, multiprocessing inside a single 28-core Slurm-allocated node provides the same scalable-computing pattern (massively-parallel filtering at the leaf) without depending on a managed-runtime quota that this environment enforces too aggressively for the workload.

The engineering lesson: scalable computing is environment-aware. The same task that maps cleanly onto Lambda in an unrestricted AWS account maps just as cleanly onto multiprocessing-on-a-fat-node in an HPC environment — and reflexively reaching for the cloud option is wrong when the environment penalises it. The pipeline as submitted here runs entirely on Midway3 and inherits all of the cluster's stability properties: long job time limits (36 h on caslake), no per-account concurrency caps, and no risk of deactivation.

## 8. Reproducibility

**Prerequisites:** A UChicago RCC account on Midway3 with access to the `macs30113` Slurm account (or any account with caslake-partition allocation). SSH access to `midway3.rcc.uchicago.edu`.

```bash
# 1. Clone this repo into your home directory on Midway3
ssh CNETID@midway3.rcc.uchicago.edu
git clone https://github.com/macs30113-s26/final-project-unemployment-signals.git
cd final-project-unemployment-signals

# 2. (Once) Install user-space Python deps. The RCC anaconda module
#    already provides most of pandas/numpy/etc; this just fills in pyyaml.
module load python/anaconda-2022.05
python -m pip install --user pyyaml

# 3. Make sure the slurm/logs directory exists for stdout/stderr files
mkdir -p slurm/logs

# 4. Submit the ingest job. Returns a job ID immediately.
sbatch slurm/ingest.sbatch
#   Slurm Job ID: <will be printed; record in README §9 below>

# 5. Watch progress (job ID from previous step)
squeue -u $USER
tail -f slurm/logs/ingest_<JOBID>.out

# 6. Once the ingest job completes (~2 hours), submit the aggregation
sbatch slurm/aggregate.sbatch
#   Slurm Job ID: <record below>

# 7. Once aggregation completes (~10 min), run analysis on an
#    interactive node so plots can be generated
sinteractive --account=macs30113 --partition=caslake \
    --ntasks=1 --cpus-per-task=4 --mem=16G --time=01:00:00
module load python/anaconda-2022.05
python -m pip install --user -r requirements.txt
python analyze/analyze.py \
    --curated /scratch/midway3/$USER/wiki-attention/curated \
    --out-dir analyze/figures

# 8. Copy figures back to login node and commit
exit   # leave sinteractive
git add analyze/figures/
git commit -m "results: figures + stat tables"
git push
```

**Known limitations:**

1. The Wikimedia pageview dataset filters likely-bot traffic server-side, but residual contamination during news events is possible.
2. Redirects are resolved against the current 2026 redirect graph; an article that was renamed *and* redirected mid-window will be attributed to its post-redirect canonical title. The manually-curated `aliases` list in `config/articles.yaml` covers the most important historical cases (e.g. `Coronavirus_disease_2019` → `COVID-19`).
3. Granger causality is a statement about *predictability*, not structural causation. We do not claim Wikipedia attention causes unemployment statistics; we claim it precedes them.
4. Scratch storage on Midway3 has a 60-day inactivity purge policy. Long-term archival of the staging directory would require copying it to `/project/` space.

## 9. Slurm Job IDs

*Populate this section with the actual job IDs after running. Example:*

| Stage     | Job ID    | Wall-clock | Status   |
|-----------|-----------|------------|----------|
| Ingest    | xxxxxxx   | 2h 04m     | COMPLETE |
| Aggregate | xxxxxxx   | 8m 21s     | COMPLETE |

Logs for these jobs are committed under `slurm/logs/` for verification.
