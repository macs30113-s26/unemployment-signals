# 4-Day Execution Runbook (AWS Console version)

This is the operator manual — what to click, in what order, to take this repo
from "code on disk" to "all figures generated and committed". Optimised for the
AWS Academy Learner Lab UI (which is the standard AWS Console with a few
restrictions: only `LabRole`, region `us-east-1`, four-hour session timeout).

**Total elapsed wall-clock: ~4 days, ~16 hours of active work.**
**Estimated total spend: ~$8–10 of the $30 budget.**

---

## Pre-flight (do this first, takes 15 min)

1. Open the GitHub Classroom link from the assignment, create your team
   ("Final-Project-YourName"), accept. The repo is now created and private.
2. `git clone` the empty repo locally. Copy everything from this directory
   in. First commit: "scaffold: project skeleton + README + Lambda code".
   Push.
3. **Pick a globally unique S3 bucket name** that nobody else has. Example:
   `lsc4ss-wiki-attention-yourcnetid-2026`. Write it down.

---

## Day 1 — Ingest pipeline live

### Morning (2–3 h): set up AWS resources

1. Open AWS Academy → **Modules** → your Learner Lab → **Start Lab**. Wait
   for the green dot.
2. Click **AWS** to open the Console.
3. In the Console, top-right region picker: **N. Virginia (us-east-1)**.
   Stay in this region for everything — cross-region transfer costs money.

#### Option A — use the setup script (recommended, 5 min)

In the lab page, click **AWS Details** → **AWS CLI** → copy the three lines
of credentials → paste into `~/.aws/credentials` on your laptop. Then:

```bash
cd wikipedia_attention_project
export BUCKET=lsc4ss-wiki-attention-yourcnetid-2026
bash setup_aws.sh
```

This creates the bucket, uploads `articles.yaml`, builds the Lambda zip, and
creates the Lambda function. All idempotent, safe to re-run.

#### Option B — click through the Console (slower, but verifiable for screenshots)

1. **S3** → Create bucket: name as above, region us-east-1, defaults
   otherwise.
2. In your new bucket → Upload `config/articles.yaml` to prefix `config/`.
3. On your laptop: `bash ingest/build_lambda_zip.sh` → produces
   `ingest/build/pageview_ingest.zip`.
4. **Lambda** → Create function → "Author from scratch":
   - Name: `pageview-ingest`
   - Runtime: Python 3.11
   - Architecture: x86_64
   - Permissions → Use existing role → `LabRole`
   - Create function.
5. On the function page:
   - Code → Upload from → .zip → choose `pageview_ingest.zip` → Save.
   - Configuration → General → Edit:
     - Memory: 1024 MB
     - Timeout: 5 min 0 sec
   - Configuration → Environment variables → Edit → add:
     - `BUCKET` = your bucket name
     - `STAGING_PREFIX` = `staging`
     - `ARTICLES_KEY` = `config/articles.yaml`

### Mid-morning (30 min): smoke-test ONE invocation

In the Lambda console → Test tab → Create new test event:

```json
{"year": 2024, "month": 1, "day": 15, "hour": 12}
```

Click **Test**. Expected: 3–10 seconds, response shape:

```json
{
  "ok": true,
  "url": "https://dumps.wikimedia.org/...",
  "s3_key": "staging/year=2024/month=01/day=15/hour=12.jsonl.gz",
  "match_count": 28,
  "elapsed_s": 3.4
}
```

If `match_count` is 0: your `articles.yaml` titles don't match what's in the
dump for that hour — go to the Wikipedia article in your browser, read the
exact URL path, and update `aliases:` accordingly.

**Screenshot moment #1:** Lambda console showing the green "Execution result:
succeeded" panel + the duration. Save to `screenshots/01_lambda_test.png`.

### Afternoon (2–3 h): full ingest

On your laptop (or a t3.micro EC2 — see notes), with the same lab creds:

```bash
python ingest/orchestrator.py \
    --start 2019-01-01 --end 2026-05-01 \
    --bucket $BUCKET --concurrency 40
```

This dispatches ~65,000 async Lambda invocations across 2–3 rounds, healing
S3 gaps between rounds. Expected wall-clock: 60–90 minutes total. **Don't
close the lab session in the middle** — if you must, you can always re-run
the orchestrator and it will only invoke for missing hours.

While it's running, open Lambda → Monitor tab → Metrics. You'll see:
- ConcurrentExecutions chart spiking to ~40
- Invocations counter climbing toward 65,000
- Error rate, hopefully < 0.1%

**Screenshot moment #2:** CloudWatch chart showing the concurrency spike.
Save as `screenshots/02_lambda_concurrency.png`.

**Screenshot moment #3:** S3 bucket → `staging/` prefix → "Calculate total
size" button. Save as `screenshots/03_staging_size.png`. You should see
~65,000 objects, ~20 MB total. The 100,000× compression from raw is one of
your README talking points.

Commit progress to GitHub at end of day. Stop the lab.

---

## Day 2 — Spark aggregation + Athena

### Morning (1 h): start the analysis EC2

1. Start Lab. Open Console → **EC2** → Launch instance.
2. Name: `pageview-spark`
3. AMI: Amazon Linux 2023 (default)
4. Instance type: `m5.2xlarge` (8 vCPU / 32 GB). Cost ≈ $0.38/hour, so
   we'll terminate within 2 hours.
5. Key pair: create or use existing. *Download the .pem!*
6. Network: defaults (default VPC, public IP enabled, default SG).
7. **Storage: 30 GB gp3.** Default 8 GB is too small for Spark + JDK.
8. **Advanced details → IAM instance profile: `LabInstanceProfile`** (this
   is the one the Lab gives EC2 instances; gives the box S3 + Athena access).
9. Launch.

SSH in (from your laptop):

```bash
chmod 600 ~/Downloads/your-key.pem
ssh -i ~/Downloads/your-key.pem ec2-user@<public-ip>
```

On the box:

```bash
sudo dnf install -y java-17-amazon-corretto python3.11 python3.11-pip git
python3.11 -m pip install --user pyspark==3.5.1 pyarrow==15.0.2 pandas==2.2.2

# Hadoop AWS jars for s3a://
mkdir -p ~/spark-jars && cd ~/spark-jars
curl -sLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -sLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar

# Clone your repo
cd ~
git clone https://github.com/lsc4ss-s26/final-project-yourteam.git proj
cd proj
```

### Midday (15 min): run the Spark job

```bash
export BUCKET=lsc4ss-wiki-attention-yourcnetid-2026

spark-submit \
    --jars ~/spark-jars/hadoop-aws-3.3.4.jar,~/spark-jars/aws-java-sdk-bundle-1.12.367.jar \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
    aggregate/aggregate_pageviews.py \
    --bucket $BUCKET
```

Should take 5–15 minutes. Watch for:
- "Staging row count: ..."  (sanity check: should be in the 50k–500k range)
- "Per-group totals:" prints a 4-row table
- Three "Writing s3a://.../curated/..." lines

**Screenshot moment #4:** SSH terminal showing the row counts + per-group
totals. Save as `screenshots/04_spark_output.png`.

### Afternoon (1 h): Athena setup

Back in the Console → **Athena**:

1. Settings → Query result location → `s3://your-bucket/athena-results/`.
2. Query editor → open `aggregate/athena_ddl.sql` in your repo, replace
   `REPLACE_ME_BUCKET` with your bucket, paste statement by statement.
   Run each. The three `MSCK REPAIR TABLE` lines must succeed.
3. Run the first example query in the SQL file. You should get a tidy table
   of weekly per-group totals.

**Screenshot moment #5:** Athena query result panel.
Save as `screenshots/05_athena_query.png`.

### **Stop the m5.2xlarge.** Critical — EC2 → Instances → select →
Instance state → **Stop** (NOT terminate; we'll restart it briefly tomorrow).
Or just terminate it — Spark is done.

---

## Day 3 — Analysis + figures

### Morning (3–4 h): run analysis on a cheap box

1. Launch a fresh **t3.medium** (or restart yesterday's box at a smaller
   size if you can). Same setup as before but smaller.
2. SSH in. Set up venv:
   ```bash
   python3.11 -m pip install --user -r ~/proj/requirements_analysis.txt
   ```
3. Run analysis:
   ```bash
   cd ~/proj
   python3.11 analyze/analyze.py \
       --athena-output s3://$BUCKET/athena-results/ \
       --out-dir analyze/figures/
   ```
4. `scp` figures back to your laptop:
   ```bash
   # from your laptop
   scp -i ~/Downloads/your-key.pem -r \
       ec2-user@<ip>:proj/analyze/figures/ ./analyze/
   ```

### Afternoon (2 h): iterate on figures

Open the PNGs. If anything looks weird (mostly: empty groups, missing
articles, weird timestamps), go back to `articles.yaml`, fix, re-run
just the aggregate step from yesterday's snapshot.

Commit figures + CSV outputs to GitHub.

---

## Day 4 — README polish + submission

### Morning (3 h)

1. Re-read the README with fresh eyes. The "Findings preview" section
   needs your actual numbers from `granger_results.csv` and
   `changepoints.csv` — replace the placeholder text.
2. Open `docs/architecture.svg` in any browser, screenshot it, save as
   `docs/architecture.png`. Or import the SVG into draw.io and export.
3. Make sure all the screenshots from Days 1–3 are committed.
4. Final cost screenshot: in the Learner Lab page, **AWS Details** → screenshot
   of the **Used $X.XX / $100** indicator. Save as
   `screenshots/06_lab_cost.png`.

### Afternoon (1 h): submit

1. One last end-to-end commit: "final: figures + screenshots + README
   findings".
2. Open the GitHub repo settings; verify it's accessible to the course staff
   (the Classroom link should have set that automatically).
3. Submit the repo URL to Canvas. Done.

---

## Cost watchpoints

| Service | Worst-case spend | Typical |
|---|---|---|
| Lambda (~65k invocations, ~3s avg, 1 GB) | $0.65 | $0.50 |
| S3 storage (1 month, ~30 MB) | $0.01 | $0.01 |
| S3 requests (PUT × 65k + GET × 130k) | $0.40 | $0.40 |
| EC2 m5.2xlarge × 2 h | $0.80 | $0.50 |
| EC2 t3.medium × 8 h | $0.33 | $0.20 |
| Athena (~5 GB scanned across 30 queries) | $0.03 | $0.02 |
| Data transfer out (figures, etc., < 1 GB) | $0.09 | $0.05 |
| **Total** | **~$2.30** | **~$1.70** |

The cushion vs your $30 budget is huge. The main risk to your budget is
**leaving an m5.2xlarge running overnight** — that's $9.12 wasted. Always
Stop or Terminate when done.

## When things go wrong

| Symptom | Cause | Fix |
|---|---|---|
| Lambda test returns `match_count: 0` | Article title in `articles.yaml` doesn't match dump | Check Wikipedia URL, update `aliases` |
| `403 Forbidden` from Wikimedia | Rare; rate-limited | Re-run orchestrator, the round-2 pass will fill gaps |
| Spark job: `ClassNotFoundException S3AFileSystem` | Missing hadoop-aws jar | Re-check the `--jars` paths |
| Athena: "Table not found" | Forgot `MSCK REPAIR TABLE` | Re-run the DDL bottom three lines |
| Athena: "0 results" | Curated tables empty | Check Spark job actually wrote — `aws s3 ls s3://$BUCKET/curated/` |
| `analyze.py` Granger test errors out | Series has too many zeros | Drop the group from the table or use `+1` smoothing |
