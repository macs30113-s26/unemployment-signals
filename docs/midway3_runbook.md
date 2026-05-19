# Midway3 Quick-Start Runbook

Step-by-step instructions to bring up the pipeline on Midway3.

## Day 1 — Setup + smoke test (~30 min active work)

### 1. SSH into Midway3 from your Windows machine

PowerShell:
```powershell
ssh CNETID@midway3.rcc.uchicago.edu
```

You'll be asked for your UChicago password and then prompted to approve a
Duo Push on your phone. After that you should land at a prompt like
`[CNETID@midway3-login3 ~]$`.

### 2. Find which Slurm account you have access to

```bash
rcchelp balance
```

You should see something like `macs30113` listed with a positive balance.
**Write down the account name** — you'll need it in the sbatch files.

If you don't see any accounts with positive balance, email
`help@rcc.uchicago.edu` mentioning you're enrolled in MACS 30113.

### 3. Clone the repo

```bash
cd ~
git clone https://github.com/macs30113-s26/final-project-unemployment-signals.git
cd final-project-unemployment-signals
```

### 4. Set up Python env (one-time, ~2 min)

```bash
module load python/anaconda-2022.05
python -m pip install --user pyyaml
```

Verify:
```bash
python -c "import yaml; print('pyyaml OK')"
```

### 5. Edit the sbatch files to use YOUR account

If your account name is `macs30113`, you're set. Otherwise:

```bash
# Replace 'macs30113' everywhere with your actual account name
sed -i 's/macs30113/<YOUR-ACCOUNT>/g' slurm/ingest.sbatch slurm/aggregate.sbatch
```

### 6. Smoke-test the ingest on a tiny window (~2 minutes)

Don't run the full 7-year job blindly. First confirm one hour works:

```bash
# Start an interactive node for testing
sinteractive --account=macs30113 --partition=caslake \
    --ntasks=1 --cpus-per-task=4 --mem=8G --time=00:30:00

# Once on the compute node:
module load python/anaconda-2022.05
mkdir -p /scratch/midway3/$USER/wiki-attention/staging

# Run only the first 4 hours of 2024 (4 files, ~30 seconds)
python ingest/ingest_pageviews.py \
    --start 2024-01-01 --end 2026-05-01 \
    --staging /scratch/midway3/$USER/wiki-attention/staging \
    --articles config/articles.yaml \
    --workers 4 \
    --max-targets 4
```

Expected output:
```
2026-... [INFO] window: 2024-01-01 ... 2026-05-01, 4 hourly targets, 4 workers
2026-... [INFO] staging dir: /scratch/midway3/<you>/wiki-attention/staging
2026-... [INFO] DONE in 0.1min: 4 done, 0 skipped, 0 failed, ~220 total matches
```

Check that the output files were written:
```bash
find /scratch/midway3/$USER/wiki-attention/staging -type f
# should show 4 jsonl.gz files

zcat /scratch/midway3/$USER/wiki-attention/staging/year=2024/month=01/day=01/hour=00.jsonl.gz | head -3
# should show JSON lines like:
#   {"domain": "en", "raw_title": "Unemployment", "canonical": "Unemployment", ...}
```

If both checks pass, exit the sinteractive session:
```bash
exit   # back to login node
```

### 7. Submit the full ingest job (background, ~2 hours)

```bash
mkdir -p slurm/logs
sbatch slurm/ingest.sbatch
# Output: "Submitted batch job 1234567"
```

Record that Job ID. Check status with:
```bash
squeue -u $USER
```

While the job runs you can disconnect from SSH — Slurm keeps the job
running on the compute node.

### 8. Wait for ingest to finish, then submit aggregation

Check status periodically:
```bash
ssh CNETID@midway3.rcc.uchicago.edu
cd ~/final-project-unemployment-signals
squeue -u $USER
# If your ingest job no longer appears, it has completed (successfully or not).

# Look at the log
tail -100 slurm/logs/ingest_<JOBID>.out

# If it ended cleanly ("DONE in ..."), submit aggregation
sbatch slurm/aggregate.sbatch
```

### 9. Run analysis on a sinteractive node (~10 min)

```bash
sinteractive --account=macs30113 --partition=caslake \
    --ntasks=1 --cpus-per-task=4 --mem=16G --time=01:00:00

# On the compute node:
module load python/anaconda-2022.05
python -m pip install --user -r requirements.txt
mkdir -p analyze/figures
python analyze/analyze.py \
    --curated /scratch/midway3/$USER/wiki-attention/curated \
    --out-dir analyze/figures

# Expected: figures/01_attention_by_group.png, 02_attention_vs_unrate.png,
#           granger_results.csv, changepoints.csv
ls -la analyze/figures/
```

### 10. Commit + push results

```bash
exit   # back to login node
cd ~/final-project-unemployment-signals

# Add Slurm logs as evidence of pipeline runs
git add slurm/logs/

# Add figures and result CSVs
git add analyze/figures/

# Update README §9 with Job IDs from your runs
# (open README.md in nano or vim, fill in the table)
nano README.md

git add README.md
git commit -m "results: ingest, aggregation, analysis complete"
git push
```

## Common gotchas

| Symptom | Cause | Fix |
|---|---|---|
| `module: command not found` | First-time login | `source /etc/profile` or just log out and back in |
| `sbatch: error: invalid account` | Wrong account in sbatch | Run `rcchelp balance`, edit sbatch files |
| Job stays in PD (pending) state | Cluster busy | Be patient (or reduce --time / --cpus-per-task) |
| Ingest fails with HTTPSConnectionPool | Wikimedia rate limit | Reduce --workers from 28 to ~16 and resubmit |
| Spark OOM | Driver memory too small | Bump `spark.driver.memory` in `aggregate_pageviews.py` to 32g |
| `module load spark` fails | No Spark module on your build | Script falls back to pip-installed pyspark — should still work |
