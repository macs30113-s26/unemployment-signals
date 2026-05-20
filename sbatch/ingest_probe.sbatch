#!/bin/bash
#SBATCH --account=macs30113
#SBATCH --partition=caslake
#SBATCH --job-name=wiki_probe
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=01:15:00
#SBATCH --output=logs/ingest_probe_%j.out
#SBATCH --error=logs/ingest_probe_%j.err

set -euo pipefail
echo "[probe] job $SLURM_JOB_ID started at $(date -Iseconds) on $(hostname)"
echo "[probe] account=$SLURM_JOB_ACCOUNT partition=$SLURM_JOB_PARTITION cpus=$SLURM_CPUS_PER_TASK"

module load python/anaconda-2022.05

cd $HOME/wiki-attention
mkdir -p logs

# Window: 2023-04-01 .. 2023-04-09 (left-closed/right-open) => 8 days = 192 hourly files.
# Chosen because:
#  (1) post-ChatGPT (2022-11-30), inside §5.5 robustness window;
#  (2) probes Wikimedia rate-limit empirically before committing larger jobs;
#  (3) existing 353 dump files are scattered samples; this gives 192 *contiguous* hours.
START=2023-04-01
END=2023-04-09
STAGING=/scratch/midway3/$USER/wiki-attention/staging
WORKERS=4

echo "[probe] window: $START .. $END (exclusive), workers=$WORKERS"
echo "[probe] staging: $STAGING"
echo "[probe] pre-run dump file count: $(find $STAGING -name '*.jsonl.gz' 2>/dev/null | wc -l)"

t0=$(date +%s)
python ingest/ingest_pageviews.py \
    --start "$START" \
    --end "$END" \
    --staging "$STAGING" \
    --workers $WORKERS \
    --log-interval 20
rc=$?
t1=$(date +%s)

echo "[probe] ingester exit code: $rc"
echo "[probe] wall time: $((t1 - t0))s"
echo "[probe] post-run dump file count: $(find $STAGING -name '*.jsonl.gz' 2>/dev/null | wc -l)"
echo "[probe] post-run dump total size: $(du -sh $STAGING | awk '{print $1}')"
echo "[probe] files in target window 2023-04:"
find $STAGING/year=2023/month=04 -name '*.jsonl.gz' 2>/dev/null | wc -l

echo "[probe] DONE at $(date -Iseconds)"
exit $rc
