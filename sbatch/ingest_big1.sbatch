#!/bin/bash
#SBATCH --account=macs30113
#SBATCH --partition=caslake
#SBATCH --job-name=wiki_big1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=04:00:00
#SBATCH --output=logs/ingest_big1_%j.out
#SBATCH --error=logs/ingest_big1_%j.err

set -euo pipefail
echo "[big1] job $SLURM_JOB_ID on $(hostname) at $(date -Iseconds)"
echo "[big1] target: 14 quarterly weeks, 2019-Q1..2022-Q2 (Jan/Apr/Jul/Oct first weeks)"

module load python/anaconda-2022.05
cd $HOME/wiki-attention
mkdir -p logs

STAGING=/scratch/midway3/$USER/wiki-attention/staging
WORKERS=4

# 14 windows = 7 days each. Pull pattern: first week of Jan/Apr/Jul/Oct per year.
# This gives even coverage 2019-2022 first half, ~2352 hourly files total.
WINDOWS=(
    "2019-01-01 2019-01-08"
    "2019-04-01 2019-04-08"
    "2019-07-01 2019-07-08"
    "2019-10-01 2019-10-08"
    "2020-01-01 2020-01-08"
    "2020-04-01 2020-04-08"
    "2020-07-01 2020-07-08"
    "2020-10-01 2020-10-08"
    "2021-01-01 2021-01-08"
    "2021-04-01 2021-04-08"
    "2021-07-01 2021-07-08"
    "2021-10-01 2021-10-08"
    "2022-01-01 2022-01-08"
    "2022-04-01 2022-04-08"
)

t_start=$(date +%s)
pre_count=$(find $STAGING -name '*.jsonl.gz' 2>/dev/null | wc -l)
echo "[big1] pre-run dump file count: $pre_count"

# Loop sequentially. If any window fails (non-zero exit), continue with the rest
# but record the failure. This is more robust than --start...--end across full
# range because: (a) cache hits early are quick (skipped), (b) one window
# failure doesn't poison the whole job.
n_ok=0
n_fail=0
for w in "${WINDOWS[@]}"; do
    read -r start end <<< "$w"
    echo "[big1] === window $start .. $end ==="
    if python ingest/ingest_pageviews.py \
        --start "$start" \
        --end "$end" \
        --staging "$STAGING" \
        --workers $WORKERS \
        --log-interval 30 ; then
        n_ok=$((n_ok+1))
    else
        n_fail=$((n_fail+1))
        echo "[big1] WINDOW FAILED: $start .. $end"
    fi
    elapsed=$(( $(date +%s) - t_start ))
    cur_count=$(find $STAGING -name '*.jsonl.gz' 2>/dev/null | wc -l)
    echo "[big1] cumulative: ${elapsed}s elapsed, ${cur_count} files staged"
done

t_end=$(date +%s)
post_count=$(find $STAGING -name '*.jsonl.gz' 2>/dev/null | wc -l)
echo "[big1] DONE at $(date -Iseconds)"
echo "[big1] total wall: $((t_end - t_start))s"
echo "[big1] windows ok: $n_ok / $((n_ok + n_fail))"
echo "[big1] file count: $pre_count -> $post_count (delta: $((post_count - pre_count)))"
echo "[big1] staging size: $(du -sh $STAGING | awk '{print $1}')"
