#!/bin/bash
#SBATCH --job-name=wiki-ingest
#SBATCH --partition=caslake
#SBATCH --account=macs30113
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=04:00:00
#SBATCH --output=slurm/logs/ingest_%j.out
#SBATCH --error=slurm/logs/ingest_%j.err
#
# Wikipedia pageview ingest — Layer 1 of the pipeline.
#
# Submits one job that uses all 28 CPU cores on a caslake node to fan out
# parallel downloads of the Wikimedia hourly dumps. ETA for the full
# 2019-2026 window: ~2 hours of wall-clock.
#
# Resource sizing rationale:
#   - cpus-per-task=28: caslake nodes have 28 physical cores. We claim all
#     of them and let multiprocessing.Pool saturate the box.
#   - mem=32G: each worker peaks at ~300 MB (one decompressed hourly file).
#     28 × 300 MB = 8.4 GB working set; 32 GB is generous and includes
#     Python overhead + filesystem cache.
#   - time=04:00:00: 4 hours gives plenty of buffer over the ~2-hour
#     expected runtime. Single-job Slurm wall-clock limit on caslake is
#     36h, so this is well under.
#
# Account: macs30113 is the course's allocation. If you don't have access,
# replace with your PI account or use `--account=pi-<youraccount>`.
#
# Usage:
#   cd ~/wiki-attention
#   sbatch slurm/ingest.sbatch
#   squeue -u $USER       # check job status
#   tail -f slurm/logs/ingest_<JOBID>.out
#
# Output goes to /scratch/midway3/$USER/wiki-attention/staging/.

set -euo pipefail

echo "===== job started $(date) ====="
echo "SLURM_JOB_ID:        $SLURM_JOB_ID"
echo "SLURM_JOB_NODELIST:  $SLURM_JOB_NODELIST"
echo "SLURM_CPUS_PER_TASK: $SLURM_CPUS_PER_TASK"
echo "host:                $(hostname)"
echo "cwd:                 $(pwd)"
echo ""

# Load the RCC-provided Python module. This gives us python 3.9 with
# numpy/pandas/etc preinstalled. We only need pyyaml from outside the
# stdlib, which we'll install with --user if not already present.
module load python/anaconda-2022.05
echo "python: $(which python)"
python --version

# Make sure pyyaml is available. If your home directory already has it,
# this is a no-op. We pip --user install so it doesn't pollute the module's
# environment.
python -m pip install --quiet --user pyyaml || {
    echo "WARN: pip install pyyaml failed; assuming module already provides it"
}

# Scratch path. Midway3 scratch is the high-performance temporary storage
# (lustre-backed). Files there are auto-purged after ~60 days of inactivity,
# which is fine for our analysis window.
STAGING_DIR="/scratch/midway3/$USER/wiki-attention/staging"
mkdir -p "$STAGING_DIR"
echo "staging dir: $STAGING_DIR"
echo ""

# Run the ingest. Pass --workers=28 to match SLURM_CPUS_PER_TASK so we
# saturate but don't oversubscribe the node.
python ingest/ingest_pageviews.py \
    --start 2019-01-01 \
    --end   2026-05-01 \
    --staging "$STAGING_DIR" \
    --articles config/articles.yaml \
    --workers 4

echo ""
echo "===== job ended $(date) ====="
echo "Final staging count:"
find "$STAGING_DIR" -type f -name '*.jsonl.gz' | wc -l
echo "Final staging size:"
du -sh "$STAGING_DIR"
