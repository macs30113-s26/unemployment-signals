#!/bin/bash
#SBATCH --job-name=wiki-rest
#SBATCH --partition=caslake
#SBATCH --account=macs30113
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=6
#SBATCH --mem=4G
#SBATCH --time=00:30:00
#SBATCH --output=slurm/logs/rest_%j.out
#SBATCH --error=slurm/logs/rest_%j.err
#
# Wikipedia REST API ingest — Layer 1 primary path.
#
# Pulls per-article daily pageview aggregates for all 31 curated articles
# from the Wikimedia REST API. ~30 HTTP requests total, completes in
# 1-3 minutes wall-clock.
#
# Output goes to /scratch/midway3/$USER/wiki-attention/staging_rest/

set -euo pipefail

echo "===== job started $(date) ====="
echo "SLURM_JOB_ID: $SLURM_JOB_ID"
echo "host:         $(hostname)"
echo ""

module load python/anaconda-2022.05
python -m pip install --quiet --user pyyaml || true

OUT_DIR="/scratch/midway3/$USER/wiki-attention/staging_rest"
mkdir -p "$OUT_DIR"
echo "output dir: $OUT_DIR"
echo ""

python ingest/ingest_pageviews_rest.py \
    --start 2019-01-01 \
    --end   2026-05-01 \
    --output "$OUT_DIR" \
    --articles config/articles.yaml \
    --workers 6

echo ""
echo "===== job ended $(date) ====="
echo "Files produced:"
ls -la "$OUT_DIR" | head
echo ""
echo "Total day-records across all articles:"
for f in "$OUT_DIR"/*.jsonl.gz; do zcat "$f" | wc -l; done | paste -sd+ | bc
