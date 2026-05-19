#!/bin/bash
#SBATCH --job-name=wiki-aggregate
#SBATCH --partition=caslake
#SBATCH --account=macs30113
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=28
#SBATCH --mem=64G
#SBATCH --time=01:00:00
#SBATCH --output=slurm/logs/aggregate_%j.out
#SBATCH --error=slurm/logs/aggregate_%j.err
#
# PySpark aggregation — Layer 2 of the pipeline.
#
# Reads ~65k staging JSONL.gz files written by the ingest layer, applies
# multi-grain rollups (daily/weekly/monthly), and writes year-partitioned
# Snappy-Parquet to scratch. ETA: 5-15 minutes wall-clock.
#
# Resource sizing rationale:
#   - cpus-per-task=28: same caslake-node sizing as ingest. Spark `local[*]`
#     will use all 28 cores for its parallel partitions.
#   - mem=64G: Spark needs more memory than pure Python because it caches
#     a working set of decompressed JSONL during the aggregation shuffle.
#     64 GB is generous; you could likely run with 32 GB if quota matters.
#   - time=01:00:00: aggregation usually finishes in <15 min. 1h is buffer.
#
# Submit:
#   sbatch slurm/aggregate.sbatch
#
# This job must be run AFTER ingest.sbatch has fully completed.

set -euo pipefail

echo "===== job started $(date) ====="
echo "SLURM_JOB_ID: $SLURM_JOB_ID"
echo "host:         $(hostname)"
echo ""

# Load Python + a Spark module. RCC provides Spark as a module on Midway3.
# If `spark/3.5` isn't available on your particular Midway3 build, try
# `module avail spark` to find the correct name.
module load python/anaconda-2022.05
module load spark/3.5.0 || module load spark/3.3 || {
    # Fallback: pip-install pyspark on the fly into user packages
    echo "no Spark module found; using pip-installed pyspark"
    python -m pip install --quiet --user pyspark==3.5.1
}

echo "python: $(which python)"
which spark-submit 2>/dev/null || echo "no spark-submit binary; using python -m pyspark"

STAGING_DIR="/scratch/midway3/$USER/wiki-attention/staging_rest"
CURATED_DIR="/scratch/midway3/$USER/wiki-attention/curated"
mkdir -p "$CURATED_DIR"

echo "staging:  $STAGING_DIR  ($(find $STAGING_DIR -type f | wc -l) files)"
echo "curated:  $CURATED_DIR"
echo ""

# Run via spark-submit if available, otherwise fall back to plain python
# (which will use the pip-installed pyspark module).
if command -v spark-submit >/dev/null 2>&1; then
    spark-submit \
        --master "local[$SLURM_CPUS_PER_TASK]" \
        --driver-memory 16g \
        aggregate/aggregate_pageviews.py \
        --staging "$STAGING_DIR" \
        --curated "$CURATED_DIR"
else
    python aggregate/aggregate_pageviews.py \
        --staging "$STAGING_DIR" \
        --curated "$CURATED_DIR"
fi

echo ""
echo "===== job ended $(date) ====="
echo "Curated output:"
find "$CURATED_DIR" -type d | sort
du -sh "$CURATED_DIR"
