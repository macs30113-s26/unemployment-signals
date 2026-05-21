#!/bin/bash
#SBATCH --account=macs30113
#SBATCH --partition=caslake
#SBATCH --job-name=dask_dump
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=00:45:00
#SBATCH --output=logs/aggregate_dump_%j.out
#SBATCH --error=logs/aggregate_dump_%j.err

set -euo pipefail
echo "[sched] scheduler job $SLURM_JOB_ID on $(hostname) at $(date -Iseconds)"

module load python/anaconda-2022.05
cd $HOME/wiki-attention
mkdir -p logs/dask
mkdir -p /scratch/midway3/$USER/dask-scratch

# Distributed aggregation of 2716 hourly dump jsonl.gz files
# (raw bytes streamed during ingest: ~130GB)
python aggregate/aggregate_dask.py \
    --source dump \
    --input-glob "/scratch/midway3/$USER/wiki-attention/staging/year=*/month=*/day=*/hour=*.jsonl.gz" \
    --output-dir "/scratch/midway3/$USER/wiki-attention/curated_dask_dump_slurm/" \
    --cluster slurm \
    --n-workers 4 \
    --cores-per-worker 2 \
    --mem-per-worker 6GB \
    --worker-walltime 00:30:00 \
    --slurm-account macs30113 \
    --slurm-partition caslake

echo "[sched] DONE at $(date -Iseconds)"
