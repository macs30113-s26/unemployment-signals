#!/bin/bash
#SBATCH --account=macs30113
#SBATCH --partition=caslake
#SBATCH --job-name=dask_agg
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=00:30:00
#SBATCH --output=logs/aggregate_dask_%j.out
#SBATCH --error=logs/aggregate_dask_%j.err

set -euo pipefail
echo "[sched] scheduler job $SLURM_JOB_ID on $(hostname) at $(date -Iseconds)"

module load python/anaconda-2022.05

cd $HOME/wiki-attention
mkdir -p logs/dask
mkdir -p /scratch/midway3/$USER/dask-scratch

# Run aggregator with SLURMCluster: this process is the Dask scheduler;
# it submits 4 separate worker jobs to Slurm via dask-jobqueue.
# Expected: squeue shows this job + 4 dask-worker jobs simultaneously.
python aggregate/aggregate_dask.py \
    --source rest \
    --input-glob "/scratch/midway3/$USER/wiki-attention/staging_rest/*.jsonl.gz" \
    --output-dir "/scratch/midway3/$USER/wiki-attention/curated_dask_slurm/" \
    --cluster slurm \
    --n-workers 4 \
    --cores-per-worker 2 \
    --mem-per-worker 6GB \
    --worker-walltime 00:30:00 \
    --slurm-account macs30113 \
    --slurm-partition caslake

echo "[sched] DONE at $(date -Iseconds)"
