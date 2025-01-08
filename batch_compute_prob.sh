#!/usr/bin/env bash
#
# batch_compute_prob_slurm.sh
#
# This script loops over each year from 1980 to 2023
# and submits a separate Slurm job for that year.
# The ensemble range is set to 0–999, with no chunk looping.

# ----------------------------
# 1) General user parameters:
# ----------------------------
INPUT_PATH="/fsx/vayu/results/event_set/*.zarr"
OUTPUT_DIR="/pscratch/sd/s/sabbih/weather_forecasting/event_set/data/test"
YEAR_START=1980
YEAR_END=2023

# ----------------------------
# 2) Loop over each year:
# ----------------------------
for YEAR in $(seq $YEAR_START $YEAR_END); do

  echo "Submitting job for year: $YEAR"

  # Submit a Slurm job using a here-doc (EOF)
  sbatch <<EOF
#!/bin/bash
#SBATCH --partition=hpc7a
#SBATCH --nodes=1
#SBATCH --constraint=hpc7a-96xlarge
#SBATCH -J eventset_${YEAR}
#SBATCH -o eventset_${YEAR}.%j.out
#SBATCH -e eventset_${YEAR}.%j.err
#SBATCH --mem=65G
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=8

# Load modules and environment
module purge
module avail

# Conda environment setup
export CONDA_DIR="/fsx/vayu/conda-install/miniforge3"
export PATH=\${CONDA_DIR}/bin:\${PATH}
conda_env="hails"
CONDA_ENV_DIR=\${CONDA_DIR}/envs/\${conda_env}

# Print environment info for debugging
echo "OUTPUT_DIR: ${OUTPUT_DIR}"
echo "Running on year: $YEAR"
echo "Slurm Job ID: \${SLURM_JOB_ID}"
echo "Node List: \${SLURM_NODELIST}"
echo "Number of Nodes: \${SLURM_NNODES}"
echo "CPUs per Task: \${SLURM_CPUS_PER_TASK}"

# Activate conda
source \${CONDA_DIR}/bin/activate \${CONDA_ENV_DIR}
conda env list

# -------------------------
# Run the Python script:
# -------------------------
srun python compute_mean_prob.py \\
    "$INPUT_PATH" \\
    "$OUTPUT_DIR" \\
    "${YEAR}-01-01" \\
    "${YEAR}-12-31" \\
    --ensemble_start 0 \\
    --ensemble_end 999

EOF

done

echo "All jobs submitted!"
