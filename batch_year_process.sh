#!/bin/bash
#
# Usage:
#   bash run.sh 2022 2023
#
# Explanation:
#   For each year passed as an argument (e.g., 2022, 2023):
#     1. Loop over each day from YYYY-01-01 through YYYY-12-31.
#     2. Submit a Slurm job (sbatch) where --start_time and --end_time
#        are set to that specific day.
#     3. The output file/path can also be customized to show each day.

set -e

# ------------------------------
# SET YOUR MAIN PATHS HERE
# ------------------------------
INPUT_BASE="/pscratch/sd/s/sabbih/scs_era5_v1.1/conus"
OUTPUT_BASE="/pscratch/sd/s/sabbih/weather_forcasting/event_set"
LOGNORM_SHAPE_FILE="/pscratch/sd/s/sabbih/weather_forcasting/event_set/lognorm_shape.nc"
LOGNORM_SCALE_FILE="/pscratch/sd/s/sabbih/weather_forcasting/event_set/lognorm_scale.nc"
# ------------------------------

if [ $# -lt 1 ]; then
  echo "Usage: bash $0 <year1> [<year2> ...]"
  exit 1
fi

for YEAR in "$@"; do
  # Define first and last day of the year
  START_OF_YEAR="${YEAR}-01-01"
  END_OF_YEAR="${YEAR}-12-31"

  # Convert to a date-compatible format (ISO 8601) for shell date manipulation
  CURRENT_DATE="${START_OF_YEAR}"

  # Loop through each day of the year
  while [ "$(date -I -d "${CURRENT_DATE}")" != "$(date -I -d "${END_OF_YEAR} + 1 day")" ]; do

    # Each job covers just ONE day, so start_time == end_time
    START_TIME="${CURRENT_DATE}"
    END_TIME="${CURRENT_DATE}"

    # Input and output files for this specific day
    INPUT_DATA_LOCATION="${INPUT_BASE}/${YEAR}/*.nc"
    OUTPUT_LOCATION="${OUTPUT_BASE}/${START_TIME}.zarr"

    # Submit Slurm job
    sbatch <<EOF
#!/bin/bash
#SBATCH -C cpu
#SBATCH -N 1               # number of nodes
#SBATCH -t 00:30:00        # job time
#SBATCH -q debug           # queue or partition
#SBATCH -J eventset_${YEAR}_${CURRENT_DATE}
#SBATCH -o eventset_${YEAR}_${CURRENT_DATE}.%j.out
#SBATCH -e eventset_${YEAR}_${CURRENT_DATE}.%j.err

# module load pytorch/2.3.1
# source activate conda_env

python eventset_generation.py \\
  --start_time ${START_TIME} \\
  --end_time ${END_TIME} \\
  --ensemble_size 1000 \\
  --input_data_location "${INPUT_DATA_LOCATION}" \\
  --output_location "${OUTPUT_LOCATION}" \\
  --lognorm_shape_file "${LOGNORM_SHAPE_FILE}" \\
  --lognorm_scale_file "${LOGNORM_SCALE_FILE}"
EOF

    echo "Submitted job for ${CURRENT_DATE}"

    # Move to the next day
    CURRENT_DATE="$(date -I -d "${CURRENT_DATE} + 1 day")"
  done
done
