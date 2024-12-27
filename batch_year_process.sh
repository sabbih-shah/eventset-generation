#!/bin/bash
#
# Usage:
#   bash run.sh 2022 2023 2024
#
# Explanation:
#   This script loops over all year arguments provided:
#   1. Sets start and end times for each year
#   2. Adjusts output directory names for each year
#   3. Updates the --input_data_location to include the given year
#   4. Submits a Slurm job (sbatch) running eventset_generation.py

# Exit script on any error
set -e

# Make sure at least one year argument is supplied
if [ $# -lt 1 ]; then
  echo "Usage: bash $0 <year1> [<year2> <year3> ...]"
  exit 1
fi

# Loop over each year provided as a command-line argument
for YEAR in "$@"; do

  # Example: use the first few days of the year in your example
  START_TIME="${YEAR}-01-01"
  END_TIME="${YEAR}-01-02"

  # Adjust these if you want a different date range, e.g., entire year or a particular window
  # START_TIME="${YEAR}-01-01"
  # END_TIME="${YEAR}-12-31"

  # Input data for this year
  INPUT_DATA_LOCATION="/pscratch/sd/s/sabbih/scs_era5_v1.1/conus/${YEAR}/*.nc"

  # Output location using the same year in the file name
  OUTPUT_LOCATION="/pscratch/sd/s/sabbih/weather_forcasting/event_set/${YEAR}-01-01_${YEAR}-01-07.zarr"

  # Here we use a HERE-doc to submit the Slurm job
  sbatch <<EOF
#!/bin/bash
#SBATCH -C cpu
#SBATCH -N 1               # number of nodes
#SBATCH -t 00:30:00        # job time, adjust as needed
#SBATCH -q debug           # queue/partition to submit to
#SBATCH -J eventset_${YEAR}
#SBATCH -o eventset_${YEAR}.%j.out  # standard output file
#SBATCH -e eventset_${YEAR}.%j.err  # standard error file

# Load any required modules if needed
# module load python/XYZ

python eventset_generation.py \\
  --start_time ${START_TIME} \\
  --end_time ${END_TIME} \\
  --ensemble_size 1000 \\
  --input_data_location "${INPUT_DATA_LOCATION}" \\
  --output_location "${OUTPUT_LOCATION}" \\
  --lognorm_shape_file "/pscratch/sd/s/sabbih/weather_forcasting/event_set/lognorm_shape.nc" \\
  --lognorm_scale_file "/pscratch/sd/s/sabbih/weather_forcasting/event_set/lognorm_scale.nc"

EOF

  echo "Submitted job for year: ${YEAR}"
done
