#!/usr/bin/env bash
#
# batch_compute_prob.sh
#
# This script runs compute_mean_prob.py in a loop over different ranges of ensemble members,
# for two specific date slices:
#   1) 1980-01-01 to 2010-12-31
#   2) 2010-01-01 to 2023-12-31

#--------------------------------------
# General user parameters (adjustable):
#--------------------------------------
INPUT_PATH="/pscratch/sd/s/sabbih/aws/vayuh/event-sets/2010-2023/*/*.zarr"
OUTPUT_DIR="/pscratch/sd/s/sabbih/weather_forecasting/event_set/data/test"
START_MEMBER=0   # inclusive
END_MEMBER=999   # inclusive
STEP=100         # step size for each chunk

#---------------------------------------
# We define the two date ranges to run:
#---------------------------------------
DATE_RANGES=(
  "1980-01-01 2010-12-31"
  "2010-01-01 2023-12-31"
)

#---------------------------------------
# Main Loop Over Date Ranges & Ensembles
#---------------------------------------
for RANGE in "${DATE_RANGES[@]}"; do
  # Parse out start_date and end_date (space-delimited)
  # e.g. "1980-01-01" and "2010-12-31"
  START_DATE=$(echo "$RANGE" | awk '{print $1}')
  END_DATE=$(echo   "$RANGE" | awk '{print $2}')
  
  echo "==============================================="
  echo "Running for date range: $START_DATE to $END_DATE"
  echo "==============================================="
  
  # Loop over ensemble ranges in chunks
  for (( i=$START_MEMBER; i<=$END_MEMBER; i+=$STEP )); do
      
      # Calculate the end of the current chunk (j), ensuring we don't exceed END_MEMBER
      j=$((i + STEP - 1))
      if [ $j -gt $END_MEMBER ]; then
          j=$END_MEMBER
      fi
      
      echo "Processing ensemble slice from $i to $j ..."
      
      # Call compute_mean_prob.py with date range and ensemble slice
      python compute_mean_prob.py \
          "$INPUT_PATH" \
          "$OUTPUT_DIR" \
          "$START_DATE" \
          "$END_DATE" \
          --ensemble_start "$i" \
          --ensemble_end "$j"
      
      echo "Done processing ensemble slice $i to $j."
      echo "------------------------------------------"
  done

  echo "Finished all chunks for date range: $START_DATE to $END_DATE"
  echo ""
done

echo "All done!"
