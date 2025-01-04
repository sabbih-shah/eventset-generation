#!/usr/bin/env bash
#
# batch_compute_prob.sh
#
# This script runs compute_mean_prob.py in a loop over different ranges of ensemble members.

#------------------
# User parameters:
#------------------
INPUT_PATH="/pscratch/sd/s/sabbih/aws/vayuh/event-sets/2010-2023/*/*.zarr"
OUTPUT_DIR="/path/to/output/dir"
START_DATE="2010"
END_DATE="2023"
START_MEMBER=0    # inclusive
END_MEMBER=999    # inclusive
STEP=100          # step size for each chunk

#------------------
# Loop in steps
#------------------
# i goes from START_MEMBER to END_MEMBER in increments of STEP
for (( i=$START_MEMBER; i<=$END_MEMBER; i+=$STEP )); do
    
    # Calculate the end of the current chunk (j), but make sure j does not exceed END_MEMBER
    j=$((i + STEP - 1))
    if [ $j -gt $END_MEMBER ]; then
        j=$END_MEMBER
    fi
    
    echo "Processing ensemble slice from $i to $j ..."
    # we can add another .sh script to batch submit to slurm
    python compute_mean_prob.py \
        "$INPUT_PATH" \
        "$OUTPUT_DIR" \
        --ensemble_start "$i" \
        --ensemble_end "$j"
    
    echo "Done processing ensemble slice from $i to $j."
    echo "------------------------------------------"
done

echo "All done!"
