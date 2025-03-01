# Hail Magnitude Sampling

This repository contains scripts that sample hail magnitudes over specified date ranges and output the results in Zarr format.

## Setup

1. **Create / Activate Environment**  
   Create and activate a virtual environment (e.g., `conda` environment or Python `venv`).

2. **Install Requirements**  
   Install the necessary dependencies using the provided `requirements.txt`:
   ```bash
   pip install -r requirements.txt

### Usage
Once the environment is set up, simply run the run.sh script to execute the hail sampling process:

``` bash
bash run.sh
```
The run.sh script will handle passing the appropriate arguments to the Python script:

```bash
   start_time
   end_time
   ensemble_size
   input_data_location
   output_location
   lognorm_shape_file
   lognorm_scale_file
```

Adjust the values in run.sh as needed for your specific data and time range. 

### Batch Year Processing

To process a complete year use the 
```bash
bash batch_year_process.sh 2023
```
Make sure to adjust the input and output paths appropriately.

The defualts are:
```bash
INPUT_BASE="/pscratch/sd/s/sabbih/scs_era5_v1.1/conus"
OUTPUT_BASE="/pscratch/sd/s/sabbih/weather_forcasting/event_set"
LOGNORM_SHAPE_FILE="/pscratch/sd/s/sabbih/weather_forcasting/event_set/lognorm_shape.nc"
LOGNORM_SCALE_FILE="/pscratch/sd/s/sabbih/weather_forcasting/event_set/lognorm_scale.nc"
```

These are used in the script as:
```bash
    INPUT_DATA_LOCATION="${INPUT_BASE}/${YEAR}/*.nc"
    OUTPUT_LOCATION="${OUTPUT_BASE}/${START_TIME}.zarr"
```

Make sure to adjust the paths accordingly and only use **frequency** data with the above. Please, update the script according to your slurm environment. 


# compute_mean_prob.py

This script computes hail probabilities for specified hail ranges, time periods, and ensembles from a collection of Zarr datasets. It can also automatically derive the start and end years from the dataset’s time dimension if not provided.

## Description

1. Reads multiple Zarr datasets specified by an `input_path` (supports globbing, e.g. `"/path/to/data/*/*.zarr"`).
2. Optionally slices over an ensemble dimension (with defaults from 0 to 99).
3. Computes the mean probability of hail occurring within predefined hail ranges.
4. Outputs the results in a new Zarr file, named based on the provided or extracted start/end dates and the selected ensemble range.

## Arguments

- **`input_path`** *(str, required)*  
  Path pattern for the Zarr datasets (for example, `"/path/to/data/*/*.zarr"`).

- **`output_dir`** *(str, required)*  
  Directory where the output Zarr file will be saved.

- **`--start_date`** *(str, required)*  
  Start date. Used for slicing the dataset and part of the saved filename.

- **`--end_date`** *(str, required)*  
  End date. Used for slicing the dataset and part of the saved filename.

- **`--ensemble_start`** *(int, optional, default=0)*  
  First ensemble member index (inclusive).

- **`--ensemble_end`** *(int, optional, default=99)*  
  Last ensemble member index (inclusive).

### Example Usage

1. Activate your python env first and then:
   ```bash
   python compute_mean_prob.py \
    "/pscratch/sd/s/sabbih/aws/vayuh/event-sets/2010-2023/*/*.zarr" \
    "/path/to/output_dir" \
    "2010-01-01" \
    "2023-12-31" \
    --ensemble_start 0 \
    --ensemble_end 99
   ```
The ensemble selection option can be used to manage the peak memory load of the calculations.

# Batch Compute Prob
The `batch_compute_prob.sh` file submits each year independently to slurm. The script loops through each year starting from `1980` to `2023` and loads all ensemble members to memory before computing the probabilites. The peak memory usage for a single year with all members is 240 GB and it takes 55 to 70 minutes to compute the probabilities and save the resultant file.

```
INPUT_PATH="/fsx/vayu/results/event_set/*.zarr"
OUTPUT_DIR="/pscratch/sd/s/sabbih/weather_forecasting/event_set/data/test"
YEAR_START=1980
YEAR_END=2023
```
Please set the above paths in the script accordingly and double check the slurm flags. 
