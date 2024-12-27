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
