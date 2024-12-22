python eventset_generation.py \
  --start_time 2023-01-01 \
  --end_time 2023-01-07 \
  --ensemble_size 1000 \
  --input_data_location "/pscratch/sd/s/sabbih/scs_era5_v1.1/conus/2023/*.nc" \
  --output_location "/pscratch/sd/s/sabbih/weather_forcasting/event_set/2023-01-01_2023-01-07.zarr"
