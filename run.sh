python eventset_generation.py \
  --start_time 2023-01-01 \
  --end_time 2023-01-01 \
  --ensemble_size 1000 \
  --input_data_location "/pscratch/sd/s/sabbih/scs_era5_v1.1/conus/2023/*.nc" \
  --output_location "/pscratch/sd/s/sabbih/weather_forcasting/event_set/2023-01-01_2023-01-01.zarr" \
  --lognorm_shape_file "/pscratch/sd/s/sabbih/weather_forcasting/event_set/lognorm_shape.nc" \
  --lognorm_scale_file "/pscratch/sd/s/sabbih/weather_forcasting/event_set/lognorm_scale.nc"
