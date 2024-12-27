import argparse
import gc
import numpy as np
import xarray as xr
from tqdm import tqdm
from scipy.stats import bernoulli
import dask.array as da
import sys
import zarr


def logits_to_prob_binary(logits):
    """
    Convert binary logits to probabilities using the sigmoid function.
    Args:
        logits: xarray DataArray of shape (time, lat, lon)
    Returns:
        xarray DataArray of shape (time, lat, lon) with probabilities
    """
    return 1 / (1 + np.exp(-logits))


def main():
    parser = argparse.ArgumentParser(description="Hail magnitude sampling script.")
    parser.add_argument("--start_time", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end_time", required=True, help="End date in YYYY-MM-DD format.")
    parser.add_argument("--ensemble_size", type=int, required=True, help="Number of ensembles per time step.")
    parser.add_argument("--input_data_location", required=True, help="Path (possibly with wildcard) to input hail dataset.")
    parser.add_argument("--output_location", required=True, help="Path to store output Zarr dataset.")
    parser.add_argument("--lognorm_shape_file", required=True, default=None, help="Path to netCDF file with precomputed lognorm_s (lognorm shape).")
    parser.add_argument("--lognorm_scale_file",  required=True, default=None, help="Path to netCDF file with precomputed lognorm_scale.")

    args = parser.parse_args()

    print("Opening dataset:", args.input_data_location)
    ds = xr.open_mfdataset(args.input_data_location, combine='by_coords')

    print(f"Slicing dataset from {args.start_time} to {args.end_time} ...")
    ds = ds.sel(time=slice(args.start_time, args.end_time))

    # Convert hail logits -> hail_prob
    hail_prob = ds["hail_logits"].map_blocks(logits_to_prob_binary).compute()
    hail_prob = hail_prob.where(hail_prob >= 0.1, 0)

    # If no data falls within the selected date range, raise an error
    if hail_prob.time.size == 0:
        raise ValueError("No data found in the specified time range.")

    # Convert ensembles per time step to a variable
    n_ensembles_per_timestep = args.ensemble_size

    # Check if lognorm_s and lognorm_scale are provided
    if args.lognorm_shape_file and args.lognorm_scale_file:
        print("Loading precomputed lognormal parameters...")

        lognorm_s = xr.open_dataarray(args.lognorm_shape_file).compute()
        lognorm_scale = xr.open_dataarray(args.lognorm_scale_file).compute()
        
        print("Step 2: Sampling Hail Magnitudes...")

    # slice to Kansas
    lognorm_s = lognorm_s.sel(lat=slice(37.0, 40.0),lon=slice(-102.0, -94.0)).compute()
    lognorm_scale = lognorm_scale.sel(lat=slice(37.0, 40.0),lon=slice(-102.0, -94.0)).compute()
    hail_prob = hail_prob.sel(lat=slice(37.0, 40.0),lon=slice(-102.0, -94.0)).compute()
    
    
    sampled_hail_magnitudes = []
    for t in tqdm(range(hail_prob.time.size), desc="Time Step Progress"):
        # Probability of hail at this time step (2D lat/lon)
        p_hail_this_t = hail_prob.isel(time=t).astype(np.float16).values

        # -- 1) Build a mask of valid cells (value, non-NaN) --
        valid_mask = np.isfinite(p_hail_this_t)

        # -- 2) Clip probabilities to [0,1] only where valid; others remain NaN or invalid ---=
        np.clip(p_hail_this_t, 0.0, 1.0, out=p_hail_this_t, where=valid_mask)

        # -- 3) Convert any NaNs (and anything outside the valid_mask) to 0.0 --
        p_hail_this_t = np.where(valid_mask, p_hail_this_t, 0.0)

        # -- 4) Bernoulli sample only from the clipped probabilities --
        random_uniform = np.random.rand(n_ensembles_per_timestep, *p_hail_this_t.shape)
        hail_events = (random_uniform < p_hail_this_t).astype(np.float16)

        # Compute vectorized lognormal samples
        mean_vals = np.log(lognorm_scale).astype(np.float16)
        sigma_vals = lognorm_s.astype(np.float16)

        # 1) Draw Z in shape (ensembles, lat, lon)
        Z = np.random.normal(loc=0.0, scale=1.0, size=(n_ensembles_per_timestep, *sigma_vals.shape))

        sigma_vals = np.broadcast_to(
            sigma_vals, 
            (n_ensembles_per_timestep, *sigma_vals.shape)
        )
        mean_vals = np.broadcast_to(
            mean_vals, 
            (n_ensembles_per_timestep, *mean_vals.shape)
        )

        # 3) Now compute lognormal samples for each ensemble independently
        lognormal_samples = np.exp(sigma_vals * Z + mean_vals)

        valid_mask_broadcast = np.broadcast_to(valid_mask, (n_ensembles_per_timestep, *p_hail_this_t.shape))

        # 4) event_magnitudes depends on hail_events and valid_mask:=-
        event_magnitudes = np.where(
            (hail_events == 1) & valid_mask_broadcast,
            lognormal_samples,
            0.0
        )


        sampled_hail_magnitudes.append(event_magnitudes.astype(np.float16))

    print("Hail Magnitude Sampling Completed!")

    print("Converting to Dask arrays and stacking...")
    da_list = [da.from_array(arr, chunks=(1, 1000, 2310)) for arr in sampled_hail_magnitudes]
    sampled_hail_magnitudes_dask = da.stack(da_list, axis=1)

    print("Building xarray DataArray from Dask array...")
    sampled_hail_magnitudes_da_log = xr.DataArray(
        sampled_hail_magnitudes_dask,
        dims=("ensemble", "time", "lat", "lon"),
        coords={
            "ensemble": np.arange(n_ensembles_per_timestep),
            "time": hail_prob.time,
            "lat": hail_prob.lat,
            "lon": hail_prob.lon
        },
        name="sampled_hail_magnitudes"
    )

    print(f"Saving file to Zarr: {args.output_location}")
    compressor = zarr.Blosc(cname='zstd', clevel=9, shuffle=2)

    encoding = {
        "sampled_hail_magnitudes": {
            "compressor": compressor,
            "dtype": "float16" 
        }
    }

    sampled_hail_magnitudes_da_log.to_zarr(
        args.output_location,
        mode="w",
        encoding=encoding
    )

    print("Done!")


if __name__ == "__main__":
    main()
