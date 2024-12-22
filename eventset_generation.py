import argparse
import gc
import numpy as np
import xarray as xr
from tqdm import tqdm
from scipy.stats import bernoulli
import dask.array as da
import sys

def logits_to_prob_binary(logits):
    """
    Convert binary logits to probabilities using sigmoid.
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
    args = parser.parse_args()

    zero_prob = 0.84

    print("Opening dataset:", args.input_data_location)
    hail_reg = xr.open_mfdataset(args.input_data_location, combine='by_coords')

    print(f"Slicing dataset from {args.start_time} to {args.end_time} ...")
    hail_reg = hail_reg.sel(time=slice(args.start_time, args.end_time))
    
    hail_reg = hail_reg['hail_logits'].map_blocks(logits_to_prob_binary).compute()
    
    # If no data falls within the selected date range, raise an error
    if hail_reg.time.size == 0:
        raise ValueError("No data found in the specified time range.")

    # Convert ensembles per time step to a variable 
    n_ensembles_per_timestep = args.ensemble_size
    
    print("Step 1: Computing Log-Normal Parameters...")

    # Keep only hail > 0, set negative or zero hail to NaN
    hail_pos = hail_reg.where(hail_reg > 0)

    # Step 1: Compute count, mean, std over 'time' dimension, ignoring NaNs
    count_hail = hail_pos.count(dim='time')            
    mean_hail  = hail_pos.mean(dim='time', skipna=True)
    std_hail   = hail_pos.std(dim='time', skipna=True)

    # Step 2: Replace any 0 or NaN mean/std with a small positive number (1e-6)
    mean_hail_filled = mean_hail.fillna(1e-6).clip(min=1e-6)
    std_hail_filled  = std_hail.fillna(1e-6).clip(min=1e-6)

    # Step 3: Use a mask for cells with at least 2 valid hail values
    valid_mask = (count_hail >= 2)

    # Step 4: Compute log-normal parameters
    # lognorm_s and lognorm_scale are xarray DataArrays with shape (lat, lon)
    lognorm_s     = np.sqrt(np.log(1.0 + (std_hail_filled / mean_hail_filled)**2))
    lognorm_scale = mean_hail_filled / np.sqrt(1.0 + (std_hail_filled / mean_hail_filled)**2)

    # Step 5: If count_hail < 2, set them to 0
    lognorm_s     = lognorm_s.where(valid_mask, other=0.0)
    lognorm_scale = lognorm_scale.where(valid_mask, other=0.0)

    # Clean up
    del count_hail, mean_hail, std_hail, mean_hail_filled, std_hail_filled, hail_pos, valid_mask
    gc.collect()

    print("Log-Normal Parameters Computed!")
    
    print("Step 2: Sampling Hail Magnitudes...")
    
    sampled_hail_magnitudes = []
    # Iterate over time steps in hail_reg
    for t in tqdm(range(hail_reg.time.size), desc="Time Step Progress"):
        # Bernoulli for zero-inflation
        zero_inflation = bernoulli.rvs(1 - zero_prob, size=hail_reg.isel(time=t).shape)

        # Compute vectorized lognormal samples using NumPy's lognormal relationship
        mean_vals = np.log(lognorm_scale)   # same lat/lon shape
        sigma_vals = lognorm_s

        # Generate normally distributed samples
        Z = np.random.normal(loc=0, scale=1, size=lognorm_s.shape)
        lognormal_samples = np.exp(sigma_vals * Z + mean_vals)

        # Broadcast for ensembles
        lognormal_samples_broadcast = np.broadcast_to(
            lognormal_samples, (n_ensembles_per_timestep, *lognorm_s.shape)
        )

        # Combine zero-inflation and log-normal samples
        event_magnitudes = np.where(zero_inflation == 1, lognormal_samples_broadcast, 0)

        # We'll treat hail_reg > 0 as valid hail area
        masked_magnitudes = event_magnitudes * (hail_reg > 0).isel(time=t).astype(int).values

        sampled_hail_magnitudes.append(masked_magnitudes)

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
            "time": hail_reg.time,
            "lat": hail_reg.lat,
            "lon": hail_reg.lon
        },
        name="sampled_hail_magnitudes"
    )
    
    print(f"Saving file to Zarr: {args.output_location}")
    sampled_hail_magnitudes_da_log.to_zarr(args.output_location, mode="w")
    print("Done!")

if __name__ == "__main__":
    main()
    
   

