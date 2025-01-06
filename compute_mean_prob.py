#!/usr/bin/env python3

import xarray as xr
import argparse
import os

def main():
    parser = argparse.ArgumentParser(description="Compute hail range means and save to Zarr.")
    
    # Required positional arguments with defaults:
    parser.add_argument(
        "input_path",
        help="Input path pattern for the Zarr datasets (e.g. /path/*/*.zarr)."
    )
    parser.add_argument(
        "output_dir",
        help="Directory where the output Zarr file will be saved."
    )
    parser.add_argument(
        "start_date",
        nargs="?",
        default="2010-01-01",  # default if user doesn't supply
        help="Start date for slicing the dataset (e.g., 2010-01-01)."
    )
    parser.add_argument(
        "end_date",
        nargs="?",
        default="2023-12-31",  # default if user doesn't supply
        help="End date for slicing the dataset (e.g., 2023-12-31)."
    )
    
    parser.add_argument(
        "--ensemble_start",
        type=int,
        default=0,
        help="First ensemble member index (inclusive). Default=0."
    )
    parser.add_argument(
        "--ensemble_end",
        type=int,
        default=99,
        help="Last ensemble member index (inclusive). Default=99."
    )
    
    args = parser.parse_args()
    start_year = args.start_date.split("-")[0]
    end_year = args.end_date.split("-")[0]
    
    # Define hail ranges
    hail_ranges = [(0.1, 1), (1, 2), (2, 25)]
    
    # 1) Open the dataset across multiple files
    ds = xr.open_mfdataset(args.input_path, engine="zarr")
    
    # 2) Chunk for better loading times (no .compute() yet)
    ds = ds.chunk({
        'lat': -1,
        'lon': -1,
        'time': 100,
        'ensemble': 10
    })
    
    # 3) Slice by ensemble range and time range, then compute
    ds = ds.sel(
        ensemble=slice(args.ensemble_start, args.ensemble_end),
        time=slice(args.start_date, args.end_date)
    ).compute()
    
    # 4) Create an output Dataset to hold results for each hail range
    ds_result = xr.Dataset()
    for (low, high) in hail_ranges:
        var_name = f"probability_{low}_{high}"
        ds_result[var_name] = (
            (ds["sampled_hail_magnitudes"] >= low)
            & (ds["sampled_hail_magnitudes"] < high)
        ).mean(dim=["ensemble", "time"])
    
    # 5) Build the output filename with just the year range
    output_filename = (
        f"{start_year}-{end_year}_ensemble_"
        f"{args.ensemble_start}-{args.ensemble_end}.zarr"
    )
    output_path = os.path.join(args.output_dir, output_filename)
    
    # 6) Save the dataset to Zarr
    ds_result.to_zarr(output_path, mode="w")
    print(f"Saved results to {output_path}")

if __name__ == "__main__":
    main()
