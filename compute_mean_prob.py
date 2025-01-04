import xarray as xr
import argparse
import os

def main():
    parser = argparse.ArgumentParser(description="Compute hail range means and save to Zarr.")
    
    parser.add_argument(
        "input_path",
        help="Input path pattern for the Zarr datasets (e.g. /path/*/*.zarr)."
    )
    parser.add_argument(
        "output_dir",
        help="Directory where the output Zarr file will be saved."
    )
    
    # Make start_date and end_date optional:
    parser.add_argument(
        "--start_date",
        help="Start date (used only in output filename). If not provided, extracted from dataset time dimension."
    )
    parser.add_argument(
        "--end_date",
        help="End date (used only in output filename). If not provided, extracted from dataset time dimension."
    )
    
    parser.add_argument(
        "--ensemble_start",
        type=int,
        default=0,
        help="First ensemble member index (inclusive). Default is 0."
    )
    parser.add_argument(
        "--ensemble_end",
        type=int,
        default=99,
        help="Last ensemble member index (inclusive). Default is 99."
    )
    
    args = parser.parse_args()
    
    # Define your hail ranges
    hail_ranges = [(0.1, 1), (1, 2), (2, 25)]
    
    # 1) Open the dataset across multiple files, selecting the specified ensemble range
    ds = xr.open_mfdataset(args.input_path, engine="zarr").sel(
        ensemble=slice(args.ensemble_start, args.ensemble_end)
    )
    
    # 2) Chunking for better loading times, then compute in memory
    ds = ds.chunk({
        'lat': -1,
        'lon': -1,
        'time': 100,
        'ensemble': 10
    }).compute()

    ds_start_year = str(ds.time[0].dt.year.values)
    ds_end_year   = str(ds.time[-1].dt.year.values)

    if args.start_date is None:
        start_date = ds_start_year
    else:
        start_date = args.start_date

    if args.end_date is None:
        end_date = ds_end_year
    else:
        end_date = args.end_date

    # 3) Create an output Dataset 
    ds_result = xr.Dataset()

    # 4) Compute the mean for each hail range
    for (low, high) in hail_ranges:
        var_name = f"probability_{low}_{high}"
        ds_result[var_name] = (
            (ds["sampled_hail_magnitudes"] >= low) 
            & (ds["sampled_hail_magnitudes"] < high)
        ).mean(dim=["ensemble", "time"])
    
    # 5) Build the output filename and write the dataset to Zarr
    output_filename = f"{start_date}-{end_date}_ensemble_{args.ensemble_start}-{args.ensemble_end}.zarr"
    output_path = os.path.join(args.output_dir, output_filename)
    
    ds_result.to_zarr(output_path, mode="w")
    print(f"Saved results to {output_path}")

if __name__ == "__main__":
    main()
