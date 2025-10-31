"""
Single day download, aggregation, and validation process for CONUS404 data.

This script is self-contained. It fetches its own fresh STAC token,
downloads and aggregates data for one day, validates the data,
and then exits with 0 (success) or 1 (failure).
"""

import datetime as dt
import os
import sys
import xarray as xr
import numpy as np
import pandas as pd
import fsspec
import pystac_client
import planetary_computer

# Import config from the parent directory
from config import (
    VARIABLE_AGG_MAP,
    DERIVED_VARS,
    DATA_DIR,
)

def print_with_timestamp(message: str):
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)

def get_signed_conus_dataset():
    print_with_timestamp("Fetching fresh STAC token from Planetary Computer...")
    try:
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace, # This gets a fresh token
        )
        
        collection = catalog.get_collection("conus404")
        asset = collection.assets["zarr-abfs"]
        
        storage_options = asset.extra_fields["xarray:storage_options"]
        open_kwargs = asset.extra_fields["xarray:open_kwargs"]
        asset_href = asset.href
        
        print_with_timestamp("Opening Zarr dataset...")
        mapper = fsspec.get_mapper(asset_href, **storage_options)
        ds = xr.open_zarr(mapper, **open_kwargs)
        return ds
    except Exception as e:
        print_with_timestamp(f"  ERROR: Failed to open dataset. Exception: {e}")
        return None

def validate_daily_file(daily_file_path: str) -> bool:
    print_with_timestamp(f"VALIDATION START: {daily_file_path}")

    # Define the "common sense" rules
    QC_RULES = {
        # Variable: {min: min_val, max: max_val}
        "T2": {"min": 220, "max": 330},      # Temp: -53°C to 57°C
        "ACRAINLSM": {"min": -1},           # Precip: Allow for near-zero
        "Q2": {"min": -1},                  # Humidity: Allow for near-zero
        "W": {"min": -1},                   # Wind Speed: Allow for near-zero
        "LAI": {"min": -1},                 # Leaf Area Index: Allow for near-zero
    }
    
    # Internal consistency check: Dewpoint (TD2) cannot be > Temperature (T2)
    TD2_T2_CHECK = True

    try:
        with xr.open_dataset(daily_file_path) as ds:
            for var, rules in QC_RULES.items():
                if var in ds:
                    data = ds[var].values
                    
                    if "min" in rules:
                        min_val = np.nanmin(data)
                        if min_val < rules["min"]:
                            print_with_timestamp(f"  QC FAIL: {var} min value {min_val:.6f} is below threshold {rules['min']}")
                            return False
                    
                    if "max" in rules:
                        max_val = np.nanmax(data)
                        if max_val > rules["max"]:
                            print_with_timestamp(f"  QC FAIL: {var} max value {max_val:.2f} is above threshold {rules['max']}")
                            return False
            
            if TD2_T2_CHECK and "T2" in ds and "TD2" in ds:
                # Allow for floating point noise (1e-3)
                if (ds["TD2"] > ds["T2"] + 1e-3).any():
                    print_with_timestamp(f"  QC FAIL: Internal consistency error. Found TD2 > T2.")
                    return False

    except Exception as e:
        print_with_timestamp(f"  QC FAIL: Could not open or read file. Error: {e}")
        return False
    
    print_with_timestamp(f"VALIDATION SUCCESS: {daily_file_path}")
    return True

def run_download_and_validation(date: dt.date) -> (str | None, bool):
    date_str = date.strftime('%Y-%m-%d')
    print_with_timestamp(f"DOWNLOAD START: {date_str}")
    
    ds = get_signed_conus_dataset()
    if ds is None:
        return None, False

    try:
        start = pd.Timestamp(date.year, date.month, date.day, 0, 0, 0)
        end = pd.Timestamp(date.year, date.month, date.day, 23, 59, 59)
        sel = ds.sel(time=slice(start, end))
        
        print_with_timestamp("Manually decoding CF conventions (fill values)...")
        sel = xr.decode_cf(sel)
        
    except Exception as e:
        print_with_timestamp(f"ERROR: Failed to select or decode time for {date_str}. Exception: {e}")
        ds.close()
        return None, False  
    if sel.time.size == 0:
        print_with_timestamp(f"WARNING: No data available for {date_str}")
        ds.close()
        return None, False
    
    print_with_timestamp(f"Found {sel.time.size} hourly records.")
    
    try:
        agg_vars = {}
        for var, is_intensive in VARIABLE_AGG_MAP.items():
            if var not in sel:
                print_with_timestamp(f"WARNING: Variable {var} not found for {date_str}, skipping")
                continue
            
            da = sel[var]
            if is_intensive:
                da_agg = da.mean(dim="time", keep_attrs=True)
            else:
                da_agg = da.sum(dim="time", keep_attrs=True)
            
            da_agg = da_agg.expand_dims(time=[pd.Timestamp(date.year, date.month, date.day)])
            agg_vars[var] = da_agg
        
        agg_ds = xr.Dataset(agg_vars)
    except Exception as e:
        print_with_timestamp(f"ERROR: Failed during daily aggregation. Exception: {e}")
        ds.close()
        return None, False
    
    try:
        if DERIVED_VARS:
            for new_var, info in DERIVED_VARS.items():
                deps = info["depends_on"]
                if all(dep in agg_ds for dep in deps):
                    computed = info["calc_fn"](*(agg_ds[dep] for dep in deps))
                    computed = computed.assign_coords(time=agg_ds.time)
                    agg_ds[new_var] = computed
    except Exception as e:
        print_with_timestamp(f"ERROR: Failed during derived var calculation. Exception: {e}")
        ds.close()
        return None, False
    
    # Close source dataset
    ds.close()
    del sel
    
    daily_dir = os.path.join(DATA_DIR, "unprocessed", "daily")
    os.makedirs(daily_dir, exist_ok=True)
    daily_file = os.path.join(daily_dir, f"conus404_daily_{date.strftime('%Y%m%d')}.nc")
    
    try:
        print_with_timestamp(f"Saving daily aggregate to {daily_file}")
        agg_ds.compute().to_netcdf(daily_file)
        print_with_timestamp(f"DOWNLOAD COMPLETE: {date_str}")
    except Exception as e:
        print_with_timestamp(f"ERROR: Failed to save final NetCDF file. Exception: {e}")
        return None, False
    
    validation_passed = validate_daily_file(daily_file)
    
    return daily_file, validation_passed


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python single_download.py <date_str>")
        sys.exit(1)
    
    date_str = sys.argv[1]
    
    try:
        date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as e:
        print(f"FATAL: Could not parse date '{date_str}'. Error: {e}")
        sys.exit(1)
        
    print_with_timestamp(f"Starting process for {date_str} (PID: {os.getpid()})")
    
    daily_file = None
    validation_passed = False
    
    try:
        # Run the main function
        daily_file, validation_passed = run_download_and_validation(date=date)
        
    except Exception as e:
        print_with_timestamp(f"FATAL: An unhandled exception occurred. Exception: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Exit with the correct code
        if daily_file and validation_passed:
            print_with_timestamp(f"SUCCESS: {daily_file} created and validated.")
            sys.exit(0)
            
        elif daily_file and not validation_passed:
            print_with_timestamp(f"FAILED: File {daily_file} was created but FAILED validation.")
            try:
                os.remove(daily_file)
                print_with_timestamp(f"Cleaned up corrupt file: {daily_file}")
            except Exception as e:
                print_with_timestamp(f"ERROR: Could not clean up corrupt file. Error: {e}")
            sys.exit(1)
            
        else:
            print_with_timestamp("FAILED: Download unsuccessful, no file created.")
            sys.exit(1)


