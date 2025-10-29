"""Single day download process for CONUS404 data."""

import datetime as dt
import os
import sys
import subprocess
import xarray as xr
import numpy as np
import pandas as pd
import fsspec
import psutil
import gc
import json
import traceback

from config import (
    VARIABLE_AGG_MAP,
    DERIVED_VARS,
    DATA_DIR,
)

# --- VERBOSE LOGGING FLAG ---
VERBOSE_LOGGING = True
# --- ------------------------ ---

# Define the new directory for logging failed jobs
FAILED_JOBS_DIR = "failed_jobs"


def print_with_timestamp(message: str):
    """Print message with timestamp."""
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def print_verbose(message: str):
    """Print message only if VERBOSE_LOGGING is True."""
    if VERBOSE_LOGGING:
        print_with_timestamp(f"  VERBOSE: {message}")


def get_memory_info():
    """Get current memory usage."""
    process = psutil.Process()
    mem_info = process.memory_info()
    virtual_mem = psutil.virtual_memory()
    
    rss_mb = mem_info.rss / (1024 * 1024)
    percent_used = virtual_mem.percent
    
    return rss_mb, percent_used


def log_failure(date_str: str, variables: list, error_message: str):
    """Logs the failed job to a JSON file."""
    try:
        os.makedirs(FAILED_JOBS_DIR, exist_ok=True)
        failure_file = os.path.join(FAILED_JOBS_DIR, f"{date_str}.json")
        
        failure_data = {
            "date": date_str,
            "variables_to_retry": variables,
            "error_message": error_message,
            "last_attempt": dt.datetime.now().isoformat()
        }
        
        with open(failure_file, 'w') as f:
            json.dump(failure_data, f, indent=4)
            
        print_with_timestamp(f"CRITICAL: Logged failure to {failure_file}")
    except Exception as e:
        print_with_timestamp(f"CRITICAL: Failed to log failure for {date_str}. Error: {e}")


def download_single_day(date: dt.date,
                        asset_href: str,
                        storage_options: dict,
                        open_kwargs: dict,
                        data_dir: str = DATA_DIR):
    """
    Download and process a single day of CONUS404 data.
    ...
    """
    
    date_str = date.strftime('%Y-%m-%d')
    print_with_timestamp(f"DOWNLOAD START: {date_str}")
    
    # Get the full list of variables intended for today
    all_vars_list = list(VARIABLE_AGG_MAP.keys())
    
    rss_mb, mem_pct = get_memory_info()
    print_with_timestamp(f"Memory before download: RSS={rss_mb:.1f}MB, System={mem_pct:.1f}%")
    
    # Define time range for the day
    start = pd.Timestamp(date.year, date.month, date.day, 0, 0, 0)
    end = pd.Timestamp(date.year, date.month, date.day, 23, 59, 59)
    print_verbose(f"Time range set: {start} to {end}")
    
    # Open the dataset
    try:
        print_with_timestamp(f"Opening dataset for {date_str}")
        print_verbose(f"Asset Href: {asset_href}")
        mapper = fsspec.get_mapper(asset_href, **storage_options)
        print_verbose("fsspec mapper created")
        ds = xr.open_zarr(mapper, **open_kwargs)
        print_verbose(f"xr.open_zarr complete. Dataset keys: {list(ds.keys())}")
    except Exception as e:
        error_msg = f"Failed to open dataset. Exception: {e}"
        print_with_timestamp(f"ERROR: {error_msg}")
        log_failure(date_str, all_vars_list, error_msg)
        return None
    
    # Select time range
    try:
        print_with_timestamp(f"Selecting time range for {date_str}")
        sel = ds.sel(time=slice(start, end))
        print_verbose(f"Time slice selected. Found {sel.time.size} time steps.")
    except Exception as e:
        error_msg = f"Failed to select time. Exception: {e}"
        print_with_timestamp(f"ERROR: {error_msg}")
        log_failure(date_str, all_vars_list, error_msg)
        ds.close()
        return None
    
    if sel.time.size == 0:
        error_msg = "No data available for this time range."
        print_with_timestamp(f"WARNING: {error_msg}")
        log_failure(date_str, all_vars_list, error_msg)
        ds.close()
        return None
    
    print_with_timestamp(f"Found {sel.time.size} hourly records for {date_str}")
    
    # Select only the variables we need
    vars_to_save = [v for v in VARIABLE_AGG_MAP.keys() if v in sel]
    print_with_timestamp(f"Selected {len(vars_to_save)} variables: {', '.join(vars_to_save)}")
    
    # Aggregate to daily
    agg_vars = {}
    try:
        print_with_timestamp(f"Aggregating to daily data for {date_str}")
        for var, is_intensive in VARIABLE_AGG_MAP.items():
            if var not in sel:
                print_with_timestamp(f"WARNING: Variable {var} not found for {date_str}, skipping")
                continue
            
            print_verbose(f"Aggregating variable: {var}")
            da = sel[var]
            if is_intensive:
                print_verbose(f"  Averaging {var} (intensive)...")
                da_agg = da.mean(dim="time", keep_attrs=True)
            else:
                print_verbose(f"  Summing {var} (extensive)...")
                da_agg = da.sum(dim="time", keep_attrs=True)
            
            print_verbose(f"  Expanding dims for {var}...")
            da_agg = da_agg.expand_dims(time=[pd.Timestamp(date.year, date.month, date.day)])
            agg_vars[var] = da_agg
            print_verbose(f"  Finished {var}")
        
        print_verbose("Creating final aggregated xr.Dataset")
        agg_ds = xr.Dataset(agg_vars)

    except Exception as e:
        error_msg = f"Failed during daily aggregation loop. Exception: {e}"
        print_with_timestamp(f"ERROR: {error_msg}")
        log_failure(date_str, all_vars_list, error_msg)
        ds.close()
        return None
    
    # Add derived variables
    try:
        if DERIVED_VARS:
            print_with_timestamp(f"Computing derived variables")
            for new_var, info in DERIVED_VARS.items():
                print_verbose(f"Checking derived variable: {new_var}")
                deps = info["depends_on"]
                if all(dep in agg_ds for dep in deps):
                    print_verbose(f"  Computing {new_var} from {deps}")
                    computed = info["calc_fn"](*(agg_ds[dep] for dep in deps))
                    computed = computed.assign_coords(time=agg_ds.time)
                    agg_ds[new_var] = computed
                else:
                    print_verbose(f"  Skipping {new_var}, missing dependencies: {deps}")
    except Exception as e:
        error_msg = f"Failed during derived variable calculation. Exception: {e}"
        print_with_timestamp(f"ERROR: {error_msg}")
        log_failure(date_str, all_vars_list, error_msg)
        ds.close()
        return None

    # Close original dataset to free memory
    try:
        print_verbose("Closing source Zarr dataset.")
        ds.close()
    except Exception as e:
        print_with_timestamp(f"WARNING: Error closing dataset for {date_str}: {e}")
    
    del sel
    gc.collect()
    
    rss_mb, mem_pct = get_memory_info()
    print_with_timestamp(f"Memory after aggregation (before save): RSS={rss_mb:.1f}MB, System={mem_pct:.1f}%")
    
    # Save daily aggregated data
    daily_dir = os.path.join(data_dir, "unprocessed", "daily")
    os.makedirs(daily_dir, exist_ok=True)
    daily_file = os.path.join(daily_dir, f"conus404_daily_{date.strftime('%Y%m%d')}.nc")
    
    try:
        print_with_timestamp(f"Saving daily aggregate to {daily_file}")
        print_verbose(f"Calling agg_ds.compute().to_netcdf('{daily_file}')...")
        agg_ds.compute().to_netcdf(daily_file)
        print_verbose("... .compute() and save complete.")
    except Exception as e:
        error_msg = f"Failed to save final NetCDF file. Exception: {e}"
        print_with_timestamp(f"ERROR: {error_msg}")
        log_failure(date_str, all_vars_list, error_msg)
        return None
        
    file_size_mb = os.path.getsize(daily_file) / (1024 * 1024)
    print_with_timestamp(f"Saved daily file: {file_size_mb:.1f}MB")
    
    # Delete aggregated dataset from memory
    del agg_ds
    gc.collect()
    
    rss_mb, mem_pct = get_memory_info()
    print_with_timestamp(f"Memory after daily save: RSS={rss_mb:.1f}MB, System={mem_pct:.1f}%")
    
    print_with_timestamp(f"DOWNLOAD COMPLETE: {date_str}")
    
    return daily_file


if __name__ == "__main__":
    # This main block is executed when run as a subprocess
    if len(sys.argv) != 5:
        print("Usage: python single_download.py <date_str> <asset_href> <storage_options_file> <open_kwargs_file>")
        # Log failure to a generic file if args are wrong
        log_failure(
            f"unknown_date_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}", 
            list(VARIABLE_AGG_MAP.keys()), 
            f"Invalid arguments: {sys.argv}"
        )
        sys.exit(1)
    
    date_str = sys.argv[1]
    asset_href = sys.argv[2]
    storage_options_file = sys.argv[3]
    open_kwargs_file = sys.argv[4]
    
    # Parse date
    try:
        date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as e:
        error_msg = f"Could not parse date '{date_str}'. Error: {e}"
        print_with_timestamp(f"FATAL: {error_msg}")
        log_failure(date_str, list(VARIABLE_AGG_MAP.keys()), error_msg)
        sys.exit(1)
        
    # Load storage options and open kwargs
    try:
        with open(storage_options_file, 'r') as f:
            storage_options = json.load(f)
        
        with open(open_kwargs_file, 'r') as f:
            open_kwargs = json.load(f)
    except Exception as e:
        error_msg = f"Could not load config files. Error: {e}"
        print_with_timestamp(f"FATAL: {error_msg}")
        log_failure(date_str, list(VARIABLE_AGG_MAP.keys()), error_msg)
        sys.exit(1)
    
    print_with_timestamp(f"Starting download process for {date_str}")
    print_with_timestamp(f"PID: {os.getpid()}")
    
    result = None
    try:
        result = download_single_day(
            date=date,
            asset_href=asset_href,
            storage_options=storage_options,
            open_kwargs=open_kwargs
        )
    except Exception as e:
        # Catchall for any unhandled exceptions in the main function
        error_msg = f"An unhandled exception occurred. Traceback: {traceback.format_exc()}"
        print_with_timestamp(f"FATAL: {error_msg}")
        log_failure(date_str, list(VARIABLE_AGG_MAP.keys()), error_msg)
        sys.exit(1)
        
    if result:
        print_with_timestamp(f"SUCCESS: {result}")
        sys.exit(0)
    else:
        print_with_timestamp(f"FAILED: Download unsuccessful (see errors above and failure log)")
        # Note: log_failure() was already called inside download_single_day
        sys.exit(1)

