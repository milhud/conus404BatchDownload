"""Download and process CONUS404 data from Planetary Computer."""

import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from typing import Dict, List
import subprocess
import xarray as xr
import numpy as np
import pandas as pd
import fsspec
import pystac_client
import planetary_computer

from config import (
    VARIABLE_AGG_MAP, 
    DERIVED_VARS, 
    CONCURRENT_DAYS,
    START_DATE,
    END_DATE,
    DATA_DIR,
    LOG_DIR
)
from logger import Logger


def get_conus404_asset():
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    
    collection = catalog.get_collection("conus404")
    asset = collection.assets["zarr-abfs"]
    
    return asset


def dates_for_range(start_date: dt.date, end_date: dt.date) -> List[dt.date]:
    return [start_date + dt.timedelta(days=i) for i in range((end_date - start_date).days + 1)]


def process_single_day(date: dt.date,
                       asset_href: str,
                       storage_options: Dict,
                       open_kwargs: Dict,
                       variable_map: Dict[str, bool],
                       derived_vars: Dict,
                       data_dir: str,
                       logger: Logger) -> str:
    
    date_str = date.strftime('%Y-%m-%d')
    logger.log_download(f"Started download for {date_str}")
    logger.log_memory(f"Before download {date_str}")
    
    # Define time range for the day
    start = pd.Timestamp(date.year, date.month, date.day, 0, 0, 0)
    end = pd.Timestamp(date.year, date.month, date.day, 23, 59, 59)
    
    # Open the dataset with explicit async handling
    try:
        mapper = fsspec.get_mapper(asset_href, **storage_options)
        ds = xr.open_zarr(mapper, **open_kwargs)
    except Exception as e:
        logger.log_download(f"Error opening dataset for {date_str}: {e}")
        return None
    
    try:
        sel = ds.sel(time=slice(start, end))
    except Exception as e:
        logger.log_download(f"Error selecting time for {date_str}: {e}")
        ds.close()
        return None
    
    if sel.time.size == 0:
        logger.log_download(f"No data for {date_str}")
        ds.close()
        return None
    
    # Select only the variables we need (memory optimization)
    vars_to_save = [v for v in variable_map.keys() if v in sel]
    sel_subset = sel[vars_to_save]
    
    # Save 24-hour data with chunking for memory efficiency
    hour_dir = os.path.join(data_dir, "unprocessed", "24hours")
    os.makedirs(hour_dir, exist_ok=True)
    hour_file = os.path.join(hour_dir, f"conus404_24hr_{date.strftime('%Y%m%d')}.nc")
    
    logger.log_download(f"Downloaded {date_str}")
    logger.log_process(f"Saving 24-hour data for {date_str}")
    
    # Compute and save (this loads data into memory, then immediately writes)
    sel_subset.compute().to_netcdf(hour_file)
    logger.log_process(f"Saved 24-hour data for {date_str}")
    logger.log_memory(f"After saving 24hr {date_str}")
    
    # Aggregate to daily
    agg_vars = {}
    for var, is_intensive in variable_map.items():
        if var not in sel:
            logger.log_process(f"Warning: {var} not found for {date_str}, skipping")
            continue
        
        da = sel[var]
        if is_intensive:
            da_agg = da.mean(dim="time", keep_attrs=True)
        else:
            da_agg = da.sum(dim="time", keep_attrs=True)
        
        da_agg = da_agg.expand_dims(time=[pd.Timestamp(date.year, date.month, date.day)])
        agg_vars[var] = da_agg
    
    agg_ds = xr.Dataset(agg_vars)
    
    # Add derived variables
    if derived_vars:
        for new_var, info in derived_vars.items():
            deps = info["depends_on"]
            if all(dep in agg_ds for dep in deps):
                computed = info["calc_fn"](*(agg_ds[dep] for dep in deps))
                computed = computed.assign_coords(time=agg_ds.time)
                agg_ds[new_var] = computed
    
    # Close original dataset to free memory - IMPORTANT: close before deleting refs
    try:
        ds.close()
    except Exception as e:
        logger.log_process(f"Warning: Error closing dataset for {date_str}: {e}")
    
    del sel, sel_subset
    logger.log_memory(f"After closing datasets {date_str}")
    
    # Save daily aggregated data
    daily_dir = os.path.join(data_dir, "unprocessed", "daily")
    os.makedirs(daily_dir, exist_ok=True)
    daily_file = os.path.join(daily_dir, f"conus404_daily_{date.strftime('%Y%m%d')}.nc")
    
    logger.log_process(f"Computing daily aggregate for {date_str}")
    agg_ds.compute().to_netcdf(daily_file)
    logger.log_process(f"Combined data for {date_str}")
    
    # Delete aggregated dataset from memory
    del agg_ds
    logger.log_memory(f"After saving daily {date_str}")
    
    # Delete 24-hour file using shell script
    script_path = os.path.join(os.path.dirname(__file__), "..", "cleanup_24hr.sh")
    try:
        # Make sure script is executable
        os.chmod(script_path, 0o755)
        subprocess.run(["/bin/bash", script_path, hour_file], check=True)
        logger.log_process(f"Deleted 24-hour file for {date_str}")
    except subprocess.CalledProcessError as e:
        logger.log_process(f"Error deleting 24-hour file for {date_str}: {e}")
    except FileNotFoundError:
        logger.log_process(f"Cleanup script not found, manually removing {hour_file}")
        try:
            os.remove(hour_file)
            logger.log_process(f"Deleted 24-hour file for {date_str}")
        except Exception as e:
            logger.log_process(f"Error manually deleting {hour_file}: {e}")
    
    return daily_file


def download_conus404_data(start_date: dt.date,
                           end_date: dt.date,
                           asset_href: str,
                           storage_options: Dict,
                           open_kwargs: Dict,
                           data_dir: str = "data",
                           log_dir: str = "logs",
                           concurrent_days: int = CONCURRENT_DAYS):
    
    logger = Logger(log_dir)
    logger.log_download(f"Starting download from {start_date} to {end_date}")
    logger.log_memory("Initial memory state")
    
    dates = dates_for_range(start_date, end_date)
    daily_files = []
    
    logger.log_download(f"Processing {len(dates)} days with {concurrent_days} concurrent downloads")
    
    # Process days concurrently
    with ThreadPoolExecutor(max_workers=concurrent_days) as exe:
        futures = {
            exe.submit(
                process_single_day,
                date,
                asset_href,
                storage_options,
                open_kwargs,
                VARIABLE_AGG_MAP,
                DERIVED_VARS,
                data_dir,
                logger
            ): date
            for date in dates
        }
        
        for fut in as_completed(futures):
            date = futures[fut]
            try:
                daily_file = fut.result()
                if daily_file is not None:
                    daily_files.append(daily_file)
            except Exception as e:
                logger.log_download(f"Error processing {date}: {e}")
    
    logger.log_process(f"Completed processing {len(daily_files)} days")
    logger.log_process(f"Daily files saved to: {os.path.join(data_dir, 'unprocessed', 'daily')}")
    logger.log_memory("Final memory state")
    logger.stop_memory_monitoring()
    
    return daily_files


if __name__ == "__main__":
    # Get asset from Planetary Computer
    print("Fetching CONUS404 asset from Planetary Computer...")
    asset = get_conus404_asset()
    print(f"Asset URL: {asset.href}")
    
    # Download using config settings
    download_conus404_data(
        start_date=START_DATE,
        end_date=END_DATE,
        asset_href=asset.href,
        storage_options=asset.extra_fields["xarray:storage_options"],
        open_kwargs=asset.extra_fields["xarray:open_kwargs"],
        data_dir=DATA_DIR,
        log_dir=LOG_DIR,
        concurrent_days=CONCURRENT_DAYS
    )
