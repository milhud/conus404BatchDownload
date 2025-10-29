"""ss and combine daily CONUS404 files into a single dataset."""

import datetime as dt
import os
from typing import List
import xarray as xr
import pandas as pd
import glob
import gc

from config import DATA_DIR, LOG_DIR
from logger import Logger


def get_daily_files(daily_dir: str) -> List[str]:
    """Get all daily NetCDF files sorted by date."""
    pattern = os.path.join(daily_dir, "conus404_daily_*.nc")
    files = sorted(glob.glob(pattern))
    return files


def combine_daily_files_memory_safe(daily_dir: str,
                                     output_file: str,
                                     logger: Logger,
                                     batch_size: int = 30):
    """
    Combine daily files into one dataset using memory-safe batching.
    
    Args:
        daily_dir: Directory containing daily NetCDF files
        output_file: Path to output combined NetCDF file
        logger: Logger instance
        batch_size: Number of days to process at once (lower = safer)
    """
    
    logger.log_process("Starting file combination process")
    logger.log_memory("Initial state")
    
    # Get all daily files
    daily_files = get_daily_files(daily_dir)
    
    if not daily_files:
        logger.log_process("ERROR: No daily files found to combine")
        return None
    
    logger.log_process(f"Found {len(daily_files)} daily files to combine")
    
    # Check memory before starting
    if logger.check_memory_critical():
        logger.log_process("CRITICAL: Memory too high to start processing")
        return None
    
    # Group files by year for efficient processing
    year_groups = {}
    for file in daily_files:
        # Extract date from filename: conus404_daily_YYYYMMDD.nc
        basename = os.path.basename(file)
        date_str = basename.replace("conus404_daily_", "").replace(".nc", "")
        year = date_str[:4]
        
        if year not in year_groups:
            year_groups[year] = []
        year_groups[year].append(file)
    
    logger.log_process(f"Files span {len(year_groups)} year(s): {sorted(year_groups.keys())}")
    
    # Process each year separately to manage memory
    year_output_files = []
    
    for year in sorted(year_groups.keys()):
        year_files = year_groups[year]
        logger.log_process(f"Processing year {year} with {len(year_files)} files")
        logger.log_memory(f"Before year {year}")
        
        # Check memory before processing year
        if logger.check_memory_critical():
            logger.log_process(f"CRITICAL: Skipping year {year} due to high memory")
            continue
        
        # Process year in batches
        year_batches = []
        for i in range(0, len(year_files), batch_size):
            batch_files = year_files[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(year_files) + batch_size - 1) // batch_size
            
            logger.log_process(f"Year {year}: Processing batch {batch_num}/{total_batches} ({len(batch_files)} files)")
            logger.log_memory(f"Before batch {batch_num}")
            
            try:
                # Open datasets with minimal memory footprint
                datasets = []
                for f in batch_files:
                    ds = xr.open_dataset(f, chunks={'time': 1})
                    datasets.append(ds)
                
                # Concatenate batch
                logger.log_process(f"Concatenating batch {batch_num}")
                batch_combined = xr.concat(datasets, dim="time")
                
                # Close individual datasets immediately
                for ds in datasets:
                    ds.close()
                del datasets
                gc.collect()
                
                year_batches.append(batch_combined)
                logger.log_memory(f"After batch {batch_num}")
                
            except Exception as e:
                logger.log_process(f"ERROR in batch {batch_num}: {e}")
                continue
        
        if not year_batches:
            logger.log_process(f"WARNING: No batches processed for year {year}")
            continue
        
        # Combine all batches for this year
        logger.log_process(f"Combining {len(year_batches)} batches for year {year}")
        try:
            year_combined = xr.concat(year_batches, dim="time")
            
            # Save year to temporary file
            year_output = output_file.replace(".nc", f"_year_{year}.nc")
            logger.log_process(f"Writing year {year} to {year_output}")
            
            # Write with chunking to manage memory
            encoding = {var: {'zlib': True, 'complevel': 1} for var in year_combined.data_vars}
            year_combined.to_netcdf(year_output, encoding=encoding)
            
            year_output_files.append(year_output)
            logger.log_process(f"Saved year {year} successfully")
            
            # Clean up
            for batch in year_batches:
                batch.close()
            year_combined.close()
            del year_batches, year_combined
            gc.collect()
            
            logger.log_memory(f"After saving year {year}")
            
        except Exception as e:
            logger.log_process(f"ERROR combining year {year}: {e}")
            continue
    
    # If only one year, rename it as final output
    if len(year_output_files) == 1:
        os.rename(year_output_files[0], output_file)
        logger.log_process(f"Single year - renamed to {output_file}")
        return output_file
    
    # If multiple years, combine them
    if len(year_output_files) > 1:
        logger.log_process(f"Combining {len(year_output_files)} years into final file")
        logger.log_memory("Before final combination")
        
        try:
            # Open all year files
            year_datasets = [xr.open_dataset(f, chunks={'time': 1}) for f in year_output_files]
            
            # Concatenate all years
            logger.log_process("Concatenating all years")
            final_combined = xr.concat(year_datasets, dim="time")
            
            # Write final output
            logger.log_process(f"Writing final combined file to {output_file}")
            encoding = {var: {'zlib': True, 'complevel': 1} for var in final_combined.data_vars}
            final_combined.to_netcdf(output_file, encoding=encoding)
            
            # Clean up
            for ds in year_datasets:
                ds.close()
            final_combined.close()
            
            # Remove temporary year files
            logger.log_process("Removing temporary year files")
            for year_file in year_output_files:
                os.remove(year_file)
                logger.log_process(f"Deleted {year_file}")
            
            logger.log_memory("After final combination")
            
        except Exception as e:
            logger.log_process(f"ERROR in final combination: {e}")
            return None
    
    # Final checks
    if os.path.exists(output_file):
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        logger.log_process(f"SUCCESS: Combined file created at {output_file}")
        logger.log_process(f"File size: {file_size_mb:.1f} MB")
        
        # Get dataset info
        try:
            ds = xr.open_dataset(output_file)
            logger.log_process(f"Dataset dimensions: {dict(ds.dims)}")
            logger.log_process(f"Variables: {list(ds.data_vars)}")
            ds.close()
        except Exception as e:
            logger.log_process(f"WARNING: Could not read combined file: {e}")
        
        return output_file
    else:
        logger.log_process("ERROR: Combined file was not created")
        return None


def process_daily_files(start_date: dt.date = None,
                        end_date: dt.date = None,
                        data_dir: str = DATA_DIR,
                        log_dir: str = LOG_DIR,
                        batch_size: int = 30):
    """
    Main function to process and combine daily files.
    
    Args:
        start_date: Optional filter for start date
        end_date: Optional filter for end date
        data_dir: Base data directory
        log_dir: Log directory
        batch_size: Number of days to process at once
    """
    
    logger = Logger(log_dir)
    
    try:
        daily_dir = os.path.join(data_dir, "unprocessed", "daily")
        processed_dir = os.path.join(data_dir, "processed")
        os.makedirs(processed_dir, exist_ok=True)
        
        # Determine output filename
        if start_date and end_date:
            output_filename = f"conus404_combined_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.nc"
        else:
            output_filename = f"conus404_combined_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.nc"
        
        output_file = os.path.join(processed_dir, output_filename)
        
        logger.log_process(f"Output will be saved to: {output_file}")
        logger.log_process(f"Processing daily files from: {daily_dir}")
        logger.log_process(f"Batch size: {batch_size} days")
        
        # Combine files
        result = combine_daily_files_memory_safe(
            daily_dir=daily_dir,
            output_file=output_file,
            logger=logger,
            batch_size=batch_size
        )
        
        if result:
            logger.log_process("Processing completed successfully")
        else:
            logger.log_process("Processing failed")
        
        return result
        
    finally:
        logger.stop_memory_monitoring()
        gc.collect()


if __name__ == "__main__":
    # Process all daily files with conservative batch size
    # Reduce batch_size if you encounter memory issues (try 10-20)
    print("Starting CONUS404 data processing...")
    result = process_daily_files(batch_size=30)
    
    if result:
        print(f"\nProcessing complete! Combined file: {result}")
    else:
        print("\nProcessing failed. Check logs for details.")
