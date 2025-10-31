# CONUS404 Data Download Pipeline

This project provides a robust, concurrent pipeline for downloading, processing, and validating hourly CONUS404 climate data from the Microsoft Planetary Computer. It aggregates hourly data into daily files, runs data quality checks, and automatically retries failed downloads.

## Features

  * **Concurrent Downloads:** Runs multiple download jobs in parallel, set by `MAX_CONCURRENT_PROCESSES`.
  * **Automatic Aggregation:** Aggregates 24-hour records into single daily files (mean for intensive, sum for extensive variables).
  * **Data Validation:** Automatically validates every downloaded file for "common sense" data quality (e.g., plausible temperature ranges, no negative precipitation).
  * **Robust Error Handling:** Catches download failures, authentication errors, and validation failures.
  * **Automatic Retry:** A built-in retry script (`retry_failed.py`) automatically re-runs any jobs that failed during the main run.
  * **Failure Logging:** Logs all initial failures to `data/failed_jobs.json` and any ultimate, persistent failures to `ultimate_failures.json`.

## Directory Structure

```
.
├── archive/              # old logs are moved here by the archive script
├── data/
│   ├── failed_jobs.json    # log of jobs that failed the main run
│   └── unprocessed/
│       ├── daily/          # final processed daily data goes here
│       └── 24hours/
├── logs/
│   ├── driver_main_run.log # main log for the driver
│   ├── retry_run.log       # log for the retry script
│   └── subprocesses/       # detailed logs for each individual day's download
├── src/
│   ├── config.py           # main configuration file (set dates here)
│   ├── driver.py           # main pipeline manager
│   ├── single_download.py  # worker script for one day (download, agg, validate)
│   └── retry_failed.py     # script to retry failed jobs
├── archive_logs.sh         # utility script to archive old logs
├── clean_data.sh           # utility script to delete all processed data
├── kill_all.sh             # emergency stop script
├── run_pipeline.sh         # the main script to start the pipeline
├── ultimate_failures.json  # log of jobs that failed *even after* the retry
└── readme.md               # this file
```

## How to Use the Pipeline

### Step 1: Configuration

1.  **Set up Environment:** Ensure you have a Python virtual environment with the required packages installed (e.g., `xarray`, `pystac-client`, `fsspec`, `planetary-computer`, `adlfs`).
2.  **Edit `src/config.py`:** This is the most important step. Open `src/config.py` and set:
      * `START_DATE` and `END_DATE` to your desired range.
      * `MAX_CONCURRENT_PROCESSES` (e.g., `2` or `5`).
      * The `VARIABLE_AGG_MAP` to include the variables you need.

### Step 2: Make Scripts Executable

Before running for the first time, you must make the shell scripts executable:

```
chmod +x run_pipeline.sh
chmod +x kill_all.sh
chmod +x clean_data.sh
chmod +x archive_logs.sh
```

### Step 3: Run the Pipeline

To start the entire process, simply run `run_pipeline.sh` from the project's root directory:

```
./run_pipeline.sh
```

### Step 4: Monitor the Log

The `run_pipeline.sh` script will immediately print the command you need to watch the log in real-time. It will look like this:

```
==================================================================
Run this command to watch the driver's log:

    tail -f logs/driver_main_run.log

==================================================================
```

You can run that `tail` command to see the progress. You can stop watching the log at any time by pressing **`Ctrl+C`**. This will **not** stop the main pipeline, which is running in the background.

**You can now safely log out of your SSH session.**

## The Workflow (What Happens Next)

1.  **`driver.py`** starts and creates a job for every day in your date range. It saves all failures to `data/failed_jobs.json`.
2.  **`retry_failed.py`** automatically starts *after* `driver.py` is finished. It reads `data/failed_jobs.json`, re-runs only the failed jobs, and logs any jobs that *still* failed to `ultimate_failures.json`.
3.  **Done.** The final, validated daily data is in `data/unprocessed/daily/`.

## Utility Scripts

### Emergency Stop

If you need to kill all running pipeline processes, run:

```
./kill_all.sh
```

This will find and kill `driver.py`, `retry_failed.py`, and any `single_download.py` workers.

### Cleaning Data

To delete all processed data and start fresh, run:

```
./clean_data.sh
```

This will empty the `data/unprocessed/daily/` and `data/unprocessed/24hours/` directories.

### Archiving Logs

Before a new run, you may want to archive your old logs:

```
./archive_logs.sh
```

This will move all files from `logs/` into a new, timestamped folder inside `archive/`.
