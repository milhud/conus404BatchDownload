"""
Script to retry failed downloads concurrently.
Reads from 'data/failed_jobs.json'.
Writes "ultimate" failures to 'ultimate_failures.json' in the root.
"""

import os
import json
import glob
import datetime as dt
import time
import sys
import subprocess
from typing import List, Dict, Tuple, Optional
import psutil

# Import config from the main project
from config import (
    LOG_DIR,
    DATA_DIR,
    VARIABLE_AGG_MAP
)

# --- Hard-code 2 concurrent processes for retry ---
MAX_RETRY_PROCESSES = 2
# Input file from driver.py
FAILED_JOBS_FILE = os.path.join(DATA_DIR, "failed_jobs.json")
# New output file for "ultimate" failures in the *project root*
# Assumes this script is run from the project root (which run_pipeline.sh does)
ULTIMATE_FAILURE_FILE = "ultimate_failures.json" 

MEMORY_CHECK_INTERVAL = 30 # seconds


def print_and_log(message: str, log_file: str):
    """Print message with timestamp and log to file."""
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {message}"
    print(log_line, flush=True)
    
    with open(log_file, "a") as f:
        f.write(log_line + "\n")

def get_memory_stats() -> dict:
    """Get current system memory statistics."""
    virtual_mem = psutil.virtual_memory()
    return {
        "available_mb": virtual_mem.available / (1024 * 1024),
        "total_mb": virtual_mem.total / (1024 * 1024),
        "percent_used": virtual_mem.percent
    }

def log_memory_stats(log_fn, context: str = ""):
    """Log current memory statistics."""
    stats = get_memory_stats()
    msg = (f"Memory Stats{' - ' + context if context else ''}: "
           f"Available={stats['available_mb']:.1f}MB/{stats['total_mb']:.1f}MB "
           f"({stats['percent_used']:.1f}% used)")
    log_fn(msg)

def log_process_stats(log_fn, active_processes):
    """Log statistics about active processes."""
    if not active_processes:
        log_fn("No active retry subprocesses")
        return
    
    log_fn(f"Active retry subprocesses: {len(active_processes)}")
    for pid, (date, proc, start_time) in active_processes.items():
        duration = time.time() - start_time
        try:
            p = psutil.Process(pid)
            mem_mb = p.memory_info().rss / (1024 * 1024)
            status = p.status()
            log_fn(f"  PID {pid}: {date} | Runtime: {duration:.1f}s | "
                   f"Memory: {mem_mb:.1f}MB | Status: {status}")
        except psutil.NoSuchProcess:
            log_fn(f"  PID {pid}: {date} | Runtime: {duration:.1f}s | Process ended")

def launch_retry_subprocess(log_fn, date: dt.date) -> Optional[subprocess.Popen]:
    """Launch a subprocess to re-download a single day."""
    date_str = date.strftime("%Y-%m-%d")
    
    # Get the directory this script is in (src/)
    script_dir = os.path.dirname(__file__)
    
    cmd = [
        sys.executable,
        os.path.join(script_dir, "single_download.py"),
        date_str
    ]
    
    log_dir = os.path.join(LOG_DIR, "subprocesses_retry")
    os.makedirs(log_dir, exist_ok=True)
    sub_log_file = os.path.join(log_dir, f"download_{date.strftime('%Y%m%d')}.log")
    
    try:
        with open(sub_log_file, 'w') as f:
            proc = subprocess.Popen(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True
            )
        
        log_fn(f"Launched RETRY subprocess PID {proc.pid} for {date_str} (log: {sub_log_file})")
        return proc
        
    except Exception as e:
        log_fn(f"ERROR: Failed to launch RETRY subprocess for {date_str}: {e}")
        return None

def main():
    """Main concurrent retry driver."""
    
    # Setup logging for this script
    retry_log_file = os.path.join(LOG_DIR, f"retry_driver_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    def log(message):
        print_and_log(message, retry_log_file)
    
    log("=" * 80)
    log("Starting Concurrent Failed Job Retry Script")
    log(f"Max concurrent processes: {MAX_RETRY_PROCESSES}")
    log(f"Reading failures from: {FAILED_JOBS_FILE}")
    log(f"Will write ultimate failures to: {ULTIMATE_FAILURE_FILE}")
    log("=" * 80)
    
    if not os.path.exists(FAILED_JOBS_FILE):
        log(f"No failure log file found ({FAILED_JOBS_FILE}). Exiting.")
        with open(ULTIMATE_FAILURE_FILE, 'w') as f:
            json.dump({}, f, indent=4) # Write empty file
        return 0

    try:
        with open(FAILED_JOBS_FILE, 'r') as f:
            failed_jobs = json.load(f)
    except Exception as e:
        log(f"ERROR: Could not read {FAILED_JOBS_FILE}. Error: {e}")
        return 1

    if not failed_jobs:
        log("Failure log is empty. No jobs to retry. Exiting.")
        with open(ULTIMATE_FAILURE_FILE, 'w') as f:
            json.dump({}, f, indent=4) # Write empty file
        return 0

    log(f"Found {len(failed_jobs)} failed job(s) to retry.")
    
    dates_to_process: List[dt.date] = []
    for date_str in failed_jobs.keys():
        try:
            date_obj = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
            dates_to_process.append(date_obj)
        except Exception as e:
            log(f"WARNING: Could not parse date {date_str} from failure log. Error: {e}")

    total_dates = len(dates_to_process)
    log(f"Loaded {total_dates} unique dates to retry.")
    
    active_processes: Dict[int, Tuple[dt.date, subprocess.Popen, float]] = {}
    completed_dates: List[dt.date] = []
    failed_dates: List[dt.date] = []
    date_index = 0
    last_memory_check = time.time()
    
    # This dict will hold only the jobs that fail this retry attempt
    remaining_failures = failed_jobs.copy()

    while date_index < total_dates or active_processes:
        # Check for completed processes
        completed_pids = []
        for pid, (date, proc, start_time) in list(active_processes.items()):
            return_code = proc.poll()
            if return_code is not None:
                duration = time.time() - start_time
                completed_pids.append(pid)
                date_str = date.strftime("%Y-%m-%d")
                
                if return_code == 0:
                    completed_dates.append(date)
                    log(f"SUCCESS (RETRY): PID {pid} completed {date_str} in {duration:.1f}s")
                    # On success, remove from the failure list
                    if date_str in remaining_failures:
                        del remaining_failures[date_str]
                else:
                    failed_dates.append(date)
                    log(f"FAILED (RETRY): PID {pid} failed for {date_str} (exit code {return_code})")
                    # Update the error message for the *ultimate failure* log
                    if date_str in remaining_failures:
                        remaining_failures[date_str]["error_message"] = f"Retry failed with exit code {return_code}"
                        remaining_failures[date_str]["last_attempt"] = dt.datetime.now().isoformat()

        # Remove completed processes
        for pid in completed_pids:
            del active_processes[pid]
            
        # Launch new processes if we have capacity
        while (len(active_processes) < MAX_RETRY_PROCESSES and 
               date_index < total_dates):
            
            date = dates_to_process[date_index]
            date_index += 1
            
            log(f"Launching retry {date_index}/{total_dates}: {date}")
            proc = launch_retry_subprocess(log, date)
            if proc:
                active_processes[proc.pid] = (date, proc, time.time())
        
        # Periodic memory monitoring
        current_time = time.time()
        if current_time - last_memory_check >= MEMORY_CHECK_INTERVAL:
            log_memory_stats(log, "Periodic check")
            log_process_stats(log, active_processes)
            last_memory_check = current_time

        time.sleep(1) # Avoid busy waiting

    log("=" * 80)
    log("Retry Script Finished")
    log(f"Successfully retried: {len(completed_dates)}/{total_dates}")
    log(f"Failed again: {len(failed_dates)}/{total_dates}")
    log("=" * 80)

    # --- NEW: Write *ultimate failures* to the root directory ---
    try:
        with open(ULTIMATE_FAILURE_FILE, 'w') as f:
            json.dump(remaining_failures, f, indent=4)
        if remaining_failures:
            log(f"Wrote {len(remaining_failures)} ultimate failures to: {ULTIMATE_FAILURE_FILE}")
        else:
            log(f"All failures resolved. Wrote empty list to: {ULTIMATE_FAILURE_FILE}")
    except Exception as e:
        log(f"ERROR: Could not write ultimate failure log. Error: {e}")

    return 1 if failed_dates else 0 # Exit 1 if failures remain

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

