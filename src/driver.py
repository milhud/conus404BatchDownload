"""Driver script to manage concurrent subprocess downloads."""

import datetime as dt
import json
import os
import subprocess
import sys
import time
from typing import List, Dict, Optional
import psutil

from config import (
    START_DATE,
    END_DATE,
    DATA_DIR,
    LOG_DIR,
    MAX_CONCURRENT_PROCESSES,
    MEMORY_CHECK_INTERVAL,
    MEMORY_WARNING_THRESHOLD,
    MEMORY_CRITICAL_THRESHOLD,
    VARIABLE_AGG_MAP
)

# This is the single JSON file for the *entire run*
FAILED_JOBS_FILE = os.path.join(DATA_DIR, "failed_jobs.json")


class DownloadDriver:
    """Manages concurrent subprocess downloads with monitoring."""

    def __init__(self, start_date: dt.date, end_date: dt.date,
                 max_processes: int = MAX_CONCURRENT_PROCESSES):
        self.start_date = start_date
        self.end_date = end_date
        self.max_processes = max_processes

        # Setup logging
        os.makedirs(LOG_DIR, exist_ok=True)
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = os.path.join(LOG_DIR, f"driver_{timestamp}.log")

        # Process tracking
        self.active_processes: Dict[int, tuple] = {}  # pid -> (date, process, start_time)
        self.completed_dates: List[dt.date] = []
        self.failed_dates: List[dt.date] = []
        
    def print_and_log(self, message: str):
        """Print message with timestamp and log to file."""
        timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] {message}"
        print(log_line, flush=True)

        with open(self.log_file, "a") as f:
            f.write(log_line + "\n")

    def log_failure(self, date: dt.date, return_code: int):
        """Logs the failed job to the main JSON file."""
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            
            # Read existing failures
            failures = {}
            if os.path.exists(FAILED_JOBS_FILE):
                try:
                    with open(FAILED_JOBS_FILE, 'r') as f:
                        failures = json.load(f)
                except json.JSONDecodeError:
                    self.print_and_log(f"WARNING: Could not parse {FAILED_JOBS_FILE}, overwriting.")
                    failures = {}

            date_str = date.strftime("%Y-%m-%d")
            all_vars_list = list(VARIABLE_AGG_MAP.keys())

            # Add or update the failure in the dictionary
            failures[date_str] = {
                "date": date_str,
                "variables_to_retry": all_vars_list,
                "error_message": f"Subprocess failed with exit code {return_code}",
                "last_attempt": dt.datetime.now().isoformat()
            }

            # Write the entire dictionary back
            with open(FAILED_JOBS_FILE, 'w') as f:
                json.dump(failures, f, indent=4)

            self.print_and_log(f"  -> Successfully logged failure to {FAILED_JOBS_FILE}")
        except Exception as e:
            self.print_and_log(f"  -> CRITICAL: Failed to log failure for {date}. Error: {e}")

    def get_memory_stats(self) -> dict:
        """Get current system memory statistics."""
        virtual_mem = psutil.virtual_memory()
        return {
            "available_mb": virtual_mem.available / (1024 * 1024),
            "total_mb": virtual_mem.total / (1024 * 1024),
            "percent_used": virtual_mem.percent
        }

    def log_memory_stats(self, context: str = ""):
        stats = self.get_memory_stats()
        msg = (f"Memory Stats{' - ' + context if context else ''}: "
               f"Available={stats['available_mb']:.1f}MB/{stats['total_mb']:.1f}MB "
               f"({stats['percent_used']:.1f}% used)")
        self.print_and_log(msg)

        if stats['percent_used'] > MEMORY_CRITICAL_THRESHOLD:
            self.print_and_log(f"CRITICAL: Memory usage above {MEMORY_CRITICAL_THRESHOLD}%!")
        elif stats['percent_used'] > MEMORY_WARNING_THRESHOLD:
            self.print_and_log(f"WARNING: Memory usage above {MEMORY_WARNING_THRESHOLD}%")

    def log_process_stats(self):
        """Log statistics about active processes."""
        if not self.active_processes:
            self.print_and_log("No active subprocesses")
            return
        
        self.print_and_log(f"Active subprocesses: {len(self.active_processes)}")
        for pid, (date, proc, start_time) in self.active_processes.items():
            duration = time.time() - start_time
            try:
                p = psutil.Process(pid)
                mem_mb = p.memory_info().rss / (1024 * 1024)
                status = p.status()
                self.print_and_log(f"  PID {pid}: {date} | Runtime: {duration:.1f}s | "
                                   f"Memory: {mem_mb:.1f}MB | Status: {status}")
            except psutil.NoSuchProcess:
                self.print_and_log(f"  PID {pid}: {date} | Runtime: {duration:.1f}s | Process ended")

    def get_dates_to_process(self) -> List[dt.date]:
        """Get list of dates to process."""
        dates = []
        current = self.start_date
        while current <= self.end_date:
            dates.append(current)
            current += dt.timedelta(days=1)
        return dates

    def launch_subprocess(self, date: dt.date) -> Optional[subprocess.Popen]:
        """Launch a subprocess to download a single day."""
        date_str = date.strftime("%Y-%m-%d")

        cmd = [
            sys.executable,
            os.path.join(os.path.dirname(__file__), "single_download.py"),
            date_str
        ]

        log_dir = os.path.join(LOG_DIR, "subprocesses")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"download_{date.strftime('%Y%m%d')}.log")

        try:
            with open(log_file, 'w') as f:
                proc = subprocess.Popen(
                    cmd,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    text=True
                )
            
            self.active_processes[proc.pid] = (date, proc, time.time())
            self.print_and_log(f"Launched subprocess PID {proc.pid} for {date_str} (log: {log_file})")
            
            return proc
            
        except Exception as e:
            self.print_and_log(f"ERROR: Failed to launch subprocess for {date_str}: {e}")
            return None

    def check_completed_processes(self):
        """Check for completed processes and update tracking."""
        completed_pids = []

        for pid, (date, proc, start_time) in list(self.active_processes.items()):
            return_code = proc.poll()

            if return_code is not None:
                duration = time.time() - start_time
                completed_pids.append(pid)

                if return_code == 0:
                    self.completed_dates.append(date)
                    self.print_and_log(f"SUCCESS: PID {pid} completed {date} in {duration:.1f}s")
                else:
                    self.failed_dates.append(date)
                    self.print_and_log(f"FAILED: PID {pid} failed for {date} (exit code {return_code}) "
                                       f"after {duration:.1f}s")
                    # Log the failure
                    self.log_failure(date, return_code)

        # Remove completed processes
        for pid in completed_pids:
            del self.active_processes[pid]

    def run(self):
        """Main driver loop."""
        self.print_and_log("=" * 80)
        self.print_and_log("CONUS404 Download Driver Starting")
        self.print_and_log("=" * 80)
        
        # --- NEW: Clear old failure log ---
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            # Clear the file by writing an empty JSON object
            with open(FAILED_JOBS_FILE, 'w') as f:
                json.dump({}, f)
            self.print_and_log(f"Cleared old failure log: {FAILED_JOBS_FILE}")
        except Exception as e:
            self.print_and_log(f"WARNING: Could not clear failure log. {e}")
        # --- END NEW BLOCK ---

        self.print_and_log(f"Date range: {self.start_date} to {self.end_date}")
        self.print_and_log(f"Max concurrent processes: {self.max_processes}")
        self.print_and_log(f"Log file: {self.log_file}")

        self.log_memory_stats("Initial")
        
        dates_to_process = self.get_dates_to_process()
        total_dates = len(dates_to_process)
        self.print_and_log(f"Total dates to process: {total_dates}")
        
        self.print_and_log("=" * 80)
        self.print_and_log("Starting download processes")
        self.print_and_log("=" * 80)
        
        date_index = 0
        last_memory_check = time.time()
        start_time = time.time()

        while date_index < total_dates or self.active_processes:
            self.check_completed_processes()

            while (len(self.active_processes) < self.max_processes and 
                   date_index < total_dates):
                
                date = dates_to_process[date_index]
                date_index += 1
                
                self.print_and_log(f"Launching download {date_index}/{total_dates}: {date}")
                self.launch_subprocess(date)
                self.log_process_stats()

            current_time = time.time()
            if current_time - last_memory_check >= MEMORY_CHECK_INTERVAL:
                self.log_memory_stats("Periodic check")
                self.log_process_stats()

                completed = len(self.completed_dates)
                failed = len(self.failed_dates)
                remaining = total_dates - completed - failed - len(self.active_processes)
                elapsed = current_time - start_time

                self.print_and_log(f"Progress: {completed} completed, {failed} failed, "
                                   f"{len(self.active_processes)} active, {remaining} pending | "
                                   f"Elapsed: {elapsed:.1f}s")
                
                last_memory_check = current_time

            time.sleep(1)

        self.print_and_log("=" * 80)
        self.print_and_log("Download Processing Complete")
        self.print_and_log("=" * 80)
        
        total_time = time.time() - start_time
        self.print_and_log(f"Total runtime: {total_time:.1f}s ({total_time/60:.1f} minutes)")
        self.print_and_log(f"Completed successfully: {len(self.completed_dates)}/{total_dates}")
        self.print_and_log(f"Failed: {len(self.failed_dates)}/{total_dates}")

        if self.failed_dates:
            self.print_and_log(f"Failed dates: {[str(d) for d in self.failed_dates]}")

            # --- AUTOMATIC RETRY BLOCK ---
            self.print_and_log("=" * 80)
            self.print_and_log("STARTING AUTOMATIC RETRY PROCESS")
            self.print_and_log("=" * 80)

            # Path is now src/retry_failed.py
            retry_cmd = [sys.executable, os.path.join("src", "retry_failed.py")]
            retry_log_file = os.path.join(LOG_DIR, f"retry_driver_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
            self.print_and_log(f"Launching retry script. Log will be in: {retry_log_file}")

            try:
                with open(retry_log_file, 'w') as f:
                    result = subprocess.run(
                        retry_cmd,
                        stdout=f,
                        stderr=subprocess.STDOUT,
                        text=True,
                        check=False 
                    )
                self.print_and_log(f"Retry script finished with exit code {result.returncode}.")
                self.print_and_log(f"Please check {retry_log_file} for details.")

            except FileNotFoundError:
                 self.print_and_log(f"FATAL: Could not find 'src/retry_failed.py'. Skipping automatic retry.")
            except Exception as e:
                self.print_and_log(f"FATAL: Failed to launch retry_failed.py: {e}")
            # --- END RETRY BLOCK ---
        else:
            self.print_and_log("No failures to retry.")

        self.log_memory_stats("Final")
        
        return len(self.failed_dates) == 0


if __name__ == "__main__":
    print(f"Starting CONUS404 download driver at {dt.datetime.now()}", flush=True)

    driver = DownloadDriver(
        start_date=START_DATE,
        end_date=END_DATE,
        max_processes=MAX_CONCURRENT_PROCESSES
    )

    success = driver.run()

    if success:
        print("\nAll downloads completed successfully!", flush=True)
        sys.exit(0)
    else:
        print(f"\nDownloads completed with failures.", flush=True)
        print(f"Check logs and {FAILED_JOBS_FILE} for details.", flush=True)
        sys.exit(1)


