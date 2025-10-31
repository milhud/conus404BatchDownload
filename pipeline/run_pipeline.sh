#!/bin/bash
# run_pipeline.sh
# Runs the full download and retry pipeline in the background.

# 1. Define log file paths
LOG_DIR="logs"
DRIVER_LOG="$LOG_DIR/driver_main_run.log"
RETRY_LOG="$LOG_DIR/retry_run.log"

# 2. Ensure log directory exists
mkdir -p "$LOG_DIR"
mkdir -p data
mkdir -p archive

# 3. Start the main driver script with nohup
echo "Starting 'src/driver.py' in the background..."
nohup python -u src/driver.py > "$DRIVER_LOG" 2>&1 &
DRIVER_PID=$!
echo "Driver script started with PID: $DRIVER_PID"
echo ""

# 4. Print the command for you to watch the log
echo "=================================================================="
echo "Run this command to watch the driver's log:"
echo ""
echo "   tail -f $DRIVER_LOG"
echo ""
echo "=================================================================="

# 5. Start the retry-watcher script with nohup
# This command chain waits for the DRIVER_PID to exit,
# and *then* runs the retry script.
echo "Staging 'src/retry_failed.py' to run after the driver finishes..."
nohup bash -c "
    echo 'Waiting for driver (PID $DRIVER_PID) to finish...'
    wait $DRIVER_PID
    echo 'Driver finished. Now running src/retry_failed.py...'
    python -u src/retry_failed.py
    echo 'Retry script finished.'
" > "$RETRY_LOG" 2>&1 &

echo "Retry watcher started. Log will be in: $RETRY_LOG"
echo ""
echo "All processes are in the background. You can now safely log out."


