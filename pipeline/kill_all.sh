#!/bin/bash
# kill_all.sh
# Finds and kills all python processes related to the pipeline
# belonging to the user 'hpmille1'.

USERNAME="hpmille1"

echo "Looking for pipeline processes for user '$USERNAME'..."

# Use pkill -f to find processes by their full command string
# This is safer than killing all 'python'
pkill -u "$USERNAME" -f "src/driver.py"
pkill -u "$USERNAME" -f "src/retry_failed.py"
pkill -u "$USERNAME" -f "src/single_download.py"

# Also kill the bash watcher script
pkill -u "$USERNAME" -f "wait.*src/retry_failed.py"

echo "Kill signals sent."
echo "Waiting for 2 seconds..."
sleep 2

echo "Checking for remaining processes..."
# Check if any are left and print them
# Added grep -v "grep" to filter out the check itself
ps -u "$USERNAME" -f | grep -E "driver.py|retry_failed.py|single_download.py" | grep -v "grep"

echo "---"
echo "Done. Listing all your processes for final check:"
ps -u "$USERNAME" -f


