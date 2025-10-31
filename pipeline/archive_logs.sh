#!/bin/bash
# archive_logs.sh
# Archives all current logs to a new timestamped folder.

echo "Archiving logs..."

LOG_DIR="logs"
ARCHIVE_ROOT="archive"

# Get current timestamp in YYYYMMDD_HHMMSS format
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
ARCHIVE_DIR="$ARCHIVE_ROOT/$TIMESTAMP"

# Ensure archive root exists
mkdir -p "$ARCHIVE_ROOT"

# Check if log directory exists and has contents
if [ -d "$LOG_DIR" ] && [ "$(ls -A $LOG_DIR)" ]; then
    echo "Moving contents of '$LOG_DIR/' to '$ARCHIVE_DIR/'"
    
    # Create the specific timestamped archive directory
    mkdir -p "$ARCHIVE_DIR"
    
    # Move all files and subdirectories from logs/ to the new archive dir
    mv "$LOG_DIR"/* "$ARCHIVE_DIR/"
    
    echo "Logs successfully archived to '$ARCHIVE_DIR'"
else
    echo "Log directory '$LOG_DIR' is empty or does not exist. Nothing to archive."
fi

