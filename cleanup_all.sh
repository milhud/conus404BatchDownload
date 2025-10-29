#!/usr/bin/env bash

# Exit on error
set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

# Directories
DATA_DIR="$PROJECT_ROOT/data"
LOGS_DIR="$PROJECT_ROOT/logs"
ARCHIVE_DIR="$PROJECT_ROOT/archive"

# Create archive directory if it doesn't exist
mkdir -p "$ARCHIVE_DIR"

# Create timestamped archive folder
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
ARCHIVE_FOLDER="$ARCHIVE_DIR/logs_$TIMESTAMP"
mkdir -p "$ARCHIVE_FOLDER"

echo "===== CONUS404 Cleanup Script ====="
echo "Timestamp: $TIMESTAMP"
echo ""

# Archive logs
if [ -d "$LOGS_DIR" ]; then
    echo "Archiving logs to $ARCHIVE_FOLDER..."
    
    # Copy all log files
    if [ -d "$LOGS_DIR/download" ] && [ "$(ls -A $LOGS_DIR/download 2>/dev/null)" ]; then
        mkdir -p "$ARCHIVE_FOLDER/download"
        cp -r "$LOGS_DIR/download/"* "$ARCHIVE_FOLDER/download/"
        echo "  - Archived download logs"
    fi
    
    if [ -d "$LOGS_DIR/process" ] && [ "$(ls -A $LOGS_DIR/process 2>/dev/null)" ]; then
        mkdir -p "$ARCHIVE_FOLDER/process"
        cp -r "$LOGS_DIR/process/"* "$ARCHIVE_FOLDER/process/"
        echo "  - Archived process logs"
    fi
    
    # Copy memory logs
    if ls "$LOGS_DIR"/memory_*.log 1> /dev/null 2>&1; then
        cp "$LOGS_DIR"/memory_*.log "$ARCHIVE_FOLDER/"
        echo "  - Archived memory logs"
    fi
    
    # Remove original logs
    echo "Removing original logs..."
    rm -rf "$LOGS_DIR/download/"*
    rm -rf "$LOGS_DIR/process/"*
    rm -f "$LOGS_DIR"/memory_*.log
    echo "  - Original logs removed"
else
    echo "No logs directory found, skipping log archival"
fi

echo ""

# Delete 24-hour data
if [ -d "$DATA_DIR/unprocessed/24hours" ]; then
    FILE_COUNT=$(find "$DATA_DIR/unprocessed/24hours" -type f -name "*.nc" 2>/dev/null | wc -l)
    if [ "$FILE_COUNT" -gt 0 ]; then
        echo "Deleting $FILE_COUNT 24-hour data files..."
        rm -f "$DATA_DIR/unprocessed/24hours/"*.nc
        echo "  - 24-hour data deleted"
    else
        echo "No 24-hour data files found"
    fi
else
    echo "No 24-hour data directory found"
fi

echo ""

# Delete daily data
if [ -d "$DATA_DIR/unprocessed/daily" ]; then
    FILE_COUNT=$(find "$DATA_DIR/unprocessed/daily" -type f -name "*.nc" 2>/dev/null | wc -l)
    if [ "$FILE_COUNT" -gt 0 ]; then
        echo "Deleting $FILE_COUNT daily data files..."
        rm -f "$DATA_DIR/unprocessed/daily/"*.nc
        echo "  - Daily data deleted"
    else
        echo "No daily data files found"
    fi
else
    echo "No daily data directory found"
fi

echo ""
echo "===== Cleanup Complete ====="
echo "Logs archived to: $ARCHIVE_FOLDER"
echo "All data files removed"
