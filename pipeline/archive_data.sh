#!/bin/bash
#
# This script archives all data from the './data/unprocessed/daily' directory
# into a new, timestamped folder within the './archive' directory.

# --- Configuration ---

# Set the source directory where the daily data lives.
SOURCE_DIR="./data/unprocessed/daily"

# Set the base directory where archives should be stored.
ARCHIVE_BASE_DIR="./archive"

# --- Script ---

echo "Starting archive process..."

# 1. Check if the source directory exists.
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory not found at $SOURCE_DIR"
    echo "Script aborted."
    exit 1
fi

# 2. Get the current timestamp in YYYY-MM-DD-HHMMSS format.
TIMESTAMP=$(date +"%Y-%m-%d-%H%M%S")

# 3. Define the full path for the new timestamped archive folder.
TARGET_DIR="$ARCHIVE_BASE_DIR/data-$TIMESTAMP"

# 4. Create the main archive directory (and any parents) if it doesn't exist.
#    The '-p' flag prevents errors if the directory already exists.
mkdir -p "$ARCHIVE_BASE_DIR"
if [ $? -ne 0 ]; then
    echo "Error: Could not create base archive directory at $ARCHIVE_BASE_DIR"
    echo "Check permissions and try again."
    exit 1
fi

# 5. Create the new, specific timestamped directory.
mkdir "$TARGET_DIR"
if [ $? -ne 0 ]; then
    echo "Error: Could not create target directory at $TARGET_DIR"
    echo "Check permissions and try again."
    exit 1
fi

# 6. Copy all data from the source directory to the target directory.
#    Using "/*" ensures we copy the *contents* of the 'daily' folder.
#    '-r' copies recursively (in case there are subfolders).
#    '-v' provides verbose output (shows which files are being copied).
echo "Copying files from $SOURCE_DIR to $TARGET_DIR..."
cp -rv "$SOURCE_DIR"/* "$TARGET_DIR/"

# 7. Check if the copy operation was successful.
if [ $? -eq 0 ]; then
    echo "------------------------------------------------"
    echo "Success: Archive complete."
    echo "Data saved to: $TARGET_DIR"
    echo "------------------------------------------------"
else
    echo "Error: File copy operation failed."
    # Optional: You could add 'rm -r "$TARGET_DIR"' here to clean up
    # the empty folder on failure.
    exit 1
fi

exit 0


