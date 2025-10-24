#!/usr/bin/env bash
# Script to delete 24-hour CONUS404 data files after daily aggregation

if [ -z "$1" ]; then
    echo "Error: No file path provided"
    exit 1
fi

FILE_PATH="$1"

if [ ! -f "$FILE_PATH" ]; then
    echo "Error: File does not exist: $FILE_PATH"
    exit 1
fi

# Delete the file
rm -f "$FILE_PATH"

if [ $? -eq 0 ]; then
    echo "Successfully deleted: $FILE_PATH"
    exit 0
else
    echo "Error: Failed to delete: $FILE_PATH"
    exit 1
fi
