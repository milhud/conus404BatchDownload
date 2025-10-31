#!/bin/bash
# clean_data.sh
# Deletes all processed data and temporary files.

echo "Cleaning data directories..."

# Define directories
DAILY_DIR="data/unprocessed/daily"
HOURLY_DIR="data/unprocessed/24hours"
TEMP_DIR="data/temp_config"
FAILED_JOBS_FILE="data/failed_jobs.json"

# Create directories if they don't exist (so rm doesn't fail)
mkdir -p "$DAILY_DIR"
mkdir -p "$HOURLY_DIR"
mkdir -p "$TEMP_DIR"

# -f (force) ignores errors if files don't exist
echo "Cleaning daily data..."
rm -f "$DAILY_DIR"/*

echo "Cleaning 24hours data..."
rm -f "$HOURLY_DIR"/*

echo "Cleaning temp config files..."
rm -f "$TEMP_DIR"/*

echo "Cleaning failed jobs log..."
rm -f "$FAILED_JOBS_FILE"

echo "Data cleaning complete."

