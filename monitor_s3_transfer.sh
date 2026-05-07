#!/bin/bash

# Path to the project directory
PROJECT_DIR="/home/admusr/Documents/real_estate_existing_data_handler"
VENV_PYTHON="$PROJECT_DIR/.venv/bin/python"
SCRIPT_PATH="$PROJECT_DIR/s3_transfer.py"
LOG_FILE="$PROJECT_DIR/logs/cron_monitor.log"

# Ensure log directory exists
mkdir -p "$PROJECT_DIR/logs"

# Check if the script is already running
# Using pgrep -f to match the script path in the process list
if pgrep -f "$SCRIPT_PATH" > /dev/null
then
    echo "$(date): s3_transfer.py is already running." >> "$LOG_FILE"
else
    echo "$(date): s3_transfer.py is not running. Starting it..." >> "$LOG_FILE"
    cd "$PROJECT_DIR"
    # Run the script in the background
    # We use the venv python to ensure all dependencies are available
    "$VENV_PYTHON" "$SCRIPT_PATH" >> "$LOG_FILE" 2>&1 &
fi
