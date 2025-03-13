#!/bin/bash

# Path to your virtual environment
VENV_PATH="/home/divesha/smartDownload/venv/bin"

# Activate the virtual environment
source "$VENV_PATH/activate"
echo "=== Starting script ==="
# Run your Python script
python /home/divesha/smartDownload/main.py

echo "=== Script finished ==="
# Deactivate the virtual environment (optional)
deactivate
