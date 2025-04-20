#!/bin/bash

# Path to your virtual environment
VENV_PATH="/home/divesha/smartDownload/venv/bin"

# Infinite loop to re-run the script if it finishes in 5 seconds or less
while true; do
    # Capture the start time
    start_time=$(date +%s)

    # Activate the virtual environment
    source "$VENV_PATH/activate"
    echo "=== Starting script ==="
    
    # Run your Python script
    python /home/divesha/smartDownload/ww3.py
    python /home/divesha/smartDownload/main.py

    echo "=== Script finished ==="
    
    # Deactivate the virtual environment (optional)
    deactivate

    # Capture the end time
    end_time=$(date +%s)

    # Calculate the elapsed time
    elapsed_time=$((end_time - start_time))

    # If the script finished in 5 seconds or less, re-run it
    if [ "$elapsed_time" -le 2 ]; then
        echo "Script finished in $elapsed_time seconds, re-running..."
    else
        echo "Script took $elapsed_time seconds, not re-running."
        break
    fi
done
