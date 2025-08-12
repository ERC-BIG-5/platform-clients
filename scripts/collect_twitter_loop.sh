#!/bin/bash
cd /home/rsoleyma/projects/big5/platform_clients || exit
. prepare.sh


 # Change this to your script path

while true; do
    echo "Starting script..."

    # Run script in background
    typer main.py run collect --run-conf phase2_twitter.yaml &
    SCRIPT_PID=$!

    # Wait random 20-30 minutes (1800-5400 seconds)
    WAIT_TIME=$((RANDOM % 1200 + 600))
    echo "Waiting $((WAIT_TIME/60)) minutes before sending SIGINT..."
    sleep $WAIT_TIME

    # Send SIGINT to script
    echo "Sending SIGINT to script (PID: $SCRIPT_PID)"
    kill -INT $SCRIPT_PID

    # Wait for script to finish and check exit code
    wait $SCRIPT_PID
    EXIT_CODE=$?

    # If script exited with error (not from SIGINT), break loop
    if [ $EXIT_CODE -ne 0 ] && [ $EXIT_CODE -ne 130 ]; then
        echo "Script exited with error code $EXIT_CODE. Stopping loop."
        break
    fi

    # Wait random 10-15
    PAUSE_TIME=$((RANDOM % 601 + 300))
    echo "Pausing for $((PAUSE_TIME/60)) minutes..."
    sleep $PAUSE_TIME
done

echo "Loop ended."#!/bin/bash