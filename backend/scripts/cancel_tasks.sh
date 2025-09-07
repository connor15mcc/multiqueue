#!/bin/bash
# Script to cancel tasks by sending task names to the multiqueue process via named pipe

PIPE="/tmp/multiqueue_cancellations"

# Display usage if no arguments provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 task1 task2 ..."
    echo "Example: $0 a b c"
    exit 1
fi

# Create the named pipe if it doesn't exist
if [ ! -p "$PIPE" ]; then
    mkfifo "$PIPE"
    chmod 0600 "$PIPE"
    echo "Created named pipe at $PIPE"
fi

echo "Requesting cancellation of tasks: $@"
echo "$@" | tr ' ' '\n' > "$PIPE"

echo "Cancellation request sent. The multiqueue process will process it shortly."
