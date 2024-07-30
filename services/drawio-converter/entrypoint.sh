#!/bin/bash
set -e

# Start Xvfb
Xvfb :99 -ac &
export DISPLAY=:99

# Run the Python script
exec python /app/drawio_converter.py
