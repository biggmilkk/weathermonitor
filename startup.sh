#!/usr/bin/env bash
set -e

echo "Installing Playwright Chromium browser..."
python -m playwright install chromium

# Start your Streamlit app
streamlit run weathermonitor.py --server.port $PORT --server.address 0.0.0.0

chmod +x startup.sh
