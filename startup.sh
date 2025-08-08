#!/usr/bin/env bash
set -e

# Try to install Chromium only if the playwright module is importable
python - <<'PY'
import sys, subprocess
try:
    import playwright  # type: ignore
except Exception:
    print("playwright not installed yet; skipping browser install")
else:
    subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=False)
PY

# Start Streamlit
streamlit run weathermonitor.py --server.port $PORT --server.address 0.0.0.0
