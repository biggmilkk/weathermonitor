#!/usr/bin/env bash
set -euo pipefail

# Always use the same interpreter Streamlit will use
PYBIN="$(python - <<'PY'
import sys; print(sys.executable)
PY
)"

# Download Playwright's Chromium if playwright is installed but the browser isn't yet
"$PYBIN" - <<'PY'
import os, sys, subprocess
try:
    import playwright  # noqa
except Exception:
    print("playwright not installed yet; skipping browser install")
    sys.exit(0)

# Optional but nice: ensure consistent cache path
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", os.path.expanduser("~/.cache/ms-playwright"))

# Try to install chromium quietly; don't fail the app if this step hiccups
subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
PY

# Launch the app
exec streamlit run weathermonitor.py --server.port "$PORT" --server.address 0.0.0.0
