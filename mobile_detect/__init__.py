from pathlib import Path
import os
import streamlit as st
import streamlit.components.v1 as components

# If you want dev mode: set env MOBILE_DETECT_DEV=1 and run `npm start` in the frontend
_DEV = os.environ.get("MOBILE_DETECT_DEV") == "1"
_COMPONENT_NAME = "mobile_detect"

if _DEV:
    # Dev server (webpack) URL
    _component_func = components.declare_component(
        _COMPONENT_NAME, url="http://localhost:3001"
    )
else:
    # Built bundle path
    _build_dir = Path(__file__).parent / "frontend" / "build"
    _component_func = components.declare_component(
        _COMPONENT_NAME, path=str(_build_dir)
    )

def mobile_viewport_width(key: str = "mobile_detect", default: int | None = None) -> int | None:
    """
    Renders the invisible component and returns the current viewport width (int).
    Returns `default` until the first value arrives.
    """
    return _component_func(key=key, default=default)

@st.cache_data(ttl=300)
def is_mobile_width(width: int | None, threshold: int = 768) -> bool:
    """Helper: treat width <= threshold as mobile."""
    try:
        return bool(width and int(width) <= int(threshold))
    except Exception:
        return False
