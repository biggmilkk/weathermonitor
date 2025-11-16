# renderers/cma.py
import html
from datetime import timezone as _tz
import streamlit as st
from dateutil import parser as dateparser

# Pure logic helpers (no UI side effects)
from computation import attach_timestamp, sort_newest

# --------------------------
# Local UI helpers (no deps)
# --------------------------

def _norm(s: str | None) -> str:
    return (s or "").strip()

def _to_utc_label(pub: str | None) -> str | None:
    """Return a uniform UTC label for display, falling back to the original string."""
    if not pub:
        return None
    try:
        dt = dateparser.parse(pub)
        if dt:
            return dt.astimezone(_tz.utc).strftime("%a, %d %b %y %H:%M:%S UTC")
    except Exception:
        pass
    return pub

def _stripe_wrap(content: str, is_new: bool) -> str:
    """
    Wrap content with a red left border if is_new is True.
    Uses HTML so it can wrap any inline markdown.
    """
    if not is_new:
        return content
    return (
        "<div style='border-left:4px solid #e40000;"
        "padding-left:10px;margin:8px 0;'>"
        f"{content}</div>"
    )

def _render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")

# --------------------------
# CMA-specific rendering
# --------------------------

CMA_COLORS = {
    "Yellow": "#FFD400",
    "Orange": "#FF7F00",
    "Red":    "#E60026",
    "Blue":   "#1E90FF",
}

def _render_card(item: dict, *, is_new: bool) -> None:
    """
    Single CMA card renderer (called per alert).
    Expects:
      - title (str), level (Yellow/Orange/Red/Blue), region (str), summary (str)
      - link (str), published (RFC3339 or parseable datetime string)
    """
    title  = _norm(item.get("title", "")) or "(no title)"
    level  = _norm(item.get("level", ""))
    color  = CMA_COLORS.get(level, "#888")  # default gray if unknown level

    # Title line with a colored bullet for level
    title_html = (
        f"<div><span style='color:{color};font-size:18px;'>&#9679;</span> "
        f"<strong>{html.escape(title)}</strong></div>"
    )
    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

    region = _norm(item.get("region", ""))
    if region:
        st.caption(f"Region: {region}")

    # Show summary if present, otherwise fall back to body
    text_block = item.get("summary") or item.get("body")
    if text_block:
        st.markdown(text_block)

    link = _norm(item.get("link"))
    if link and title:
        st.markdown(f"[Read more]({link})")

    published = _to_utc_label(item.get("published"))
    if published:
        st.caption(f"Published: {published}")

    st.markdown("---")

# --------------------------
# Public renderer entrypoint
# --------------------------

def render(entries: list[dict], conf: dict) -> None:
    """
    CMA renderer (list-aware, read-only).
    - Accepts the full entries list from the controller.
    - Normalizes timestamps and sorts newest-first.
    - Highlights each item as NEW if its timestamp > feed-level last_seen_time.
    - DOES NOT commit 'seen' state; clear-on-close is handled by the controller.
    """
    feed_key = conf.get("key", "cma")
    items = entries or []

    if not items:
        _render_empty_state()
        return

    # Normalize & order
    items = sort_newest(attach_timestamp(items))

    # Read-only 'seen' reference (controller commits on CLOSE)
    last_seen_ts = float(st.session_state.get(f"{feed_key}_last_seen_time") or 0.0)

    for item in items:
        ts = float(item.get("timestamp") or 0.0)
        if ts <= 0.0:
            # fallback if any item missed normalization
            try:
                ts = dateparser.parse(item.get("published") or "").timestamp()
            except Exception:
                ts = 0.0
        is_new = ts > last_seen_ts
        _render_card(item, is_new=is_new)
