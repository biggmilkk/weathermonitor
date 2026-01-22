# renderers/cma.py
import html
from datetime import timezone as _tz
import streamlit as st
from dateutil import parser as dateparser

# Pure logic helpers (no UI side effects)
from computation import attach_timestamp, sort_newest

# --------------------------
# Local UI helpers
# --------------------------

def _norm(s: str | None) -> str:
    return (s or "").strip()

def _to_utc_label(pub: str | None) -> str | None:
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
    if not is_new:
        return content
    return (
        "<div style='border-left:4px solid #e40000;"
        "padding-left:10px;margin:8px 0;'>"
        f"{content}</div>"
    )

def _render_empty_state():
    st.info("No active CMA warnings meeting severity thresholds.")

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
    Single CMA alert card (province bucket aware).
    """

    title = _norm(item.get("title")) or "(no title)"
    level = _norm(item.get("level"))
    color = CMA_COLORS.get(level, "#888")

    region = _norm(item.get("region"))

    # ---- Title line (bullet + title + province) ----
    title_html = (
        f"<div>"
        f"<span style='color:{color};font-size:18px;'>&#9679;</span> "
        f"<strong>{html.escape(title)}</strong>"
        f"{f' <span style=\"color:#666;\">— {html.escape(region)}</span>' if region else ''}"
        f"</div>"
    )
    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

    # ---- Summary ----
    text_block = _norm(item.get("summary") or item.get("body"))
    if text_block:
        # Render as safe markdown-compatible text
        st.markdown(html.escape(text_block).replace("\n", "  \n"))

    # ---- Link ----
    link = _norm(item.get("link"))
    if link:
        st.markdown(f"[Read more]({link})")

    # ---- Published ----
    published = _to_utc_label(item.get("published"))
    if published:
        st.caption(f"Published: {published}")

    st.markdown("---")

# --------------------------
# Public renderer entrypoint
# --------------------------

def render(entries: list[dict], conf: dict) -> None:
    """
    CMA renderer (province-bucket aware).
    """
    feed_key = conf.get("key", "cma")
    items = entries or []

    if not items:
        _render_empty_state()
        return

    # Normalize & order
    items = sort_newest(attach_timestamp(items))

    last_seen_ts = float(st.session_state.get(f"{feed_key}_last_seen_time") or 0.0)

    for item in items:
        ts = float(item.get("timestamp") or 0.0)
        if ts <= 0.0:
            try:
                ts = dateparser.parse(item.get("published") or "").timestamp()
            except Exception:
                ts = 0.0

        is_new = ts > last_seen_ts
        _render_card(item, is_new=is_new)
