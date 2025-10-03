# renderers/cma.py
import html
import streamlit as st
from dateutil import parser as dateparser


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
            # Force UTC label
            return dt.astimezone().strftime("%a, %d %b %y %H:%M:%S UTC")
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

def render_empty_state():
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

def render(item: dict, conf: dict) -> None:
    """
    CMA item renderer (single-card style).
    Expected fields in `item`:
      - title (str), level (str in {Yellow, Orange, Red, Blue}), region (str), summary (str)
      - link (str), published (RFC3339 or parseable datetime string)
      - is_new (bool) -> adds red stripe accent
    """
    is_new = bool(item.get("is_new"))
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

    summary = item.get("summary")
    if summary:
        st.markdown(summary)

    link = _norm(item.get("link"))
    if link and title:
        st.markdown(f"[Read more]({link})")

    published = _to_utc_label(item.get("published"))
    if published:
        st.caption(f"Published: {published}")

    st.markdown("---")
