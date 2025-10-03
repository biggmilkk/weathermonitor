# renderers/pagasa.py
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
# PAGASA-specific renderer
# --------------------------

def render(item: dict, conf: dict) -> None:
    """
    PAGASA renderer with colored bullets:
      - Severe   -> red (#E60026)
      - Moderate -> amber (#FF7F00)

    Expected item fields:
      - title/bucket, severity, region, summary, link, published, is_new
    """
    severity = (_norm(item.get("severity")) or "").title()
    color = "#E60026" if severity == "Severe" else "#FF7F00"  # amber for Moderate

    title = _norm(item.get("title") or item.get("bucket") or "PAGASA Alert")
    title_html = (
        f"<div><span style='color:{color};font-size:16px;'>&#9679;</span> "
        f"<strong>{html.escape(title)}</strong></div>"
    )

    is_new = bool(item.get("is_new"))
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

    pub_label = _to_utc_label(item.get("published"))
    if pub_label:
        st.caption(f"Published: {pub_label}")

    st.markdown("---")
