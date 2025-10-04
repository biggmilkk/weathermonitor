# renderers/imd.py
import html
import streamlit as st
from dateutil import parser as dateparser
from datetime import timezone as _tz

# --------------------------
# Local helpers
# --------------------------

_IMD_DOT = {"Orange": "#FF9900", "Red": "#FF0000"}

def _fmt_short_day(pub: str | None) -> str | None:
    if not pub:
        return None
    try:
        dt = dateparser.parse(pub)
        try:
            return dt.strftime("%a, %-d %b %y")
        except Exception:
            return dt.strftime("%a, %d %b %y").replace(" 0", " ")
    except Exception:
        return pub

def _bullet_line(sev: str, hazards: list[str], is_new: bool) -> str:
    color = _IMD_DOT.get((sev or "").title(), "#888")
    dot   = f"<span style='color:{color};font-size:16px;'>&#9679;</span>"
    new_tag = "[NEW] " if is_new else ""
    sev_tag = f"[{sev.title()}]" if sev else ""
    hz_txt = ", ".join(hazards or [])
    return f"{dot} {new_tag}{sev_tag} {html.escape(hz_txt)}"

def _stripe_wrap(content: str, is_new: bool) -> str:
    if not is_new:
        return content
    return (
        "<div style='border-left:4px solid #e40000;"
        "padding-left:10px;margin:8px 0;'>"
        f"{content}</div>"
    )

def _render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")

def _render_region_block(item: dict) -> None:
    """
    Render a single IMD region block:
      - Region header (striped if any 'new')
      - Today + Tomorrow bullets (if present)
      - Fallback single-day bullet if 'days' missing
    """
    region = (item.get("region") or "IMD Sub-division").strip()
    days   = item.get("days") or {}
    link   = item.get("source_url")
    pub    = _fmt_short_day(item.get("published"))

    is_new_item     = bool(item.get("is_new"))
    is_new_today    = bool((days.get("today") or {}).get("is_new"))
    is_new_tomorrow = bool((days.get("tomorrow") or {}).get("is_new"))
    is_new_any      = is_new_item or is_new_today or is_new_tomorrow

    header_html = _stripe_wrap(f"<h2>{html.escape(region)}</h2>", is_new_any)
    st.markdown(header_html, unsafe_allow_html=True)

    def _render_day(label: str, d: dict | None):
        if not d:
            return
        st.markdown(f"<h4 style='margin-top:16px'>{label}</h4>", unsafe_allow_html=True)
        sev = (d.get("severity") or "").title()
        hazards = d.get("hazards") or []
        st.markdown(
            _bullet_line(sev, hazards, d.get("is_new", is_new_item)),
            unsafe_allow_html=True
        )

    # Multi-day form
    _render_day("Today",    days.get("today"))
    _render_day("Tomorrow", days.get("tomorrow"))

    # Fallback: flat (no 'days' dict)
    if not days:
        st.markdown("<h4 style='margin-top:16px'>Today</h4>", unsafe_allow_html=True)
        sev = (item.get("severity") or "").title()
        haz = item.get("hazards") or []
        if not isinstance(haz, list):
            haz = [str(haz)]
        st.markdown(_bullet_line(sev, haz, is_new_item), unsafe_allow_html=True)

    if link:
        st.markdown(f"[Read more]({link})")
    if pub:
        st.caption(f"Published: {pub}")

    st.markdown("---")


# --------------------------
# Public renderer entrypoint (list-aware, read-only)
# --------------------------

def render(entries: list[dict], conf: dict) -> None:
    """
    IMD (India) – Compact renderer (LIST-AWARE).
    - Accepts the full entries list from the controller.
    - Entries already have 'timestamp' and 'is_new' computed upstream (compute_imd_timestamps).
    - Sort newest-first using 'timestamp' if available; falls back to 'published'.
    - DOES NOT commit 'seen' state; controller handles clear-on-close.
    """
    items = entries or []
    if not items:
        _render_empty_state()
        return

    def _ts(e: dict) -> float:
        t = e.get("timestamp")
        if isinstance(t, (int, float)):
            return float(t)
        try:
            return dateparser.parse(e.get("published") or "").timestamp()
        except Exception:
            return 0.0

    # Newest first
    items = sorted(items, key=_ts, reverse=True)

    for item in items:
        _render_region_block(item)
