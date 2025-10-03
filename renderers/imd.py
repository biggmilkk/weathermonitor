# renderers/imd.py
import html
import streamlit as st
from dateutil import parser as dateparser

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

def render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")

# --------------------------
# IMD Renderer
# --------------------------

def render(item: dict, conf: dict) -> None:
    """
    IMD (India) – Compact rendering:
    - Region header
    - Today + Tomorrow hazards
    - Severity colored dots (Orange/Red)
    - "NEW" highlight if applicable
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

    _render_day("Today",    days.get("today"))
    _render_day("Tomorrow", days.get("tomorrow"))

    if not days:
        st.markdown("<h4 style='margin-top:16px'>Today</h4>", unsafe_allow_html=True)
        sev = (item.get("severity") or "").title()
        haz = item.get("hazards") or []
        st.markdown(
            _bullet_line(sev, haz if isinstance(haz, list) else [str(haz)], is_new_item),
            unsafe_allow_html=True
        )

    if link:
        st.markdown(f"[Read more]({link})")
    if pub:
        st.caption(f"Published: {pub}")

    st.markdown("---")
