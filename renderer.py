# ============================================================
# IMD (India) compact renderer (Meteoalarm-like; Today/Tomorrow)
# ============================================================

import html
import streamlit as st
from dateutil import parser as dateparser

_IMD_BULLETS = {
    "Orange": "#FF9900",
    "Red":    "#FF0000",
}

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

def render_imd_compact(item: dict, conf: dict) -> None:
    """
    Expected fields:
      - region (str)
      - severity ('Orange'|'Red')
      - hazards (list[str])
      - day ('today'|'tomorrow'|...)
      - day_num (int)
      - day_date (str)  # optional context
      - published (str) # Date of Issue
      - source_url (str)
      - is_new (bool)
    """
    region   = (item.get("region") or "IMD Sub-division").strip()
    severity = (item.get("severity") or "").title()
    hazards  = item.get("hazards") or []
    is_new   = bool(item.get("is_new"))
    pub_str  = _fmt_short_day(item.get("published"))
    link     = item.get("source_url")
    day_lbl  = item.get("day") or "today"  # 'today' or 'tomorrow'

    # Header: Region
    st.markdown(f"**{html.escape(region)}**")

    # Subheader: Today / Tomorrow
    st.caption("Today" if day_lbl == "today" else "Tomorrow" if day_lbl == "tomorrow" else day_lbl.title())

    # Bullet line
    color = _IMD_BULLETS.get(severity, "#888")
    dot   = f"<span style='color:{color};font-size:16px;'>&#9679;</span>"
    new_tag = "[NEW] " if is_new else ""
    sev_tag = f"[{severity}]" if severity else ""

    hazards_txt = ", ".join(hazards) if isinstance(hazards, list) else (hazards or "")
    bullet_html = f"{dot} {new_tag}{sev_tag} {html.escape(hazards_txt)}"
    st.markdown(bullet_html, unsafe_allow_html=True)

    # Read more
    if link:
        st.markdown(f"[Read more]({link})")

    # Published (short)
    if pub_str:
        st.caption(f"Published: {pub_str}")

    st.markdown("---")

# ============================================================
# Renderer Registry
# ============================================================

RENDERERS = {
    'json': render_json,
    'ec_grouped_compact': render_ec_grouped_compact,
    'nws_grouped_compact': render_nws_grouped_compact,
    'rss_cma': render_cma,
    'rss_meteoalarm': render_meteoalarm,
    'rss_bom_multi': render_bom_grouped,
    'rss_jma': render_jma_grouped,
    'uk_grouped_compact': render_uk_grouped,
    'rss_pagasa': render_pagasa,
    'imd_current_orange_red': render_imd_compact,
}
