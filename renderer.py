# renderer.py
import html
import time
from collections import OrderedDict
from datetime import timezone as _tz

import streamlit as st
from dateutil import parser as dateparser

from renderers.nws import render as render_nws_grouped_compact
from renderers.ec import render as render_ec_grouped_compact
from renderers.uk import render as render_uk_grouped
from renderers.cma import render as render_cma
from renderers.meteoalarm import render as render_meteoalarm
from renderers.bom import render as render_bom_grouped
from renderers.jma import render as render_jma_grouped
from renderers.pagasa import render as render_pagasa
from renderers.imd import render as render_imd_compact

# Pure helpers from computation.py (logic lives there)
from computation import (
    attach_timestamp,
    sort_newest,
    mark_is_new_ts,
    alphabetic_with_last,
    ec_bucket_from_title,  # keeps EC bucket detection in one place
)

# ============================================================
# Shared presentation utilities (UI-only)
# ============================================================

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

def _fmt_utc(ts: float) -> str:
    return time.strftime("%a, %d %b %y %H:%M:%S UTC", time.gmtime(ts))

def _as_list(entries):
    if not entries:
        return []
    return entries if isinstance(entries, list) else [entries]

def _norm(s: str | None) -> str:
    return (s or "").strip()

def _stripe_wrap(content: str, is_new: bool) -> str:
    """
    Wrap content with a red left border if is_new is True.
    Uses HTML, not Markdown headers, to ensure rendering inside a styled div.
    """
    if not is_new:
        return content
    return (
        "<div style='border-left:4px solid #e40000;"
        "padding-left:10px;margin:8px 0;'>"
        f"{content}</div>"
    )

def draw_badge(placeholder, count: int):
    """Render the ❗ New badge in a consistent style."""
    try:
        count = int(count)
    except Exception:
        count = 0
    if count > 0:
        placeholder.markdown(
            "<span style='margin-left:8px;padding:2px 6px;"
            "border-radius:4px;background:#ffeecc;color:#000;font-size:0.9em;font-weight:bold;'>"
            f"❗ {count} New</span>",
            unsafe_allow_html=True,
        )
    else:
        placeholder.empty()

def render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")

def _safe_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

# ============================================================
# Generic JSON-like renderer (simple cards)
# ============================================================

def render_json(item, conf):
    """
    Generic JSON/RSS-like item renderer.
    Shows a left stripe on the title if item['is_new'] is True.
    """
    is_new = bool(item.get("is_new"))
    title = item.get("title") or item.get("headline") or "(no title)"
    title_html = _stripe_wrap(f"<strong>{html.escape(_norm(title))}</strong>", is_new)
    st.markdown(title_html, unsafe_allow_html=True)

    region = _norm(item.get("region", ""))
    province = _norm(item.get("province", ""))
    parts = [p for p in [region, province] if p]
    if parts:
        st.caption(f"Region: {', '.join(parts)}")

    body = item.get("summary") or item.get("description") or ""
    if body:
        st.markdown(body)

    link = _norm(item.get("link"))
    if link and title:
        st.markdown(f"[Read more]({link})")

    published = _to_utc_label(item.get("published"))
    if published:
        st.caption(f"Published: {published}")

    st.markdown("---")

# ============================================================
# Renderer Registry
# ============================================================

RENDERERS = {
    "json": render_json,
    "ec_grouped_compact": render_ec_grouped_compact,
    "nws_grouped_compact": render_nws_grouped_compact,
    "uk_grouped_compact": render_uk_grouped,
    "rss_cma": render_cma,
    "rss_meteoalarm": render_meteoalarm,
    "rss_bom_multi": render_bom_grouped,
    "rss_jma": render_jma_grouped,
    "rss_pagasa": render_pagasa,
    "imd_current_orange_red": render_imd_compact,
}
