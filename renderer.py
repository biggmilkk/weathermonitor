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
# JMA (Japan) – grouped by region, dedup titles, colored bullets
# ============================================================

JMA_COLORS = {"Warning": "#FF7F00", "Emergency": "#E60026"}

def render_jma_grouped(entries, conf):
    """
    Grouped renderer for JMA feed.
    Marks NEW using a single last-seen timestamp stored in session_state.
    """
    feed_key = conf.get("key", "jma")
    items = _as_list(entries)
    if not items:
        render_empty_state()
        return

    items = sort_newest(attach_timestamp(items))
    last_seen = float(st.session_state.get(f"{feed_key}_last_seen_time") or 0.0)
    items = mark_is_new_ts(items, last_seen_ts=last_seen)

    # group by region
    groups = OrderedDict()
    for e in items:
        region = _norm(e.get("region", "")) or "(Unknown Region)"
        groups.setdefault(region, []).append(e)

    any_rendered = False
    for region, alerts in groups.items():
        if not alerts:
            continue
        any_rendered = True

        region_header = _stripe_wrap(
            f"<h2>{html.escape(region)}</h2>",
            any(a.get("_is_new") for a in alerts)
        )
        st.markdown(region_header, unsafe_allow_html=True)

        # title -> is_new_any
        title_new_map = OrderedDict()
        for a in alerts:
            t = _norm(a.get("title", ""))
            if not t:
                continue
            title_new_map[t] = title_new_map.get(t, False) or bool(a.get("_is_new"))

        for t, is_new_any in title_new_map.items():
            level = "Emergency" if "Emergency" in t else ("Warning" if "Warning" in t else None)
            color = JMA_COLORS.get(level, "#888")
            prefix = "[NEW] " if is_new_any else ""
            st.markdown(
                f"<div style='margin-bottom:4px;'>"
                f"<span style='color:{color};font-size:16px;'>&#9679;</span> {prefix}{html.escape(t)}"
                f"</div>",
                unsafe_allow_html=True
            )

        newest = alerts[0]
        ts = float(newest.get("timestamp") or 0.0)
        if ts:
            st.caption(f"Published: {_fmt_utc(ts)}")
        link = _norm(newest.get("link"))
        if link:
            st.markdown(f"[Read more]({link})")

        st.markdown("---")

    if not any_rendered:
        render_empty_state()

    st.session_state[f"{feed_key}_last_seen_time"] = time.time()

# ============================================================
# PAGASA renderer (colored bullet)
# ============================================================

def render_pagasa(item, conf):
    """
    PAGASA renderer with colored bullets:
      - Severe   -> red (#E60026)
      - Moderate -> amber (#FF7F00)
    """
    severity = (_norm(item.get("severity")) or "").title()
    color = "#E60026" if severity == "Severe" else "#FF7F00"  # amber for Moderate

    title = _norm(item.get("title") or item.get("bucket") or "PAGASA Alert")
    title_html = f"<div><span style='color:{color};font-size:16px;'>&#9679;</span> <strong>{html.escape(title)}</strong></div>"

    is_new = bool(item.get("is_new"))
    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

    region = _norm(item.get("region", ""))
    if region:
        st.caption(f"Region: {region}")

    if item.get("summary"):
        st.markdown(item["summary"])

    link = _norm(item.get("link"))
    if link and title:
        st.markdown(f"[Read more]({link})")

    pub_label = _to_utc_label(item.get("published"))
    if pub_label:
        st.caption(f"Published: {pub_label}")

    st.markdown('---')

# ============================================================
# IMD (India) – compact: Today + Tomorrow in one card
# ============================================================

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

def render_imd_compact(item: dict, conf: dict) -> None:
    """
    One card per region:
      - region (str)
      - days: {"today": {"severity","hazards","date","is_new"?}, "tomorrow": {...}}
      - published (str)
      - source_url (str)
      - is_new (bool)
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
        st.markdown(_bullet_line(sev, hazards, d.get("is_new", is_new_item)), unsafe_allow_html=True)

    _render_day("Today",    days.get("today"))
    _render_day("Tomorrow", days.get("tomorrow"))

    if not days:
        st.markdown("<h4 style='margin-top:16px'>Today</h4>", unsafe_allow_html=True)
        sev = (item.get("severity") or "").title()
        haz = item.get("hazards") or []
        st.markdown(_bullet_line(sev, haz if isinstance(haz, list) else [str(haz)], is_new_item), unsafe_allow_html=True)

    if link:
        st.markdown(f"[Read more]({link})")
    if pub:
        st.caption(f"Published: {pub}")

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
