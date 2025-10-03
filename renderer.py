# renderer.py
import html
import time
from collections import OrderedDict
from datetime import timezone as _tz

import streamlit as st
from dateutil import parser as dateparser

from renderers.nws import render as render_nws_grouped_compact

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
# Environment Canada (EC) – grouped compact
# ============================================================

# Map 2-letter codes → full names for EC grouping (presentation concern)
_PROVINCE_NAMES = {
    "AB": "Alberta",
    "BC": "British Columbia",
    "MB": "Manitoba",
    "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador",
    "NT": "Northwest Territories",
    "NS": "Nova Scotia",
    "NU": "Nunavut",
    "ON": "Ontario",
    "PE": "Prince Edward Island",
    "QC": "Quebec",
    "SK": "Saskatchewan",
    "YT": "Yukon",
}

# Province ordering for grouped EC view
_PROVINCE_ORDER = [
    "Alberta",
    "British Columbia",
    "Manitoba",
    "New Brunswick",
    "Newfoundland and Labrador",
    "Northwest Territories",
    "Nova Scotia",
    "Nunavut",
    "Ontario",
    "Prince Edward Island",
    "Quebec",
    "Saskatchewan",
    "Yukon",
]

def _ec_province_name(code_or_name: str) -> str:
    return _PROVINCE_NAMES.get(code_or_name, code_or_name)

def render_ec_grouped_compact(entries, conf):
    """
    Grouped compact renderer for Environment Canada:
      Province (canonical name)
        → Warning bucket
          → list of alerts

    Maintains per-bucket last-seen keyed by "Province|Warning" in session_state.
    """
    feed_key = conf.get("key", "ec")

    open_key        = f"{feed_key}_active_bucket"
    pending_map_key = f"{feed_key}_bucket_pending_seen"
    lastseen_key    = f"{feed_key}_bucket_last_seen"
    rerun_guard_key = f"{feed_key}_rerun_guard"

    if st.session_state.get(rerun_guard_key):
        st.session_state.pop(rerun_guard_key, None)

    st.session_state.setdefault(open_key, None)
    st.session_state.setdefault(pending_map_key, {})
    st.session_state.setdefault(lastseen_key, {})

    active_bucket   = st.session_state[open_key]
    pending_seen    = st.session_state[pending_map_key]
    bucket_lastseen = st.session_state[lastseen_key]

    # Normalize: timestamps, filter by known EC warning types, annotate province/bucket/bkey
    items = attach_timestamp(_as_list(entries))
    items = sort_newest(items)
    filtered = []
    for e in items:
        bucket = ec_bucket_from_title(e.get("title", ""))
        if not bucket:
            continue
        code = e.get("province", "")
        prov_name = _ec_province_name(code if isinstance(code, str) else str(code))
        d = dict(e, bucket=bucket, province_name=prov_name, bkey=f"{prov_name}|{bucket}")
        filtered.append(d)

    if not filtered:
        render_empty_state()
        return

    # Group by province (preserve chosen order)
    groups = OrderedDict()
    for e in filtered:
        groups.setdefault(e["province_name"], []).append(e)
    provinces = [p for p in _PROVINCE_ORDER if p in groups] + [p for p in groups if p not in _PROVINCE_ORDER]

    # Draw provinces/buckets
    for prov in provinces:
        alerts = groups.get(prov, [])
        if not alerts:
            continue

        # Any NEW in this province?
        def _prov_has_new() -> bool:
            for a in alerts:
                last_seen = float(bucket_lastseen.get(a["bkey"], 0.0))
                if float(a.get("timestamp") or 0.0) > last_seen:
                    return True
            return False

        st.markdown(_stripe_wrap(f"<h2>{html.escape(prov)}</h2>", _prov_has_new()), unsafe_allow_html=True)

        # Bucket by warning type within province
        buckets = OrderedDict()
        for a in alerts:
            buckets.setdefault(a["bucket"], []).append(a)

        for label, items in buckets.items():
            bkey = f"{prov}|{label}"
            cols = st.columns([0.7, 0.3])

            # Toggle button
            with cols[0]:
                if st.button(label, key=f"{feed_key}:{bkey}:btn", use_container_width=True):
                    state_changed = False
                    prev = active_bucket
                    if prev and prev != bkey:
                        ts_opened_prev = float(pending_seen.pop(prev, time.time()))
                        bucket_lastseen[prev] = ts_opened_prev
                    if active_bucket == bkey:
                        ts_opened = float(pending_seen.pop(bkey, time.time()))
                        bucket_lastseen[bkey] = ts_opened
                        st.session_state[open_key] = None
                        state_changed = True
                    else:
                        st.session_state[open_key] = bkey
                        pending_seen[bkey] = time.time()
                        state_changed = True
                    if state_changed and not st.session_state.get(rerun_guard_key, False):
                        st.session_state[rerun_guard_key] = True
                        _safe_rerun()
                        return

            # NEW count for this bucket (committed last_seen)
            last_seen = float(bucket_lastseen.get(bkey, 0.0))
            new_count = sum(1 for x in items if float(x.get("timestamp") or 0.0) > last_seen)

            # Badges
            with cols[1]:
                active_count = len(items)
                st.markdown(
                    "<span style='margin-left:6px;padding:2px 6px;"
                    "border-radius:4px;background:#eef0f3;color:#000;font-size:0.9em;"
                    "font-weight:600;display:inline-block;'>"
                    f"{active_count} Active</span>",
                    unsafe_allow_html=True,
                )
                if new_count > 0:
                    st.markdown(
                        "<span style='margin-left:6px;padding:2px 6px;"
                        "border-radius:4px;background:#ffeecc;color:#000;font-size:0.9em;"
                        "font-weight:bold;display:inline-block;'>"
                        f"❗ {new_count} New</span>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.write("")

            # Render list if open — show [NEW] per item using committed last_seen
            if st.session_state.get(open_key) == bkey:
                for a in items:
                    is_new = float(a.get("timestamp") or 0.0) > last_seen
                    prefix = "[NEW] " if is_new else ""
                    title  = _norm(a.get("title", ""))
                    region = _norm(a.get("region", ""))
                    link   = _norm(a.get("link"))
                    if link and title:
                        st.markdown(f"{prefix}**[{title}]({link})**")
                    else:
                        st.markdown(f"{prefix}**{title}**")
                    if region:
                        st.caption(f"Region: {region}")
                    pub_label = _to_utc_label(a.get("published"))
                    if pub_label:
                        st.caption(f"Published: {pub_label}")
                    st.markdown("---")

        st.markdown("---")

# ============================================================
# UK (Met Office) – grouped
# ============================================================

def render_uk_grouped(entries, conf):
    """
    Render UK like BOM:
      Region header
        → flat list of items
    Uses a single feed-level last_seen_time.
    """
    feed_key = conf.get("key", "uk")
    items = _as_list(entries)
    if not items:
        render_empty_state()
        return

    items = sort_newest(attach_timestamp(items))
    last_seen = float(st.session_state.get(f"{feed_key}_last_seen_time") or 0.0)

    # Group by region
    groups = OrderedDict()
    for e in items:
        groups.setdefault(_norm(e.get("region") or "Unknown"), []).append(e)

    any_rendered = False
    for region, alerts in groups.items():
        if not alerts:
            continue
        any_rendered = True

        region_header = _stripe_wrap(
            f"<h2>{html.escape(region)}</h2>",
            any(float(a.get("timestamp") or 0.0) > last_seen for a in alerts),
        )
        st.markdown(region_header, unsafe_allow_html=True)

        for a in alerts:
            is_new = float(a.get("timestamp") or 0.0) > last_seen
            prefix = "[NEW] " if is_new else ""
            title  = a.get("bucket") or _norm(a.get("title", ""))
            link   = _norm(a.get("link"))

            if title and link:
                st.markdown(f"{prefix}**[{title}]({link})**")
            else:
                st.markdown(f"{prefix}**{title}**")

            if a.get("summary"):
                st.write(a["summary"])

            pub_label = _to_utc_label(a.get("published"))
            if pub_label:
                st.caption(f"Published: {pub_label}")

        st.markdown("---")

    if not any_rendered:
        render_empty_state()

    st.session_state[f"{feed_key}_last_seen_time"] = time.time()

# ============================================================
# CMA renderer (simple colored bullet)
# ============================================================

CMA_COLORS = {
    "Yellow": "#FFD400",
    "Orange": "#FF7F00",
    "Red":    "#E60026",
    "Blue":   "#1E90FF",
}

def render_cma(item, conf):
    """
    CMA item renderer. Shows left stripe on the title line if item['is_new'] True.
    """
    is_new = bool(item.get("is_new"))
    title  = _norm(item.get("title", ""))
    level  = _norm(item.get("level", ""))
    bullet_color = CMA_COLORS.get(level, "#888")  # default to gray

    title_html = (
        f"<div><span style='color:{bullet_color};font-size:18px;'>&#9679;</span> "
        f"<strong>{html.escape(title)}</strong></div>"
    )
    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

    region = _norm(item.get("region", ""))
    if region:
        st.caption(f"Region: {region}")

    if item.get("summary"):
        st.markdown(item["summary"])

    link = _norm(item.get("link"))
    if link and title:
        st.markdown(f"[Read more]({link})")

    published = _to_utc_label(item.get("published"))
    if published:
        st.caption(f"Published: {published}")

    st.markdown("---")

# ============================================================
# Meteoalarm renderer (country block)
# ============================================================

def _is_new_flag(obj) -> bool:
    """Accept either '_is_new' (from computation.meteoalarm_mark_and_sort) or 'is_new'."""
    return bool((obj or {}).get("_is_new") or (obj or {}).get("is_new"))

def _alerts_for_day(alerts_map: dict, day: str):
    """Case-insensitive access for 'today'/'tomorrow' keys."""
    return (
        (alerts_map or {}).get(day)
        or (alerts_map or {}).get(day.capitalize())
        or (alerts_map or {}).get(day.title())
        or []
    )

def render_meteoalarm(item, conf):
    """
    Render a single Meteoalarm country block.
    Stripe the country header if any alert (today/tomorrow) is marked as new.
    """
    def _any_new(country) -> bool:
        alerts_dict = (country.get("alerts") or {})
        for day in ("today", "tomorrow"):
            for e in _alerts_for_day(alerts_dict, day):
                if _is_new_flag(e):
                    return True
        return False

    # Country header (with total severe from scraper)
    try:
        total_severe = int(item.get("total_alerts") or 0)
    except Exception:
        total_severe = 0

    title = _norm(item.get("title", ""))
    header_txt  = f"{title} ({total_severe} active)" if total_severe > 0 else title
    header_html = _stripe_wrap(f"<h2>{html.escape(header_txt)}</h2>", _any_new(item))
    st.markdown(header_html, unsafe_allow_html=True)

    counts   = item.get("counts") or {}
    by_day   = counts.get("by_day")  if isinstance(counts, dict) else {}
    by_type  = counts.get("by_type") if isinstance(counts, dict) else {}

    def _day_level_type_count(day: str, level: str, typ: str) -> int | None:
        """Prefer exact per-day count; fall back to per-type bucket totals if missing."""
        if isinstance(by_day, dict):
            d = by_day.get(day) or by_day.get(day.capitalize()) or by_day.get(day.title())
            if isinstance(d, dict):
                n = d.get(f"{level}|{typ}")
                if isinstance(n, int) and n > 0:
                    return n
        if isinstance(by_type, dict):
            bucket = by_type.get(typ)
            if isinstance(bucket, dict):
                n = bucket.get(level) or bucket.get("total")
                if isinstance(n, int) and n > 0:
                    return n
        return None

    for day in ["today", "tomorrow"]:
        alerts = _alerts_for_day(item.get("alerts") or {}, day)
        if alerts:
            st.markdown(f"<h4 style='margin-top:16px'>{day.capitalize()}</h4>", unsafe_allow_html=True)
            for e in alerts:
                try:
                    dt1 = dateparser.parse(e.get("from", "")).strftime("%b %d %H:%M UTC")
                    dt2 = dateparser.parse(e.get("until", "")).strftime("%b %d %H:%M UTC")
                except Exception:
                    dt1, dt2 = e.get("from", ""), e.get("until", "")

                level = _norm(e.get("level", ""))
                typ   = _norm(e.get("type", ""))
                color = {"Orange": "#FF7F00", "Red": "#E60026"}.get(level, "#888")
                prefix = "[NEW] " if _is_new_flag(e) else ""

                n = _day_level_type_count(day, level, typ)
                count_str = f" ({n} active)" if isinstance(n, int) and n > 0 else ""

                text = f"{prefix}[{level}] {typ}{count_str} – {dt1} to {dt2}"
                st.markdown(
                    f"<div style='margin-bottom:6px;'>"
                    f"<span style='color:{color};font-size:16px;'>&#9679;</span> {text}</div>",
                    unsafe_allow_html=True,
                )

    link = _norm(item.get("link"))
    if link and title:
        st.markdown(f"[Read more]({link})")

    published = _to_utc_label(item.get("published"))
    if published:
        st.caption(f"Published: {published}")

    st.markdown('---')

# ============================================================
# BOM (Australia) – grouped by state
# ============================================================

_BOM_ORDER = [
    "NSW & ACT",
    "Northern Territory",
    "Queensland",
    "South Australia",
    "Tasmania",
    "Victoria",
    "Western Australia",
]

def render_bom_grouped(entries, conf):
    """
    Grouped renderer for BOM multi-state feed.
    Marks NEW using a single last-seen timestamp stored in session_state.
    """
    feed_key = conf.get("key", "bom")
    items = sort_newest(attach_timestamp(_as_list(entries)))

    last_seen = float(st.session_state.get(f"{feed_key}_last_seen_time") or 0.0)
    items = mark_is_new_ts(items, last_seen_ts=last_seen)

    groups = OrderedDict()
    for e in items:
        st_name = _norm(e.get("state", ""))
        groups.setdefault(st_name, []).append(e)

    any_rendered = False
    for state in _BOM_ORDER:
        alerts = groups.get(state, [])
        if not alerts:
            continue
        any_rendered = True

        state_header = _stripe_wrap(
            f"<h2>{html.escape(state)}</h2>",
            any(a.get("_is_new") for a in alerts)
        )
        st.markdown(state_header, unsafe_allow_html=True)

        for a in alerts:
            prefix = "[NEW] " if a.get("_is_new") else ""
            title = _norm(a.get("title", ""))
            link = _norm(a.get("link"))
            if title and link:
                st.markdown(f"{prefix}**[{title}]({link})**")
            else:
                st.markdown(f"{prefix}**{title}**")
            if a.get("summary"):
                st.write(a["summary"])
            pub_label = _to_utc_label(a.get("published"))
            if pub_label:
                st.caption(f"Published: {pub_label}")
        st.markdown("---")

    if not any_rendered:
        render_empty_state()

    st.session_state[f"{feed_key}_last_seen_time"] = time.time()

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
