import html
import re
import time
from collections import OrderedDict
from functools import lru_cache
from datetime import timezone as _tz

import streamlit as st
from dateutil import parser as dateparser

# ============================================================
# Shared utilities
# ============================================================

@lru_cache(maxsize=4096)
def _to_ts(pub: str | None) -> float:
    """Parse a published string to a timestamp (seconds since epoch)."""
    if not pub:
        return 0.0
    try:
        return dateparser.parse(pub).timestamp()
    except Exception:
        return 0.0

def _to_utc_label(pub: str | None) -> str | None:
    """Return a uniform UTC label for display, falling back to original string."""
    if not pub:
        return None
    try:
        dt = dateparser.parse(pub)
        if dt:
            return dt.astimezone(_tz.utc).strftime("%a, %d %b %y %H:%M:%S UTC")
    except Exception:
        pass
    return pub

def _as_list(entries):
    if not entries:
        return []
    return entries if isinstance(entries, list) else [entries]

def _norm(s: str | None) -> str:
    return (s or "").strip()

def _fmt_utc(ts: float) -> str:
    return time.strftime("%a, %d %b %y %H:%M:%S UTC", time.gmtime(ts))

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

# --------- Badge + int helpers ---------

def safe_int(x) -> int:
    try:
        return max(0, int(x))
    except Exception:
        return 0

def draw_badge(placeholder, count: int):
    """Render the ❗ New badge in a consistent style."""
    if count and count > 0:
        placeholder.markdown(
            "<span style='margin-left:8px;padding:2px 6px;"
            "border-radius:4px;background:#ffeecc;color:#000;font-size:0.9em;font-weight:bold;'>"
            f"❗ {count} New</span>",
            unsafe_allow_html=True,
        )
    else:
        placeholder.empty()

# --------- Meteoalarm helpers ---------

def alert_id(e: dict) -> str:
    """Canonical Meteoalarm alert ID (used for NEW marking and snapshots)."""
    return f"{e.get('level','')}|{e.get('type','')}|{e.get('from','')}|{e.get('until','')}"

def meteoalarm_country_has_alerts(country: dict) -> bool:
    a = (country.get("alerts") or {})
    return bool(a.get("today")) or bool(a.get("tomorrow"))

def meteoalarm_mark_and_sort(countries: list[dict], seen_ids: set[str]) -> list[dict]:
    """Mark per-alert is_new using seen_ids and sort countries by title (case-insensitive)."""
    for country in countries:
        for alerts in (country.get("alerts", {}) or {}).values():
            for e in (alerts or []):
                e["is_new"] = (alert_id(e) not in seen_ids)
    countries.sort(key=lambda c: (c.get("title", "").casefold()))
    return countries

def meteoalarm_snapshot_ids(countries_or_entries: list[dict]) -> tuple[str, ...]:
    """Snapshot all alert IDs from a list of country dicts (feed entries)."""
    snap = {
        alert_id(e)
        for country in countries_or_entries
        for alerts in (country.get("alerts", {}) or {}).values()
        for e in (alerts or [])
    }
    return tuple(sorted(snap))

# Uniform empty-state for grouped renderers
def render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")

# ============================================================
# Generic JSON-like renderer
# ============================================================

def render_json(item, conf):
    """
    Generic JSON/NWS-like item renderer.
    Shows a left stripe on the title if item['is_new'] is True.
    """
    is_new = bool(item.get("is_new"))
    title = item.get('title') or item.get('headline') or '(no title)'
    title_html = _stripe_wrap(f"<strong>{html.escape(_norm(title))}</strong>", is_new)
    st.markdown(title_html, unsafe_allow_html=True)

    region = _norm(item.get('region', ''))
    province = _norm(item.get('province', ''))
    parts = [p for p in [region, province] if p]
    if parts:
        st.caption(f"Region: {', '.join(parts)}")

    body = item.get('summary') or item.get('description') or ''
    if body:
        st.markdown(body)

    link = _norm(item.get('link'))
    if link and title:
        st.markdown(f"[Read more]({link})")

    published = _to_utc_label(item.get('published'))
    if published:
        st.caption(f"Published: {published}")

    st.markdown('---')

# ============================================================
# EC compact
# ============================================================

# Map 2-letter codes → full names for EC grouping
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

# Full province ordering for grouped EC view
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

# Keep ONLY these warning buckets
EC_WARNING_TYPES = [
    "Arctic Outflow Warning",
    "Blizzard Warning",
    "Blowing Snow Warning",
    "Coastal Flooding Warning",
    "Dust Storm Warning",
    "Extreme Cold Warning",
    "Flash Freeze Warning",
    "Fog Warning",
    "Freezing Drizzle Warning",
    "Freezing Rain Warning",
    "Frost Warning",
    "Heat Warning",
    "Hurricane Warning",
    "Rainfall Warning",
    "Severe Thunderstorm Warning",
    "Severe Thunderstorm Watch",
    "Snowfall Warning",
    "Snow Squall Warning",
    "Tornado Warning",
    "Tropical Storm Warning",
    "Tsunami Warning",
    "Weather Warning",
    "Wind Warning",
    "Winter Storm Warning",
]

# Precise, case-insensitive, word-boundary matching
_EC_BUCKET_PATTERNS = {
    w: re.compile(rf"\b{re.escape(w)}\b", flags=re.IGNORECASE)
    for w in EC_WARNING_TYPES
}

def _ec_bucket_from_title(title: str) -> str | None:
    """Return canonical warning bucket if the title contains one of EC_WARNING_TYPES."""
    if not title:
        return None
    for canon, pat in _EC_BUCKET_PATTERNS.items():
        if pat.search(title):
            return canon
    return None

def _ec_entry_ts(e) -> float:
    return _to_ts(e.get("published"))

# Public helpers

def ec_bucket_from_title(title: str) -> str | None:
    return _ec_bucket_from_title(title)

def ec_remaining_new_total(feed_key: str, entries: list) -> int:
    """
    Total remaining NEW across all *warning* buckets for EC, using the per-bucket
    last_seen map maintained by the compact EC renderer:
      st.session_state[f"{feed_key}_bucket_last_seen"]  (bkey = "Province|Warning Type")
    """
    lastseen_map = st.session_state.get(f"{feed_key}_bucket_last_seen", {}) or {}
    total = 0
    for e in _as_list(entries):
        bucket = _ec_bucket_from_title(e.get("title", ""))
        if not bucket:
            continue 
        code = e.get("province", "")
        prov_name = _PROVINCE_NAMES.get(code, code) if isinstance(code, str) else str(code)
        bkey = f"{prov_name}|{bucket}"
        last_seen = float(lastseen_map.get(bkey, 0.0))
        ts = _ec_entry_ts(e)
        if ts > last_seen:
            total += 1
    return int(max(0, total))

def render_ec_grouped_compact(entries, conf):
    """
    Grouped compact renderer for Environment Canada:
      Province (canonical name)
        → Warning bucket (filtered list above)
          → list of alerts

    Maintains per-bucket last-seen keyed by "Province|Warning".
    """
    feed_key = conf.get("key", "ec")

    def _safe_rerun():
        if hasattr(st, "rerun"):
            st.rerun()
        elif hasattr(st, "experimental_rerun"):
            st.experimental_rerun()

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

    entries = _as_list(entries)
    filtered = []
    for e in entries:
        e["timestamp"] = _ec_entry_ts(e)
        bucket = _ec_bucket_from_title(e.get("title", ""))
        if not bucket:
            continue
        e["bucket"] = bucket
        code = e.get("province", "")
        prov_name = _PROVINCE_NAMES.get(code, code) if isinstance(code, str) else str(code)
        e["province_name"] = prov_name
        filtered.append(e)
    filtered.sort(key=lambda x: x["timestamp"], reverse=True)

    if not filtered:
        render_empty_state()
        st.session_state[f"{feed_key}_remaining_new_total"] = 0
        return

    # Group by province
    groups = OrderedDict()
    for e in filtered:
        groups.setdefault(e["province_name"], []).append(e)

    provinces = [p for p in _PROVINCE_ORDER if p in groups] + [p for p in groups if p not in _PROVINCE_ORDER]

    total_remaining_new = 0
    did_close_toggle    = False

    for prov in provinces:
        alerts = groups.get(prov, [])
        if not alerts:
            continue

        def _prov_has_new() -> bool:
            for a in alerts:
                bkey = f"{prov}|{a['bucket']}"
                if a.get("timestamp", 0.0) > float(bucket_lastseen.get(bkey, 0.0)):
                    return True
            return False

        st.markdown(_stripe_wrap(f"<h2>{html.escape(prov)}</h2>", _prov_has_new()), unsafe_allow_html=True)

        # Bucket by warning type
        buckets = OrderedDict()
        for a in alerts:
            buckets.setdefault(a["bucket"], []).append(a)

        for label, items in buckets.items():
            bkey = f"{prov}|{label}"

            cols = st.columns([0.7, 0.3])

            # Toggle first
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
                        active_bucket = None
                        state_changed = True
                    else:
                        st.session_state[open_key] = bkey
                        active_bucket = bkey
                        pending_seen[bkey] = time.time()
                        state_changed = True

                    if state_changed and not st.session_state.get(rerun_guard_key, False):
                        st.session_state[rerun_guard_key] = True
                        _safe_rerun()
                        return

            # Compute NEW vs committed last_seen (unchanged while open)
            last_seen = float(bucket_lastseen.get(bkey, 0.0))
            new_count = sum(1 for x in items if x.get("timestamp", 0.0) > last_seen)
            total_remaining_new += new_count

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
                        "<span style='margin-left:6px;padding:2px 6px;"""
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
                    is_new = a.get("timestamp", 0.0) > last_seen
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

    st.session_state[f"{feed_key}_remaining_new_total"] = int(max(0, total_remaining_new or 0))

    if did_close_toggle and not st.session_state.get(rerun_guard_key, False):
        st.session_state[rerun_guard_key] = True
        _safe_rerun()

# ============================================================
# NWS compact
# ============================================================

def nws_remaining_new_total(feed_key: str, entries: list) -> int:
    """
    Total remaining NEW across all buckets for NWS, using the per-bucket
    last_seen map maintained by the compact NWS renderer:
      st.session_state[f"{feed_key}_bucket_last_seen"]  (bkey = "State|Bucket")
    """
    lastseen_map = st.session_state.get(f"{feed_key}_bucket_last_seen", {}) or {}
    total = 0
    for e in _as_list(entries):
        state  = _norm(e.get("state") or e.get("state_name") or e.get("state_code") or "")
        bucket = _norm(e.get("bucket") or e.get("event") or "")
        if not state or not bucket:
            continue
        bkey = f"{state}|{bucket}"
        last_seen = float(lastseen_map.get(bkey, 0.0))
        ts = _to_ts(e.get("published"))
        if ts > last_seen:
            total += 1
    return int(max(0, total))

def render_nws_grouped_compact(entries, conf):
    """
    Grouped compact renderer for NWS, similar to EC:
      State (e.g., California, Texas, Marine)
        → Event bucket (e.g., Tornado Warning)
          → list of alerts
    Relies on scraper-populated fields: 'state', 'bucket' (alias 'event'), 'region', 'link', 'published'.
    Maintains per-bucket last-seen keyed by "State|Bucket".
    """
    feed_key = conf.get("key", "nws")

    def _safe_rerun():
        if hasattr(st, "rerun"):
            st.rerun()
        elif hasattr(st, "experimental_rerun"):
            st.experimental_rerun()

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

    # Normalize & sort
    entries = _as_list(entries)
    for e in entries:
        e["timestamp"] = _to_ts(e.get("published"))
        e["state"]  = _norm(e.get("state") or e.get("state_name") or e.get("state_code") or "Unknown")
        e["bucket"] = _norm(e.get("bucket") or e.get("event") or e.get("title") or "Alert")
    entries.sort(key=lambda x: x["timestamp"], reverse=True)  # newest first

    # Filter out items with no state/bucket
    filtered = [e for e in entries if e.get("state") and e.get("bucket")]
    if not filtered:
        render_empty_state()
        st.session_state[f"{feed_key}_remaining_new_total"] = 0
        return

    # Group by state
    groups = OrderedDict()
    for e in filtered:
        groups.setdefault(e["state"], []).append(e)

    # Order states alphabetically, placing Marine last if present
    def _state_sort_key(s: str):
        return (s.lower() == "marine", s.lower())
    states = sorted(groups.keys(), key=_state_sort_key)

    total_remaining_new = 0
    did_close_toggle    = False

    for state in states:
        alerts = groups.get(state, [])
        if not alerts:
            continue

        # Does the state have any "new" across buckets?
        def _state_has_new() -> bool:
            for a in alerts:
                bkey = f"{state}|{a['bucket']}"
                if a.get("timestamp", 0.0) > float(bucket_lastseen.get(bkey, 0.0)):
                    return True
            return False

        st.markdown(_stripe_wrap(f"<h2>{html.escape(state)}</h2>", _state_has_new()), unsafe_allow_html=True)

        # Bucket by event type
        buckets = OrderedDict()
        for a in alerts:
            buckets.setdefault(a["bucket"], []).append(a)

        # Render each bucket with toggle and counts
        for label, items in buckets.items():
            bkey = f"{state}|{label}"

            cols = st.columns([0.7, 0.3])

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
                        active_bucket = None
                        state_changed = True
                    else:
                        st.session_state[open_key] = bkey
                        active_bucket = bkey
                        pending_seen[bkey] = time.time()
                        state_changed = True

                    if state_changed and not st.session_state.get(rerun_guard_key, False):
                        st.session_state[rerun_guard_key] = True
                        _safe_rerun()
                        return

            last_seen = float(bucket_lastseen.get(bkey, 0.0))
            new_count = sum(1 for x in items if x.get("timestamp",0.0) > last_seen)
            total_remaining_new += new_count

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

            # List items if this bucket is open
            if st.session_state.get(open_key) == bkey:
                for a in items:
                    is_new = a.get("timestamp",0.0) > last_seen
                    prefix = "[NEW] " if is_new else ""
                    title  = _norm(a.get("title",""))
                    region = _norm(a.get("region",""))
                    link   = _norm(a.get("link"))
                    if title and link:
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

    st.session_state[f"{feed_key}_remaining_new_total"] = int(max(0, total_remaining_new or 0))

    if did_close_toggle and not st.session_state.get(rerun_guard_key, False):
        st.session_state[rerun_guard_key] = True
        _safe_rerun()

# ============================================================
# UK (Met Office)
# ============================================================

def render_uk_grouped(entries, conf):
    """
    Render UK like BOM:
      Region header
        → flat list of items
    Uses a single feed-level last_seen_time.
    No 'Mark all as seen' or per-bucket logic.
    """
    feed_key = conf.get("key", "uk")
    items = _as_list(entries)
    if not items:
        render_empty_state()
        return

    for e in items:
        e["timestamp"] = _to_ts(e.get("published"))
        e["region"] = _norm(e.get("region") or "Unknown")
        e["bucket"] = _norm(e.get("bucket") or e.get("title") or "Alert")
    items.sort(key=lambda x: x["timestamp"], reverse=True)

    # Last seen is a single timestamp
    last_seen = float(st.session_state.get(f"{feed_key}_last_seen_time") or 0.0)

    # Group by region
    groups = OrderedDict()
    for e in items:
        groups.setdefault(e["region"], []).append(e)

    any_rendered = False
    for region, alerts in groups.items():
        if not alerts:
            continue
        any_rendered = True

        # Stripe region if any alert is NEW
        region_header = _stripe_wrap(
            f"<h2>{html.escape(region)}</h2>",
            any(a.get("timestamp",0.0) > last_seen for a in alerts)
        )
        st.markdown(region_header, unsafe_allow_html=True)

        for a in alerts:
            is_new = a.get("timestamp",0.0) > last_seen
            prefix = "[NEW] " if is_new else ""
            title  = a.get("bucket") or _norm(a.get("title",""))
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
# CMA renderer
# ============================================================

# Add explicit colors for all CMA levels (incl. Blue as CMA uses it)
CMA_COLORS = {
    'Yellow': '#FFD400',  # bright yellow
    'Orange': '#FF7F00',  # orange
    'Red':    '#E60026',  # red
    'Blue':   '#1E90FF',  # (optional) blue alerts
}

def render_cma(item, conf):
    """
    CMA item renderer. Shows left stripe on the title line if item['is_new'] True.
    """
    is_new = bool(item.get("is_new"))
    title  = _norm(item.get('title',''))
    level  = _norm(item.get('level', ''))
    bullet_color = CMA_COLORS.get(level, '#888')  # default to gray if unknown/missing

    title_html = (
        f"<div><span style='color:{bullet_color};font-size:18px;'>&#9679;</span> "
        f"<strong>{html.escape(title)}</strong></div>"
    )
    st.markdown(_stripe_wrap(title_html, is_new), unsafe_allow_html=True)

    region = _norm(item.get('region', ''))
    if region:
        st.caption(f"Region: {region}")

    if item.get('summary'):
        st.markdown(item['summary'])

    link = _norm(item.get('link'))
    if link and title:
        st.markdown(f"[Read more]({link})")

    published = _to_utc_label(item.get('published'))
    if published:
        st.caption(f"Published: {published}")

    st.markdown('---')

# ============================================================
# Meteoalarm renderer
# ============================================================

def render_meteoalarm(item, conf):
    """
    Render a single Meteoalarm country block.
    Stripe the country header if any alert (today/tomorrow) is marked is_new.
    The main app sets e['is_new'] per alert using the alert_id snapshot.
    """
    def _any_new(country) -> bool:
        alerts_dict = (country.get("alerts") or {})
        for day in ("today", "tomorrow"):
            for e in alerts_dict.get(day, []) or []:
                if e.get("is_new"):
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
            d = by_day.get(day)
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
        alerts = (item.get("alerts", {}) or {}).get(day, [])
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
                prefix = "[NEW] " if e.get("is_new") else ""

                n = _day_level_type_count(day, level, typ)
                count_str = f" ({n} active)" if isinstance(n, int) and n > 0 else ""

                text = f"{prefix}[{level}] {typ}{count_str} – {dt1} to {dt2}"
                st.markdown(
                    f"<div style='margin-bottom:6px;'>"
                    f"<span style='color:{color};font-size:16px;'>&#9679;</span> {text}</div>"
                    ,
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
# BOM grouped renderer
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
    Computes is_new inside the renderer using a last-seen timestamp in session_state.
    """
    feed_key = conf.get("key", "bom")
    entries = _as_list(entries)

    # 1) attach timestamps & sort
    for e in entries:
        e["timestamp"] = _to_ts(e.get("published"))
    entries.sort(key=lambda x: x["timestamp"], reverse=True)

    # 2) mark new vs last seen
    last_seen = float(st.session_state.get(f"{feed_key}_last_seen_time") or 0.0)
    for e in entries:
        e["is_new"] = e["timestamp"] > last_seen

    # 3) group by state
    groups = OrderedDict()
    for e in entries:
        st_name = _norm(e.get("state",""))
        groups.setdefault(st_name, []).append(e)

    # 4) render in desired order, skipping empties
    any_rendered = False
    for state in _BOM_ORDER:
        alerts = groups.get(state, [])
        if not alerts:
            continue
        any_rendered = True

        # Stripe the state header if any alert is NEW
        state_header = _stripe_wrap(
            f"<h2>{html.escape(state)}</h2>",
            any(a.get("is_new") for a in alerts)
        )
        st.markdown(state_header, unsafe_allow_html=True)

        for a in alerts:
            prefix = "[NEW] " if a.get("is_new") else ""
            title = _norm(a.get('title',''))
            link = _norm(a.get('link'))
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

    # 5) snapshot last seen
    st.session_state[f"{feed_key}_last_seen_time"] = time.time()

# ============================================================
# JMA grouped renderer
# ============================================================

JMA_COLORS = {'Warning': '#FF7F00', 'Emergency': '#E60026'}

def render_jma_grouped(entries, conf):
    """
    Grouped renderer for JMA feed.
    Computes is_new inside the renderer using a last-seen timestamp in session_state.
    """
    entries = _as_list(entries)
    if not entries:
        render_empty_state()
        return

    # 1) attach timestamps & sort newest→oldest
    for e in entries:
        e["timestamp"] = _to_ts(e.get("published"))
    entries.sort(key=lambda x: x["timestamp"], reverse=True)

    # 2) mark new vs last seen (per entry)
    last_seen = float(st.session_state.get(f"{conf['key']}_last_seen_time") or 0.0)
    for e in entries:
        e["is_new"] = e["timestamp"] > last_seen

    # 3) group by region
    groups = OrderedDict()
    for e in entries:
        region = _norm(e.get("region", "")) or "(Unknown Region)"
        groups.setdefault(region, []).append(e)

    # 4) render each region with deduped titles + colored bullets
    any_rendered = False
    for region, alerts in groups.items():
        if not alerts:
            continue
        any_rendered = True

        region_header = _stripe_wrap(
            f"<h2>{html.escape(region)}</h2>",
            any(a.get("is_new") for a in alerts)
        )
        st.markdown(region_header, unsafe_allow_html=True)

        # title -> is_new_any
        title_new_map = OrderedDict()
        for a in alerts:
            t = _norm(a.get("title", ""))
            if not t:
                continue
            title_new_map[t] = title_new_map.get(t, False) or bool(a.get("is_new"))

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
        ts = newest.get("timestamp", 0.0)
        if ts:
            st.caption(f"Published: {_fmt_utc(ts)}")
        link = _norm(newest.get("link"))
        if link:
            st.markdown(f"[Read more]({link})")

        st.markdown("---")

    if not any_rendered:
        render_empty_state()

    st.session_state[f"{conf['key']}_last_seen_time"] = time.time()

# ============================================================
# PAGASA renderer
# ============================================================

def render_pagasa(item, conf):
    """
    PAGASA renderer with colored bullets:
      - Severe   -> red (#E60026)
      - Moderate -> amber (#FF7F00)
    Uses existing helpers: _stripe_wrap, _norm, _to_utc_label
    """
    severity = (_norm(item.get("severity")) or "").title()
    color = "#E60026" if severity == "Severe" else "#FF7F00"  # amber for Moderate

    # Title line with colored bullet
    title = _norm(item.get("title") or item.get("bucket") or "PAGASA Alert")
    title_html = f"<div><span style='color:{color};font-size:16px;'>&#9679;</span> <strong>{html.escape(title)}</strong></div>"

    # If the generic pipeline computed is_new (feed-level path), stripe it; otherwise just render
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
}
