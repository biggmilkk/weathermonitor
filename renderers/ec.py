# renderers/ec.py
import html
import re
import time
from collections import OrderedDict

import streamlit as st
from dateutil import parser as dateparser

from computation import (
    attach_timestamp,
    sort_newest,
    ec_bucket_from_title,
)

# ============================================================
# Helpers
# ============================================================

def _to_utc_label(pub: str | None) -> str | None:
    if not pub:
        return None
    try:
        dt = dateparser.parse(pub)
        if dt:
            return dt.astimezone().strftime("%a, %d %b %y %H:%M:%S UTC")
    except Exception:
        pass
    return pub

def _norm(s: str | None) -> str:
    return (s or "").strip()

def _stripe_wrap(content: str, is_new: bool) -> str:
    if not is_new:
        return content
    return (
        "<div style='border-left:4px solid #e40000;"
        "padding-left:10px;margin:8px 0;'>"
        f"{content}</div>"
    )

def _safe_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

def render_empty_state():
    st.info("No active warnings that meet thresholds at the moment.")

def _entry_title(e: dict) -> str:
    return _norm(
        e.get("title")
        or e.get("headline")
        or e.get("name")
        or e.get("summary")
    )

def _entry_province(e: dict) -> str:
    return _norm(
        e.get("province_name")
        or e.get("province")
        or e.get("region")
    ) or "Unknown"

def _entry_area(e: dict) -> str:
    return _norm(
        e.get("region")
        or e.get("area")
        or e.get("location")
        or e.get("zone")
    )

def _title_bucket_specific(title: str) -> str | None:
    """
    Pretty display label only.
    Examples:
      - Yellow Warning - Snowfall
      - Orange Warning - Wind
      - Red Warning - Rain
    """
    t = _norm(title)
    if not t:
        return None

    tl = t.lower()

    severity = None
    if "red" in tl:
        severity = "Red"
    elif "orange" in tl:
        severity = "Orange"
    elif "yellow" in tl:
        severity = "Yellow"

    type_name = None

    # Pattern 1: "YELLOW WARNING - SNOWFALL"
    m = re.search(r"\b(red|orange|yellow)\s+warning\s*[-:]\s*([a-z /-]+)\b", tl, flags=re.IGNORECASE)
    if m:
        severity = m.group(1).title()
        type_name = m.group(2)

    # Pattern 2: "Yellow Snowfall Warning"
    if not type_name:
        m = re.search(r"\b(red|orange|yellow)\s+([a-z /-]+?)\s+warning\b", tl, flags=re.IGNORECASE)
        if m:
            severity = m.group(1).title()
            type_name = m.group(2)

    # Pattern 3: "Snowfall warning"
    if not type_name:
        m = re.search(r"\b([a-z /-]+?)\s+warning\b", tl, flags=re.IGNORECASE)
        if m:
            type_name = m.group(1)

    if type_name:
        type_name = re.sub(r"\s+", " ", type_name).strip(" -:/").title()

        replacements = {
            "Rainfall": "Rain",
            "Snowfall": "Snowfall",
            "Thunderstorm": "Thunderstorm",
            "Wind": "Wind",
            "Blizzard": "Blizzard",
            "Heat": "Heat",
            "Extreme Cold": "Extreme Cold",
            "Freezing Rain": "Freezing Rain",
            "Snow Squall": "Snow Squall",
            "Special Weather": "Special Weather",
        }
        type_name = replacements.get(type_name, type_name)

    if severity and type_name:
        return f"{severity} Warning - {type_name}"

    # pretty fallback
    generic = ec_bucket_from_title(t)
    return generic or "Weather Warning"

# ============================================================
# Province ordering
# ============================================================

_PROVINCE_ORDER = [
    "Alberta", "British Columbia", "Manitoba", "New Brunswick",
    "Newfoundland and Labrador", "Northwest Territories", "Nova Scotia",
    "Nunavut", "Ontario", "Prince Edward Island", "Quebec",
    "Saskatchewan", "Yukon",
]

# ============================================================
# EC Grouped Compact Renderer
# ============================================================

def render(entries, conf):
    """
    Grouped compact renderer for Environment Canada.

    Important:
      - bucket_key stays generic and stable for seen-state + main badge logic
      - bucket_label is specific and user-friendly for display
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
    st.session_state.setdefault(f"{feed_key}_remaining_new_total", 0)

    active_bucket   = st.session_state[open_key]
    pending_seen    = st.session_state[pending_map_key]
    bucket_lastseen = st.session_state[lastseen_key]

    items = sort_newest(attach_timestamp(entries or []))

    filtered = []
    for e in items:
        title_txt = _entry_title(e)

        # stable key used everywhere else
        bucket_key = ec_bucket_from_title(title_txt)
        if not bucket_key:
            continue

        # nicer display label just for UI
        bucket_label = _title_bucket_specific(title_txt) or bucket_key

        prov_name = _entry_province(e)
        d = dict(
            e,
            bucket_key=bucket_key,
            bucket_label=bucket_label,
            province_name=prov_name,
            bkey=f"{prov_name}|{bucket_key}",   # <-- stable key
        )
        filtered.append(d)

    if not filtered:
        render_empty_state()
        return

    cols_actions = st.columns([1, 6])
    with cols_actions[0]:
        if st.button("Mark all as seen", key=f"{feed_key}_mark_all_seen"):
            now_ts = time.time()
            for a in filtered:
                bucket_lastseen[a["bkey"]] = now_ts
            pending_seen.clear()
            st.session_state[open_key] = None
            st.session_state[lastseen_key] = bucket_lastseen
            st.session_state[pending_map_key] = pending_seen
            st.session_state[f"{feed_key}_remaining_new_total"] = 0
            _safe_rerun()
            return

    groups: OrderedDict[str, list[dict]] = OrderedDict()
    for e in filtered:
        groups.setdefault(e["province_name"], []).append(e)

    provinces = [p for p in _PROVINCE_ORDER if p in groups] + [
        p for p in groups if p not in _PROVINCE_ORDER
    ]

    for prov in provinces:
        alerts = groups.get(prov, []) or []
        if not alerts:
            continue

        def _prov_has_new() -> bool:
            for a in alerts:
                last_seen = float(bucket_lastseen.get(a["bkey"], 0.0))
                if float(a.get("timestamp") or 0.0) > last_seen:
                    return True
            return False

        st.markdown(
            _stripe_wrap(f"<h2>{html.escape(prov)}</h2>", _prov_has_new()),
            unsafe_allow_html=True
        )

        # group by stable key, but keep first display label
        buckets: OrderedDict[str, dict] = OrderedDict()
        for a in alerts:
            bk = a["bucket_key"]
            if bk not in buckets:
                buckets[bk] = {
                    "label": a["bucket_label"],
                    "items": [],
                }
            buckets[bk]["items"].append(a)

        def _bucket_sort_key(label: str):
            ll = _norm(label).lower()
            if ll.startswith("red"):
                sev_rank = 0
            elif ll.startswith("orange"):
                sev_rank = 1
            elif ll.startswith("yellow"):
                sev_rank = 2
            else:
                sev_rank = 3
            return (sev_rank, ll)

        bucket_keys = sorted(
            buckets.keys(),
            key=lambda bk: _bucket_sort_key(buckets[bk]["label"])
        )

        for bucket_key in bucket_keys:
            label = buckets[bucket_key]["label"]
            bucket_items = buckets[bucket_key]["items"]
            bkey = f"{prov}|{bucket_key}"   # <-- stable key again
            cols = st.columns([0.7, 0.3])

            with cols[0]:
                clicked = st.button(label, key=f"{feed_key}:{bkey}:btn", use_container_width=True)

                if clicked:
                    state_changed = False
                    prev = active_bucket

                    # switching buckets: commit previous as seen
                    if prev and prev != bkey:
                        ts_opened_prev = float(pending_seen.pop(prev, time.time()))
                        bucket_lastseen[prev] = ts_opened_prev
                        st.session_state[lastseen_key] = bucket_lastseen
                        st.session_state[pending_map_key] = pending_seen

                    if active_bucket == bkey:
                        # closing same bucket: commit this bucket as seen
                        ts_opened = float(pending_seen.pop(bkey, time.time()))
                        bucket_lastseen[bkey] = ts_opened
                        st.session_state[lastseen_key] = bucket_lastseen
                        st.session_state[pending_map_key] = pending_seen
                        st.session_state[open_key] = None
                        active_bucket = None
                        state_changed = True
                    else:
                        # opening new bucket: start pending timer only
                        st.session_state[open_key] = bkey
                        pending_seen[bkey] = time.time()
                        st.session_state[pending_map_key] = pending_seen
                        active_bucket = bkey
                        state_changed = True

                    if state_changed and not st.session_state.get(rerun_guard_key, False):
                        st.session_state[rerun_guard_key] = True
                        _safe_rerun()
                        return

            last_seen = float(bucket_lastseen.get(bkey, 0.0))
            new_count = sum(1 for x in bucket_items if float(x.get("timestamp") or 0.0) > last_seen)

            with cols[1]:
                active_count = len(bucket_items)
                badges_html = (
                    "<span style='margin-left:6px;padding:2px 6px;"
                    "border-radius:4px;background:#eef0f3;color:#000;font-size:0.9em;"
                    "font-weight:600;display:inline-block;'>"
                    f"{active_count} Active</span>"
                )
                if new_count > 0:
                    badges_html += (
                        "<span style='margin-left:8px;padding:2px 6px;"
                        "border-radius:4px;background:#FFEB99;color:#000;font-size:0.9em;"
                        "font-weight:bold;display:inline-block;'>"
                        f"❗ {new_count} New</span>"
                    )
                st.markdown(badges_html, unsafe_allow_html=True)

            if st.session_state.get(open_key) == bkey:
                for a in bucket_items:
                    is_new = float(a.get("timestamp") or 0.0) > last_seen
                    prefix = "[NEW] " if is_new else ""
                    title  = _entry_title(a) or "(no title)"
                    area   = _entry_area(a)

                    heading = f"{prefix}<strong>{html.escape(title)}</strong>"
                    if area:
                        heading += f"<br><span style='opacity:0.85;'>Location: {html.escape(area)}</span>"

                    st.markdown(
                        _stripe_wrap(heading, is_new),
                        unsafe_allow_html=True
                    )

                    pub_label = _to_utc_label(a.get("published"))
                    if pub_label:
                        st.caption(f"Published: {pub_label}")

                    link = _norm(a.get("link"))
                    if link:
                        st.markdown(f"[Read more]({link})")

                    st.markdown("---")

        st.markdown("---")
