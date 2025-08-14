import streamlit as st
from dateutil import parser as dateparser
from collections import OrderedDict
import time
from datetime import timezone
import re

# ---------- Generic JSON/NWS renderer ----------

def render_json(item, conf):
    title = item.get('title') or item.get('headline') or '(no title)'
    st.markdown(f"**{title}**")

    region = item.get('region', '')
    province = item.get('province', '')
    if region or province:
        parts = [r for r in [region, province] if r]
        st.caption(f"Region: {', '.join(parts)}")

    body = item.get('summary') or item.get('description') or ''
    if body:
        st.markdown(body)

    link = item.get('link')
    if link:
        st.markdown(f"[Read more]({link})")

    published = item.get('published')
    if published:
        try:
            dt_obj = dateparser.parse(published)
            if dt_obj:
                dt_obj_utc = dt_obj.astimezone(timezone.utc)
                published_str = dt_obj_utc.strftime("%a, %d %b %y %H:%M:%S UTC")
                st.caption(f"Published: {published_str}")
            else:
                st.caption(f"Published: {published}")
        except Exception:
            st.caption(f"Published: {published}")

    st.markdown('---')


# ---------- EC renderer (simple, unchanged) ----------

def render_ec(item, conf):
    st.markdown(f"**{item.get('title','')}**")
    region = item.get('region','')
    province = item.get('province','')
    if region or province:
        st.caption(f"Region: {region}, {province}")
    st.markdown(item.get('summary',''))
    link = item.get('link')
    if link:
        st.markdown(f"[Read more]({link})")
    published = item.get('published')
    if published:
        st.caption(f"Published: {published}")
    st.markdown('---')


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


# ---------- Compact EC renderer (Province → Warning Type → entries) ----------

# Keep ONLY these warning buckets (plus Severe Thunderstorm Watch), case-insensitive match
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
    "Severe Thunderstorm Watch",  # included per request
    "Snowfall Warning",
    "Snow Squall Warning",
    "Tornado Warning",
    "Tropical Storm Warning",
    "Tsunami Warning",
    "Weather Warning",
    "Wind Warning",
    "Winter Storm Warning",
]

_EC_WARNING_TYPES_LC = [w.lower() for w in EC_WARNING_TYPES]
_EC_WARNING_CANON = {w.lower(): w for w in EC_WARNING_TYPES}

def _ec_bucket_from_title(title: str) -> str | None:
    """Return canonical warning bucket if the title contains one of EC_WARNING_TYPES."""
    if not title:
        return None
    t = title.lower()
    for w in _EC_WARNING_TYPES_LC:
        if w in t:
            return _EC_WARNING_CANON[w]
    return None

def _ec_entry_ts(e) -> float:
    try:
        return dateparser.parse(e.get("published","")).timestamp()
    except Exception:
        return 0.0

# --- EC helpers exported for main app (warnings-only math matches buckets) ---

def ec_bucket_from_title(title: str) -> str | None:
    """Public alias so main app (or others) can reuse bucket logic if needed."""
    return _ec_bucket_from_title(title)

def ec_remaining_new_total(feed_key: str, entries: list) -> int:
    """
    Total remaining NEW across all *warning* buckets for EC, using the per-bucket
    last_seen map maintained by the compact EC renderer:
      st.session_state[f"{feed_key}_bucket_last_seen"]  (bkey = "Province|Warning Type")
    """
    lastseen_map = st.session_state.get(f"{feed_key}_bucket_last_seen", {}) or {}
    total = 0
    for e in entries:
        bucket = _ec_bucket_from_title(e.get("title", ""))
        if not bucket:
            continue  # ignore non-warnings
        code = e.get("province", "")
        prov_name = _PROVINCE_NAMES.get(code, code) if isinstance(code, str) else str(code)
        bkey = f"{prov_name}|{bucket}"
        last_seen = float(lastseen_map.get(bkey, 0.0))
        ts = _ec_entry_ts(e)
        if ts > last_seen:
            total += 1
    return int(total)

def render_ec_grouped_compact(entries, conf):
    """
    Province → Warning Type summary using BUTTONS (no arrows).
    - While OPEN: do NOT advance last_seen; bucket badge + [NEW] stay visible.
    - On CLOSE: set last_seen to the time it was opened (pending), clearing the NEWs.
    - Writes aggregate NEW to st.session_state['{feed_key}_remaining_new_total'].
    - Shows an ACTIVE count badge per bucket so users can see totals without opening.
    """
    feed_key = conf.get("key", "ec")

    # ---- safe rerun helper (Streamlit >=1.31 uses st.rerun) ----
    def _safe_rerun():
        if hasattr(st, "rerun"):
            st.rerun()
        elif hasattr(st, "experimental_rerun"):
            st.experimental_rerun()

    open_key        = f"{feed_key}_active_bucket"           # current bkey or None
    pending_map_key = f"{feed_key}_bucket_pending_seen"     # bkey -> float (when it was opened)
    lastseen_key    = f"{feed_key}_bucket_last_seen"        # bkey -> float (committed "seen up to")
    rerun_guard_key = f"{feed_key}_rerun_guard"             # prevent infinite loop

    # Clear guard at the start of a normal render
    if st.session_state.get(rerun_guard_key):
        st.session_state.pop(rerun_guard_key, None)

    st.session_state.setdefault(open_key, None)
    st.session_state.setdefault(pending_map_key, {})
    st.session_state.setdefault(lastseen_key, {})

    active_bucket   = st.session_state[open_key]
    pending_seen    = st.session_state[pending_map_key]
    bucket_lastseen = st.session_state[lastseen_key]
    
    # Attach timestamps & sort newest→oldest
    for e in entries:
        try:
            e["timestamp"] = dateparser.parse(e.get("published","")).timestamp()
        except Exception:
            e["timestamp"] = 0.0
    entries.sort(key=lambda x: x["timestamp"], reverse=True)

    # Filter to warnings/watch buckets and assign bucket
    filtered = []
    for e in entries:
        bucket = _ec_bucket_from_title(e.get("title",""))
        if not bucket:
            continue
        e["bucket"] = bucket
        filtered.append(e)

    if not filtered:
        st.info("No active warnings at the moment.")
        st.session_state[f"{feed_key}_remaining_new_total"] = 0
        return

    # Group by province
    groups = OrderedDict()
    for e in filtered:
        code = e.get("province","")
        prov_name = _PROVINCE_NAMES.get(code, code) if isinstance(code, str) else str(code)
        groups.setdefault(prov_name, []).append(e)

    provinces = [p for p in _PROVINCE_ORDER if p in groups] + [p for p in groups if p not in _PROVINCE_ORDER]

    total_remaining_new = 0
    did_close_toggle    = False  # rerun only if we closed something

    for prov in provinces:
        alerts = groups.get(prov, [])
        if not alerts:
            continue

        # Red bar if any bucket in province has NEW
        def _prov_has_new():
            for a in alerts:
                bkey = f"{prov}|{a['bucket']}"
                if a.get("timestamp",0.0) > float(bucket_lastseen.get(bkey, 0.0)):
                    return True
            return False

        if _prov_has_new():
            st.markdown(
                "<div style='height:4px;background:red;margin:8px 0;'></div>",
                unsafe_allow_html=True
            )
        st.markdown(f"## {prov}")

        # Bucket by warning type
        buckets = OrderedDict()
        for a in alerts:
            buckets.setdefault(a["bucket"], []).append(a)

        for label, items in buckets.items():
            bkey = f"{prov}|{label}"

            cols = st.columns([0.7, 0.3])

            # --- HANDLE CLICK FIRST ---
            with cols[0]:
                if st.button(label, key=f"{feed_key}:{bkey}:btn", use_container_width=True):
                    if active_bucket == bkey:
                        # CLOSE: commit last_seen to when it was opened (or now)
                        ts_opened = float(pending_seen.pop(bkey, time.time()))
                        bucket_lastseen[bkey] = ts_opened
                        st.session_state[open_key] = None
                        active_bucket = None
                        did_close_toggle = True
                    else:
                        # OPEN: set active + remember "opened at" in pending; DO NOT change last_seen
                        st.session_state[open_key] = bkey
                        active_bucket = bkey
                        pending_seen[bkey] = time.time()

            # Compute NEW vs committed last_seen (unchanged while open)
            last_seen = float(bucket_lastseen.get(bkey, 0.0))
            new_count = sum(1 for x in items if x.get("timestamp",0.0) > last_seen)
            total_remaining_new += new_count

            # --- BADGES (right side): ACTIVE (always) + NEW (if >0) ---
            with cols[1]:
                # Active count badge (neutral)
                active_count = len(items)
                st.markdown(
                    "<span style='margin-left:6px;padding:2px 6px;"
                    "border-radius:4px;background:#eef0f3;color:#000;font-size:0.9em;"
                    "font-weight:600;display:inline-block;'>"
                    f"{active_count} Active</span>",
                    unsafe_allow_html=True,
                )
                # New count badge (attention)
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
                    is_new = a.get("timestamp",0.0) > last_seen
                    prefix = "[NEW] " if is_new else ""
                    title  = a.get("title","")
                    region = a.get("region","")
                    link   = a.get("link")
                    if link:
                        st.markdown(f"{prefix}**[{title}]({link})**")
                    else:
                        st.markdown(f"{prefix}**{title}**")
                    if region:
                        st.caption(f"Region: {region}")
                    pub = a.get("published")
                    if pub:
                        try:
                            dt_obj = dateparser.parse(pub)
                            published_display = dt_obj.astimezone(timezone.utc).strftime("%a, %d %b %y %H:%M:%S UTC")
                        except Exception:
                            published_display = pub
                        st.caption(f"Published: {published_display}")
                    st.markdown("---")

        st.markdown("---")

    # Aggregate NEW total (matches what you see in badges right now)
    st.session_state[f"{feed_key}_remaining_new_total"] = int(total_remaining_new)

    # One-shot rerun only on CLOSE so the top-row EC badge updates immediately
    if did_close_toggle and not st.session_state.get(rerun_guard_key, False):
        st.session_state[rerun_guard_key] = True
        _safe_rerun()

# ---------- Original EC grouped renderer (kept, unchanged) ----------

def render_ec_grouped(entries, conf):
    """
    Grouped, ordered renderer for Environment Canada feeds.
    """
    feed_key = conf.get("key", "ec")

    # 1) attach timestamps & sort
    for e in entries:
        try:
            e_ts = dateparser.parse(e.get("published", "")).timestamp()
        except Exception:
            e_ts = 0.0
        e["timestamp"] = e_ts
    entries.sort(key=lambda x: x["timestamp"], reverse=True)

    # 2) mark new vs last seen
    last_seen = st.session_state.get(f"{feed_key}_last_seen_time") or 0.0
    for e in entries:
        e["is_new"] = e["timestamp"] > last_seen

    # 3) group by full province name
    groups = OrderedDict()
    for e in entries:
        code = e.get("province", "")
        name = _PROVINCE_NAMES.get(code, code)
        groups.setdefault(name, []).append(e)

    # 4) render each province in order, hiding empties
    for prov in _PROVINCE_ORDER:
        alerts = groups.get(prov, [])
        if not alerts:
            continue
        if any(a.get("is_new") for a in alerts):
            st.markdown(
                "<div style='height:4px;background:red;margin:8px 0;'></div>",
                unsafe_allow_html=True
            )
        st.markdown(f"## {prov}")
        for a in alerts:
            prefix = "[NEW] " if a.get("is_new") else ""
            st.markdown(f"{prefix}**{a.get('title','')}**")
            if a.get("region"):
                st.caption(f"Region: {a['region']}")
            if a.get("published"):
                try:
                    dt_obj = dateparser.parse(a['published'])
                    published_display = dt_obj.strftime("%a, %d %b %y %H:%M:%S UTC")
                except Exception:
                    published_display = a['published']
                st.caption(f"Published: {published_display}")
            if a.get("link"):
                st.markdown(f"[Read more]({a['link']})")
        st.markdown("---")

    # 5) snapshot last seen
    st.session_state[f"{feed_key}_last_seen_time"] = time.time()


# ---------- CMA renderer ----------

CMA_COLORS = {'Orange': '#FF7F00', 'Red': '#E60026'}

def render_cma(item, conf):
    level = item.get('level', 'Orange')
    color = CMA_COLORS.get(level, '#888')

    st.markdown(
        f"<div style='margin-bottom:8px;'>"
        f"<span style='color:{color};font-size:18px;'>&#9679;</span> "
        f"<strong>{item.get('title','')}</strong></div>",
        unsafe_allow_html=True
    )

    region = item.get('region', '')
    if region:
        st.caption(f"Region: {region}")

    st.markdown(item.get('summary', ''))

    link = item.get('link')
    if link:
        st.markdown(f"[Read more]({link})")

    published = item.get('published')
    if published:
        # Normalize +0000 → UTC
        published_display = published.replace('+0000', 'UTC')
        st.caption(f"Published: {published_display}")

    st.markdown('---')


# ---------- Meteoalarm renderer ----------

def render_meteoalarm(item, conf):
    st.markdown(f"<h3 style='margin-bottom:4px'>{item.get('title','')}</h3>",
                unsafe_allow_html=True)
    for day in ['today', 'tomorrow']:
        alerts = item.get('alerts', {}).get(day, [])
        if alerts:
            st.markdown(f"<h4 style='margin-top:16px'>{day.capitalize()}</h4>",
                        unsafe_allow_html=True)
            for e in alerts:
                try:
                    # Format: Aug 07 %H:%M UTC
                    dt1 = dateparser.parse(e['from']).strftime('%b %d %H:%M UTC')
                    dt2 = dateparser.parse(e['until']).strftime('%b %d %H:%M UTC')
                except Exception:
                    dt1, dt2 = e.get('from', ''), e.get('until', '')

                color = {'Orange': '#FF7F00', 'Red': '#E60026'}.get(
                    e.get('level', ''), '#888'
                )
                prefix = '[NEW] ' if e.get('is_new') else ''
                text = f"{prefix}[{e.get('level','')}] {e.get('type','')} – {dt1} to {dt2}"
                st.markdown(
                    f"<div style='margin-bottom:6px;'>"
                    f"<span style='color:{color};font-size:16px;'>&#9679;</span> {text}</div>",
                    unsafe_allow_html=True
                )
    link = item.get('link')
    if link:
        st.markdown(f"[Read more]({link})")

    published = item.get('published')
    if published:
        published_display = published.replace('+0000', 'UTC')
        st.caption(f"Published: {published_display}")

    st.markdown('---')


# ---------- BOM grouped renderer ----------

_BOM_ORDER = [
    "NSW & ACT",
    "Northern Territory",
    "Queensland",
    "South Australia",
    "Tasmania",
    "Victoria",
    "West Australia",
]

def render_bom_grouped(entries, conf):
    """
    Grouped renderer for BOM multi-state feed.
    """
    feed_key = conf.get("key", "bom")

    # 1) attach timestamps & sort
    for e in entries:
        try:
            e_ts = dateparser.parse(e.get("published","")).timestamp()
        except Exception:
            e_ts = 0.0
        e["timestamp"] = e_ts
    entries.sort(key=lambda x: x["timestamp"], reverse=True)

    # 2) mark new vs last seen
    last_seen = st.session_state.get(f"{feed_key}_last_seen_time") or 0.0
    for e in entries:
        e["is_new"] = e["timestamp"] > last_seen

    # 3) group by state
    groups = OrderedDict()
    for e in entries:
        st_name = e.get("state","")
        groups.setdefault(st_name, []).append(e)

    # 4) render in desired order, skipping empties
    for state in _BOM_ORDER:
        alerts = groups.get(state, [])
        if not alerts:
            continue
        if any(a.get("is_new") for a in alerts):
            st.markdown(
                "<div style='height:4px;background:red;margin:8px 0;'></div>",
                unsafe_allow_html=True
            )
        st.markdown(f"## {state}")
        for a in alerts:
            prefix = "[NEW] " if a.get("is_new") else ""
            if a.get("link"):
                st.markdown(f"{prefix}**[{a.get('title','')}]({a['link']})**")
            else:
                st.markdown(f"{prefix}**{a.get('title','')}**")
            if a.get("summary"):
                st.write(a["summary"])
            if a.get("published"):
                st.caption(f"Published: {a['published']}")
        st.markdown("---")

    # 5) snapshot last seen
    st.session_state[f"{feed_key}_last_seen_time"] = time.time()


# ---------- JMA grouped renderer ----------

JMA_COLORS = {'Warning': '#FF7F00', 'Emergency': '#E60026'}

def _fmt_utc(ts: float) -> str:
    return time.strftime("%a, %d %b %y %H:%M:%S UTC", time.gmtime(ts))

def render_jma_grouped(entries, conf):
    if not entries:
        return

    if isinstance(entries, dict):
        entries = [entries]

    # 1) attach timestamps & sort newest→oldest
    for e in entries:
        pub = e.get("published")
        try:
            e_ts = dateparser.parse(pub).timestamp() if pub else 0.0
        except Exception:
            e_ts = 0.0
        e["timestamp"] = e_ts
    entries.sort(key=lambda x: x["timestamp"], reverse=True)

    # 2) mark new vs last seen (per entry)
    last_seen = st.session_state.get(f"{conf['key']}_last_seen_time") or 0.0
    for e in entries:
        e["is_new"] = e["timestamp"] > last_seen

    # 3) group by region
    groups = OrderedDict()
    for e in entries:
        region = e.get("region", "").strip() or "(Unknown Region)"
        groups.setdefault(region, []).append(e)

    # 4) render each region with deduped titles + colored bullets
    for region, alerts in groups.items():
        if any(a.get("is_new") for a in alerts):
            st.markdown(
                "<div style='height:4px;background:red;margin:8px 0;'></div>",
                unsafe_allow_html=True
            )
        st.markdown(f"## {region}")

        # title -> is_new_any
        title_new_map = OrderedDict()
        for a in alerts:
            t = a.get("title", "").strip()
            if not t:
                continue
            title_new_map[t] = title_new_map.get(t, False) or bool(a.get("is_new"))

        for t, is_new_any in title_new_map.items():
            # Color by level keyword in the title
            level = "Emergency" if "Emergency" in t else ("Warning" if "Warning" in t else None)
            color = JMA_COLORS.get(level, "#888")
            prefix = "[NEW] " if is_new_any else ""
            st.markdown(
                f"<div style='margin-bottom:4px;'>"
                f"<span style='color:{color};font-size:16px;'>&#9679;</span> {prefix}{t}"
                f"</div>",
                unsafe_allow_html=True
            )

        newest = alerts[0]
        ts = newest.get("timestamp", 0.0)
        if ts:
            st.caption(f"Published: {_fmt_utc(ts)}")
        if newest.get("link"):
            st.markdown(f"[Read more]({newest['link']})")

        st.markdown("---")

    st.session_state[f"{conf['key']}_last_seen_time"] = time.time()


# ---------- Renderer Registry ----------

RENDERERS = {
    'json': render_json,
    'ec_async': render_ec,
    'ec_grouped': render_ec_grouped,                 # original list view
    'ec_grouped_compact': render_ec_grouped_compact, # compact warnings-only view
    'rss_cma': render_cma,
    'rss_meteoalarm': render_meteoalarm,
    'rss_bom_multi': render_bom_grouped,
    'rss_jma': render_jma_grouped,
}
