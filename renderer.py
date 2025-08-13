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

# Keep ONLY these warning buckets (case-insensitive match against title)
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
    """
    Return canonical warning bucket if the title contains one of EC_WARNING_TYPES.
    Otherwise return None (so the item can be filtered out).
    """
    if not title:
        return None
    t = title.lower()
    for w in _EC_WARNING_TYPES_LC:
        if w in t:
            return _EC_WARNING_CANON[w]
    return None

# --- helper for stable per-entry IDs (title|region|published is stable enough for EC)
def _ec_entry_id(e) -> str:
    return f"{e.get('title','')}|{e.get('region','')}|{e.get('published','')}"

def render_ec_grouped_compact(entries, conf):
    """
    Province → Warning Type summary with toggles per type (no chips).
    Filters to ONLY the provided warning types.

    NEW behavior:
      - While a bucket is OPEN, keep [NEW] visible so users can see what's new.
      - When a bucket transitions from OPEN → CLOSED, mark its items as seen.
    """
    feed_key = conf.get("key", "ec")

    # ensure seen-id set exists
    seen_key = f"{feed_key}_seen_ids"
    if seen_key not in st.session_state:
        st.session_state[seen_key] = set()
    seen_ids = st.session_state[seen_key]

    # 1) attach timestamps & sort newest→oldest
    for e in entries:
        try:
            e_ts = dateparser.parse(e.get("published", "")).timestamp()
        except Exception:
            e_ts = 0.0
        e["timestamp"] = e_ts
    entries.sort(key=lambda x: x["timestamp"], reverse=True)

    # 2) annotate canonical bucket; FILTER to warnings only; figure 'is_new' via seen_ids
    filtered = []
    for e in entries:
        bucket = _ec_bucket_from_title(e.get("title", ""))
        if not bucket:
            continue  # drop non-warning types (watches/advisories/statements/etc.)
        e["bucket"] = bucket
        e["_id"] = _ec_entry_id(e)
        e["is_new"] = e["_id"] not in seen_ids
        filtered.append(e)

    if not filtered:
        st.info("No active warnings at the moment.")
        return

    # 3) group by province name
    groups = OrderedDict()
    for e in filtered:
        code = e.get("province", "")
        prov_name = _PROVINCE_NAMES.get(code, code) if isinstance(code, str) else str(code)
        groups.setdefault(prov_name, []).append(e)

    # 4) province render order: canonical first, then any extras
    provinces = [p for p in _PROVINCE_ORDER if p in groups] + [
        p for p in groups.keys() if p not in _PROVINCE_ORDER
    ]

    for prov in provinces:
        alerts = groups.get(prov, [])
        if not alerts:
            continue

        # compute NEW at province level based on seen_ids
        has_new = any(a.get("is_new") for a in alerts)
        if has_new:
            st.markdown(
                "<div style='height:4px;background:red;margin:8px 0;'></div>",
                unsafe_allow_html=True
            )
        st.markdown(f"## {prov}")

        # 4a) bucket by canonical warning type (NO chips; show as toggles)
        buckets = OrderedDict()
        for a in alerts:
            buckets.setdefault(a["bucket"], []).append(a)

        for label, items in buckets.items():
            # items already newest→oldest due to global sort
            new_count = sum(1 for x in items if x.get("is_new"))
            hdr = f"{label} ({len(items)})" + (f" — {new_count} new" if new_count else "")
            exp_key = f"{feed_key}:{prov}:{label}:open"
            prev_key = f"{exp_key}:prev"

            opened = st.checkbox(hdr, key=exp_key, value=False)
            prev_open = st.session_state.get(prev_key, False)

            if opened:
                # Render list WITH [NEW] badges (we do NOT clear them while open)
                for a in items:
                    prefix = "[NEW] " if a.get("is_new") else ""
                    title = a.get("title", "")
                    region = a.get("region", "")
                    link = a.get("link")

                    # Title line (linked if available)
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
                            # normalize to UTC display
                            published_display = dt_obj.astimezone(timezone.utc).strftime("%a, %d %b %y %H:%M:%S UTC")
                        except Exception:
                            published_display = pub
                        st.caption(f"Published: {published_display}")

                    st.markdown("---")
            else:
                # If it was previously open and now closed, mark items as seen
                if prev_open:
                    bucket_ids = {x["_id"] for x in items}
                    if bucket_ids - seen_ids:
                        seen_ids.update(bucket_ids)

            # update previous open state
            st.session_state[prev_key] = opened

        st.markdown("---")


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
                    # Format: Aug 07 22:00 UTC
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
