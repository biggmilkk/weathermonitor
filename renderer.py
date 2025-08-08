import streamlit as st
from dateutil import parser as dateparser
from collections import OrderedDict
import time

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
            # Parse with dateparser to handle timezone offsets
            dt_obj = dateparser.parse(published)
            if dt_obj:
                # Convert to UTC
                dt_obj_utc = dt_obj.astimezone(datetime.timezone.utc)
                published_str = dt_obj_utc.strftime("%a, %d %b %y %H:%M:%S UTC")
                st.caption(f"Published: {published_str}")
            else:
                st.caption(f"Published: {published}")
        except Exception:
            st.caption(f"Published: {published}")

    st.markdown('---')

# ---------- EC renderer ----------

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

def _fmt_utc(ts: float) -> str:
    # Fri, 08 Aug 25 13:09:00 UTC
    return time.strftime("%a, %d %b %y %H:%M:%S UTC", time.gmtime(ts))

def render_jma_grouped(entries, conf):
    """
    Group JMA items by region and list warnings under the region header.
    Show [NEW] in front of titles that have any entry newer than last_seen_time.
    """
    if not entries:
        return

    # Coerce single dict → list[dict]
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

    # 4) render each region with de-duped titles, but keep "new if any"
    for region, alerts in groups.items():
        # red bar if any new in this region
        if any(a.get("is_new") for a in alerts):
            st.markdown(
                "<div style='height:4px;background:red;margin:8px 0;'></div>",
                unsafe_allow_html=True
            )

        st.markdown(f"## {region}")

        # Build ordered mapping: title -> is_new_any
        title_new_map = OrderedDict()
        for a in alerts:
            t = a.get("title", "").strip()
            if not t:
                continue
            is_new_any = title_new_map.get(t, False) or bool(a.get("is_new"))
            title_new_map[t] = is_new_any

        # List warnings with [NEW] where appropriate
        for t, is_new_any in title_new_map.items():
            prefix = "[NEW] " if is_new_any else ""
            st.markdown(f"{prefix}{t}")

        # Use newest alert in this region for published/link
        newest = alerts[0]
        ts = newest.get("timestamp", 0.0)
        if ts:
            st.caption(f"Published: {time.strftime('%a, %d %b %y %H:%M:%S UTC', time.gmtime(ts))}")
        if newest.get("link"):
            st.markdown(f"[Read more]({newest['link']})")

        st.markdown("---")

    # 5) snapshot last seen time so subsequent refreshes only mark newer
    st.session_state[f"{conf['key']}_last_seen_time"] = time.time()

# ---------- Renderer Registry ----------
RENDERERS = {
    'json': render_json,
    'ec_async': render_ec,
    'ec_grouped': render_ec_grouped,
    'rss_cma': render_cma,
    'rss_meteoalarm': render_meteoalarm,
    'rss_bom_multi': render_bom_grouped,
    'rss_jma': render_jma_grouped,
}
