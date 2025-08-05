import streamlit as st
from dateutil import parser as dateparser
from collections import OrderedDict
import time

# Generic JSON/NWS renderer
def render_json(item, conf):
    title = item.get('title') or item.get('headline') or '(no title)'
    st.markdown(f"**{title}**")
    region = item.get('region','')
    province = item.get('province','')
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
        st.caption(f"Published: {published}")
    st.markdown('---')

# Environment Canada simple per-item renderer
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

# --- New grouped EC renderer ---
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
    entries: list of EC alert dicts (with 'province','region','published',etc.)
    conf: original feed config dict plus 'key' for session_state markers
    """

    # 1) parse timestamps & sort newest-first
    for e in entries:
        try:
            e_ts = dateparser.parse(e["published"]).timestamp()
        except:
            e_ts = 0.0
        e["timestamp"] = e_ts
    entries.sort(key=lambda x: x["timestamp"], reverse=True)

    # 2) mark new vs last seen
    last_seen = st.session_state.get(f"{conf['key']}_last_seen_time") or 0.0
    for e in entries:
        e["is_new"] = e["timestamp"] > last_seen

    # 3) group by province
    groups = OrderedDict()
    for e in entries:
        prov = e.get("province", "")
        groups.setdefault(prov, []).append(e)

    # 4) render per‐province in desired order, hiding empties
    for prov in _PROVINCE_ORDER:
        alerts = groups.get(prov, [])
        if not alerts:
            continue
        # red bar if any new in this province
        if any(a["is_new"] for a in alerts):
            st.markdown(
                "<div style='height:4px;background:red;margin:8px 0;'></div>",
                unsafe_allow_html=True
            )
        st.markdown(f"## {prov}")
        for a in alerts:
            prefix = "[NEW] " if a["is_new"] else ""
            st.markdown(f"{prefix}**{a['title']}**")
            if a.get("region"):
                st.caption(f"Region: {a['region']}")
            st.caption(f"Published: {a['published']}")
            if a.get("link"):
                st.markdown(f"[More details]({a['link']})")
        st.markdown('---')

    # 5) snapshot last-seen timestamp
    st.session_state[f"{conf['key']}_last_seen_time"] = time.time()

# CMA China renderer
CMA_COLORS = {'Orange':'#FF7F00','Red':'#E60026'}
def render_cma(item, conf):
    level = item.get('level','Orange')
    color = CMA_COLORS.get(level,'#888')
    st.markdown(
        f"<div style='margin-bottom:8px;'>"
        f"<span style='color:{color};font-size:18px;'>&#9679;</span> "
        f"<strong>{item.get('title','')}</strong></div>",
        unsafe_allow_html=True
    )
    region = item.get('region','')
    if region:
        st.caption(f"Region: {region}")
    st.markdown(item.get('summary',''))
    link = item.get('link')
    if link:
        st.markdown(f"[Read more]({link})")
    published = item.get('published')
    if published:
        st.caption(f"Published: {published}")
    st.markdown('---')

# MeteoAlarm renderer
def render_meteoalarm(item, conf):
    st.markdown(f"<h3 style='margin-bottom:4px'>{item.get('title','')}</h3>",
                unsafe_allow_html=True)
    for day in ['today','tomorrow']:
        alerts = item.get('alerts',{}).get(day, [])
        if alerts:
            st.markdown(f"<h4 style='margin-top:16px'>{day.capitalize()}</h4>",
                        unsafe_allow_html=True)
            for e in alerts:
                try:
                    dt1 = dateparser.parse(e['from']).strftime('%H:%M UTC %B %d')
                    dt2 = dateparser.parse(e['until']).strftime('%H:%M UTC %B %d')
                except:
                    dt1, dt2 = e['from'], e['until']
                color = {'Orange':'#FF7F00','Red':'#E60026'}.get(
                    e.get('level',''), '#888'
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
        st.caption(f"Published: {published}")
    st.markdown('---')

# BOM multi-state renderer
def render_bom_multi(item, conf):
    st.markdown(f"### {item.get('state','')}")
    title = item.get('title','(no title)').strip()
    link  = item.get('link','').strip()
    if link:
        st.markdown(f"**[{title}]({link})**")
    else:
        st.markdown(f"**{title}**")
    pub = item.get('published','').strip()
    if pub:
        st.caption(f"Published: {pub}")
    summary = item.get('summary','').strip()
    if summary:
        st.markdown(summary)
    st.markdown('---')

# Renderer registry
RENDERERS = {
    'json': render_json,
    'ec_async': render_ec,             # per-item fallback
    'ec_grouped': render_ec_grouped,   # new grouped view
    'rss_cma': render_cma,
    'rss_meteoalarm': render_meteoalarm,
    'rss_bom_multi': render_bom_multi,
}
