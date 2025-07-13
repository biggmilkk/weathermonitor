import streamlit as st
from dateutil import parser as dateparser

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

# Environment Canada renderer
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

# CMA China renderer with color bullets
CMA_COLORS = {'Orange':'#FF7F00','Red':'#E60026'}
def render_cma(item, conf):
    level = item.get('level','Orange')
    color = CMA_COLORS.get(level,'#888')
    st.markdown(
        f"<div style='margin-bottom:8px;'><span style='color:{color};font-size:18px;'>&#9679;</span> <strong>{item.get('title','')}</strong></div>",
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

# MeteoAlarm renderer showing per-alert [NEW]
def render_meteoalarm(item, conf):
    # Country heading
    st.markdown(f"<h3 style='margin-bottom:4px'>{item.get('title','')}</h3>", unsafe_allow_html=True)
    # Iterate each day's alerts
    for day in ['today','tomorrow']:
        alerts = item.get('alerts',{}).get(day,[])
        if alerts:
            st.markdown(f"<h4 style='margin-top:16px'>{day.capitalize()}</h4>", unsafe_allow_html=True)
            for e in alerts:
                try:
                    dt1 = dateparser.parse(e['from']).strftime('%H:%M UTC %B %d')
                    dt2 = dateparser.parse(e['until']).strftime('%H:%M UTC %B %d')
                except Exception:
                    dt1, dt2 = e['from'], e['until']
                # color bullet
                color = {'Orange':'#FF7F00','Red':'#E60026'}.get(e.get('level',''), '#888')
                # NEW prefix
                prefix = '[NEW] ' if e.get('is_new') else ''
                text = f"{prefix}[{e.get('level','')}] {e.get('type','')} - {dt1} - {dt2}"
                st.markdown(
                    f"<div style='margin-bottom:6px;'><span style='color:{color};font-size:16px;'>&#9679;</span> {text}</div>",
                    unsafe_allow_html=True
                )
    # Footer link and timestamp
    link = item.get('link')
    if link:
        st.markdown(f"[Read more]({link})")
    published = item.get('published')
    if published:
        st.caption(f"Published: {published}")
    st.markdown('---')

# Renderer registry
RENDERERS = {
    'json': render_json,
    'ec_async': render_ec,
    'rss_cma': render_cma,
    'rss_meteoalarm': render_meteoalarm,
}
