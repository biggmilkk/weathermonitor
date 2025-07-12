import streamlit as st
from dateutil import parser as dateparser

# Rendering functions for different feed types

def render_ec(item, conf):
    """
    Render an Environment Canada alert.
    """
    st.markdown(f"**{item.get('title','')}**")
    region = item.get('region', '')
    province = item.get('province', '')
    if region or province:
        st.caption(f"Region: {region}, {province}")
    st.markdown(item.get('summary',''))
    link = item.get('link')
    if link:
        st.markdown(f"[Read more]({link})")
    published = item.get('published')
    if published:
        st.caption(f"Published: {published}")
    st.markdown("---")


CMA_COLORS = {
    'Orange': '#FF7F00',  # Orange
    'Red':    '#E60026',  # Red
}

def render_cma(item, conf):
    """
    Render a China CMA alert with a colored bullet.
    """
    level = item.get('level', 'Orange')
    color = CMA_COLORS.get(level, '#888')
    title = item.get('title', '')
    st.markdown(
        f"<div style='margin-bottom:8px;'>"
        f"<span style='color:{color};font-size:18px;'>&#9679;</span> <strong>{title}</strong>"
        f"</div>", unsafe_allow_html=True
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
    st.markdown("---")


def render_meteoalarm(item, conf):
    """
    Render a MeteoAlarm country block with its alerts.
    """
    # Country heading
    st.markdown(f"<h3 style='margin-bottom:4px'>{item.get('title','')}</h3>", unsafe_allow_html=True)
    # Iterate days
    for day in ['today', 'tomorrow']:
        alerts = item.get('alerts', {}).get(day, [])
        if alerts:
            st.markdown(f"<h4 style='margin-top:16px'>{day.capitalize()}</h4>", unsafe_allow_html=True)
            for e in alerts:
                # Format times
                try:
                    dt_from = dateparser.parse(e['from']).strftime('%H:%M UTC %B %d')
                    dt_until = dateparser.parse(e['until']).strftime('%H:%M UTC %B %d')
                except Exception:
                    dt_from, dt_until = e['from'], e['until']
                # Color bullet
                level = e.get('level','')
                color = {'Orange':'#FF7F00','Red':'#E60026'}.get(level, '#888')
                # New prefix
                prefix = '[NEW] ' if e.get('is_new') else ''
                label = f"{prefix}[{level}] {e.get('type','')} - {dt_from} - {dt_until}"
                st.markdown(
                    f"<div style='margin-bottom:6px;'>"
                    f"<span style='color:{color};font-size:16px;'>&#9679;</span> {label}"
                    f"</div>", unsafe_allow_html=True
                )
    # Footer link and timestamp
    link = item.get('link')
    if link:
        st.markdown(f"[Read more]({link})")
    published = item.get('published')
    if published:
        st.caption(f"Published: {published}")
    st.markdown("---")


# Registry mapping feed types to renderer functions
RENDERERS = {
    'ec_async':        render_ec,
    'rss_cma':         render_cma,
    'rss_meteoalarm':  render_meteoalarm,
}
