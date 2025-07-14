import time
import streamlit as st
import os
import sys
import logging
import gc
import asyncio
from dateutil import parser as dateparser
from feeds import get_feed_definitions
from utils.scraper_registry import SCRAPER_REGISTRY
from streamlit_autorefresh import st_autorefresh
from computation import compute_counts, advance_seen
from renderer import RENDERERS

# Constants
FETCH_TTL = 60     # seconds

# Ensure scrapers are on path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Page configuration
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# Auto-refresh every minute
st_autorefresh(interval=60 * 1000, key="autorefresh")

# Timing constants
now = time.time()
REFRESH_INTERVAL = FETCH_TTL

# Load feed definitions
FEED_CONFIG = get_feed_definitions()

# Initialize session state
for key, conf in FEED_CONFIG.items():
    st.session_state.setdefault(f"{key}_data", [])
    st.session_state.setdefault(f"{key}_last_fetch", 0)
    st.session_state.setdefault(f"{key}_last_seen_time", 0.0)
    st.session_state.setdefault(f"{key}_pending_seen_time", None)
    if conf['type'] == 'rss_meteoalarm':
        st.session_state.setdefault(f"{key}_last_seen_alerts", set())

st.session_state.setdefault('last_refreshed', now)
st.session_state.setdefault('active_feed', None)

# Unique identifier for MeteoAlarm entries
def alert_id(entry):
    return f"{entry['level']}|{entry['type']}|{entry['from']}|{entry['until']}"

# Fetch fresh data and advance seen markers
for key, conf in FEED_CONFIG.items():
    last_fetch = st.session_state[f"{key}_last_fetch"] or 0
    if now - last_fetch > REFRESH_INTERVAL:
        try:
            scraper = SCRAPER_REGISTRY[conf['type']]
            # Call scraper with appropriate arguments
            if conf['type'] == 'ec_async':
                raw_data = scraper(conf.get('sources', []))
            else:
                raw_data = scraper(conf)
            # Normalize entries list
            if isinstance(raw_data, dict):
                entries = raw_data.get('entries', [])
            elif isinstance(raw_data, list):
                entries = raw_data
            else:
                entries = []

            st.session_state[f"{key}_data"] = entries
            st.session_state[f"{key}_last_fetch"] = now
            st.session_state['last_refreshed'] = now

            # Advance seen on idle refresh if open and no new alerts
            if st.session_state.get('active_feed') == key:
                if conf['type'] == 'rss_meteoalarm':
                    last_seen = st.session_state[f"{key}_last_seen_alerts"]
                else:
                    last_seen = st.session_state[f"{key}_last_seen_time"]
                total, new_count = compute_counts(entries, conf, last_seen, alert_id_fn=alert_id)
                if new_count == 0:
                    if conf['type'] == 'rss_meteoalarm':
                        all_ids = {
                            alert_id(e)
                            for country in entries
                            for alerts in country.get('alerts', {}).values()
                            for e in alerts
                        }
                        st.session_state[f"{key}_last_seen_alerts"] = all_ids
                    else:
                        st.session_state[f"{key}_last_seen_time"] = now

            # Run garbage collector to free unused memory
            gc.collect()
        except Exception as e:
            logging.warning(f"[{key.upper()} FETCH ERROR] {e}")
            st.session_state[f"{key}_data"] = []

# UI Header
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown('---')

# Feed selection buttons with separate badge
cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]
    seen = (
        st.session_state[f"{key}_last_seen_alerts"]
        if conf['type'] == 'rss_meteoalarm'
        else st.session_state[f"{key}_last_seen_time"]
    )
    total, new_count = compute_counts(entries, conf, seen, alert_id_fn=alert_id)
    with cols[i]:
        clicked = st.button(conf['label'], key=f"btn_{key}", use_container_width=True)
        if new_count > 0:
            st.markdown(
                f"<span style='margin-left:8px;padding:2px 6px;border-radius:4px;"
                f"background:#ffeecc;font-size:0.9em;'>❗ {new_count} New</span>",
                unsafe_allow_html=True,
            )
        if clicked:
            # Toggle open/close
            if st.session_state['active_feed'] == key:
                if conf['type'] == 'rss_meteoalarm':
                    snap = {
                        alert_id(e)
                        for country in entries
                        for alerts in country.get('alerts', {}).values()
                        for e in alerts
                    }
                    st.session_state[f"{key}_last_seen_alerts"] = snap
                else:
                    st.session_state[f"{key}_last_seen_time"] = time.time()
                st.session_state['active_feed'] = None
            else:
                st.session_state['active_feed'] = key
                st.session_state[f"{key}_pending_seen_time"] = time.time()

# Display selected feed details
active = st.session_state['active_feed']
if active:
    st.markdown('---')
    conf = FEED_CONFIG[active]
    entries = st.session_state[f"{active}_data"]
    # Sort newest-first
    data_list = sorted(entries, key=lambda x: x.get('published', ''), reverse=True)

    # Tag MeteoAlarm alerts with is_new
    if conf['type'] == 'rss_meteoalarm':
        seen_ids = st.session_state[f"{active}_last_seen_alerts"]
        for country in data_list:
            for alerts in country.get('alerts', {}).values():
                for e in alerts:
                    e['is_new'] = alert_id(e) not in seen_ids

    # Determine seen for red bar
    seen = (
        st.session_state[f"{active}_last_seen_alerts"]
        if conf['type'] == 'rss_meteoalarm'
        else st.session_state[f"{active}_last_seen_time"]
    )
    for item in data_list:
        if conf['type'] == 'rss_meteoalarm':
            alerts = [e for alerts in item['alerts'].values() for e in alerts]
            if any(e.get('is_new') for e in alerts):
                st.markdown("<div style='height:4px;background:red;margin:8px 0;'></div>", unsafe_allow_html=True)
        else:
            pub = item.get('published')
            if pub:
                try:
                    ts = dateparser.parse(pub).timestamp()
                except Exception:
                    ts = 0.0
                seen_ts = seen if isinstance(seen, (int, float)) else 0.0
                if ts > seen_ts:
                    st.markdown("<div style='height:4px;background:red;margin:8px 0;'></div>", unsafe_allow_html=True)
        # Render item
        RENDERERS.get(conf['type'], lambda i, c: None)(item, conf)

    # Snapshot last seen after render
    pkey = f"{active}_pending_seen_time"
    if pkey in st.session_state:
        if conf['type'] == 'rss_meteoalarm':
            snap = {
                alert_id(e)
                for country in data_list
                for alerts in country.get('alerts', {}).values()
                for e in alerts
            }
            st.session_state[f"{active}_last_seen_alerts"] = snap
        else:
            st.session_state[f"{active}_last_seen_time"] = st.session_state.pop(pkey)
        st.session_state.pop(pkey, None)
