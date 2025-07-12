import time
import streamlit as st
import os
import sys
import logging
from dateutil import parser as dateparser
from feeds import get_feed_definitions
from utils.scraper_registry import SCRAPER_REGISTRY
from streamlit_autorefresh import st_autorefresh
from computation import compute_counts, advance_seen
from renderer import RENDERERS

# Ensure scrapers are on path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# Auto-refresh every minute
st_autorefresh(interval=60 * 1000, key="autorefresh")

# Timing constants
now = time.time()
REFRESH_INTERVAL = 60  # seconds

# Load feed definitions
FEED_CONFIG = get_feed_definitions()

# Initialize session state
for key, conf in FEED_CONFIG.items():
    st.session_state.setdefault(f"{key}_data", [])
    st.session_state.setdefault(f"{key}_last_fetch", 0)
    st.session_state.setdefault(f"{key}_last_seen_time", 0.0)
    st.session_state.setdefault(f"{key}_pending_seen_time", None)
    if conf["type"] == "rss_meteoalarm":
        st.session_state.setdefault(f"{key}_last_seen_alerts", set())

st.session_state.setdefault("last_refreshed", now)
st.session_state.setdefault("active_feed", None)

# Unique identifier for MeteoAlarm entries
def alert_id(entry):
    return f"{entry['level']}|{entry['type']}|{entry['from']}|{entry['until']}"

# Fetch and update cache for each feed
for key, conf in FEED_CONFIG.items():
    last_fetch = st.session_state[f"{key}_last_fetch"]
    if now - last_fetch > REFRESH_INTERVAL:
        try:
            scraper = SCRAPER_REGISTRY[conf["type"]]
            entries = scraper(conf)["entries"]
            st.session_state[f"{key}_data"] = entries
            st.session_state[f"{key}_last_fetch"] = now
            st.session_state["last_refreshed"] = now
            # Advance seen markers on idle refresh if feed is open
            if st.session_state.get("active_feed") == key:
                if conf["type"] == "rss_meteoalarm":
                    last_seen = st.session_state[f"{key}_last_seen_alerts"]
                else:
                    last_seen = st.session_state[f"{key}_last_seen_time"]
                marker = advance_seen(conf, entries, last_seen, now, alert_id)
                if marker is not None:
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{key}_last_seen_alerts"] = marker
                    else:
                        st.session_state[f"{key}_last_seen_time"] = marker
        except Exception as e:
            logging.warning(f"[{key.upper()} FETCH ERROR] {e}")
            st.session_state[f"{key}_data"] = []

# UI Header
st.title("Global Weather Monitor")
st.caption(f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}")
st.markdown("---")

# Feed selection buttons
cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    with cols[i]:
        if st.button(conf['label'], key=f"btn_{key}", use_container_width=True):
            # Toggle open/close
            if st.session_state['active_feed'] == key:
                # Closing: mark all as seen
                if conf['type'] == 'rss_meteoalarm':
                    all_ids = {alert_id(e) for country in st.session_state[f"{key}_data"]
                               for alerts in country['alerts'].values() for e in alerts}
                    st.session_state[f"{key}_last_seen_alerts"] = all_ids
                else:
                    st.session_state[f"{key}_last_seen_time"] = time.time()
                st.session_state['active_feed'] = None
            else:
                # Opening: defer snapshot until after render
                st.session_state['active_feed'] = key
                st.session_state[f"{key}_pending_seen_time"] = time.time()

# New/Total counters
tabs = st.columns(len(FEED_CONFIG))
for idx, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]
    if conf['type'] == 'rss_meteoalarm':
        seen = st.session_state[f"{key}_last_seen_alerts"]
    else:
        seen = st.session_state[f"{key}_last_seen_time"]
    total, new_count = compute_counts(entries, conf, seen, alert_id)
    with tabs[idx]:
        badge = f"❗ {total} total / {new_count} new" if new_count else f"{total} total / {new_count} new"
        style = "background-color:#ffeecc;" if new_count else ""
        st.markdown(f"<div style='padding:8px;border-radius:6px;{style}'>{badge}</div>", unsafe_allow_html=True)

# Display selected feed details
active = st.session_state['active_feed']
if active:
    st.markdown("---")
    conf = FEED_CONFIG[active]
    entries = st.session_state[f"{active}_data"]
    # Determine seen for bar logic
    if conf['type'] == 'rss_meteoalarm':
        seen = st.session_state[f"{active}_last_seen_alerts"]
    else:
        seen = st.session_state[f"{active}_last_seen_time"]
    # Render items with red bar indicator
    for item in entries:
        if conf['type'] == 'rss_meteoalarm':
            # Country level new
            country_alerts = [e for alerts in item['alerts'].values() for e in alerts]
            if any(alert_id(e) not in seen for e in country_alerts):
                st.markdown("<div style='height:4px;background:red;margin:8px 0;'></div>", unsafe_allow_html=True)
        else:
            pub = item.get('published')
            if pub and dateparser.parse(pub).timestamp() > seen:
                st.markdown("<div style='height:4px;background:red;margin:8px 0;'></div>", unsafe_allow_html=True)
        # Dispatch rendering
        RENDERERS.get(conf['type'], lambda i, c: None)(item, conf)
    # Snapshot last seen after rendering
    pkey = f"{active}_pending_seen_time"
    if pkey in st.session_state:
        if conf['type'] == 'rss_meteoalarm':
            snap = {alert_id(e) for country in entries for alerts in country['alerts'].values() for e in alerts}
            st.session_state[f"{active}_last_seen_alerts"] = snap
        else:
            st.session_state[f"{active}_last_seen_time"] = st.session_state.pop(pkey)
        st.session_state.pop(pkey, None)
