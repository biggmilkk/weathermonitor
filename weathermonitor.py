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

# Page config
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

# Unique ID for MeteoAlarm entries
def alert_id(entry):
    return f"{entry['level']}|{entry['type']}|{entry['from']}|{entry['until']}"

# Fetch data and advance seen markers
for key, conf in FEED_CONFIG.items():
    last_fetch = st.session_state[f"{key}_last_fetch"]
    if now - last_fetch > REFRESH_INTERVAL:
        try:
            scraper = SCRAPER_REGISTRY[conf["type"]]
            entries = scraper(conf).get("entries", [])
            st.session_state[f"{key}_data"] = entries
            st.session_state[f"{key}_last_fetch"] = now
            st.session_state["last_refreshed"] = now
            # Advance seen on idle refresh if open
            if st.session_state.get("active_feed") == key:
                last_seen = (st.session_state[f"{key}_last_seen_alerts"]
                             if conf['type']=='rss_meteoalarm'
                             else st.session_state[f"{key}_last_seen_time"])
                marker = advance_seen(conf, entries, last_seen, now, alert_id)
                if marker is not None:
                    if conf['type']=='rss_meteoalarm':
                        st.session_state[f"{key}_last_seen_alerts"] = marker
                    else:
                        st.session_state[f"{key}_last_seen_time"] = marker
        except Exception as e:
            logging.warning(f"[{key.upper()} FETCH ERROR] {e}")
            st.session_state[f"{key}_data"] = []

# UI Header
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown("---")

# Feed buttons and counters
cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]
    seen = (st.session_state[f"{key}_last_seen_alerts"]
            if conf['type']=='rss_meteoalarm'
            else st.session_state[f"{key}_last_seen_time"])
    total, new_cnt = compute_counts(entries, conf, seen, alert_id)
    badge = f"❗ {total} total / {new_cnt} new" if new_cnt else f"{total} total / {new_cnt} new"
    style = "background-color:#ffeecc;" if new_cnt else ""
    with cols[i]:
        if st.button(badge if new_cnt else conf['label'], key=f"btn_{key}", use_container_width=True):
            # Toggle
            if st.session_state['active_feed']==key:
                # closing
                if conf['type']=='rss_meteoalarm':
                    snap = {alert_id(e)
                            for country in entries
                            for alerts in country['alerts'].values()
                            for e in alerts}
                    st.session_state[f"{key}_last_seen_alerts"] = snap
                else:
                    st.session_state[f"{key}_last_seen_time"] = time.time()
                st.session_state['active_feed']=None
            else:
                # opening
                st.session_state['active_feed']=key
                st.session_state[f"{key}_pending_seen_time"] = time.time()

# Display feed
active = st.session_state['active_feed']
if active:
    st.markdown("---")
    conf = FEED_CONFIG[active]
    data_list = sorted(st.session_state[f"{active}_data"],
                       key=lambda x: x.get('published',''), reverse=True)
    seen = (st.session_state[f"{active}_last_seen_alerts"]
            if conf['type']=='rss_meteoalarm'
            else st.session_state[f"{active}_last_seen_time"])
    for item in data_list:
        if conf['type']=='rss_meteoalarm':
            ca = [e for alerts in item['alerts'].values() for e in alerts]
            if any(alert_id(e) not in seen for e in ca):
                st.markdown("<div style='height:4px;background:red;margin:8px 0;'></div>", unsafe_allow_html=True)
        else:
            pub = item.get('published')
            if pub:
                try:
                    ts = dateparser.parse(pub).timestamp()
                except Exception:
                    ts = 0.0
                seen_ts = seen if isinstance(seen,(int,float)) else 0.0
                if ts > seen_ts:
                    st.markdown("<div style='height:4px;background:red;margin:8px 0;'></div>", unsafe_allow_html=True)
        RENDERERS.get(conf['type'], lambda i,c: None)(item, conf)
    # snapshot
    pkey = f"{active}_pending_seen_time"
    if pkey in st.session_state:
        if conf['type']=='rss_meteoalarm':
            snap = {alert_id(e)
                    for country in data_list
                    for alerts in country.get('alerts',{}).values()
                    for e in alerts}
            st.session_state[f"{active}_last_seen_alerts"] = snap
        else:
            st.session_state[f"{active}_last_seen_time"] = st.session_state.pop(pkey)
        st.session_state.pop(pkey,None)
