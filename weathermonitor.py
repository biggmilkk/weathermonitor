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
from clients import get_async_client
from streamlit_autorefresh import st_autorefresh
from computation import compute_counts
from renderer import RENDERERS

# Constants
FETCH_TTL = 60  # seconds
MAX_CONCURRENCY = 20  # max parallel scrapers

# Ensure scrapers are on path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Page configuration
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# Auto-refresh every minute
st_autorefresh(interval=FETCH_TTL * 1000, key="autorefresh")

# Load feed definitions
FEED_CONFIG = get_feed_definitions()

# Initialize session state defaults
now = time.time()
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

# Async fetch helper
async def _fetch_all_feeds(configs: dict):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    client = get_async_client()

    async def bound_fetch(key, conf):
        async with sem:
            func = SCRAPER_REGISTRY[conf['type']]
            try:
                data = await func(conf, client)
            except Exception as e:
                logging.warning(f"[{key.upper()} FETCH ERROR] {e}")
                data = {'entries': [], 'error': str(e), 'source': conf}
            return key, data

    tasks = [bound_fetch(key, conf) for key, conf in configs.items()]
    return await asyncio.gather(*tasks)

# Trigger fetches for stale feeds
now = time.time()
st.session_state.setdefault('last_refreshed', now)
# Collect feeds that need refresh
to_fetch = [ (key, conf) for key, conf in FEED_CONFIG.items()
            if now - st.session_state[f"{key}_last_fetch"] > FETCH_TTL ]
if to_fetch:
    # Run async fetch
    results = asyncio.run(_fetch_all_feeds(dict(to_fetch)))
    for key, raw_data in results:
        # Normalize entries
        entries = raw_data.get('entries', []) if isinstance(raw_data, dict) else []
        # Update state
        st.session_state[f"{key}_data"] = entries
        st.session_state[f"{key}_last_fetch"] = now
        st.session_state['last_refreshed'] = now

        # Advance seen markers on idle
        conf = FEED_CONFIG[key]
        if st.session_state.get('active_feed') == key:
            if conf['type'] == 'rss_meteoalarm':
                last_seen = st.session_state[f"{key}_last_seen_alerts"]
            else:
                last_seen = st.session_state[f"{key}_last_seen_time"]
            total, new_count = compute_counts(entries, conf, last_seen, alert_id_fn=alert_id)
            if new_count == 0:
                # No new alerts, snapshot
                if conf['type'] == 'rss_meteoalarm':
                    ids = {alert_id(e) for country in entries
                           for alerts in country.get('alerts', {}).values() for e in alerts}
                    st.session_state[f"{key}_last_seen_alerts"] = ids
                else:
                    st.session_state[f"{key}_last_seen_time"] = now
        gc.collect()

# UI Header
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown('---')

# Feed selection buttons
cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]
    seen = (st.session_state[f"{key}_last_seen_alerts"]
            if conf['type'] == 'rss_meteoalarm'
            else st.session_state[f"{key}_last_seen_time"] )
    total, new_count = compute_counts(entries, conf, seen, alert_id_fn=alert_id)
    with cols[i]:
        clicked = st.button(conf['label'], key=f"btn_{key}", use_container_width=True)
        if new_count > 0:
            st.markdown(
                f"<span style='margin-left:8px;padding:2px 6px;border-radius:4px;"
                f"background:#ffeecc;font-size:0.9em;'>❗ {new_count} New</span>",
                unsafe_allow_html=True
            )
        if clicked:
            if st.session_state['active_feed'] == key:
                # close
                if conf['type'] == 'rss_meteoalarm':
                    ids = {alert_id(e) for country in entries
                           for alerts in country.get('alerts', {}).values() for e in alerts}
                    st.session_state[f"{key}_last_seen_alerts"] = ids
                else:
                    st.session_state[f"{key}_last_seen_time"] = time.time()
                st.session_state['active_feed'] = None
            else:
                # open
                st.session_state['active_feed'] = key
                st.session_state[f"{key}_pending_seen_time"] = time.time()

# Display details
active = st.session_state['active_feed']
if active:
    st.markdown('---')
    conf = FEED_CONFIG[active]
    entries = st.session_state[f"{active}_data"]
    data_list = sorted(entries, key=lambda x: x.get('published', ''), reverse=True)

    if conf['type'] == 'rss_meteoalarm':
        seen_ids = st.session_state[f"{active}_last_seen_alerts"]
        for country in data_list:
            for alerts in country.get('alerts', {}).values():
                for e in alerts:
                    e['is_new'] = alert_id(e) not in seen_ids

    seen = (st.session_state[f"{active}_last_seen_alerts"]
            if conf['type'] == 'rss_meteoalarm'
            else st.session_state[f"{active}_last_seen_time"] )
    for item in data_list:
        if conf['type'] == 'rss_meteoalarm':
            alerts = [e for alerts in item['alerts'].values() for e in alerts]
            if any(e.get('is_new') for e in alerts):
                st.markdown("<div style='height:4px;background:red;margin:8px 0;'></div>", unsafe_allow_html=True)
        else:
            pub = item.get('published')
            try:
                ts = dateparser.parse(pub).timestamp() if pub else 0.0
            except Exception:
                ts = 0.0
            if ts > (seen if isinstance(seen, (int, float)) else 0.0):
                st.markdown("<div style='height:4px;background:red;margin:8px 0;'></div>", unsafe_allow_html=True)
        RENDERERS.get(conf['type'], lambda i, c: None)(item, conf)

    # Snapshot last seen
    pkey = f"{active}_pending_seen_time"
    if pkey in st.session_state:
        if conf['type'] == 'rss_meteoalarm':
            ids = {alert_id(e) for country in data_list
                   for alerts in country.get('alerts', {}).values() for e in alerts}
            st.session_state[f"{active}_last_seen_alerts"] = ids
        else:
            st.session_state[f"{active}_last_seen_time"] = st.session_state.pop(pkey)
        st.session_state.pop(pkey, None)
