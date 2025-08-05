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
from utils.clients import get_async_client
from streamlit_autorefresh import st_autorefresh
from computation import compute_counts
from renderer import RENDERERS

# Constants
FETCH_TTL = 60  # seconds
MAX_CONCURRENCY = 20  # max parallel scrapers

# Ensure module path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Page config
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# Auto-refresh every minute (unique key)
st_autorefresh(interval=FETCH_TTL * 1000, key="auto_refresh_main")

# Load definitions and initialize state
FEED_CONFIG = get_feed_definitions()
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

# Unique ID for MeteoAlarm
def alert_id(entry):
    return f"{entry['level']}|{entry['type']}|{entry['from']}|{entry['until']}"

# Async fetcher
async def _fetch_all_feeds(configs: dict):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    client = get_async_client()
    async def bound_fetch(key, conf):
        async with sem:
            try:
                data = await SCRAPER_REGISTRY[conf['type']](conf, client)
            except Exception as e:
                logging.warning(f"[{key.upper()} FETCH ERROR] {e}")
                data = {'entries': [], 'error': str(e), 'source': conf}
            return key, data
    tasks = [bound_fetch(k, cfg) for k, cfg in configs.items()]
    return await asyncio.gather(*tasks)

# Helper to reuse event loop
def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

# Fetch stale feeds
now = time.time()
to_fetch = {k: v for k, v in FEED_CONFIG.items() if now - st.session_state[f"{k}_last_fetch"] > FETCH_TTL}
if to_fetch:
    results = run_async(_fetch_all_feeds(to_fetch))
    for key, raw_data in results:
        entries = raw_data.get('entries', [])
        st.session_state[f"{key}_data"] = entries
        st.session_state[f"{key}_last_fetch"] = now
        st.session_state['last_refreshed'] = now
        conf = FEED_CONFIG[key]
        if st.session_state.get('active_feed') == key:
            last_seen = (st.session_state[f"{key}_last_seen_alerts"]
                         if conf['type'] == 'rss_meteoalarm'
                         else st.session_state[f"{key}_last_seen_time"])
            _, new_count = compute_counts(entries, conf, last_seen, alert_id_fn=alert_id)
            if new_count == 0:
                if conf['type'] == 'rss_meteoalarm':
                    ids = {alert_id(e) for country in entries
                           for alerts in country.get('alerts', {}).values() for e in alerts}
                    st.session_state[f"{key}_last_seen_alerts"] = ids
                else:
                    st.session_state[f"{key}_last_seen_time"] = now
        gc.collect()

# UI header
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown('---')

# Feed buttons
cols = st.columns(len(FEED_CONFIG))
for i, (key, conf) in enumerate(FEED_CONFIG.items()):
    entries = st.session_state[f"{key}_data"]
    seen = (st.session_state[f"{key}_last_seen_alerts"]
            if conf['type'] == 'rss_meteoalarm'
            else st.session_state[f"{key}_last_seen_time"])
    _, new_count = compute_counts(entries, conf, seen, alert_id_fn=alert_id)
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
                if conf['type'] == 'rss_meteoalarm':
                    ids = {alert_id(e) for country in entries
                           for alerts in country.get('alerts', {}).values() for e in alerts}
                    st.session_state[f"{key}_last_seen_alerts"] = ids
                else:
                    st.session_state[f"{key}_last_seen_time"] = time.time()
                st.session_state['active_feed'] = None
            else:
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
            else st.session_state[f"{active}_last_seen_time"])
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
