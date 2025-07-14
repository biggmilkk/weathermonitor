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
from computation import compute_counts
from renderer import RENDERERS

# Constants
FETCH_TTL = 60  # seconds

# Ensure scrapers are on path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Page configuration
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# Auto-refresh every minute
st_autorefresh(interval=FETCH_TTL * 1000, key="autorefresh")

# Helper to fetch all feeds in parallel
def fetch_all_feeds(feed_config):
    async def _runner():
        loop = asyncio.get_running_loop()
        tasks = [
            loop.run_in_executor(None, SCRAPER_REGISTRY[conf['type']], conf)
            for _, conf in feed_config.items()
        ]
        results = await asyncio.gather(*tasks)
        return {key: res for key, res in zip(feed_config.keys(), results)}
    return asyncio.run(_runner())

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

# Fetch fresh data in parallel and update session state
if now - st.session_state['last_refreshed'] > REFRESH_INTERVAL:
    try:
        all_results = fetch_all_feeds(FEED_CONFIG)
        for key, result in all_results.items():
            entries = result.get('entries', []) if isinstance(result, dict) else []
            st.session_state[f"{key}_data"] = entries
            st.session_state[f"{key}_last_fetch"] = now
        st.session_state['last_refreshed'] = now
        # Optionally advance seen markers for active_feed on idle refresh
        active = st.session_state.get('active_feed')
        if active:
            conf = FEED_CONFIG[active]
            entries = st.session_state[f"{active}_data"]
            if conf['type'] == 'rss_meteoalarm':
                last_seen = st.session_state[f"{active}_last_seen_alerts"]
            else:
                last_seen = st.session_state[f"{active}_last_seen_time"]
            total, new_count = compute_counts(entries, conf, last_seen, alert_id_fn=alert_id)
            if new_count == 0:
                # snapshot seen
                if conf['type'] == 'rss_meteoalarm':
                    snap = {
                        alert_id(e)
                        for country in entries
                        for alerts in country.get('alerts', {}).values()
                        for e in alerts
                    }
                    st.session_state[f"{active}_last_seen_alerts"] = snap
                else:
                    st.session_state[f"{active}_last_seen_time"] = now
    except Exception as e:
        logging.warning(f"[FETCH ERROR] {e}")
        # On error, clear data
        for key in FEED_CONFIG:
            st.session_state[f"{key}_data"] = []
    finally:
        gc.collect()

# UI Header
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown('---')

# Feed selection buttons with badges
tabs = st.tabs([conf['label'] for conf in FEED_CONFIG.values()])
for tab, (key, conf) in zip(tabs, FEED_CONFIG.items()):
    with tab:
        entries = st.session_state[f"{key}_data"]
        seen = (
            st.session_state[f"{key}_last_seen_alerts"]
            if conf['type'] == 'rss_meteoalarm'
            else st.session_state[f"{key}_last_seen_time"]
        )
        total, new_count = compute_counts(entries, conf, seen, alert_id_fn=alert_id)
        if new_count > 0:
            st.markdown(
                f"<span style='padding:4px 8px; background:#ffeecc; border-radius:4px;'>❗ {new_count} New</span>",
                unsafe_allow_html=True,
            )
        if new_count >= 0:
            # Render feed details
            data_list = sorted(entries, key=lambda x: x.get('published', ''), reverse=True)
            # Mark new on render
            if conf['type'] == 'rss_meteoalarm':
                seen_ids = st.session_state[f"{key}_last_seen_alerts"]
                for country in data_list:
                    for alerts in country.get('alerts', {}).values():
                        for e in alerts:
                            e['is_new'] = alert_id(e) not in seen_ids
            for item in data_list:
                # New indicator bar
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
                        if ts > seen:
                            st.markdown("<div style='height:4px;background:red;margin:8px 0;'></div>", unsafe_allow_html=True)
                # Render the item
                RENDERERS.get(conf['type'], lambda i, c: None)(item, conf)
            # Snapshot last seen after rendering
            if conf['type'] == 'rss_meteoalarm':
                snap = {
                    alert_id(e)
                    for country in data_list
                    for alerts in country.get('alerts', {}).values()
                    for e in alerts
                }
                st.session_state[f"{key}_last_seen_alerts"] = snap
            else:
                st.session_state[f"{key}_last_seen_time"] = time.time()
