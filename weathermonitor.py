import streamlit as st
import os
import sys
import json
import time
import logging
import asyncio
from utils.domain_router import get_scraper
from scraper.environment_canada import scrape_async
from streamlit_autorefresh import st_autorefresh

# Extend import path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Page setup
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

# Auto-refresh every 60 seconds
st_autorefresh(interval=60 * 1000, key="autorefresh")

now = time.time()
REFRESH_INTERVAL = 60  # seconds

# --- Feed Configuration ---
FEEDS = {
    "nws": {
        "label": "NWS Alerts",
        "fetcher": lambda: get_scraper("api.weather.gov")("https://api.weather.gov/alerts/active"),
        "entries_key": "nws_data",
        "seen_key": "nws_seen_count",
        "last_fetch_key": "nws_last_fetch",
    },
    "ec": {
        "label": "Environment Canada",
        "fetcher": lambda: asyncio.run(scrape_async(json.load(open("environment_canada_sources.json")))),
        "entries_key": "ec_data",
        "seen_key": "ec_seen_count",
        "last_fetch_key": "ec_last_fetch",
    },
}

# --- Initialize Session State ---
for key in FEEDS:
    st.session_state.setdefault(FEEDS[key]["entries_key"], {"entries": []})
    st.session_state.setdefault(FEEDS[key]["seen_key"], 0)
    st.session_state.setdefault(FEEDS[key]["last_fetch_key"], 0)
st.session_state.setdefault("last_refreshed", now)
st.session_state.setdefault("active_feed", None)

# --- Fetch Feed Data ---
for feed_key, config in FEEDS.items():
    if now - st.session_state[config["last_fetch_key"]] > REFRESH_INTERVAL:
        try:
            data = config["fetcher"]()
            if data:
                st.session_state[config["entries_key"]] = data
                st.session_state[config["last_fetch_key"]] = now
                st.session_state["last_refreshed"] = now
        except Exception as e:
            st.session_state[config["entries_key"]] = {"entries": [], "error": str(e)}

# --- UI Header ---
st.title("Global Weather Monitor")
st.caption(
    f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown("---")

# --- Feed Buttons ---
with st.container():
    cols = st.columns(len(FEEDS))
    clicked_feed = None
    for idx, (feed_key, config) in enumerate(FEEDS.items()):
        if cols[idx].button(config["label"], key=f"btn_{feed_key}", use_container_width=True):
            if st.session_state["active_feed"] == feed_key:
                # Closing this feed
                st.session_state[config["seen_key"]] = len(st.session_state[config["entries_key"]]["entries"])
                st.session_state["active_feed"] = None
            else:
                # Switching feeds: clear the *previous* feed's seen count
                prev = st.session_state["active_feed"]
                if prev and prev in FEEDS:
                    prev_key = FEEDS[prev]["seen_key"]
                    st.session_state[prev_key] = len(st.session_state[FEEDS[prev]["entries_key"]]["entries"])
                st.session_state["active_feed"] = feed_key
            clicked_feed = feed_key

# --- Feed Counters ---
with st.container():
    cols = st.columns(len(FEEDS))
    for idx, (feed_key, config) in enumerate(FEEDS.items()):
        entries = st.session_state[config["entries_key"]]["entries"]
        total = len(entries)
        seen = st.session_state[config["seen_key"]]
        new = max(0, total - seen)
        cols[idx].markdown(f"**{config['label']}:** {total} total / {new} new")

# --- Feed Viewer ---
feed = st.session_state["active_feed"]
if feed and feed in FEEDS:
    st.markdown("---")
    entries = st.session_state[FEEDS[feed]["entries_key"]]["entries"]
    seen_count = st.session_state[FEEDS[feed]["seen_key"]]
    new_count = max(0, len(entries) - seen_count)

    st.subheader(FEEDS[feed]["label"])
    for i, alert in enumerate(entries):
        is_new = i < new_count
        if is_new:
            st.markdown(
                "<div style='height:4px;background:red;margin:10px 0;border-radius:2px;'></div>",
                unsafe_allow_html=True,
            )
        st.markdown(f"**{alert.get('title', '')}**")
        region = alert.get("region", "")
        province = alert.get("province", "")
        if region or province:
            st.caption(f"Region: {region}, {province}")
        st.markdown(alert.get("summary", "")[:300] or "_No summary available._")
        if alert.get("link"):
            st.markdown(f"[Read more]({alert['link']})")
        if alert.get("published"):
            st.caption(f"Published: {alert['published']}")
        st.markdown("---")
