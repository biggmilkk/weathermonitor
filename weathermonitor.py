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

# --- Session State Defaults ---
defaults = {
    "nws_seen_count": 0,
    "ec_seen_count": 0,
    "nws_data": None,
    "ec_data": [],
    "nws_last_fetch": 0,
    "ec_last_fetch": 0,
    "last_refreshed": now,
    "active_feed": None,
}
for key, val in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = val

# --- NWS Fetch ---
nws_scraper = get_scraper("api.weather.gov")
nws_url = "https://api.weather.gov/alerts/active"
if now - st.session_state["nws_last_fetch"] > REFRESH_INTERVAL:
    try:
        nws_data = nws_scraper(nws_url)
        if nws_data:
            st.session_state["nws_data"] = nws_data
            st.session_state["nws_last_fetch"] = now
            st.session_state["last_refreshed"] = now
    except Exception as e:
        st.session_state["nws_data"] = {"entries": [], "error": str(e)}

nws_alerts = sorted(
    st.session_state["nws_data"].get("entries", []),
    key=lambda x: x.get("published", ""),
    reverse=True,
)
total_nws = len(nws_alerts)
new_nws = max(0, total_nws - st.session_state["nws_seen_count"])

# --- EC Fetch ---
ec_sources = []
try:
    with open("environment_canada_sources.json") as f:
        ec_sources = json.load(f)
except Exception as e:
    logging.warning(f"[EC LOAD ERROR] {e}")

if now - st.session_state["ec_last_fetch"] > REFRESH_INTERVAL:
    entries = asyncio.run(scrape_async(ec_sources))
    st.session_state["ec_data"] = entries.get("entries", [])
    st.session_state["ec_last_fetch"] = now
    st.session_state["last_refreshed"] = now

ec_alerts = sorted(
    st.session_state["ec_data"], key=lambda x: x.get("published", ""), reverse=True
)
total_ec = len(ec_alerts)
new_ec = max(0, total_ec - st.session_state["ec_seen_count"])

# --- UI Header ---
st.title("Global Weather Monitor")
st.caption(
    f"🔄 Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}"
)
st.markdown("---")

# --- Tile Buttons ---
col1, col2 = st.columns(2)

# --- Tile: NWS ---
with col1:
    label = f"NWS Alerts ({total_nws} total / {new_nws} new)"
    if st.button(label, key="btn_nws", use_container_width=True):
        if st.session_state["active_feed"] == "nws":
            # If already open, close it
            st.session_state["active_feed"] = None
        else:
            # Open and clear new count in one click
            st.session_state["active_feed"] = "nws"
            st.session_state["nws_seen_count"] = total_nws

# --- Tile: Environment Canada ---
with col2:
    label = f"Environment Canada ({total_ec} total / {new_ec} new)"
    if st.button(label, key="btn_ec", use_container_width=True):
        if st.session_state["active_feed"] == "ec":
            st.session_state["active_feed"] = None
        else:
            st.session_state["active_feed"] = "ec"
            st.session_state["ec_seen_count"] = total_ec

# --- Read-Only Feed Panel ---
feed = st.session_state["active_feed"]
if feed:
    st.markdown("---")
    if feed == "nws":
        st.subheader("NWS Active Alerts")
        for i, alert in enumerate(nws_alerts):
            is_new = i < new_nws
            if is_new:
                st.markdown(
                    "<div style='height:4px;background:red;margin:10px 0;border-radius:2px;'></div>",
                    unsafe_allow_html=True,
                )
            st.markdown(f"**{alert.get('title', '')}**")
            st.markdown(alert.get("summary", "")[:300] or "_No summary available._")
            if alert.get("link"):
                st.markdown(f"[Read more]({alert['link']})")
            if alert.get("published"):
                st.caption(f"Published: {alert['published']}")
            st.markdown("---")
    elif feed == "ec":
        st.subheader("Environment Canada Alerts")
        for i, alert in enumerate(ec_alerts):
            is_new = i < new_ec
            if is_new:
                st.markdown(
                    "<div style='height:4px;background:red;margin:10px 0;border-radius:2px;'></div>",
                    unsafe_allow_html=True,
                )
            st.markdown(f"**{alert.get('title', '')}**")
            st.caption(
                f"Region: {alert.get('region', '')}, {alert.get('province', '')}"
            )
            st.markdown(alert.get("summary", "")[:300] or "_No summary available._")
            if alert.get("link"):
                st.markdown(f"[Read more]({alert['link']})")
            if alert.get("published"):
                st.caption(f"Published: {alert['published']}")
            st.markdown("---")
