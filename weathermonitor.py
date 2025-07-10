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

# Page config
st.set_page_config(page_title="Global Weather Monitor", layout="wide")

# Logging
logging.basicConfig(level=logging.WARNING)

# --- UI Auto Refresh Every 60s ---
st_autorefresh(interval=60 * 1000, key="autorefresh_key")

# Shared timestamp
now = time.time()
REFRESH_INTERVAL = 60

# --- SESSION STATE INIT ---
state_defaults = {
    "nws_seen_count": 0,
    "nws_show_alerts": False,
    "nws_data": None,
    "nws_last_fetch": 0,
    "ec_show_alerts": False,
    "ec_seen_count": 0,
    "ec_data": [],
    "ec_last_fetch": 0,
    "active_feed": None,
    "previous_counts": {},
}
for key, val in state_defaults.items():
    if key not in st.session_state:
        st.session_state[key] = val

# --- NWS FETCH ---
nws_url = "https://api.weather.gov/alerts/active"
nws_scraper = get_scraper("api.weather.gov")
if now - st.session_state["nws_last_fetch"] > REFRESH_INTERVAL:
    try:
        fetched = nws_scraper(nws_url)
        if fetched:
            st.session_state["nws_data"] = fetched
            st.session_state["nws_last_fetch"] = now
            logging.warning("[NWS] Refreshed")
    except Exception as e:
        st.session_state["nws_data"] = {"entries": [], "error": str(e)}

nws_alerts = sorted(st.session_state["nws_data"].get("entries", []), key=lambda x: x.get("published", ""), reverse=True)
total_nws = len(nws_alerts)
new_nws = max(0, total_nws - st.session_state["nws_seen_count"])

# --- EC FETCH ---
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
    logging.warning("[EC] Refreshed")

ec_alerts = sorted(st.session_state["ec_data"], key=lambda x: x.get("published", ""), reverse=True)
total_ec = len(ec_alerts)
new_ec = max(0, total_ec - st.session_state["ec_seen_count"])

# --- TILE BUTTONS ---
st.title("Global Weather Monitor")
col1, col2 = st.columns(2)

with col1:
    if st.button(f"NWS Alerts ({total_nws})", use_container_width=True):
        st.session_state["active_feed"] = "nws"
        st.session_state["nws_seen_count"] = total_nws

with col2:
    if st.button(f"Environment Canada ({total_ec})", use_container_width=True):
        st.session_state["active_feed"] = "ec"
        st.session_state["ec_seen_count"] = total_ec

# --- ACTIVE READING PANE ---
active = st.session_state["active_feed"]
if active == "nws":
    st.subheader("NWS Alerts")
    for i, alert in enumerate(nws_alerts):
        is_new = i < new_nws
        if is_new:
            st.markdown("<div style='height: 4px; background-color: red; margin: 10px 0; border-radius: 2px;'></div>", unsafe_allow_html=True)
        st.markdown(f"**{alert.get('title', '')}**")
        st.markdown(alert.get("summary", "")[:300] or "_No summary available._")
        if alert.get("link"):
            st.markdown(f"[Read more]({alert['link']})")
        if alert.get("published"):
            st.caption(f"Published: {alert['published']}")
        st.markdown("---")
elif active == "ec":
    st.subheader("Environment Canada Alerts")
    for i, alert in enumerate(ec_alerts):
        is_new = i < new_ec
        if is_new:
            st.markdown("<div style='height: 4px; background-color: red; margin: 10px 0; border-radius: 2px;'></div>", unsafe_allow_html=True)
        st.markdown(f"**{alert.get('title', '')}**")
        st.caption(f"Region: {alert.get('region', '')}, {alert.get('province', '')}")
        st.markdown(alert.get("summary", "")[:300] or "_No summary available._")
        if alert.get("link"):
            st.markdown(f"[Read more]({alert['link']})")
        if alert.get("published"):
            st.caption(f"Published: {alert['published']}")
        st.markdown("---")
