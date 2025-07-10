import streamlit as st
import os
import sys
import json
import time
import logging
from utils.domain_router import get_scraper

# Extend import path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Page config
st.set_page_config(page_title="Global Weather Monitor", layout="wide")

# Logging
logging.basicConfig(level=logging.WARNING)

# Constants
REFRESH_INTERVAL = 60  # seconds

# Session state defaults
defaults = {
    "nws_seen_count": 0,
    "nws_show_alerts": False,
    "nws_data": None,
    "nws_last_fetch": 0,
    "ec_seen_count": 0,
    "ec_show_alerts": False,
    "ec_data": None,
    "ec_last_fetch": 0
}
for key, value in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = value

# Determine if any tile is open (used to pause refresh)
any_tile_open = st.session_state["nws_show_alerts"] or st.session_state["ec_show_alerts"]

# Current time
now = time.time()

# --- NWS FETCH ---
nws_url = "https://api.weather.gov/alerts/active"
nws_scraper = get_scraper("api.weather.gov")

if not any_tile_open and now - st.session_state["nws_last_fetch"] > REFRESH_INTERVAL:
    if nws_scraper:
        try:
            st.session_state["nws_data"] = nws_scraper(nws_url)
        except Exception as e:
            st.session_state["nws_data"] = {
                "entries": [],
                "error": str(e),
                "source": nws_url
            }
    st.session_state["nws_last_fetch"] = now

nws_data = st.session_state.get("nws_data", {})
nws_alerts = nws_data.get("entries", [])
total_nws = len(nws_alerts)
new_nws = max(0, total_nws - st.session_state.get("nws_seen_count", 0))

# --- UI ---
st.title("Global Weather Monitor")
cols = st.columns(2)

# --- TILE: NWS ---
with cols[0]:
    st.subheader("NWS Active Alerts")
    st.markdown(f"- **{total_nws}** total alerts")
    st.markdown(f"- **{new_nws}** new since last view")
    st.caption(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['nws_last_fetch']))}")

    if st.button("View Alerts", key="nws_toggle_btn"):
        st.session_state["nws_show_alerts"] = not st.session_state["nws_show_alerts"]
        if st.session_state["nws_show_alerts"]:
            st.session_state["nws_seen_count"] = total_nws

    if st.session_state["nws_show_alerts"]:
        for i, alert in enumerate(nws_alerts):
            title = str(alert.get("title", f"Alert #{i+1}")).strip()
            summary = (alert.get("summary", "") or "")[:300]
            link = alert.get("link", "")
            published = alert.get("published", "")
            is_new = i >= total_nws - new_nws

            if is_new:
                st.markdown("<div style='height: 4px; background-color: red; margin: 10px 0; border-radius: 2px;'></div>", unsafe_allow_html=True)

            st.markdown(f"**{title}**")
            st.markdown(summary if summary.strip() else "_No summary available._")
            if link:
                st.markdown(f"[Read more]({link})")
            if published:
                st.caption(f"Published: {published}")
            st.markdown("---")

# --- TILE: Environment Canada (Coming Soon) ---
with cols[1]:
    st.subheader("Environment Canada Alerts")
    st.markdown("- No data yet")
    st.caption("Coming soon...")
