import streamlit as st
import os
import sys
import json
import time
import logging
from datetime import datetime
from utils.domain_router import get_scraper

# Extend import path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Logging setup
logging.basicConfig(level=logging.WARNING)

# Streamlit page config
st.set_page_config(page_title="Weather Monitor Dashboard", layout="wide")

# Session state initialization
if "nws_seen_count" not in st.session_state:
    st.session_state["nws_seen_count"] = 0
if "nws_show_alerts" not in st.session_state:
    st.session_state["nws_show_alerts"] = False
if "nws_data" not in st.session_state:
    st.session_state["nws_data"] = None
if "nws_last_fetch" not in st.session_state:
    st.session_state["nws_last_fetch"] = 0
if "nws_last_updated" not in st.session_state:
    st.session_state["nws_last_updated"] = "Never"

# Auto-refresh logic every 60 seconds
REFRESH_INTERVAL = 60  # seconds
now = time.time()
if now - st.session_state["nws_last_fetch"] > REFRESH_INTERVAL:
    st.session_state["nws_last_fetch"] = now
    st.rerun()

# Fetch NWS alerts if not already stored
nws_url = "https://api.weather.gov/alerts/active"
if st.session_state["nws_data"] is None:
    scraper = get_scraper("api.weather.gov")
    if scraper:
        try:
            data = scraper(nws_url)
            st.session_state["nws_data"] = data
            st.session_state["nws_last_updated"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception as e:
            st.error(f"Error fetching NWS alerts: {e}")
    else:
        st.error("No scraper found for NWS")

# Extract alert data
nws_alerts = st.session_state["nws_data"]["entries"] if st.session_state["nws_data"] else []
total_nws = len(nws_alerts)
new_nws = max(0, total_nws - st.session_state.get("nws_seen_count", 0))

# Layout
st.title("National Weather Service - Active Alerts")
st.markdown(f"- **Total Alerts:** {total_nws}")
st.markdown(f"- **New Since Last View:** {new_nws}")
st.markdown(f"- **Last Updated:** {st.session_state['nws_last_updated']}")

if st.button("Toggle Alert View", key="nws_toggle_btn"):
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
            st.markdown("<hr style='border: 2px solid red;'>", unsafe_allow_html=True)

        st.subheader(title)
        st.markdown(summary if summary.strip() else "_No summary available._")
        if link:
            st.markdown(f"[Read more]({link})", unsafe_allow_html=True)
        if published:
            st.caption(f"Published: {published}")
        st.markdown("---")
