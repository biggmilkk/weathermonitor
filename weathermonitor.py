import streamlit as st
import os
import sys
import json
import logging
import time
from utils.domain_router import get_scraper

# Extend import path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Page config
st.set_page_config(page_title="Weather Alert Monitor", layout="wide")

# Logging
logging.basicConfig(level=logging.WARNING)

# Session state initialization
if "nws_seen_count" not in st.session_state:
    st.session_state["nws_seen_count"] = 0
if "nws_show_alerts" not in st.session_state:
    st.session_state["nws_show_alerts"] = False
if "nws_data" not in st.session_state:
    st.session_state["nws_data"] = None
if "nws_last_fetch" not in st.session_state:
    st.session_state["nws_last_fetch"] = 0

# Check if we need to re-fetch (every 60 seconds)
now = time.time()
if now - st.session_state["nws_last_fetch"] > 60:
    scraper = get_scraper("api.weather.gov")
    if scraper:
        try:
            data = scraper("https://api.weather.gov/alerts/active")
            st.session_state["nws_data"] = data
            st.session_state["nws_last_fetch"] = now
        except Exception as e:
            st.error(f"Failed to fetch NWS alerts: {e}")
    else:
        st.error("No scraper found for NWS.")

# Use cached data
data = st.session_state.get("nws_data", {"entries": []})
nws_alerts = data.get("entries", [])
total_nws = len(nws_alerts)
new_nws = max(0, total_nws - st.session_state.get("nws_seen_count", 0))

# --- Display NWS Alerts ---
st.markdown("## NWS Active Alerts")
st.markdown(f"- **{total_nws}** total alerts")
st.markdown(f"- **{new_nws}** new since last view")
st.caption(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st.session_state['nws_last_fetch']))}")

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
            st.markdown(
                "<div style='height: 4px; background-color: red; margin: 10px 0; border-radius: 2px;'></div>",
                unsafe_allow_html=True
            )

        st.markdown(f"**{title}**")
        st.markdown(summary if summary.strip() else "_No summary available._")
        if link:
            st.markdown(f"[Read more]({link})")
        if published:
            st.caption(f"Published: {published}")
        st.markdown("---")
