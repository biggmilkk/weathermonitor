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

# Session state initialization
if "nws_seen_count" not in st.session_state:
    st.session_state["nws_seen_count"] = 0
if "nws_show_alerts" not in st.session_state:
    st.session_state["nws_show_alerts"] = False
if "nws_data" not in st.session_state:
    st.session_state["nws_data"] = None
if "nws_last_fetch" not in st.session_state:
    st.session_state["nws_last_fetch"] = 0

# Constants
nws_url = "https://api.weather.gov/alerts/active"
REFRESH_INTERVAL = 60  # seconds
now = time.time()
scraper = get_scraper("api.weather.gov")

# Only auto-refresh if tile is closed
should_fetch_nws = (
    not st.session_state["nws_show_alerts"]
    and now - st.session_state["nws_last_fetch"] > REFRESH_INTERVAL
)

# Fetch data
if should_fetch_nws and scraper:
    try:
        st.session_state["nws_data"] = scraper(nws_url)
        st.session_state["nws_last_fetch"] = now  # ✅ Only update timestamp on actual fetch
        logging.warning(f"[NWS] Data refreshed at {time.strftime('%H:%M:%S', time.gmtime(now))}")
    except Exception as e:
        st.session_state["nws_data"] = {
            "entries": [],
            "error": str(e),
            "source": nws_url
        }

# Extract alert data
nws_data = st.session_state.get("nws_data", {})
nws_alerts = nws_data.get("entries", [])
# Sort latest first
nws_alerts.sort(key=lambda x: x.get("published", ""), reverse=True)
total_nws = len(nws_alerts)
new_nws = max(0, total_nws - st.session_state.get("nws_seen_count", 0))

# --- UI ---
st.title("Global Weather Monitor")

with st.container():
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
            is_new = i < new_nws  # Because sorted newest-first

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
