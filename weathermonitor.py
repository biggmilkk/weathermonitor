import streamlit as st
import os
import sys
import json
from utils.domain_router import get_scraper

# Extend import path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Page config
st.set_page_config(page_title="Global Weather Monitor", layout="wide")

# Initialize session state
if "nws_seen_count" not in st.session_state:
    st.session_state["nws_seen_count"] = 0
if "nws_show_alerts" not in st.session_state:
    st.session_state["nws_show_alerts"] = False

# Layout grid
cols = st.columns(3)

# --- NWS ALERTS FETCH ---
nws_alerts = []
total_nws = 0
new_nws = 0

nws_url = "https://api.weather.gov/alerts/active"
scraper = get_scraper("api.weather.gov")
if scraper:
    try:
        data = scraper(nws_url)
        if isinstance(data, dict) and "entries" in data:
            nws_alerts = data["entries"]
            total_nws = len(nws_alerts)
            new_nws = max(0, total_nws - st.session_state.get("nws_seen_count", 0))
    except Exception as e:
        st.error(f"Error fetching NWS data: {e}")
else:
    st.error("No scraper found for National Weather Service.")

# --- TILE: NWS Active Alerts ---
with cols[0]:
    with st.container():
        st.markdown("### National Weather Service - Active Alerts")
        st.markdown(f"- **{total_nws}** total alerts")
        st.markdown(f"- **{new_nws}** new since last view")

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
                        "<div style='height: 3px; background-color: #cc0000; margin: 10px 0; border-radius: 2px;'></div>",
                        unsafe_allow_html=True
                    )

                st.markdown(f"**{title}**")
                st.markdown(summary if summary.strip() else "_No summary available._")
                if link:
                    st.markdown(f"[Read more]({link})")
                if published:
                    st.caption(f"Published: {published}")
                st.markdown("---")
