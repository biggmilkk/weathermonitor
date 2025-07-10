import streamlit as st
import os
import sys
import json
import logging
from utils.domain_router import get_scraper

# Logging setup
logging.basicConfig(level=logging.DEBUG)

# Extend import path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Page config
st.set_page_config(page_title="NWS Weather Monitor", layout="wide")

# Initialize session state
if "nws_seen_count" not in st.session_state:
    st.session_state["nws_seen_count"] = 0
if "nws_show_alerts" not in st.session_state:
    st.session_state["nws_show_alerts"] = False

# Layout grid
cols = st.columns(1)

# --- NWS ALERTS FETCH ---
nws_alerts = []
total_nws = 0
new_nws = 0

nws_url = "https://api.weather.gov/alerts/active"
scraper = get_scraper("api.weather.gov")

if not scraper:
    st.error("[ERROR] No scraper registered for 'api.weather.gov'")
else:
    try:
        data = scraper(nws_url)

        st.subheader("📦 NWS Raw Scraper Output")
        st.json(data)

        if isinstance(data, dict) and "entries" in data:
            nws_alerts = data["entries"]
            total_nws = len(nws_alerts)
            new_nws = max(0, total_nws - st.session_state.get("nws_seen_count", 0))
        else:
            st.warning("[WARNING] 'entries' key missing or data is not a dict")
    except Exception as e:
        st.error(f"[ERROR] Failed fetching NWS data: {e}")

# --- TILE: NWS Active Alerts ---
with cols[0]:
    with st.container():
        st.markdown("### 🚨 NWS Active Alerts")
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
