import streamlit as st
import os
import sys
import json

# Extend import path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.domain_router import get_scraper

st.set_page_config(page_title="Global Weather Monitor", layout="wide")

# Session state
if "nws_seen_count" not in st.session_state:
    st.session_state["nws_seen_count"] = 0
if "nws_show_alerts" not in st.session_state:
    st.session_state["nws_show_alerts"] = False

# Load bookmarks
try:
    with open("bookmarks.json", "r") as f:
        bookmarks = json.load(f)
except Exception as e:
    st.error(f"Error loading bookmarks: {e}")
    st.stop()

# Filter NWS only
nws_alerts = []
for bm in bookmarks:
    if bm.get("domain") == "api.weather.gov":
        scraper = get_scraper("api.weather.gov")
        if scraper:
            try:
                data = scraper(bm.get("url"))
                if isinstance(data, dict) and "entries" in data:
                    nws_alerts.extend(data["entries"])
            except:
                continue

# Alert counters
total_nws = len(nws_alerts)
new_nws = max(0, total_nws - st.session_state["nws_seen_count"])

# Grid: 3 columns (adjust to 7-10 later)
cols = st.columns(3)

# Tile 1: NWS
with cols[0]:
    with st.container():
        # Tile-style card using text formatting only
        st.markdown("### 📡 NWS Active Alerts")
        st.markdown(f"- **{total_nws}** total alerts")
        st.markdown(f"- **{new_nws}** new since last view")

        view_clicked = st.button("View Alerts", key="nws_toggle_btn")

        if view_clicked:
            st.session_state["nws_show_alerts"] = not st.session_state["nws_show_alerts"]
            if st.session_state["nws_show_alerts"]:
                st.session_state["nws_seen_count"] = total_nws

        if st.session_state["nws_show_alerts"]:
            for i, alert in enumerate(nws_alerts):
                title = alert.get("title", f"Alert #{i+1}").strip()
                summary = alert.get("summary", "") or ""
                summary = summary[:300] + "..." if len(summary) > 300 else summary
                published = alert.get("published", "")
                link = alert.get("link", "")
                is_new = i >= total_nws - new_nws
                prefix = "🆕 " if is_new else ""

                st.markdown(f"**{prefix}{title}**")
                st.markdown(summary if summary.strip() else "_No summary available._")
                if link:
                    st.markdown(f"[Read more]({link})")
                if published:
                    st.caption(f"Published: {published}")
                st.markdown("---")
