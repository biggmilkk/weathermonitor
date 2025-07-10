import streamlit as st
import os
import sys
import json
import logging
logging.basicConfig(level=logging.WARNING)

# Extend import path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.domain_router import get_scraper

# Page config
st.set_page_config(page_title="Global Weather Monitor", layout="wide")

# Initialize session state
if "nws_seen_count" not in st.session_state:
    st.session_state["nws_seen_count"] = 0
if "nws_show_alerts" not in st.session_state:
    st.session_state["nws_show_alerts"] = False
if "ec_seen_count" not in st.session_state:
    st.session_state["ec_seen_count"] = 0
if "ec_show_alerts" not in st.session_state:
    st.session_state["ec_show_alerts"] = False

# Load bookmarks
try:
    with open("bookmarks.json", "r") as f:
        bookmarks = json.load(f)
except Exception as e:
    st.error(f"Error loading bookmarks.json: {e}")
    st.stop()

# Load Environment Canada sources
try:
    with open("environment_canada_sources.json", "r") as f:
        ec_sources = json.load(f)
except Exception as e:
    st.error(f"Error loading environment_canada_sources.json: {e}")
    ec_sources = []

# Layout grid
cols = st.columns(3)  # Adjust this to 7 or 10 when more tiles are added

# Initialize alert lists and counters
nws_alerts = []
total_nws = 0
new_nws = 0

# Get NWS alerts
for bm in bookmarks:
    if bm.get("domain") == "api.weather.gov":
        scraper = get_scraper("api.weather.gov")
        if scraper:
            try:
                data = scraper(bm.get("url"))
                st.write(f"[DEBUG] Scraper returned: {type(data)} - keys: {list(data.keys()) if data else 'None'}")

                if isinstance(data, dict) and "entries" in data:
                    nws_alerts.extend(data["entries"])
                    st.write(f"[DEBUG] Added {len(data['entries'])} entries from {bm['url']}")
                else:
                    st.warning(f"[WARNING] No 'entries' in data from {bm['url']}")
            except Exception as e:
                st.error(f"[ERROR] Failed fetching NWS data: {e}")

# Compute counts regardless of scraper success
total_nws = len(nws_alerts)
new_nws = max(0, total_nws - st.session_state.get("nws_seen_count", 0))

# --- TILE: NWS Active Alerts ---
with cols[0]:
    with st.container():
        st.markdown("### NWS Active Alerts")
        st.markdown(f"- **{total_nws}** total alerts")
        st.markdown(f"- **{new_nws}** new since last view")

        if st.button("View Alerts", key="nws_toggle_btn"):
            st.session_state["nws_show_alerts"] = not st.session_state["nws_show_alerts"]
            if st.session_state["nws_show_alerts"]:
                st.session_state["nws_seen_count"] = total_nws

        if st.session_state["nws_show_alerts"]:
            for i, alert in enumerate(nws_alerts):
                raw_title = alert.get("title")
                title = str(raw_title).strip() if raw_title else f"Alert #{i+1}"

                summary = alert.get("summary", "") or ""
                summary = summary[:300] + "..." if len(summary) > 300 else summary

                published = alert.get("published", "")
                link = alert.get("link", "")
                is_new = i >= total_nws - new_nws

                # Visual new indicator
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

# Get Environment Canada alerts
ec_alerts = []
for src in ec_sources:
    scraper = get_scraper("weather.gc.ca")
    if scraper:
        try:
            data = scraper(src.get("url"))
            if isinstance(data, dict) and "entries" in data:
                ec_alerts.extend(data["entries"])
        except Exception:
            continue

# Count EC alerts
total_ec = len(ec_alerts)
new_ec = max(0, total_ec - st.session_state["ec_seen_count"])

# --- TILE: Environment Canada Alerts ---
with cols[1]:
    with st.container():
        st.markdown("### Environment Canada Alerts")
        st.markdown(f"- **{total_ec}** total alerts")
        st.markdown(f"- **{new_ec}** new since last view")

        if st.button("View Alerts", key="ec_toggle_btn"):
            st.session_state["ec_show_alerts"] = not st.session_state["ec_show_alerts"]
            if st.session_state["ec_show_alerts"]:
                st.session_state["ec_seen_count"] = total_ec

        if st.session_state["ec_show_alerts"]:
            for i, alert in enumerate(ec_alerts):
                raw_title = alert.get("title")
                title = str(raw_title).strip() if raw_title else f"Alert #{i+1}"

                summary = alert.get("summary", "") or ""
                summary = summary[:300] + "..." if len(summary) > 300 else summary

                published = alert.get("published", "")
                link = alert.get("link", "")
                is_new = i >= total_ec - new_ec

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

