import streamlit as st
import os
import sys
import json
import logging
from utils.domain_router import get_scraper

# Logging setup
logging.basicConfig(level=logging.WARNING)

# Extend import path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

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

# Load Environment Canada sources
try:
    with open("environment_canada_sources.json", "r") as f:
        ec_sources = json.load(f)
except Exception as e:
    st.error(f"Error loading environment_canada_sources.json: {e}")
    ec_sources = []

# Layout grid
cols = st.columns(3)  # Expand as needed

# --- NWS ALERTS FETCH ---
nws_alerts = []
total_nws = 0
new_nws = 0

nws_url = "https://api.weather.gov/alerts/active"
scraper = get_scraper("api.weather.gov")
if scraper:
    try:
        data = scraper(nws_url)

        # Log and display what we received
        st.write("[DEBUG] Raw NWS scraper data:")
        st.json(data)

        if isinstance(data, dict) and "entries" in data:
            nws_alerts = data["entries"]
            total_nws = len(nws_alerts)
            new_nws = max(0, total_nws - st.session_state.get("nws_seen_count", 0))
        else:
            st.warning("[WARNING] NWS scraper returned unexpected structure or missing 'entries'")
    except Exception as e:
        st.error(f"[ERROR] Failed fetching NWS data: {e}")
else:
    st.error("[ERROR] No scraper found for 'api.weather.gov'")


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

# --- ENVIRONMENT CANADA ALERTS ---
ec_alerts = []
total_ec = 0
new_ec = 0

scraper = get_scraper("weather.gc.ca")
if scraper:
    for src in ec_sources:
        url = src.get("url") or src.get("ATOM URL")
        if not url:
            continue
        try:
            data = scraper(url)
            if isinstance(data, dict) and "entries" in data:
                ec_alerts.extend(data["entries"])
        except Exception as e:
            logging.warning(f"[EC SCRAPER WARNING] Failed {url}: {e}")
            continue

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
                title = str(alert.get("title", f"Alert #{i+1}")).strip()
                summary = (alert.get("summary", "") or "")[:300]
                link = alert.get("link", "")
                published = alert.get("published", "")
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
