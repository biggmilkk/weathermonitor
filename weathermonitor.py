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

# --- NWS ALERTS ---

# Session state
if "nws_seen_count" not in st.session_state:
    st.session_state["nws_seen_count"] = 0
if "nws_show_alerts" not in st.session_state:
    st.session_state["nws_show_alerts"] = False
if "nws_data" not in st.session_state:
    st.session_state["nws_data"] = None
if "nws_last_fetch" not in st.session_state:
    st.session_state["nws_last_fetch"] = 0

# --- NWS ALERTS FETCH ---
nws_url = "https://api.weather.gov/alerts/active"
scraper = get_scraper("api.weather.gov")
now = time.time()
REFRESH_INTERVAL = 60  # seconds

should_fetch = (
    not st.session_state["nws_show_alerts"] and
    (now - st.session_state["nws_last_fetch"] > REFRESH_INTERVAL)
)

if should_fetch and scraper:
    try:
        fetched_data = scraper(nws_url)
        if fetched_data:
            st.session_state["nws_data"] = fetched_data
            st.session_state["nws_last_fetch"] = now
            logging.warning(f"[NWS] Refreshed at {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(now))}")
    except Exception as e:
        st.session_state["nws_data"] = {
            "entries": [],
            "error": str(e),
            "source": nws_url
        }

nws_data = st.session_state.get("nws_data", {})
nws_alerts = nws_data.get("entries", [])
nws_alerts = sorted(nws_alerts, key=lambda a: a.get("published", ""), reverse=True)  # sort newest first
total_nws = len(nws_alerts)
new_nws = max(0, total_nws - st.session_state.get("nws_seen_count", 0))

# --- UI ---
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
            is_new = i < new_nws  # newest are at top

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

# Load EC sources from JSON file
ec_sources = []
try:
    with open("environment_canada_sources.json") as f:
        ec_sources = json.load(f)
except Exception as e:
    logging.warning(f"Failed to load EC sources: {e}")

ec_tile_key = "ec_show_alerts"
ec_seen_key = "ec_seen_count"
ec_data_key = "ec_data"
ec_last_fetch_key = "ec_last_fetch"

# Init session state
if ec_tile_key not in st.session_state:
    st.session_state[ec_tile_key] = False
if ec_seen_key not in st.session_state:
    st.session_state[ec_seen_key] = 0
if ec_data_key not in st.session_state:
    st.session_state[ec_data_key] = []
if ec_last_fetch_key not in st.session_state:
    st.session_state[ec_last_fetch_key] = 0

# Only fetch if collapsed and interval exceeded
now = time.time()
if not st.session_state[ec_tile_key] and (now - st.session_state[ec_last_fetch_key] > REFRESH_INTERVAL):
    all_entries = []
    scraper = get_scraper("weather.gc.ca")
    for region in ec_sources:
        url = region["ATOM URL"]
        try:
            data = scraper(url)
            all_entries.extend(data.get("entries", []))
        except Exception as e:
            logging.warning(f"[EC FETCH ERROR] {region['Region Name']}: {e}")
    st.session_state[ec_data_key] = all_entries
    st.session_state[ec_last_fetch_key] = now

# Prepare display
ec_alerts = sorted(st.session_state[ec_data_key], key=lambda x: x.get("published", ""), reverse=True)
total_ec = len(ec_alerts)
new_ec = max(0, total_ec - st.session_state[ec_seen_key])

# UI
with st.container():
    st.subheader("Environment Canada Alerts")
    st.markdown(f"- **{total_ec}** total alerts")
    st.markdown(f"- **{new_ec}** new since last view")
    st.caption(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st.session_state[ec_last_fetch_key]))}")

    if st.button("View Alerts", key="ec_toggle_btn"):
        st.session_state[ec_tile_key] = not st.session_state[ec_tile_key]
        if st.session_state[ec_tile_key]:
            st.session_state[ec_seen_key] = total_ec

    if st.session_state[ec_tile_key]:
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
