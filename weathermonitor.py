import streamlit as st
import os
import sys
import json

# Extend path to enable clean imports from subfolders
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.domain_router import get_scraper

st.set_page_config(page_title="Global Weather Monitor", layout="wide")

# Session state
if "nws_seen_count" not in st.session_state:
    st.session_state["nws_seen_count"] = 0
if "nws_show_alerts" not in st.session_state:
    st.session_state["nws_show_alerts"] = False

# Header
st.markdown("<h2 style='text-align: center;'>Global Weather Dashboard</h2>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; font-size: 0.9rem; color: grey;'>Monitor live weather conditions from your global bookmarks. Each tile represents a live feed, with expandable details.</p>", unsafe_allow_html=True)

# Load bookmarks
try:
    with open("bookmarks.json", "r") as file:
        bookmarks = json.load(file)
except Exception as e:
    st.error(f"Unable to load bookmarks.json: {e}")
    st.stop()

# Filter only NWS for now
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

# Alert count
total_nws = len(nws_alerts)
new_nws = total_nws - st.session_state["nws_seen_count"]
if new_nws < 0:
    new_nws = 0

# Columns for grid (you can increase this up to 10 later)
cols = st.columns(3)

# Tile 1: NWS Alerts
with cols[0]:
    st.markdown("""
        <style>
        .card {
            background-color: #f8f9fa;
            border: 1px solid #ddd;
            padding: 1rem;
            border-radius: 10px;
            height: 100%;
        }
        .card-header {
            font-size: 1.1rem;
            font-weight: 600;
            margin-bottom: 0.3rem;
        }
        .metric {
            margin: 0.2rem 0;
        }
        </style>
    """, unsafe_allow_html=True)

    with st.container():
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown("<div class='card-header'>📡 NWS Active Alerts</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric'><strong>{total_nws}</strong> total alerts</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric'><strong>{new_nws}</strong> new since last viewed</div>", unsafe_allow_html=True)

        if st.button("View Alerts", key="nws_toggle_btn"):
            st.session_state["nws_show_alerts"] = not st.session_state["nws_show_alerts"]
            if st.session_state["nws_show_alerts"]:
                st.session_state["nws_seen_count"] = total_nws

        if st.session_state["nws_show_alerts"]:
            for i, entry in enumerate(nws_alerts):
                raw_title = entry.get("title", "")
                title = str(raw_title).strip() or f"⚠️ Alert #{i + 1}"
                summary = str(entry.get("summary", "") or "")
                if len(summary) > 300:
                    summary = summary[:300] + "..."
                published = str(entry.get("published", "") or "")
                link = entry.get("link", "").strip()
                is_new = i >= total_nws - new_nws
                prefix = "🆕 " if is_new else ""

                st.markdown(f"<div style='margin-top:0.8rem;'><strong>{prefix}{title}</strong></div>", unsafe_allow_html=True)
                st.markdown(summary or "_No description available._", unsafe_allow_html=True)
                if link:
                    st.markdown(f"[Read more]({link})")
                if published:
                    st.caption(f"Published: {published}")
                st.markdown("<hr>", unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)
