import streamlit as st
import os
import sys
import json

# Extend path to enable clean imports from subfolders
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.domain_router import get_scraper

# Page config
st.set_page_config(page_title="Global Weather Monitor", layout="wide")

# Session state: track "seen" + UI toggle per feed
if "nws_seen_count" not in st.session_state:
    st.session_state["nws_seen_count"] = 0
if "nws_show_alerts" not in st.session_state:
    st.session_state["nws_show_alerts"] = False

# Header
st.markdown("<h2 style='text-align: center;'>Global Weather Dashboard</h2>", unsafe_allow_html=True)
st.markdown(
    "<p style='text-align: center; font-size: 0.9rem; color: grey;'>Monitor live weather conditions from your global bookmarks. Search, refresh, and view structured data from weather and alert feeds.</p>",
    unsafe_allow_html=True
)

# Load bookmarks
try:
    with open("bookmarks.json", "r") as file:
        bookmarks = json.load(file)
except Exception as e:
    st.error(f"Unable to load bookmarks.json: {e}")
    st.stop()

# Sidebar
with st.sidebar:
    query = st.text_input("Search by title or domain", "")
    refresh = st.button("Refresh All Feeds")

# Filter bookmarks
filtered = [
    bm for bm in bookmarks
    if query.lower() in bm["title"].lower() or query.lower() in bm["domain"].lower()
]

# Collect NWS alerts
nws_alerts = []
for bm in filtered:
    domain = bm.get("domain", "")
    url = bm.get("url")
    if domain == "api.weather.gov":
        scraper = get_scraper(domain)
        if scraper:
            try:
                data = scraper(url)
                if isinstance(data, dict) and "entries" in data:
                    nws_alerts.extend(data["entries"])
            except:
                continue

# Count new alerts
total_nws = len(nws_alerts)
new_nws = total_nws - st.session_state["nws_seen_count"]
if new_nws < 0:
    new_nws = 0

# Card UI for NWS
with st.container():
    st.markdown("""
    <style>
    .card {
        background-color: #f8f9fa;
        border: 1px solid #ddd;
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
    }
    .card-header {
        font-size: 1.2rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown(f"""
        <div class="card">
            <div class="card-header">📡 NWS Active Alerts</div>
            <p><strong>{total_nws}</strong> total alerts</p>
            <p><strong>{new_nws}</strong> new since last viewed</p>
        """, unsafe_allow_html=True)

    if st.button("View Alerts", key="nws_toggle_btn"):
        # Toggle visibility and reset "new" count
        st.session_state["nws_show_alerts"] = not st.session_state["nws_show_alerts"]
        if st.session_state["nws_show_alerts"]:
            st.session_state["nws_seen_count"] = total_nws

    if st.session_state["nws_show_alerts"]:
        for i, entry in enumerate(nws_alerts):
            raw_title = entry.get("title", "")
            title = str(raw_title).strip() or f"⚠️ Alert #{i + 1}"
            summary = str(entry.get("summary", "") or "")
            if len(summary) > 500:
                summary = summary[:500] + "..."
            published = str(entry.get("published", "") or "")
            link = entry.get("link", "").strip()
            is_new = i >= total_nws - new_nws
            prefix = "🆕 " if is_new else ""

            st.markdown(f"**{prefix}{title}**")
            if summary.strip():
                st.markdown(summary)
            if link:
                st.markdown(f"[Read more]({link})")
            if published:
                st.caption(f"Published: {published}")
            st.markdown("---")

    st.markdown("</div>", unsafe_allow_html=True)
