import streamlit as st
import os
import sys
import json

# Extend path to enable clean imports from subfolders
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.domain_router import get_scraper

# Page config
st.set_page_config(page_title="Global Weather Monitor", layout="wide")

# Session state: track "seen" NWS alerts
if "nws_seen_count" not in st.session_state:
    st.session_state["nws_seen_count"] = 0

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

# Collect NWS alerts separately
nws_alerts = []
other_feeds = []

for bm in filtered:
    domain = bm.get("domain", "")
    url = bm.get("url")
    title = bm.get("title", "Untitled")

    scraper = get_scraper(domain)
    if not scraper:
        other_feeds.append((bm, None, f"No scraper for domain: {domain}"))
        continue

    try:
        data = scraper(url)
        if domain == "api.weather.gov" and isinstance(data, dict) and "entries" in data:
            nws_alerts.extend(data["entries"])
        else:
            other_feeds.append((bm, data, None))
    except Exception as e:
        other_feeds.append((bm, None, str(e)))

# Display NWS alerts with "new" logic
if nws_alerts:
    st.markdown(f"### NWS Active Alerts ({len(nws_alerts)} total)")

    new_alerts_count = len(nws_alerts) - st.session_state["nws_seen_count"]
    if new_alerts_count < 0:
        new_alerts_count = 0

    show_nws = st.checkbox(f"📡 Show NWS Alerts ({new_alerts_count} new)", value=False)

    if show_nws:
        st.session_state["nws_seen_count"] = len(nws_alerts)

        with st.expander("NWS Alerts", expanded=True):
            for i, entry in enumerate(nws_alerts):
                raw_title = entry.get("title", "")
                title = str(raw_title).strip() or f"⚠️ Alert #{i + 1}"
                summary = str(entry.get("summary", "") or "")
                if len(summary) > 500:
                    summary = summary[:500] + "..."

                published = str(entry.get("published", "") or "")
                link = entry.get("link", "").strip()

                is_new = i >= len(nws_alerts) - new_alerts_count
                prefix = "🆕 " if is_new else ""

                try:
                    with st.expander(label=f"{prefix}{title}"):
                        st.markdown(summary if summary.strip() else "_No description available._")
                        if link:
                            st.markdown(f"[View full alert]({link})")
                        if published:
                            st.caption(f"Published: {published}")
                except Exception as e:
                    st.error(f"⚠️ Failed to display alert: {e}")

# Display all other feeds normally
for bm, data, error in other_feeds:
    domain = bm.get("domain")
    title = bm.get("title")
    url = bm.get("url")

    if error:
        st.warning(f"{title}: {error}")
        continue

    if isinstance(data, dict) and "entries" in data:
        st.markdown(f"### {data.get('feed_title', title)}")
        for i, entry in enumerate(data.get("entries", [])):
            raw_title = entry.get("title", "")
            item_title = str(raw_title).strip() or f"⚠️ Alert #{i + 1}"

            summary = str(entry.get("summary", "") or "")
            if len(summary) > 500:
                summary = summary[:500] + "..."

            published = str(entry.get("published", "") or "")
            link = entry.get("link", "").strip()

            try:
                with st.expander(label=item_title):
                    st.markdown(summary if summary.strip() else "_No description available._")
                    if link:
                        st.markdown(f"[View full alert]({link})")
                    if published:
                        st.caption(f"Published: {published}")
            except Exception as e:
                st.error(f"⚠️ Failed to display alert: {e}")
    else:
        st.markdown(f"### {data.get('location', title)}")
        st.markdown(f"**Temperature:** {data.get('temperature', 'N/A')}")
        st.markdown(f"**Condition:** {data.get('condition', 'N/A')}")
        st.markdown(f"[View source]({data.get('source', url)})")
        st.markdown("---")
