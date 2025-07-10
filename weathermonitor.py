import streamlit as st
import os
import sys
import json

# Extend path to enable clean imports from subfolders
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.domain_router import get_scraper

# Page configuration
st.set_page_config(
    page_title="Global Weather Monitor",
    layout="wide"
)

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

# Sidebar controls
with st.sidebar:
    query = st.text_input("Search by title or domain", "")
    refresh = st.button("Refresh All Feeds")

# Filter bookmarks
filtered = [
    bm for bm in bookmarks
    if query.lower() in bm["title"].lower() or query.lower() in bm["domain"].lower()
]

if not filtered:
    st.info("No bookmarks matched your search criteria.")
else:
    for bm in filtered:
        domain = bm.get("domain")
        url = bm.get("url")
        title = bm.get("title")

        scraper = get_scraper(domain)

        if scraper:
            with st.spinner(f"Fetching data from {domain}..."):
                try:
                    data = scraper(url)
                except Exception as e:
                    st.error(f"Error fetching data from {url}: {e}")
                    continue

                # RSS-style feed
                if isinstance(data, dict) and "entries" in data:
                    st.markdown(f"### {data.get('feed_title', bm['title'])}")
                    for entry in data["entries"]:
                        raw_title = entry.get("title", "")
                        title = str(raw_title).strip()
                        if not title:
                        title = "Alert (untitled)"

                        summary = entry.get("summary", "")
                        summary = summary[:500] + "..." if len(summary) > 500 else summary
                        published = entry.get("published", "")
                        link = entry.get("link", "")

                        with st.expander(label=title):
                            st.markdown(summary or "_No description available._")
                            if link:
                                st.markdown(f"[View full alert]({link})")
                            if published:
                                st.caption(f"Published: {published}")

                # Static weather data
                else:
                    st.markdown(f"### {data.get('location', title)}")
                    st.markdown(f"**Temperature:** {data.get('temperature', 'N/A')}")
                    st.markdown(f"**Condition:** {data.get('condition', 'N/A')}")
                    st.markdown(f"[View source]({data.get('source', url)})")
                    st.markdown("---")
        else:
            st.warning(f"No scraper available for domain: {domain}")
