import streamlit as st
import json
from utils.domain_router import get_scraper

st.set_page_config(page_title="🌍 Global Weather Monitor", layout="wide")
st.title("🌦️ Global Weather Dashboard")

# Load bookmarks
with open("bookmarks.json", "r") as f:
    bookmarks = json.load(f)

# Search bar
query = st.text_input("🔍 Search by title or domain")
refresh = st.button("🔄 Refresh All")

# Filter bookmarks
filtered = [b for b in bookmarks if query.lower() in b["title"].lower() or query.lower() in b["domain"].lower()]

if not filtered:
    st.info("No bookmarks matched your search.")
else:
    for bm in filtered:
        scraper = get_scraper(bm["domain"])
        if scraper:
            with st.spinner(f"Fetching from {bm['domain']}..."):
                data = scraper(bm["url"]) if refresh else scraper(bm["url"])  # Always live for now
                st.subheader(data.get("location", bm["title"]))
                st.write(f"**Temperature**: {data.get('temperature', 'N/A')}")
                st.write(f"**Condition**: {data.get('condition', '')}")
                st.markdown(f"[🔗 Source]({data.get('source', bm['url'])})")
                st.markdown("---")
        else:
            st.warning(f"No scraper for domain: {bm['domain']}")
