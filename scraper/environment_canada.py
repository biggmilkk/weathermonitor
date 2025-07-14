import streamlit as st
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
import logging
import re
from datetime import datetime

# Namespace for Atom feeds
en = {"atom": "http://www.w3.org/2005/Atom"}
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

async def fetch_feed(session, region):
    """
    Fetch and parse a single ATOM feed. Returns list of alert dicts:
    {"alert": str, "area": str, "published": isoformat str}
    """
    url = region.get("ATOM URL")
    if not url:
        return []
    try:
        async with session.get(url, timeout=10) as resp:
            text = await resp.text()
    except Exception as e:
        logging.warning(f"[EC FETCH ERROR] {url} - {e}")
        return []

    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        logging.warning(f"[EC PARSE ERROR] {url} - {e}")
        return []

    entries = []
    for entry in root.findall("atom:entry", en):
        title_elem = entry.find("atom:title", en)
        pub_elem = entry.find("atom:published", en) or entry.find("atom:updated", en)
        if title_elem is None or not title_elem.text:
            continue
        title_text = title_elem.text.strip()
        # skip expiry notices
        if re.search(r"ended", title_text, re.IGNORECASE):
            continue
        # split into alert and area
        parts = [p.strip() for p in title_text.split(",", 1)]
        alert_type = parts[0]
        area = parts[1] if len(parts) == 2 else region.get("Region Name", "")
        # filter for warnings only
        if not (re.search(r"warning\b", alert_type, re.IGNORECASE)
                or re.match(r"severe thunderstorm watch", alert_type, re.IGNORECASE)):
            continue
        # parse published time
        time_text = pub_elem.text.strip() if pub_elem is not None and pub_elem.text else ""
        try:
            dt = datetime.strptime(time_text, TIME_FORMAT)
            published = dt.isoformat()
        except ValueError:
            published = time_text
        entries.append({
            "alert": alert_type,
            "area": area,
            "published": published
        })
    return entries

async def scrape_all(sources):
    """
    Concurrently fetch all feeds and aggregate entries, sorted by published desc.
    """
    tasks = []
    async with aiohttp.ClientSession() as session:
        for region in sources:
            if isinstance(region, dict) and isinstance(region.get("ATOM URL"), str):
                tasks.append(fetch_feed(session, region))
        results = await asyncio.gather(*tasks)
    all_entries = [item for sub in results for item in sub]
    # sort by published datetime (fallback unsorted)
    def _key(e):
        try:
            return datetime.fromisoformat(e["published"])
        except Exception:
            return datetime.min
    all_entries.sort(key=_key, reverse=True)
    return all_entries

@st.cache_data(ttl=60)
def scrape_ec(sources):
    """
    sources: list of dicts each with 'ATOM URL' and 'Region Name'.
    Returns dict with 'entries' (filtered and sorted) and 'source'.
    """
    if not isinstance(sources, list):
        logging.error(f"[EC SCRAPER ERROR] Expected liste of sources, got {type(sources)}")
        return {"entries": [], "error": "Invalid sources type", "source": "Environment Canada"}

    entries = asyncio.run(scrape_all(sources))
    return {"entries": entries, "source": "Environment Canada"}
