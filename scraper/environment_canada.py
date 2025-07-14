import streamlit as st
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
import logging
import re
from datetime import datetime

# Atom namespace and timestamp format
en = {"atom": "http://www.w3.org/2005/Atom"}
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# Regex to extract province code from URL, e.g. /battleboard/nl3_e.xml
PROVINCE_FROM_URL = re.compile(r"/battleboard/([a-z]{2})\d+_e\.xml", re.IGNORECASE)

async def fetch_feed(session, region):
    """
    Fetch one Atom feed and return list of warning-level alerts.
    Each entry dict has keys: title, region, province, published, link.
    """
    url = region.get("ATOM URL")
    if not isinstance(url, str) or not url:
        return []
    try:
        async with session.get(url, timeout=10) as resp:
            xml_text = await resp.text()
    except Exception as e:
        logging.warning(f"[EC FETCH ERROR] {url} - {e}")
        return []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logging.warning(f"[EC PARSE ERROR] {url} - {e}")
        return []

    # Determine province: prefer explicit, else derive from URL
    explicit = region.get("Province-Territory") or region.get("province")
    if isinstance(explicit, str) and explicit:
        province = explicit
    else:
        m = PROVINCE_FROM_URL.search(url)
        province = m.group(1).upper() if m else ""

    entries = []
    for elem in root.findall("atom:entry", en):
        title_elem = elem.find("atom:title", en)
        if title_elem is None or not title_elem.text:
            continue
        raw_title = title_elem.text.strip()
        # Skip expired alerts
        if re.search(r"ended", raw_title, re.IGNORECASE):
            continue
        # Split into alert type and area
        parts = [p.strip() for p in raw_title.split(",", 1)]
        alert_type = parts[0]
        area = parts[1] if len(parts) == 2 else region.get("Region Name", "")
        # Only warnings or severe thunderstorm watches
        if not (re.search(r"warning\b", alert_type, re.IGNORECASE)
                or re.match(r"severe thunderstorm watch", alert_type, re.IGNORECASE)):
            continue
        # Extract published or updated timestamp
        pub_elem = elem.find("atom:published", en) or elem.find("atom:updated", en)
        time_text = pub_elem.text.strip() if pub_elem is not None and pub_elem.text else ""
        try:
            dt = datetime.strptime(time_text, TIME_FORMAT)
            published = dt.isoformat()
        except ValueError:
            published = time_text
        # Extract read-more link
        link_elem = elem.find("atom:link", en)
        link = link_elem.attrib.get("href", "") if link_elem is not None else url
        entries.append({
            "title": alert_type,
            "region": area,
            "province": province,
            "published": published,
            "link": link
        })
    return entries

async def scrape_all(sources):
    """
    Concurrently fetch all feeds and return combined list sorted by published desc.
    """
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_feed(session, r)
                 for r in sources
                 if isinstance(r, dict) and isinstance(r.get("ATOM URL"), str)]
        results = await asyncio.gather(*tasks)
    all_entries = [e for sub in results for e in sub]
    # Sort most recent first
    def key_fn(item):
        try:
            return datetime.fromisoformat(item["published"])
        except Exception:
            return datetime.min
    return sorted(all_entries, key=key_fn, reverse=True)

@st.cache_data(ttl=60)
def scrape_ec(conf):
    """
    Wrapper for Streamlit: accepts either a list of region dicts or a dict with 'sources'.
    Returns {'entries': [...], 'source': 'Environment Canada'}.
    Each entry has: title, region, province, published, link.
    """
    if isinstance(conf, dict):
        sources = conf.get("sources", [])
    else:
        sources = conf if isinstance(conf, list) else []

    if not isinstance(sources, list):
        logging.error(f"[EC SCRAPER ERROR] Invalid sources type: {type(sources)}")
        return {"entries": [], "error": "Invalid sources type", "source": "Environment Canada"}

    try:
        entries = asyncio.run(scrape_all(sources))
    except Exception as e:
        logging.warning(f"[EC SCRAPER ERROR] {e}")
        return {"entries": [], "error": str(e), "source": "Environment Canada"}

    return {"entries": entries, "source": "Environment Canada"}
