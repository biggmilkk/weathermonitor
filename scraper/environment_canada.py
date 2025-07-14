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
PROVINCE_FROM_URL = re.compile(r"/battleboard/([a-z]{2})\d+_e\.xml", re.IGNORECASE)

async def fetch_feed_async(url, region_name, province):
    """
    Async fetch and parse a single Atom feed URL with given region_name and province.
    Returns list of entries dicts.
    """
    try:
        async with aiohttp.ClientSession() as session:
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

    entries = []
    for elem in root.findall("atom:entry", en):
        title_elem = elem.find("atom:title", en)
        if title_elem is None or not title_elem.text:
            continue
        raw_title = title_elem.text.strip()
        if re.search(r"ended", raw_title, re.IGNORECASE):
            continue
        parts = [p.strip() for p in raw_title.split(",", 1)]
        alert_type = parts[0]
        area = parts[1] if len(parts) == 2 else region_name
        if not (re.search(r"warning\b", alert_type, re.IGNORECASE)
                or re.match(r"severe thunderstorm watch", alert_type, re.IGNORECASE)):
            continue
        pub_elem = elem.find("atom:published", en) or elem.find("atom:updated", en)
        time_text = pub_elem.text.strip() if pub_elem is not None and pub_elem.text else ""
        try:
            dt = datetime.strptime(time_text, TIME_FORMAT)
            published = dt.isoformat()
        except ValueError:
            published = time_text
        link_elem = elem.find("atom:link", en)
        link = link_elem.attrib.get("href", url) if link_elem is not None else url
        entries.append({
            "title": alert_type,
            "region": area,
            "province": province,
            "published": published,
            "link": link
        })
    return entries

@st.cache_data(ttl=60)
def fetch_feed(url: str, region_name: str, province: str):
    """
    Cached wrapper around the async fetch_feed_async function.
    TTL=60s to limit memory churn when running many feeds.
    """
    return asyncio.run(fetch_feed_async(url, region_name, province))

@st.cache_data(ttl=60)
def scrape_ec(sources):
    """
    Synchronously fetch and aggregate all feeds.
    Uses per-feed caching to reduce RAM and network overhead.
    """
    all_entries = []
    for region in sources or []:
        url = region.get("ATOM URL")
        if not url:
            continue
        region_name = region.get("Region Name", "")
        explicit = region.get("Province-Territory") or region.get("province") or ""
        if explicit:
            province = explicit
        else:
            m = PROVINCE_FROM_URL.search(url)
            province = m.group(1).upper() if m else ""
        entries = fetch_feed(url, region_name, province)
        all_entries.extend(entries)
    # sort by most recent published
    try:
        all_entries.sort(
            key=lambda e: datetime.fromisoformat(e["published"]),
            reverse=True
        )
    except Exception:
        pass
    return {"entries": all_entries, "source": "Environment Canada"}
