import streamlit as st
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
import logging
import re
from datetime import datetime

# Atom namespace and timestamp format
ns = {"atom": "http://www.w3.org/2005/Atom"}
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
PROVINCE_FROM_URL = re.compile(r"/battleboard/([a-z]{2})\d+_e\.xml", re.IGNORECASE)

async def _fetch_one(session, region):
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
    # derive metadata
    region_name = region.get("Region Name", "")
    explicit = region.get("Province-Territory") or region.get("province") or ""
    if explicit:
        province = explicit
    else:
        m = PROVINCE_FROM_URL.search(url)
        province = m.group(1).upper() if m else ""
    entries = []
    for entry in root.findall("atom:entry", ns):
        title_elem = entry.find("atom:title", ns)
        if title_elem is None or not title_elem.text:
            continue
        raw = title_elem.text.strip()
        # skip expired
        if re.search(r"ended", raw, re.IGNORECASE):
            continue
        parts = [p.strip() for p in raw.split(",", 1)]
        alert = parts[0]
        if not (re.search(r"warning\b", alert, re.IGNORECASE)
                or re.match(r"severe thunderstorm watch", alert, re.IGNORECASE)):
            continue
        area = parts[1] if len(parts) == 2 else region_name
        # timestamp
        pub = entry.find("atom:published", ns) or entry.find("atom:updated", ns)
        ts = pub.text.strip() if pub is not None and pub.text else ""
        try:
            published = datetime.strptime(ts, TIME_FORMAT).isoformat()
        except Exception:
            published = ts
        # link
        link_elem = entry.find("atom:link", ns)
        link = link_elem.get("href") if link_elem is not None else url
        entries.append({
            "title": alert,
            "region": area,
            "province": province,
            "published": published,
            "link": link
        })
    return entries

async def _scrape_async(sources):
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_one(session, r) for r in sources if isinstance(r, dict) and r.get("ATOM URL")]
        results = await asyncio.gather(*tasks)
    # flatten
    all_entries = [e for sub in results for e in sub]
    # sort
    def key(e):
        try:
            return datetime.fromisoformat(e["published"])
        except:
            return datetime.min
    return sorted(all_entries, key=key, reverse=True)

@st.cache_data(ttl=60, show_spinner=False)
def scrape_ec(sources):
    """
    sources: list of {'ATOM URL', 'Region Name', optional 'Province-Territory'}
    Returns {'entries': [...], 'source': 'Environment Canada'}
    """
    if not isinstance(sources, list):
        logging.error(f"[EC SCRAPER ERROR] Invalid sources type: {type(sources)}")
        return {"entries": [], "error": "Invalid sources type", "source": "Environment Canada"}
    # run async
    entries = asyncio.run(_scrape_async(sources))
    logging.warning(f"[EC DEBUG] Successfully parsed {len(entries)} alerts")
    return {"entries": entries, "source": "Environment Canada"}
