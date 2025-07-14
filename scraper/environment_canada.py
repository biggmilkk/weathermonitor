import streamlit as st
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
import logging
import re
from datetime import datetime

async def _fetch_and_parse(session, region):
    # region should be a dict with keys including "ATOM URL"
    if not isinstance(region, dict):
        logging.warning(f"[EC FETCH ERROR] Invalid region config: {region}")
        return []
    url = region.get("ATOM URL")
    if not url:
        return []
    try:
        async with session.get(url, timeout=10) as resp:
            text = await resp.text()
            root = ET.fromstring(text)
    except Exception as e:
        logging.warning(f"[EC FETCH ERROR] {url} - {e}")
        return []

    entries = []
    for item in root.findall("{http://www.w3.org/2005/Atom}entry"):
        title_elem = item.find("{http://www.w3.org/2005/Atom}title")
        summary_elem = item.find("{http://www.w3.org/2005/Atom}summary")
        link_elem = item.find("{http://www.w3.org/2005/Atom}link")
        pub_elem = item.find("{http://www.w3.org/2005/Atom}published")

        title = title_elem.text if title_elem is not None else ""
        if not title:
            continue
        if 'ENDED' in title.upper() or title.strip().upper().startswith('NO ALERT'):
            continue

        alert_type = re.split(r",\s*", title)[0].strip().upper()
        if 'WARNING' not in alert_type and alert_type != 'SEVERE THUNDERSTORM WATCH':
            continue

        raw_pub = pub_elem.text if pub_elem is not None else ""
        try:
            dt = datetime.strptime(raw_pub, "%Y-%m-%dT%H:%M:%SZ")
            pub_iso = dt.isoformat()
        except Exception:
            pub_iso = raw_pub

        entries.append({
            "title": alert_type,
            "summary": summary_elem.text[:500] if summary_elem is not None else "",
            "link": link_elem.attrib.get("href", "") if link_elem is not None else "",
            "published": pub_iso,
            "region": region.get("Region Name", ""),
            "province": region.get("Province-Territory", "")
        })
    return entries

async def _scrape_ec_async(sources):
    async with aiohttp.ClientSession() as session:
        # filter out invalid configs
        tasks = [
            _fetch_and_parse(session, region)
            for region in sources
            if isinstance(region, dict)
        ]
        results = await asyncio.gather(*tasks)
        all_entries = [e for sub in results for e in sub]
        logging.warning(f"[EC DEBUG] Successfully fetched {len(all_entries)} alerts")
        return all_entries

@st.cache_data(ttl=60)
def scrape_ec(conf):
    """
    Fetch and parse Environment Canada Atom feeds for multiple regions.
    Accepts either:
      - conf as a dict with key 'sources': a list of region dicts
      - conf directly as a list of region dicts
    Cached for 60 seconds to minimize network and XML parsing.
    Returns a dict with 'entries' and 'source'.
    """
    # Support both dict and list inputs
    if isinstance(conf, dict):
        sources = conf.get("sources", [])
        label = conf.get("label", "Environment Canada")
    elif isinstance(conf, list):
        sources = conf
        label = "Environment Canada"
    else:
        logging.warning(f"[EC SCRAPER ERROR] Invalid conf type: {type(conf)}")
        return {"entries": [], "error": "Invalid configuration", "source": "Environment Canada"}

    try:
        all_entries = asyncio.run(_scrape_ec_async(sources))
        return {"entries": all_entries, "source": label}
    except Exception as e:
        logging.warning(f"[EC SCRAPER ERROR] {e}")
        return {"entries": [], "error": str(e), "source": label}
