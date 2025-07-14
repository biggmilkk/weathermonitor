import streamlit as st
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
import logging
import re
from datetime import datetime

# Internal async fetch for a single region
async def _fetch_and_parse(session, region):
    url = region.get("ATOM URL")
    if not url:
        return []

    try:
        async with session.get(url, timeout=10) as resp:
            text = await resp.text()
            root = ET.fromstring(text)
            entries = []

            for entry in root.findall("{http://www.w3.org/2005/Atom}entry"):
                title_elem = entry.find("{http://www.w3.org/2005/Atom}title")
                summary_elem = entry.find("{http://www.w3.org/2005/Atom}summary")
                link_elem = entry.find("{http://www.w3.org/2005/Atom}link")
                published_elem = entry.find("{http://www.w3.org/2005/Atom}published")

                title = title_elem.text if title_elem is not None else ""
                if not title:
                    continue

                # Skip ended alerts and 'no alert' entries
                up = title.upper()
                if 'ENDED' in up or up.startswith('NO ALERT'):
                    continue

                alert_type = re.split(r",\s*", title)[0].strip().upper()
                if 'WARNING' not in alert_type and alert_type != 'SEVERE THUNDERSTORM WATCH':
                    continue

                raw_pub = published_elem.text if published_elem is not None else ""
                try:
                    pub_dt = datetime.strptime(raw_pub, "%Y-%m-%dT%H:%M:%SZ")
                    pub_iso = pub_dt.isoformat()
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
    except Exception as e:
        logging.warning(f"[EC FETCH ERROR] {url} - {e}")
        return []

# Cached entry point for Streamlit
@st.cache_data(ttl=60)
def scrape_ec(sources):
    """
    Fetch and parse Environment Canada Atom feeds for multiple regions.
    Cached for 60 seconds to minimize repeated network and XML parsing.
    Returns a dict with 'entries' (list) and 'source' identifier.
    """
    try:
        all_entries = asyncio.run(_scrape_ec_async(sources))
        return {"entries": all_entries, "source": "Environment Canada"}
    except Exception as e:
        logging.warning(f"[EC SCRAPER ERROR] {e}")
        return {"entries": [], "error": str(e), "source": "Environment Canada"}

# Helper async runner
async def _scrape_ec_async(sources):
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_and_parse(session, region) for region in sources if region.get("ATOM URL")]
        results = await asyncio.gather(*tasks)
        all_entries = []
        for result in results:
            all_entries.extend(result)
        logging.warning(f"[EC DEBUG] Successfully fetched {len(all_entries)} alerts")
        return all_entries
