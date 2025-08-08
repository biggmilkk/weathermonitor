import streamlit as st
import asyncio
import httpx
import xml.etree.ElementTree as ET
import logging
import re
from datetime import datetime
from typing import List, Dict, Any, Optional

# Atom namespace and timestamp format
ns = {"atom": "http://www.w3.org/2005/Atom"}
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
PROVINCE_FROM_URL = re.compile(r"/battleboard/([a-z]{2})\d+_e\.xml", re.IGNORECASE)

# Browser-like headers to reduce chance of 403s / weird blocks
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    ),
    "Accept": "application/atom+xml, application/xml;q=0.9, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://weather.gc.ca/",
}

def _parse_atom(xml_text: str, url: str, region: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Parse one EC Atom XML string into normalized entries.
    Skips 'ended' items and passes through only warnings and severe thunderstorm watches.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logging.warning(f"[EC PARSE ERROR] {url} - {e}")
        return []

    region_name = region.get("Region Name", "") or region.get("region") or ""
    explicit = region.get("Province-Territory") or region.get("province") or ""
    if explicit:
        province = explicit
    else:
        m = PROVINCE_FROM_URL.search(url or "")
        province = (m.group(1).upper() if m else "")

    entries: List[Dict[str, str]] = []
    for entry in root.findall("atom:entry", ns):
        title_elem = entry.find("atom:title", ns)
        if title_elem is None or not (title_elem.text or "").strip():
            continue

        raw = title_elem.text.strip()

        # Skip expired/cancelled items
        if re.search(r"\bended\b", raw, re.IGNORECASE):
            continue

        # Keep warnings + severe thunderstorm watches
        if not (
            re.search(r"\bwarning\b", raw, re.IGNORECASE)
            or re.match(r"severe thunderstorm watch", raw, re.IGNORECASE)
        ):
            continue

        # Try to split "Alert, Area" pattern; fall back to region name
        parts = [p.strip() for p in raw.split(",", 1)]
        alert = parts[0]
        area = parts[1] if len(parts) == 2 else region_name

        pub = entry.find("atom:published", ns) or entry.find("atom:updated", ns)
        ts = (pub.text or "").strip() if pub is not None else ""
        try:
            # Normalize to ISO for sorting
            published = datetime.strptime(ts, TIME_FORMAT).isoformat()
        except Exception:
            published = ts

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

async def _fetch_one_httpx(client: httpx.AsyncClient, region: Dict[str, Any], sem: asyncio.Semaphore, timeout: float) -> List[Dict[str, str]]:
    url = region.get("ATOM URL")
    if not url:
        return []

    try:
        async with sem:
            resp = await client.get(url, headers=HEADERS, timeout=timeout, follow_redirects=True)
            resp.raise_for_status()
            text = resp.text
    except Exception as e:
        logging.warning(f"[EC FETCH ERROR] {url} - {e}")
        return []

    return _parse_atom(text, url, region)

async def _scrape_async_httpx(sources: List[Dict[str, Any]], client: Optional[httpx.AsyncClient] = None) -> List[Dict[str, str]]:
    """
    Internal async fetch for Environment Canada sources using httpx.
    If a client is supplied, reuse it; otherwise, create a temporary one.
    """
    # Sensible defaults; can be overridden by adding these keys into any source dict if needed
    timeout = 12.0
    max_conc = 500  # EC has many feeds; keep it polite

    sem = asyncio.Semaphore(max_conc)

    close_client = False
    if client is None:
        client = httpx.AsyncClient()
        close_client = True

    try:
        tasks = [
            _fetch_one_httpx(client, r, sem, timeout)
            for r in sources
            if isinstance(r, dict) and r.get("ATOM URL")
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)
    finally:
        if close_client:
            await client.aclose()

    # flatten and sort reverse-chronological
    flat = [e for sub in results for e in sub]

    def key(e: Dict[str, str]):
        try:
            return datetime.fromisoformat(e.get("published", ""))
        except Exception:
            return datetime.min

    return sorted(flat, key=key, reverse=True)

@st.cache_data(ttl=60, show_spinner=False)
def scrape_ec(sources: List[Dict[str, Any]]) -> dict:
    """
    Synchronous wrapper: spins a temporary AsyncClient and runs the async pipeline.
    """
    if not isinstance(sources, list):
        logging.error(f"[EC SCRAPER ERROR] Invalid sources type: {type(sources)}")
        return {"entries": [], "error": "Invalid sources type", "source": "Environment Canada"}

    entries = asyncio.run(_scrape_async_httpx(sources))
    logging.warning(f"[EC DEBUG] Successfully parsed {len(entries)} alerts")
    return {"entries": entries, "source": "Environment Canada"}

async def scrape_ec_async(sources: List[Dict[str, Any]], client: httpx.AsyncClient) -> dict:
    """
    Async wrapper for Environment Canada scraper that **reuses the shared httpx client**.
    This lets the app fetch EC feeds concurrently alongside other feeds with one pool.
    """
    try:
        entries = await _scrape_async_httpx(sources, client=client)
        logging.warning(f"[EC DEBUG] Aync parsed {len(entries)} alerts")
        return {"entries": entries, "source": "Environment Canada"}
    except Exception as e:
        logging.warning(f"[EC ERROR] Async fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": "Environment Canada"}
