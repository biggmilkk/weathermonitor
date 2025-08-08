import streamlit as st
import logging
import re
from datetime import datetime
import xml.etree.ElementTree as ET
from typing import List, Dict, Any

import httpx
import asyncio

# Browser-like headers (EC can be picky)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml, text/xml, application/atom+xml;q=0.9, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://weather.gc.ca/",
}

# Atom namespace and timestamp format
ns = {"atom": "http://www.w3.org/2005/Atom"}
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
PROVINCE_FROM_URL = re.compile(r"/battleboard/([a-z]{2})\d+_e\.xml", re.IGNORECASE)

def _parse_atom(text: str, url: str, region_name: str, province_hint: str) -> List[Dict[str, Any]]:
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        logging.warning(f"[EC PARSE ERROR] {url} - {e}")
        return []

    entries: List[Dict[str, Any]] = []
    for entry in root.findall("atom:entry", ns):
        title_elem = entry.find("atom:title", ns)
        if title_elem is None or not title_elem.text:
            continue

        raw = (title_elem.text or "").strip()

        # skip expired-style lines
        if re.search(r"\bended\b", raw, re.IGNORECASE):
            continue

        # Keep Warnings and Severe Thunderstorm Watch
        if not (re.search(r"\bwarning\b", raw, re.IGNORECASE)
                or re.match(r"severe thunderstorm watch", raw, re.IGNORECASE)):
            continue

        parts = [p.strip() for p in raw.split(",", 1)]
        alert = parts[0]
        area  = parts[1] if len(parts) == 2 else region_name

        pub = entry.find("atom:published", ns) or entry.find("atom:updated", ns)
        ts = pub.text.strip() if pub is not None and pub.text else ""
        try:
            published = datetime.strptime(ts, TIME_FORMAT).isoformat()
        except Exception:
            published = ts

        link_elem = entry.find("atom:link", ns)
        link = link_elem.get("href") if link_elem is not None else url

        entries.append({
            "title": alert,
            "region": area,
            "province": province_hint,
            "published": published,
            "link": link
        })
    return entries

async def _fetch_one_httpx(client: httpx.AsyncClient, region: dict) -> List[Dict[str, Any]]:
    url = region.get("ATOM URL")
    if not url:
        return []

    # Province code: explicit > from URL > ""
    region_name = region.get("Region Name", "")
    explicit = region.get("Province-Territory") or region.get("province") or ""
    if explicit:
        province = explicit
    else:
        m = PROVINCE_FROM_URL.search(url or "")
        province = m.group(1).upper() if m else ""

    try:
        resp = await client.get(url, headers=HEADERS, timeout=20, follow_redirects=True)
        resp.raise_for_status()
        text = resp.text
    except httpx.HTTPStatusError as e:
        logging.warning(f"[EC FETCH ERROR] {url} - HTTP {e.response.status_code} {e.response.reason_phrase}")
        return []
    except Exception as e:
        logging.warning(f"[EC FETCH ERROR] {url} - {e.__class__.__name__}: {e}")
        return []

    return _parse_atom(text, url, region_name, province)

async def _scrape_async_httpx(sources: list, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    tasks = [
        _fetch_one_httpx(client, r)
        for r in sources
        if isinstance(r, dict) and r.get("ATOM URL")
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    flat = [e for sub in results for e in sub]

    def key(e):
        try:
            return datetime.fromisoformat(e["published"])
        except Exception:
            return datetime.min

    return sorted(flat, key=key, reverse=True)

@st.cache_data(ttl=60, show_spinner=False)
def scrape_ec(sources: list) -> dict:
    """
    Synchronous wrapper (kept for API compatibility).
    Uses its own short-lived httpx client.
    """
    if not isinstance(sources, list):
        logging.error(f"[EC SCRAPER ERROR] Invalid sources type: {type(sources)}")
        return {"entries": [], "error": "Invalid sources type", "source": "Environment Canada"}

    async def _runner():
        async with httpx.AsyncClient() as client:
            return await _scrape_async_httpx(sources, client)

    entries = asyncio.run(_runner())
    logging.warning(f"[EC DEBUG] Successfully parsed {len(entries)} alerts (sync wrapper)")
    return {"entries": entries, "source": "Environment Canada"}

async def scrape_ec_async(sources: list, client: httpx.AsyncClient) -> dict:
    """
    Async entry point used by the app.
    Reuses the shared httpx AsyncClient passed in by the framework.
    """
    try:
        entries = await _scrape_async_httpx(sources, client)
        logging.warning(f"[EC DEBUG] Parsed {len(entries)} alerts (async)")
        return {"entries": entries, "source": "Environment Canada"}
    except Exception as e:
        logging.warning(f"[EC ERROR] Async fetch failed: {e}")
        return {"entries": [], "error": str(e), "source": "Environment Canada"}
