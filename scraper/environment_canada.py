import aiohttp
import asyncio
import xml.etree.ElementTree as ET
import logging
import re
from datetime import datetime

# --------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------
ns = {"atom": "http://www.w3.org/2005/Atom"}
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
PROVINCE_FROM_URL = re.compile(r"/battleboard/([a-z]{2})\d+_e\.xml", re.IGNORECASE)

PROVINCE_NAMES = {
    "AB": "Alberta",
    "BC": "British Columbia",
    "MB": "Manitoba",
    "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador",
    "NT": "Northwest Territories",
    "NS": "Nova Scotia",
    "NU": "Nunavut",
    "ON": "Ontario",
    "PE": "Prince Edward Island",
    "QC": "Quebec",
    "SK": "Saskatchewan",
    "YT": "Yukon",
}

# --------------------------------------------------------------------
# Core fetch/parse
# --------------------------------------------------------------------
async def _fetch_one(session: aiohttp.ClientSession, region: dict) -> list:
    url = (region or {}).get("ATOM URL")
    if not url:
        return []
    region_name = (region.get("Region Name") or "").strip()
    prov_code = (region.get("Province-Territory") or "").strip().upper()

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                logging.warning(f"[EC] {url} -> HTTP {resp.status}")
                return []
            text = await resp.text()
    except Exception as e:
        logging.warning(f"[EC] fetch error {url}: {e}")
        return []

    try:
        root = ET.fromstring(text)
    except Exception as e:
        logging.warning(f"[EC] XML parse error {url}: {e}")
        return []

    if not prov_code:
        m = PROVINCE_FROM_URL.search(url)
        prov_code = m.group(1).upper() if m else ""

    entries = []
    for entry in root.findall("atom:entry", ns):
        title_elem = entry.find("atom:title", ns)
        if title_elem is None or not (title_elem.text or "").strip():
            continue

        raw = title_elem.text.strip()
        if re.search(r"\bended\b", raw, re.IGNORECASE):
            continue

        parts = [p.strip() for p in raw.split(",", 1)]
        alert = parts[0]
        if not (re.search(r"warning\b", alert, re.IGNORECASE) or
                re.match(r"severe thunderstorm watch", alert, re.IGNORECASE)):
            continue
        area = parts[1] if len(parts) == 2 else region_name

        pub = entry.find("atom:published", ns) or entry.find("atom:updated", ns)
        ts = (pub.text or "").strip() if pub is not None else ""
        try:
            published = datetime.strptime(ts, TIME_FORMAT).isoformat()
        except Exception:
            published = ts

        link_e = entry.find("atom:link", ns)
        link = link_e.get("href") if link_e is not None else ""

        pcode = prov_code
        if not pcode:
            m2 = re.search(r",\s*([A-Z]{2})$", raw)
            pcode = m2.group(1) if m2 else ""

        pname = PROVINCE_NAMES.get(pcode, pcode)

        entries.append({
            "title": alert,
            "region": area or region_name,
            "province": pcode,
            "province_name": pname,  # self-contained (no constants.py)
            "published": published,
            "link": link,
        })
    return entries

async def _scrape_async(sources: list) -> list:
    sources = sources or []
    timeout = aiohttp.ClientTimeout(total=25)
    connector = aiohttp.TCPConnector(limit=12, ssl=False)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [_fetch_one(session, r) for r in sources if isinstance(r, dict)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    out = []
    for r in results:
        if isinstance(r, Exception):
            logging.warning(f"[EC] task error: {r}")
            continue
        out.extend(r or [])
    return out

# --------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------
async def scrape_ec_async(sources: list, client) -> dict:
    try:
        entries = await _scrape_async(sources)
        return {"entries": entries, "source": "Environment Canada"}
    except Exception as e:
        logging.warning(f"[EC] async failed: {e}")
        return {"entries": [], "error": str(e), "source": "Environment Canada"}
