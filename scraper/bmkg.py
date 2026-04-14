# scraper/bmkg.py
from __future__ import annotations

import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from typing import Any

import aiohttp
from email.utils import parsedate_to_datetime

# --------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------

RSS_URL_EN = "https://www.bmkg.go.id/alerts/nowcast/en/rss.xml"

RSS_NS = {
    "atom": "http://www.w3.org/2005/Atom",
}

CAP_NS = {
    "cap": "urn:oasis:names:tc:emergency:cap:1.2",
}

TIMEOUT_TOTAL = 25
CONNECTOR_LIMIT = 10  # keep this conservative vs BMKG's 60 req/min/IP guidance

# Severity mapping for later renderer / computation use
CAP_SEVERITY_TO_LEVEL = {
    "Minor": "Blue",
    "Moderate": "Yellow",
    "Severe": "Orange",
    "Extreme": "Red",
}

PROVINCE_FROM_HEADLINE_RE = re.compile(r"\bin\s+(.+?)\s*$", re.IGNORECASE)


# --------------------------------------------------------------------
# Generic helpers
# --------------------------------------------------------------------

def _norm(s: Any) -> str:
    return str(s or "").strip()

def _first_text(parent: ET.Element | None, path: str, ns: dict[str, str]) -> str:
    if parent is None:
        return ""
    el = parent.find(path, ns)
    return _norm(el.text if el is not None else "")

def _all_texts(parent: ET.Element | None, path: str, ns: dict[str, str]) -> list[str]:
    if parent is None:
        return []
    out: list[str] = []
    for el in parent.findall(path, ns):
        txt = _norm(el.text)
        if txt:
            out.append(txt)
    return out

def _parse_rfc2822_to_iso(s: str) -> str:
    s = _norm(s)
    if not s:
        return ""
    try:
        return parsedate_to_datetime(s).isoformat()
    except Exception:
        return s

def _province_from_headline(headline: str) -> str:
    """
    Example:
      'Thunderstorm This Afternoon in Jawa Barat' -> 'Jawa Barat'
    """
    h = _norm(headline)
    if not h:
        return "Indonesia"

    m = PROVINCE_FROM_HEADLINE_RE.search(h)
    if m:
        return _norm(m.group(1)) or "Indonesia"

    return "Indonesia"

def _cap_info_for_language(root: ET.Element, lang: str = "en") -> ET.Element | None:
    """
    CAP can contain multiple <info> blocks. Prefer the requested language.
    """
    infos = root.findall("cap:info", CAP_NS)
    if not infos:
        return None

    want = lang.lower().strip()
    for info in infos:
        language = _first_text(info, "cap:language", CAP_NS).lower()
        if language == want:
            return info

    # fallback to first info block
    return infos[0]

def _extract_web_link(info_el: ET.Element | None) -> str:
    """
    BMKG documents a CAP parameter key called 'web'.
    It usually appears under:
      <parameter>
        <valueName>web</valueName>
        <value>...</value>
      </parameter>
    """
    if info_el is None:
        return ""
    for param in info_el.findall("cap:parameter", CAP_NS):
        name = _first_text(param, "cap:valueName", CAP_NS).lower()
        if name == "web":
            return _first_text(param, "cap:value", CAP_NS)
    return ""

def _parse_cap_xml(xml_text: str, fallback_rss_item: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """
    Parse one BMKG CAP alert XML into a normalized entry.
    """
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        logging.warning(f"[BMKG] CAP XML parse error: {e}")
        return None

    info = _cap_info_for_language(root, lang="en")
    if info is None:
        return None

    identifier  = _first_text(root, "cap:identifier", CAP_NS)
    sent        = _first_text(root, "cap:sent", CAP_NS)
    status      = _first_text(root, "cap:status", CAP_NS)
    msg_type    = _first_text(root, "cap:msgType", CAP_NS)
    scope       = _first_text(root, "cap:scope", CAP_NS)

    language    = _first_text(info, "cap:language", CAP_NS)
    category    = _first_text(info, "cap:category", CAP_NS)
    event       = _first_text(info, "cap:event", CAP_NS)
    urgency     = _first_text(info, "cap:urgency", CAP_NS)
    severity    = _first_text(info, "cap:severity", CAP_NS)
    certainty   = _first_text(info, "cap:certainty", CAP_NS)
    effective   = _first_text(info, "cap:effective", CAP_NS)
    expires     = _first_text(info, "cap:expires", CAP_NS)
    sender_name = _first_text(info, "cap:senderName", CAP_NS)
    headline    = _first_text(info, "cap:headline", CAP_NS)
    description = _first_text(info, "cap:description", CAP_NS)
    instruction = _first_text(info, "cap:instruction", CAP_NS)

    # eventCode / OET if present
    oet_code = ""
    for ec in info.findall("cap:eventCode", CAP_NS):
        value_name = _first_text(ec, "cap:valueName", CAP_NS)
        if value_name == "OET":
            oet_code = _first_text(ec, "cap:value", CAP_NS)
            break

    # affected areas
    area_descs = _all_texts(info, "cap:area/cap:areaDesc", CAP_NS)

    # prefer BMKG's documented 'web' parameter; fallback to RSS link
    web_link = _extract_web_link(info)
    rss_link = _norm((fallback_rss_item or {}).get("link"))
    final_link = web_link or rss_link

    # province from headline/title
    province_name = _province_from_headline(headline or _norm((fallback_rss_item or {}).get("title")))

    # title/headline fallback chain
    title = headline or _norm((fallback_rss_item or {}).get("title")) or event or "BMKG Weather Alert"

    # summary fallback chain
    summary = description or _norm((fallback_rss_item or {}).get("description"))

    level = CAP_SEVERITY_TO_LEVEL.get(severity, severity or "")

    # Store the first areaDesc as "region" for later renderers, but keep the full list too
    region = area_descs[0] if area_descs else province_name

    return {
        "id": identifier,
        "identifier": identifier,
        "title": title,
        "headline": headline or title,
        "summary": summary,
        "description": description,
        "instruction": instruction,
        "event": event,
        "level": level,              # normalized color-ish level
        "severity": severity,        # raw CAP severity
        "urgency": urgency,
        "certainty": certainty,
        "status": status,
        "msg_type": msg_type,
        "scope": scope,
        "category": category,
        "language": language,
        "oet_code": oet_code,
        "effective": effective,
        "expires": expires,
        "published": sent or _norm((fallback_rss_item or {}).get("published")),
        "sender_name": sender_name,
        "province": province_name,
        "province_name": province_name,
        "region": region,
        "areas": area_descs,
        "link": final_link,
        "source": "BMKG",
    }


# --------------------------------------------------------------------
# RSS parsing
# --------------------------------------------------------------------

def _parse_rss_items(xml_text: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        logging.warning(f"[BMKG] RSS XML parse error: {e}")
        return []

    channel = root.find("channel")
    if channel is None:
        return []

    items: list[dict[str, Any]] = []
    for item in channel.findall("item"):
        title = _norm(item.findtext("title"))
        link = _norm(item.findtext("link"))
        description = _norm(item.findtext("description"))
        pub_date = _parse_rfc2822_to_iso(_norm(item.findtext("pubDate")))
        author = _norm(item.findtext("author"))

        if not link:
            continue

        items.append({
            "title": title,
            "link": link,
            "description": description,
            "published": pub_date,
            "author": author,
        })

    return items


# --------------------------------------------------------------------
# Fetchers
# --------------------------------------------------------------------

async def _fetch_text(session: aiohttp.ClientSession, url: str) -> str | None:
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                logging.warning(f"[BMKG] {url} -> HTTP {resp.status}")
                return None
            return await resp.text()
    except Exception as e:
        logging.warning(f"[BMKG] fetch error {url}: {e}")
        return None

async def _fetch_cap_detail(
    session: aiohttp.ClientSession,
    rss_item: dict[str, Any],
    sem: asyncio.Semaphore,
) -> dict[str, Any] | None:
    url = _norm(rss_item.get("link"))
    if not url:
        return None

    async with sem:
        xml_text = await _fetch_text(session, url)
        if not xml_text:
            return None
        return _parse_cap_xml(xml_text, fallback_rss_item=rss_item)


# --------------------------------------------------------------------
# Public async API
# --------------------------------------------------------------------

async def scrape_bmkg_async(conf: dict | None, client=None) -> dict[str, Any]:
    """
    Async BMKG scraper using official RSS + CAP XML.

    Returns:
      {
        "entries": [...],
        "source": "BMKG",
      }
    """
    conf = conf or {}
    rss_url = _norm(conf.get("url")) or RSS_URL_EN
    max_concurrency = int(conf.get("max_concurrency") or 8)

    timeout = aiohttp.ClientTimeout(total=TIMEOUT_TOTAL)
    connector = aiohttp.TCPConnector(limit=CONNECTOR_LIMIT, ssl=False)
    headers = {
        "User-Agent": "weather-monitor/1.0",
        "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
    }

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        rss_text = await _fetch_text(session, rss_url)
        if not rss_text:
            return {"entries": [], "error": "Failed to fetch BMKG RSS feed", "source": "BMKG"}

        rss_items = _parse_rss_items(rss_text)
        if not rss_items:
            return {"entries": [], "source": "BMKG"}

        sem = asyncio.Semaphore(max(1, max_concurrency))
        tasks = [_fetch_cap_detail(session, item, sem) for item in rss_items]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    entries: list[dict[str, Any]] = []
    for r in results:
        if isinstance(r, Exception):
            logging.warning(f"[BMKG] CAP task error: {r}")
            continue
        if isinstance(r, dict):
            entries.append(r)

    # newest first
    def _sort_key(e: dict[str, Any]) -> str:
        return _norm(e.get("published") or e.get("effective") or "")

    entries.sort(key=_sort_key, reverse=True)
    logging.warning(f"[BMKG] Parsed {len(entries)} active alerts")

    return {"entries": entries, "source": "BMKG"}


# --------------------------------------------------------------------
# Optional sync wrapper
# --------------------------------------------------------------------

def scrape_bmkg(conf: dict | None = None) -> dict[str, Any]:
    return asyncio.run(scrape_bmkg_async(conf))
