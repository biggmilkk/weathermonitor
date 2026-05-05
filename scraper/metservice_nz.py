# scraper/metservice_nz.py
from __future__ import annotations

import asyncio
import hashlib
import logging
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp

# --------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------

DEFAULT_ATOM_URL = "https://alerts.metservice.com/cap/atom"

ATOM_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "cap": "urn:oasis:names:tc:emergency:cap:1.2",
}

CAP_NS = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

TIMEOUT_TOTAL = 25
CONNECTOR_LIMIT = 10

logger = logging.getLogger(__name__)

NZ_SEVERITY_ORDER = {
    "Extreme": 4,
    "Severe": 3,
    "Moderate": 2,
    "Minor": 1,
    "Unknown": 0,
}


# --------------------------------------------------------------------
# Helpers
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


def _parse_dt_to_iso(s: str) -> str:
    s = _norm(s)
    if not s:
        return ""
    try:
        if "," in s:
            return parsedate_to_datetime(s).isoformat()
        return s
    except Exception:
        return s


def _slug_hash(*parts: str) -> str:
    src = "|".join(_norm(p) for p in parts if _norm(p))
    return hashlib.sha1(src.encode("utf-8")).hexdigest()[:12]


def _severity_rank(severity: str) -> int:
    return NZ_SEVERITY_ORDER.get(_norm(severity), 0)


def _passes_min_severity(item: dict[str, Any], min_severity: str) -> bool:
    threshold = _severity_rank(min_severity)
    current = _severity_rank(item.get("severity"))
    return current >= threshold


def _semantic_alert_key(item: dict[str, Any]) -> str:
    """
    Collapse repeated updates that are materially the same alert.
    """
    return "|".join([
        _norm(item.get("event")),
        _norm(item.get("severity")),
        _norm(item.get("urgency")),
        _norm(item.get("certainty")),
        _norm(item.get("effective") or item.get("onset")),
        _norm(item.get("expires")),
        _norm(item.get("headline") or item.get("title")),
        _norm(item.get("description") or item.get("summary")),
        _norm(item.get("instruction")),
        "|".join(sorted(_norm(a) for a in (item.get("areas") or []) if _norm(a))),
        _norm(item.get("polygon")),
    ])


# --------------------------------------------------------------------
# Atom parsing
# --------------------------------------------------------------------

def _parse_atom_feed(xml_text: str) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    entries: list[dict[str, Any]] = []

    for entry in root.findall("atom:entry", ATOM_NS):
        entry_id = _first_text(entry, "atom:id", ATOM_NS)
        title = _first_text(entry, "atom:title", ATOM_NS)
        updated = _parse_dt_to_iso(_first_text(entry, "atom:updated", ATOM_NS))
        published = _parse_dt_to_iso(_first_text(entry, "atom:published", ATOM_NS))
        summary = _first_text(entry, "atom:summary", ATOM_NS)

        link = ""
        for link_el in entry.findall("atom:link", ATOM_NS):
            href = _norm(link_el.attrib.get("href"))
            rel = _norm(link_el.attrib.get("rel"))
            if href and (not rel or rel == "alternate"):
                link = href
                break

        detail_url = link or entry_id

        if not detail_url and not title:
            continue

        entries.append({
            "id": entry_id or detail_url or title,
            "title": title,
            "headline": title,
            "link": detail_url,
            "summary": summary,
            "description": summary,
            "published": published or updated,
            "updated": updated,
        })

    return entries


# --------------------------------------------------------------------
# CAP detail parsing
# --------------------------------------------------------------------

def _parse_cap_alert_xml(xml_text: str, fallback: dict[str, Any]) -> dict[str, Any] | None:
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        logger.warning("[MetService NZ] CAP XML parse error: %s", e)
        return None

    identifier = _first_text(root, "cap:identifier", CAP_NS)
    sent = _first_text(root, "cap:sent", CAP_NS)
    status = _first_text(root, "cap:status", CAP_NS)
    msg_type = _first_text(root, "cap:msgType", CAP_NS)
    scope = _first_text(root, "cap:scope", CAP_NS)

    info = root.find("cap:info", CAP_NS)
    if info is None:
        return None

    language = _first_text(info, "cap:language", CAP_NS)
    category = _first_text(info, "cap:category", CAP_NS)
    event = _first_text(info, "cap:event", CAP_NS)
    urgency = _first_text(info, "cap:urgency", CAP_NS)
    severity = _first_text(info, "cap:severity", CAP_NS)
    certainty = _first_text(info, "cap:certainty", CAP_NS)
    onset = _first_text(info, "cap:onset", CAP_NS)
    effective = _first_text(info, "cap:effective", CAP_NS)
    expires = _first_text(info, "cap:expires", CAP_NS)
    headline = _first_text(info, "cap:headline", CAP_NS)
    description = _first_text(info, "cap:description", CAP_NS)
    instruction = _first_text(info, "cap:instruction", CAP_NS)
    sender_name = _first_text(info, "cap:senderName", CAP_NS)

    area_descs = _all_texts(info, "cap:area/cap:areaDesc", CAP_NS)
    polygons = _all_texts(info, "cap:area/cap:polygon", CAP_NS)
    geocodes = _all_texts(info, "cap:area/cap:geocode/cap:value", CAP_NS)

    title = headline or fallback.get("title") or event or "MetService Alert"
    area_text = ", ".join(area_descs) if area_descs else "New Zealand"

    stable_suffix = _slug_hash(
        identifier or fallback.get("link") or title,
        event,
        severity,
        effective or onset,
        expires,
        area_text,
    )

    return {
        "id": f"{identifier or fallback.get('id') or fallback.get('link') or title}|{stable_suffix}",
        "identifier": identifier,
        "title": title,
        "headline": headline or title,
        "summary": description or fallback.get("summary") or "",
        "description": description or fallback.get("description") or "",
        "instruction": instruction,
        "event": event or "Alert",
        "severity": severity,
        "urgency": urgency,
        "certainty": certainty,
        "status": status,
        "msg_type": msg_type,
        "scope": scope,
        "category": category,
        "language": language,
        "onset": onset,
        "effective": effective or onset,
        "expires": expires,
        "published": sent or fallback.get("published") or fallback.get("updated") or "",
        "updated": fallback.get("updated") or "",
        "sender_name": sender_name or "MetService New Zealand",
        "region": area_text,
        "areas": area_descs[:],
        "geocodes": geocodes[:],
        "polygon": " | ".join(polygons),
        "link": fallback.get("link") or "",
        "source": "MetService New Zealand",
    }


# --------------------------------------------------------------------
# Networking
# --------------------------------------------------------------------

async def _fetch_text(session: aiohttp.ClientSession, url: str) -> tuple[str | None, str]:
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning("[MetService NZ] %s -> HTTP %s", url, resp.status)
                return None, ""
            return await resp.text(), (resp.headers.get("Content-Type") or "")
    except Exception as e:
        logger.warning("[MetService NZ] fetch error %s: %s", url, e)
        return None, ""


async def _fetch_detail(session: aiohttp.ClientSession, item: dict[str, Any], sem: asyncio.Semaphore) -> dict[str, Any] | None:
    link = _norm(item.get("link"))
    if not link:
        return None

    async with sem:
        text, _content_type = await _fetch_text(session, link)
        if not text:
            return None

        parsed = _parse_cap_alert_xml(text, item)
        if parsed:
            return parsed

        return None


# --------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------

async def scrape_metservice_nz_async(conf: dict | None = None, client=None) -> dict[str, Any]:
    """
    New Zealand MetService CAP scraper.

    Strategy:
      1) fetch Atom feed
      2) parse entries
      3) follow entry link/id to full CAP alert
      4) normalize CAP fields
      5) optionally filter by minimum severity
      6) dedupe materially identical alert updates
    """
    conf = conf or {}
    atom_url = _norm(conf.get("url")) or DEFAULT_ATOM_URL
    max_concurrency = int(conf.get("max_concurrency") or 6)
    min_severity = _norm(conf.get("min_severity") or "Severe")

    timeout = aiohttp.ClientTimeout(total=TIMEOUT_TOTAL)
    connector = aiohttp.TCPConnector(limit=CONNECTOR_LIMIT, ssl=False)
    headers = {
        "User-Agent": "WeatherMonitor/1.0",
        "Accept": "application/atom+xml, application/xml, text/xml, */*;q=0.8",
    }

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        atom_text, _ = await _fetch_text(session, atom_url)
        if not atom_text:
            return {"entries": [], "error": "Failed to fetch MetService Atom feed", "source": "MetService New Zealand"}

        try:
            feed_items = _parse_atom_feed(atom_text)
        except Exception as e:
            logger.warning("[MetService NZ] Atom parse error: %s", e)
            return {"entries": [], "error": str(e), "source": "MetService New Zealand"}

        if not feed_items:
            return {"entries": [], "source": "MetService New Zealand"}

        sem = asyncio.Semaphore(max(1, max_concurrency))
        tasks = [_fetch_detail(session, item, sem) for item in feed_items]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_entries: list[dict[str, Any]] = []
    for r in results:
        if isinstance(r, Exception):
            logger.warning("[MetService NZ] detail task error: %s", r)
            continue
        if isinstance(r, dict):
            all_entries.append(r)

    # severity filter
    filtered = [e for e in all_entries if _passes_min_severity(e, min_severity)]

    # newest first
    filtered = sorted(
        filtered,
        key=lambda x: _norm(x.get("published") or x.get("effective") or x.get("onset")),
        reverse=True,
    )

    # dedupe same alert repeated with tiny feed/update differences
    deduped: list[dict[str, Any]] = []
    seen_semantic: set[str] = set()

    for item in filtered:
        s_key = _semantic_alert_key(item)
        if s_key in seen_semantic:
            continue
        seen_semantic.add(s_key)
        deduped.append(item)

    logger.warning("[MetService NZ] Parsed %d alerts", len(deduped))
    return {"entries": deduped, "source": "MetService New Zealand"}


def scrape_metservice_nz(conf: dict | None = None) -> dict[str, Any]:
    return asyncio.run(scrape_metservice_nz_async(conf))
