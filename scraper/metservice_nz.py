# scraper/metservice_nz.py
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import xml.etree.ElementTree as ET
from typing import Any

import aiohttp
from dateutil import parser as dateparser

# --------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------

DEFAULT_ATOM_URL = "https://alerts.metservice.com/cap/atom"

ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}
CAP_NS = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

TIMEOUT_TOTAL = 25
CONNECTOR_LIMIT = 10

logger = logging.getLogger(__name__)

WATCH_RE = re.compile(r"\bwatch\b", re.IGNORECASE)
WARNING_RE = re.compile(r"\bwarning\b", re.IGNORECASE)
ORANGE_RE = re.compile(r"\borange\b", re.IGNORECASE)
RED_RE = re.compile(r"\bred\b", re.IGNORECASE)


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


def _parse_dt_to_iso(s: str) -> str:
    s = _norm(s)
    if not s:
        return ""
    try:
        return dateparser.parse(s).isoformat()
    except Exception:
        return s


def _slug_hash(*parts: str) -> str:
    src = "|".join(_norm(p) for p in parts if _norm(p))
    return hashlib.sha1(src.encode("utf-8")).hexdigest()[:12]


def _event_to_display(event: str) -> str:
    e = _norm(event).lower()
    mapping = {
        "rain": "Rain",
        "wind": "Wind",
        "snow": "Snow",
        "thunderstorm": "Thunderstorm",
        "thunderstorms": "Thunderstorm",
        "flood": "Flood",
        "ice": "Ice",
        "fog": "Fog",
        "frost": "Frost",
    }
    if e in mapping:
        return mapping[e]
    if not e:
        return "Alert"
    return e.replace("_", " ").replace("-", " ").title()


def _extract_cap_parameter(info: ET.Element | None, value_name: str) -> str:
    if info is None:
        return ""
    for param in info.findall("cap:parameter", CAP_NS):
        name = _first_text(param, "cap:valueName", CAP_NS)
        if name == value_name:
            return _first_text(param, "cap:value", CAP_NS)
    return ""


def _public_level_from_title_or_colour(title: str, colour_code: str) -> str:
    """
    Public NZ level should be driven by MetService's colour scheme.
    """
    t = _norm(title)

    if _norm(colour_code).lower() == "red" or RED_RE.search(t):
        return "Red"
    if _norm(colour_code).lower() == "orange" or ORANGE_RE.search(t):
        return "Orange"
    return ""


def _classify_product(title: str) -> str:
    """
    Returns Warning / Watch / Alert
    """
    t = _norm(title)
    if WARNING_RE.search(t):
        return "Warning"
    if WATCH_RE.search(t):
        return "Watch"
    return "Alert"


def _should_keep_entry(*, product_type: str, public_level: str) -> bool:
    """
    Keep only live Orange/Red warnings/alerts.
    Drop watches by default.
    """
    if public_level not in {"Orange", "Red"}:
        return False
    if product_type == "Watch":
        return False
    return True


def _semantic_alert_key(item: dict[str, Any]) -> str:
    areas = item.get("areas") or []
    if isinstance(areas, list):
        areas_key = "|".join(sorted(_norm(a) for a in areas if _norm(a)))
    else:
        areas_key = _norm(areas)

    return "|".join([
        _norm(item.get("headline") or item.get("title")),
        _norm(item.get("colour_code") or item.get("level")),
        _norm(item.get("event")),
        _norm(item.get("effective") or item.get("onset")),
        _norm(item.get("expires")),
        _norm(item.get("description") or item.get("summary")),
        _norm(item.get("instruction")),
        areas_key,
    ])


# --------------------------------------------------------------------
# Atom parsing
# --------------------------------------------------------------------

def _parse_atom_entries(xml_text: str) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)

    entries: list[dict[str, Any]] = []
    for entry in root.findall("atom:entry", ATOM_NS):
        title = _first_text(entry, "atom:title", ATOM_NS)
        entry_id = _first_text(entry, "atom:id", ATOM_NS)
        summary = _first_text(entry, "atom:summary", ATOM_NS)
        updated = _parse_dt_to_iso(_first_text(entry, "atom:updated", ATOM_NS))
        published = _parse_dt_to_iso(_first_text(entry, "atom:published", ATOM_NS))
        author = _first_text(entry, "atom:author/atom:name", ATOM_NS)

        related_link = ""
        web_link = ""
        for link_el in entry.findall("atom:link", ATOM_NS):
            rel = _norm(link_el.attrib.get("rel"))
            href = _norm(link_el.attrib.get("href"))
            link_type = _norm(link_el.attrib.get("type"))
            if rel == "related" and "cap+xml" in link_type and href:
                related_link = href
            elif href and not web_link:
                web_link = href

        if not entry_id and not related_link and not title:
            continue

        entries.append({
            "id": entry_id or related_link or title,
            "title": title,
            "headline": title,
            "summary": summary,
            "description": summary,
            "updated": updated,
            "published": published or updated,
            "sender_name": author,
            "cap_link": related_link,
            "link": web_link or related_link,
        })

    return entries


# --------------------------------------------------------------------
# CAP detail parsing
# --------------------------------------------------------------------

def _parse_cap_alert_xml(xml_text: str, fallback: dict[str, Any]) -> dict[str, Any] | None:
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        logger.warning("[NZ MetService] CAP XML parse error: %s", e)
        return None

    identifier = _first_text(root, "cap:identifier", CAP_NS)
    sender = _first_text(root, "cap:sender", CAP_NS)
    sent = _parse_dt_to_iso(_first_text(root, "cap:sent", CAP_NS))
    status = _first_text(root, "cap:status", CAP_NS)
    msg_type = _first_text(root, "cap:msgType", CAP_NS)
    scope = _first_text(root, "cap:scope", CAP_NS)

    info = root.find("cap:info", CAP_NS)
    if info is None:
        return None

    category = _first_text(info, "cap:category", CAP_NS)
    event_raw = _first_text(info, "cap:event", CAP_NS)
    response_type = _first_text(info, "cap:responseType", CAP_NS)
    urgency = _first_text(info, "cap:urgency", CAP_NS)
    severity_cap = _first_text(info, "cap:severity", CAP_NS)
    certainty = _first_text(info, "cap:certainty", CAP_NS)
    onset = _parse_dt_to_iso(_first_text(info, "cap:onset", CAP_NS))
    effective = _parse_dt_to_iso(_first_text(info, "cap:effective", CAP_NS))
    expires = _parse_dt_to_iso(_first_text(info, "cap:expires", CAP_NS))
    sender_name = _first_text(info, "cap:senderName", CAP_NS)
    headline = _first_text(info, "cap:headline", CAP_NS)
    description = _first_text(info, "cap:description", CAP_NS)
    instruction = _first_text(info, "cap:instruction", CAP_NS)
    web = _first_text(info, "cap:web", CAP_NS)

    colour_code = _extract_cap_parameter(info, "ColourCode")
    colour_code_hex = _extract_cap_parameter(info, "ColourCodeHex")
    chance_of_upgrade = _extract_cap_parameter(info, "ChanceOfUpgrade")
    next_update = _parse_dt_to_iso(_extract_cap_parameter(info, "NextUpdate"))

    area_descs = _all_texts(info, "cap:area/cap:areaDesc", CAP_NS)
    primary_area = area_descs[0] if area_descs else "New Zealand"
    region = ", ".join(area_descs) if area_descs else primary_area

    title = headline or fallback.get("title") or "MetService Alert"
    product_type = _classify_product(title)
    public_level = _public_level_from_title_or_colour(title, colour_code)

    if not _should_keep_entry(product_type=product_type, public_level=public_level):
        return None

    event_display = _event_to_display(event_raw)

    stable_suffix = _slug_hash(
        identifier or fallback.get("id") or title,
        public_level,
        region,
        effective or onset,
        expires,
    )

    return {
        "id": f"{identifier or fallback.get('id') or title}|{stable_suffix}",
        "identifier": identifier,
        "title": title,
        "headline": headline or title,
        "summary": description or fallback.get("summary") or "",
        "description": description or fallback.get("description") or "",
        "instruction": instruction,
        "event": event_display,
        "event_raw": event_raw,
        "severity": severity_cap,
        "level": public_level,
        "bucket": product_type,
        "response_type": response_type,
        "urgency": urgency,
        "certainty": certainty,
        "status": status,
        "msg_type": msg_type,
        "scope": scope,
        "category": category,
        "onset": onset,
        "effective": effective or onset,
        "expires": expires,
        "published": sent or fallback.get("published") or "",
        "updated": fallback.get("updated") or sent or "",
        "sender": sender,
        "sender_name": sender_name or fallback.get("sender_name") or "Meteorological Service of New Zealand Limited",
        "region": primary_area,
        "area_desc": primary_area,
        "location": primary_area,
        "areas": area_descs[:] if area_descs else [primary_area],
        "area_count": len(area_descs) if area_descs else 1,
        "primary_area": primary_area,
        "colour_code": public_level or colour_code,
        "colour_code_hex": colour_code_hex,
        "chance_of_upgrade": chance_of_upgrade,
        "next_update": next_update,
        "link": web or fallback.get("cap_link") or fallback.get("link") or "",
        "web": web or fallback.get("link") or "",
        "source": "MetService New Zealand",
    }


# --------------------------------------------------------------------
# Networking
# --------------------------------------------------------------------

async def _fetch_text(session: aiohttp.ClientSession, url: str) -> tuple[str | None, str]:
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning("[NZ MetService] %s -> HTTP %s", url, resp.status)
                return None, ""
            return await resp.text(), (resp.headers.get("Content-Type") or "")
    except Exception as e:
        logger.warning("[NZ MetService] fetch error %s: %s", url, e)
        return None, ""


async def _fetch_cap_detail(
    session: aiohttp.ClientSession,
    atom_item: dict[str, Any],
    sem: asyncio.Semaphore,
) -> dict[str, Any] | None:
    cap_link = _norm(atom_item.get("cap_link"))
    if not cap_link:
        return None

    async with sem:
        text, _ = await _fetch_text(session, cap_link)
        if not text:
            return None
        return _parse_cap_alert_xml(text, atom_item)


# --------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------

async def scrape_metservice_nz_async(conf: dict | None, client=None) -> dict[str, Any]:
    """
    MetService New Zealand CAP scraper.

    Strategy:
      1) fetch official Atom index
      2) follow each CAP detail URL from link rel="related" type="application/cap+xml"
      3) parse CAP fields directly
      4) classify using headline/ColourCode first
      5) keep only Orange/Red warnings/alerts
      6) drop watches
      7) dedupe repeated same-alert copies to latest semantic version
    """
    conf = conf or {}
    atom_url = _norm(conf.get("url")) or DEFAULT_ATOM_URL
    max_concurrency = int(conf.get("max_concurrency") or 6)

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
            atom_items = _parse_atom_entries(atom_text)
        except Exception as e:
            logger.warning("[NZ MetService] Atom parse error: %s", e)
            return {"entries": [], "error": str(e), "source": "MetService New Zealand"}

        if not atom_items:
            return {"entries": [], "source": "MetService New Zealand"}

        sem = asyncio.Semaphore(max(1, max_concurrency))
        tasks = [_fetch_cap_detail(session, item, sem) for item in atom_items]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_entries: list[dict[str, Any]] = []

    for r in results:
        if isinstance(r, Exception):
            logger.warning("[NZ MetService] detail task error: %s", r)
            continue
        if isinstance(r, dict):
            all_entries.append(r)

    all_entries = sorted(
        all_entries,
        key=lambda x: _norm(x.get("published") or x.get("effective") or x.get("onset")),
        reverse=True,
    )

    deduped: list[dict[str, Any]] = []
    seen_semantic: set[str] = set()

    for item in all_entries:
        s_key = _semantic_alert_key(item)
        if s_key in seen_semantic:
            continue
        seen_semantic.add(s_key)
        deduped.append(item)

    logger.warning("[NZ MetService] Parsed %d alerts", len(deduped))
    return {"entries": deduped, "source": "MetService New Zealand"}


def scrape_metservice_nz(conf: dict | None = None) -> dict[str, Any]:
    return asyncio.run(scrape_metservice_nz_async(conf))
