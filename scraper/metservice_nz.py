from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp

# --------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------

DEFAULT_ATOM_URL = "https://alerts.metservice.com/cap/atom"
CAP_NS = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}
ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}

TIMEOUT_TOTAL = 25
CONNECTOR_LIMIT = 10

logger = logging.getLogger(__name__)

NZ_EVENT_MAP = {
    "rain": "Rain",
    "wind": "Wind",
    "snow": "Snow",
    "thunderstorm": "Thunderstorm",
    "thunderstorms": "Thunderstorm",
    "heavy rain": "Rain",
    "heavy snow": "Snow",
    "heavy wind": "Wind",
    "storm": "Storm",
}


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


def _slug_hash(*parts: str) -> str:
    src = "|".join(_norm(p) for p in parts if _norm(p))
    return hashlib.sha1(src.encode("utf-8")).hexdigest()[:12]


def _title_case_event(event_text: str) -> str:
    t = _norm(event_text).lower()
    if not t:
        return "Alert"
    return NZ_EVENT_MAP.get(t, t.title())


def _extract_alert_class(title: str, headline: str) -> str:
    text = f"{_norm(title)} {_norm(headline)}"
    if re.search(r"\bwarning\b", text, flags=re.IGNORECASE):
        return "Warning"
    if re.search(r"\bwatch\b", text, flags=re.IGNORECASE):
        return "Watch"
    if re.search(r"\badvisory\b", text, flags=re.IGNORECASE):
        return "Advisory"
    return ""


def _extract_colour_from_title(title: str, headline: str) -> str:
    text = f"{_norm(title)} {_norm(headline)}"
    m = re.search(r"\b(red|orange|yellow)\b", text, flags=re.IGNORECASE)
    if not m:
        return ""
    return m.group(1).title()


def _extract_param_map(info: ET.Element | None) -> dict[str, str]:
    out: dict[str, str] = {}
    if info is None:
        return out

    for param in info.findall("cap:parameter", CAP_NS):
        name = _first_text(param, "cap:valueName", CAP_NS)
        value = _first_text(param, "cap:value", CAP_NS)
        if name:
            out[name] = value
    return out


def _severity_rank(colour_code: str, alert_class: str) -> int:
    colour = _norm(colour_code).title()
    klass = _norm(alert_class).title()

    if colour == "Red" and klass == "Warning":
        return 5
    if colour == "Orange" and klass == "Warning":
        return 4
    if colour == "Yellow" and klass == "Warning":
        return 3
    if klass == "Warning":
        return 2
    if klass == "Watch":
        return 1
    return 0


def _should_keep_alert(entry: dict[str, Any], conf: dict[str, Any]) -> bool:
    include_watches = bool(conf.get("include_watches", False))
    min_colour = _norm(conf.get("min_colour") or "Orange").title()
    min_rank = {
        "Red": 5,
        "Orange": 4,
        "Yellow": 3,
    }.get(min_colour, 4)

    rank = _severity_rank(entry.get("colour_code"), entry.get("alert_class"))

    if include_watches:
        return rank >= 1 and (
            rank >= min_rank or _norm(entry.get("alert_class")) == "Watch"
        )

    return rank >= min_rank


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
        updated = _first_text(entry, "atom:updated", ATOM_NS)
        published = _first_text(entry, "atom:published", ATOM_NS)

        link = ""
        for lnk in entry.findall("atom:link", ATOM_NS):
            rel = _norm(lnk.attrib.get("rel"))
            href = _norm(lnk.attrib.get("href"))
            ltype = _norm(lnk.attrib.get("type"))
            if rel == "related" and href:
                link = href
                break
            if ltype == "application/cap+xml" and href:
                link = href
                break
            if not link and href:
                link = href

        if not title and not entry_id and not link:
            continue

        entries.append({
            "id": entry_id or link or title,
            "title": title,
            "headline": title,
            "summary": summary,
            "description": summary,
            "published": published or updated,
            "updated": updated,
            "link": link,
        })

    return entries


# --------------------------------------------------------------------
# CAP parsing
# --------------------------------------------------------------------

def _build_entry_from_cap(fallback: dict[str, Any], xml_text: str) -> dict[str, Any] | None:
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        logger.warning("[NZ] CAP XML parse error: %s", e)
        return None

    identifier = _first_text(root, "cap:identifier", CAP_NS)
    sent = _first_text(root, "cap:sent", CAP_NS)
    status = _first_text(root, "cap:status", CAP_NS)
    msg_type = _first_text(root, "cap:msgType", CAP_NS)
    scope = _first_text(root, "cap:scope", CAP_NS)

    info = root.find("cap:info", CAP_NS)
    if info is None:
        return None

    category = _first_text(info, "cap:category", CAP_NS)
    raw_event = _first_text(info, "cap:event", CAP_NS)
    response_type = _first_text(info, "cap:responseType", CAP_NS)
    urgency = _first_text(info, "cap:urgency", CAP_NS)
    severity = _first_text(info, "cap:severity", CAP_NS)
    certainty = _first_text(info, "cap:certainty", CAP_NS)
    onset = _first_text(info, "cap:onset", CAP_NS)
    expires = _first_text(info, "cap:expires", CAP_NS)
    sender_name = _first_text(info, "cap:senderName", CAP_NS)
    headline = _first_text(info, "cap:headline", CAP_NS)
    description = _first_text(info, "cap:description", CAP_NS)
    instruction = _first_text(info, "cap:instruction", CAP_NS)
    web = _first_text(info, "cap:web", CAP_NS)

    area_descs = _all_texts(info, "cap:area/cap:areaDesc", CAP_NS)
    polygon_texts = _all_texts(info, "cap:area/cap:polygon", CAP_NS)

    param_map = _extract_param_map(info)
    colour_code = _norm(param_map.get("ColourCode")).title()
    colour_hex = _norm(param_map.get("ColourCodeHex"))
    chance_of_upgrade = _norm(param_map.get("ChanceOfUpgrade"))
    next_update = _norm(param_map.get("NextUpdate"))

    title = _norm(headline or fallback.get("title") or "MetService Alert")
    event = _title_case_event(raw_event)
    alert_class = _extract_alert_class(title, headline)
    if not colour_code:
        colour_code = _extract_colour_from_title(title, headline)

    areas = [a for a in area_descs if _norm(a)]
    if not areas:
        areas = ["New Zealand"]

    region = ", ".join(areas)
    stable_suffix = _slug_hash(
        identifier or fallback.get("id") or title,
        title,
        region,
        onset,
        expires,
    )

    entry = {
        "id": f"{identifier or fallback.get('id') or fallback.get('link') or title}|{stable_suffix}",
        "identifier": identifier or fallback.get("id") or "",
        "title": title,
        "headline": headline or title,
        "summary": description or fallback.get("summary") or "",
        "description": description or fallback.get("description") or "",
        "instruction": instruction,
        "event": event,
        "event_raw": raw_event,
        "alert_class": alert_class,
        "colour_code": colour_code,
        "colour_code_hex": colour_hex,
        "severity": severity,
        "urgency": urgency,
        "certainty": certainty,
        "status": status,
        "msg_type": msg_type,
        "scope": scope,
        "category": category,
        "response_type": response_type,
        "chance_of_upgrade": chance_of_upgrade,
        "next_update": next_update,
        "onset": onset,
        "effective": onset or sent or fallback.get("published") or "",
        "expires": expires,
        "published": sent or fallback.get("published") or "",
        "updated": fallback.get("updated") or sent or "",
        "sender_name": sender_name or "Meteorological Service of New Zealand Limited",
        "region": region,
        "areas": areas[:],
        "polygons": polygon_texts[:],
        "polygon": polygon_texts[0] if polygon_texts else "",
        "link": web or fallback.get("link") or "",
        "detail_link": fallback.get("link") or "",
        "source": "MetService New Zealand",
    }

    return entry


# --------------------------------------------------------------------
# Networking
# --------------------------------------------------------------------

async def _fetch_text(session: aiohttp.ClientSession, url: str) -> tuple[str | None, str]:
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning("[NZ] %s -> HTTP %s", url, resp.status)
                return None, ""
            return await resp.text(), (resp.headers.get("Content-Type") or "")
    except Exception as e:
        logger.warning("[NZ] fetch error %s: %s", url, e)
        return None, ""


async def _fetch_detail(
    session: aiohttp.ClientSession,
    item: dict[str, Any],
    sem: asyncio.Semaphore,
    conf: dict[str, Any],
) -> dict[str, Any] | None:
    link = _norm(item.get("link"))
    if not link:
        return None

    async with sem:
        text, _ = await _fetch_text(session, link)
        if not text:
            return None

        entry = _build_entry_from_cap(item, text)
        if not entry:
            return None

        if not _should_keep_alert(entry, conf):
            return None

        return entry


# --------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------

async def scrape_nz_metservice_async(conf: dict | None = None, client=None) -> dict[str, Any]:
    """
    New Zealand MetService CAP scraper.

    Notes:
      - Uses the Atom feed as the index
      - Fetches each CAP detail URL from rel=related
      - Prioritizes MetService ColourCode + Warning/Watch over raw CAP severity
      - By default keeps Orange/Red Warnings only
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
            atom_entries = _parse_atom_entries(atom_text)
        except Exception as e:
            logger.warning("[NZ] Atom parse error: %s", e)
            return {"entries": [], "error": str(e), "source": "MetService New Zealand"}

        if not atom_entries:
            return {"entries": [], "source": "MetService New Zealand"}

        sem = asyncio.Semaphore(max(1, max_concurrency))
        tasks = [_fetch_detail(session, item, sem, conf) for item in atom_entries]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    entries: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for r in results:
        if isinstance(r, Exception):
            logger.warning("[NZ] detail task error: %s", r)
            continue
        if not isinstance(r, dict):
            continue

        dedupe_key = _norm(r.get("identifier") or r.get("id"))
        if dedupe_key and dedupe_key in seen_ids:
            continue
        if dedupe_key:
            seen_ids.add(dedupe_key)

        entries.append(r)

    entries = sorted(
        entries,
        key=lambda x: _norm(x.get("published") or x.get("effective") or x.get("onset")),
        reverse=True,
    )

    logger.warning("[NZ] Parsed %d alerts", len(entries))
    return {"entries": entries, "source": "MetService New Zealand"}


def scrape_nz_metservice(conf: dict | None = None) -> dict[str, Any]:
    return asyncio.run(scrape_nz_metservice_async(conf))
