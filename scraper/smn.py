# scraper/smn.py
from __future__ import annotations

import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp
from bs4 import BeautifulSoup

# --------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------

DEFAULT_RSS_URL = "https://ssl.smn.gob.ar/feeds/CAP/rss_alertaCAP_nuevo.xml"

CAP_NS = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

TIMEOUT_TOTAL = 25
CONNECTOR_LIMIT = 10

logger = logging.getLogger(__name__)

# SMN uses Spanish alert colors publicly
SPANISH_SEVERITY_ORDER = {
    "Rojo": 4,
    "Naranja": 3,
    "Amarillo": 2,
    "Verde": 1,
}

COLOR_WORD_RE = re.compile(r"\b(rojo|naranja|amarillo|verde)\b", re.IGNORECASE)


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

def _guess_severity_from_text(*parts: str) -> str:
    text = " ".join(_norm(p) for p in parts if _norm(p))
    if not text:
        return ""
    m = COLOR_WORD_RE.search(text)
    if not m:
        return ""
    word = m.group(1).lower()
    return {
        "rojo": "Rojo",
        "naranja": "Naranja",
        "amarillo": "Amarillo",
        "verde": "Verde",
    }.get(word, "")

def _guess_event_from_title(title: str) -> str:
    t = _norm(title)
    if not t:
        return "Alerta"
    # Remove common leading severity words
    t = re.sub(r"^\s*(Alerta|Advertencia)\s+", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\b(rojo|naranja|amarillo|verde)\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip(" -,:")
    return t or "Alerta"

def _extract_areas_from_text(text: str) -> list[str]:
    """
    Best-effort extraction of affected areas from RSS/HTML text.
    Looks for phrases like:
      '... afecta a ...'
      '... para las siguientes zonas: ...'
    """
    t = _norm(text)
    if not t:
        return []

    patterns = [
        r"(?:afecta a|afecta el área de|afecta las zonas de)\s+(.+?)(?:\.|$)",
        r"(?:para las siguientes zonas|para las zonas)\s*:\s*(.+?)(?:\.|$)",
        r"(?:área de cobertura|zona de cobertura)\s*:\s*(.+?)(?:\.|$)",
    ]

    for pat in patterns:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if m:
            raw = _norm(m.group(1))
            if raw:
                parts = [p.strip(" .;") for p in re.split(r",|;|/|\sy\s", raw) if p.strip()]
                return parts[:20]

    return []

def _province_from_areas(areas: list[str]) -> str:
    if not areas:
        return "Argentina"
    return areas[0]

def _xml_looks_like_cap(text: str) -> bool:
    t = (text or "")[:2000]
    return ("urn:oasis:names:tc:emergency:cap:1.2" in t) or ("<alert" in t and "<info" in t)

def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    return _norm(soup.get_text("\n", strip=True))


# --------------------------------------------------------------------
# RSS parsing
# --------------------------------------------------------------------

def _parse_rss_items(xml_text: str) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    channel = root.find("channel")
    if channel is None:
        return []

    items: list[dict[str, Any]] = []
    for item in channel.findall("item"):
        title = _norm(item.findtext("title"))
        link = _norm(item.findtext("link"))
        description = _norm(item.findtext("description"))
        pub_date = _parse_rfc2822_to_iso(_norm(item.findtext("pubDate")))
        guid = _norm(item.findtext("guid"))

        if not title and not link:
            continue

        items.append({
            "id": guid or link or title,
            "title": title,
            "headline": title,
            "link": link,
            "summary": description,
            "description": description,
            "published": pub_date,
        })

    return items


# --------------------------------------------------------------------
# CAP detail parsing
# --------------------------------------------------------------------

def _parse_cap_alert_xml(xml_text: str, fallback: dict[str, Any]) -> dict[str, Any] | None:
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        logger.warning("[SMN] CAP XML parse error: %s", e)
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
    effective = _first_text(info, "cap:effective", CAP_NS)
    expires = _first_text(info, "cap:expires", CAP_NS)
    headline = _first_text(info, "cap:headline", CAP_NS)
    description = _first_text(info, "cap:description", CAP_NS)
    instruction = _first_text(info, "cap:instruction", CAP_NS)
    sender_name = _first_text(info, "cap:senderName", CAP_NS)

    area_descs = _all_texts(info, "cap:area/cap:areaDesc", CAP_NS)
    if not area_descs:
        area_descs = _extract_areas_from_text(description)

    title = headline or fallback.get("title") or event or "Alerta SMN"

    # If CAP severity is empty or in English CAP terms, keep raw and also guess Spanish display severity from text
    severity_es = _guess_severity_from_text(title, headline, description)
    severity_final = severity_es or severity or ""

    region = area_descs[0] if area_descs else _province_from_areas([])
    province_name = _province_from_areas(area_descs)

    return {
        "id": identifier or fallback.get("id") or fallback.get("link") or title,
        "identifier": identifier,
        "title": title,
        "headline": headline or title,
        "summary": description or fallback.get("summary") or "",
        "description": description or fallback.get("description") or "",
        "instruction": instruction,
        "event": event or _guess_event_from_title(title),
        "severity": severity_final,
        "urgency": urgency,
        "certainty": certainty,
        "status": status,
        "msg_type": msg_type,
        "scope": scope,
        "category": category,
        "language": language,
        "effective": effective,
        "expires": expires,
        "published": sent or fallback.get("published") or "",
        "sender_name": sender_name,
        "province": province_name,
        "province_name": province_name,
        "region": region,
        "areas": area_descs,
        "link": fallback.get("link") or "",
        "source": "SMN Argentina",
    }


# --------------------------------------------------------------------
# HTML detail fallback parsing
# --------------------------------------------------------------------

def _parse_html_detail(html_text: str, fallback: dict[str, Any]) -> dict[str, Any]:
    soup = BeautifulSoup(html_text or "", "html.parser")
    text = _html_to_text(html_text)

    title = ""
    for sel in ("h1", "title", "h2"):
        el = soup.select_one(sel)
        if el:
            title = _norm(el.get_text(" ", strip=True))
            if title:
                break

    title = title or fallback.get("title") or "Alerta SMN"
    severity = _guess_severity_from_text(title, text)
    event = _guess_event_from_title(title)
    areas = _extract_areas_from_text(text)
    province_name = _province_from_areas(areas)
    region = areas[0] if areas else province_name

    effective = ""
    expires = ""
    m_eff = re.search(r"(?:vigencia|desde)\s*:?[\s\-]*(.+?)(?:hasta|$)", text, flags=re.IGNORECASE)
    m_exp = re.search(r"(?:hasta)\s*:?[\s\-]*(.+?)(?:\.|$)", text, flags=re.IGNORECASE)
    if m_eff:
        effective = _norm(m_eff.group(1))
    if m_exp:
        expires = _norm(m_exp.group(1))

    return {
        "id": fallback.get("id") or fallback.get("link") or title,
        "identifier": "",
        "title": title,
        "headline": title,
        "summary": fallback.get("summary") or "",
        "description": text or fallback.get("description") or "",
        "instruction": "",
        "event": event,
        "severity": severity,
        "urgency": "",
        "certainty": "",
        "status": "",
        "msg_type": "",
        "scope": "",
        "category": "",
        "language": "es",
        "effective": effective,
        "expires": expires,
        "published": fallback.get("published") or "",
        "sender_name": "Servicio Meteorológico Nacional",
        "province": province_name,
        "province_name": province_name,
        "region": region,
        "areas": areas,
        "link": fallback.get("link") or "",
        "source": "SMN Argentina",
    }


# --------------------------------------------------------------------
# Networking
# --------------------------------------------------------------------

async def _fetch_text(session: aiohttp.ClientSession, url: str) -> tuple[str | None, str]:
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning("[SMN] %s -> HTTP %s", url, resp.status)
                return None, ""
            return await resp.text(), (resp.headers.get("Content-Type") or "")
    except Exception as e:
        logger.warning("[SMN] fetch error %s: %s", url, e)
        return None, ""

async def _fetch_detail(session: aiohttp.ClientSession, item: dict[str, Any], sem: asyncio.Semaphore) -> dict[str, Any]:
    link = _norm(item.get("link"))
    if not link:
        return dict(item)

    async with sem:
        text, content_type = await _fetch_text(session, link)
        if not text:
            # RSS-only fallback
            d = dict(item)
            d["event"] = _guess_event_from_title(d.get("title") or "")
            d["severity"] = _guess_severity_from_text(d.get("title"), d.get("summary"), d.get("description"))
            areas = _extract_areas_from_text(d.get("description") or d.get("summary") or "")
            prov = _province_from_areas(areas)
            d["province"] = prov
            d["province_name"] = prov
            d["region"] = areas[0] if areas else prov
            d["areas"] = areas
            d["source"] = "SMN Argentina"
            return d

        is_xml = ("xml" in content_type.lower()) or _xml_looks_like_cap(text)

        if is_xml:
            parsed = _parse_cap_alert_xml(text, item)
            if parsed:
                return parsed

        # fallback HTML/text parse
        return _parse_html_detail(text, item)


# --------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------

async def scrape_smn_argentina_async(conf: dict | None, client=None) -> dict[str, Any]:
    """
    Argentina SMN alerts scraper.

    Strategy:
      1) fetch official RSS index
      2) follow each entry link
      3) prefer CAP/XML detail parsing
      4) fallback to HTML/text or RSS-only fields
    """
    conf = conf or {}
    rss_url = _norm(conf.get("url")) or DEFAULT_RSS_URL
    max_concurrency = int(conf.get("max_concurrency") or 6)

    timeout = aiohttp.ClientTimeout(total=TIMEOUT_TOTAL)
    connector = aiohttp.TCPConnector(limit=CONNECTOR_LIMIT, ssl=False)
    headers = {
        "User-Agent": "WeatherMonitor/1.0",
        "Accept": "application/rss+xml, application/xml, text/xml, text/html;q=0.9, */*;q=0.8",
    }

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        rss_text, _ = await _fetch_text(session, rss_url)
        if not rss_text:
            return {"entries": [], "error": "Failed to fetch SMN RSS feed", "source": "SMN Argentina"}

        try:
            rss_items = _parse_rss_items(rss_text)
        except Exception as e:
            logger.warning("[SMN] RSS parse error: %s", e)
            return {"entries": [], "error": str(e), "source": "SMN Argentina"}

        if not rss_items:
            return {"entries": [], "source": "SMN Argentina"}

        sem = asyncio.Semaphore(max(1, max_concurrency))
        tasks = [_fetch_detail(session, item, sem) for item in rss_items]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    entries: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for r in results:
        if isinstance(r, Exception):
            logger.warning("[SMN] detail task error: %s", r)
            continue
        if not isinstance(r, dict):
            continue

        dedupe_key = _norm(r.get("id") or r.get("identifier") or r.get("link") or r.get("title"))
        if dedupe_key and dedupe_key in seen_ids:
            continue
        if dedupe_key:
            seen_ids.add(dedupe_key)

        entries.append(r)

    entries = sorted(entries, key=lambda x: _norm(x.get("published") or x.get("effective")), reverse=True)
    logger.warning("[SMN] Parsed %d alerts", len(entries))
    return {"entries": entries, "source": "SMN Argentina"}


def scrape_smn_argentina(conf: dict | None = None) -> dict[str, Any]:
    return asyncio.run(scrape_smn_argentina_async(conf))
