# scraper/smn.py
from __future__ import annotations

import asyncio
import json
import logging
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from functools import lru_cache
from typing import Any

import aiohttp
from bs4 import BeautifulSoup
from shapely.geometry import Polygon, shape

# --------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------

DEFAULT_RSS_URL = "https://ssl.smn.gob.ar/feeds/CAP/rss_alertaCAP_nuevo.xml"
PROVINCES_GEOJSON_PATH = "argentina_provinces.geojson"

CAP_NS = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

TIMEOUT_TOTAL = 25
CONNECTOR_LIMIT = 10

logger = logging.getLogger(__name__)

# SMN sometimes surfaces public color words in pages/text
SPANISH_SEVERITY_ORDER = {
    "Rojo": 4,
    "Naranja": 3,
    "Amarillo": 2,
    "Verde": 1,
    # CAP-native values
    "Extreme": 4,
    "Severe": 3,
    "Moderate": 2,
    "Minor": 1,
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
# Polygon / province helpers
# --------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_argentina_provinces(path: str = PROVINCES_GEOJSON_PATH) -> list[tuple[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    provinces: list[tuple[str, Any]] = []
    for feat in data.get("features", []):
        props = feat.get("properties", {}) or {}
        name = _norm(
            props.get("nombre")
            or props.get("name")
            or props.get("provincia")
            or props.get("nam")
        )
        geom = feat.get("geometry")
        if not name or not geom:
            continue

        try:
            g = shape(geom)
            if not g.is_valid:
                g = g.buffer(0)
            if not g.is_empty:
                provinces.append((name, g))
        except Exception as e:
            logger.warning("[SMN] Province geometry parse failed for %s: %s", name, e)

    return provinces

def _cap_polygon_to_shapely(poly_text: str):
    pts = []
    for pair in (poly_text or "").split():
        try:
            lat_s, lon_s = pair.split(",", 1)
            lat = float(lat_s)
            lon = float(lon_s)
            pts.append((lon, lat))  # shapely uses lon,lat
        except Exception:
            continue

    if len(pts) < 3:
        return None

    if pts[0] != pts[-1]:
        pts.append(pts[0])

    poly = Polygon(pts)
    if not poly.is_valid:
        poly = poly.buffer(0)

    return None if poly.is_empty else poly

def _match_provinces_from_polygon(poly_text: str) -> list[str]:
    poly = _cap_polygon_to_shapely(poly_text)
    if poly is None:
        return []

    matched: list[str] = []
    for name, prov_geom in _load_argentina_provinces():
        try:
            if poly.intersects(prov_geom):
                inter = poly.intersection(prov_geom)
                if not inter.is_empty and inter.area > 0:
                    matched.append(name)
        except Exception:
            continue

    return matched


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

def _build_entries_from_cap(
    *,
    fallback: dict[str, Any],
    identifier: str,
    sent: str,
    status: str,
    msg_type: str,
    scope: str,
    language: str,
    category: str,
    event: str,
    urgency: str,
    severity: str,
    certainty: str,
    onset: str,
    effective: str,
    expires: str,
    headline: str,
    description: str,
    instruction: str,
    sender_name: str,
    web: str,
    area_descs: list[str],
    polygon_text: str,
) -> list[dict[str, Any]]:
    title = headline or fallback.get("title") or event or "Alerta SMN"
    severity_es = _guess_severity_from_text(title, headline, description)
    severity_final = severity_es or severity or ""

    # Prefer explicit names if present; else polygon-to-province; else text extraction
    named_areas = [a for a in area_descs if a]
    if not named_areas:
        named_areas = _match_provinces_from_polygon(polygon_text)
    if not named_areas:
        named_areas = _extract_areas_from_text(description)

    if not named_areas:
        named_areas = ["Argentina"]

    base = {
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
        "onset": onset,
        "effective": effective or onset,
        "expires": expires,
        "published": sent or fallback.get("published") or "",
        "sender_name": sender_name,
        "areas": named_areas[:],
        "polygon": polygon_text,
        "link": web or fallback.get("link") or "",
        "source": "SMN Argentina",
    }

    # Duplicate one alert per province/area for cleaner province grouping
    out: list[dict[str, Any]] = []
    for area_name in named_areas:
        prov = _norm(area_name) or "Argentina"
        d = dict(base)
        d["id"] = f'{base["id"]}|{prov}'
        d["province"] = prov
        d["province_name"] = prov
        d["region"] = prov
        out.append(d)

    return out

def _parse_cap_alert_xml(xml_text: str, fallback: dict[str, Any]) -> list[dict[str, Any]] | None:
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
    onset = _first_text(info, "cap:onset", CAP_NS)
    effective = _first_text(info, "cap:effective", CAP_NS)
    expires = _first_text(info, "cap:expires", CAP_NS)
    headline = _first_text(info, "cap:headline", CAP_NS)
    description = _first_text(info, "cap:description", CAP_NS)
    instruction = _first_text(info, "cap:instruction", CAP_NS)
    sender_name = _first_text(info, "cap:senderName", CAP_NS)
    web = _first_text(info, "cap:web", CAP_NS)

    area_descs = _all_texts(info, "cap:area/cap:areaDesc", CAP_NS)
    polygon_text = _first_text(info, "cap:area/cap:polygon", CAP_NS)

    return _build_entries_from_cap(
        fallback=fallback,
        identifier=identifier,
        sent=sent,
        status=status,
        msg_type=msg_type,
        scope=scope,
        language=language,
        category=category,
        event=event,
        urgency=urgency,
        severity=severity,
        certainty=certainty,
        onset=onset,
        effective=effective,
        expires=expires,
        headline=headline,
        description=description,
        instruction=instruction,
        sender_name=sender_name,
        web=web,
        area_descs=area_descs,
        polygon_text=polygon_text,
    )


# --------------------------------------------------------------------
# HTML detail fallback parsing
# --------------------------------------------------------------------

def _parse_html_detail(html_text: str, fallback: dict[str, Any]) -> list[dict[str, Any]]:
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

    areas = _extract_areas_from_text(text) or ["Argentina"]

    effective = ""
    expires = ""
    m_eff = re.search(r"(?:vigencia|desde)\s*:?[\s\-]*(.+?)(?:hasta|$)", text, flags=re.IGNORECASE)
    m_exp = re.search(r"(?:hasta)\s*:?[\s\-]*(.+?)(?:\.|$)", text, flags=re.IGNORECASE)
    if m_eff:
        effective = _norm(m_eff.group(1))
    if m_exp:
        expires = _norm(m_exp.group(1))

    out: list[dict[str, Any]] = []
    base_id = fallback.get("id") or fallback.get("link") or title
    for prov in areas:
        province_name = _norm(prov) or "Argentina"
        out.append({
            "id": f"{base_id}|{province_name}",
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
            "onset": "",
            "effective": effective,
            "expires": expires,
            "published": fallback.get("published") or "",
            "sender_name": "Servicio Meteorológico Nacional",
            "province": province_name,
            "province_name": province_name,
            "region": province_name,
            "areas": areas[:],
            "polygon": "",
            "link": fallback.get("link") or "",
            "source": "SMN Argentina",
        })

    return out


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

async def _rss_only_fallback_entries(item: dict[str, Any]) -> list[dict[str, Any]]:
    d = dict(item)
    d["event"] = _guess_event_from_title(d.get("title") or "")
    d["severity"] = _guess_severity_from_text(d.get("title"), d.get("summary"), d.get("description"))
    areas = _extract_areas_from_text(d.get("description") or d.get("summary") or "") or ["Argentina"]

    out: list[dict[str, Any]] = []
    base_id = d.get("id") or d.get("link") or d.get("title") or "SMN"
    for prov in areas:
        province_name = _norm(prov) or "Argentina"
        dd = dict(d)
        dd["id"] = f"{base_id}|{province_name}"
        dd["province"] = province_name
        dd["province_name"] = province_name
        dd["region"] = province_name
        dd["areas"] = areas[:]
        dd["polygon"] = ""
        dd["source"] = "SMN Argentina"
        out.append(dd)

    return out

async def _fetch_detail(session: aiohttp.ClientSession, item: dict[str, Any], sem: asyncio.Semaphore) -> list[dict[str, Any]]:
    link = _norm(item.get("link"))
    if not link:
        return await _rss_only_fallback_entries(item)

    async with sem:
        text, content_type = await _fetch_text(session, link)
        if not text:
            return await _rss_only_fallback_entries(item)

        is_xml = ("xml" in content_type.lower()) or _xml_looks_like_cap(text)

        if is_xml:
            parsed = _parse_cap_alert_xml(text, item)
            if parsed:
                return parsed

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
      4) resolve polygon -> province names using local GeoJSON
      5) duplicate one alert per matched province
      6) fallback to HTML/text or RSS-only fields
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

    # Warm the local province file early so errors are obvious in logs
    try:
        _load_argentina_provinces(PROVINCES_GEOJSON_PATH)
    except Exception as e:
        logger.warning("[SMN] Failed to load province GeoJSON (%s): %s", PROVINCES_GEOJSON_PATH, e)

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
        if not isinstance(r, list):
            continue

        for item in r:
            if not isinstance(item, dict):
                continue

            dedupe_key = _norm(item.get("id") or item.get("identifier") or item.get("link") or item.get("title"))
            if dedupe_key and dedupe_key in seen_ids:
                continue
            if dedupe_key:
                seen_ids.add(dedupe_key)

            entries.append(item)

    entries = sorted(entries, key=lambda x: _norm(x.get("published") or x.get("effective") or x.get("onset")), reverse=True)
    logger.warning("[SMN] Parsed %d alerts", len(entries))
    return {"entries": entries, "source": "SMN Argentina"}


def scrape_smn_argentina(conf: dict | None = None) -> dict[str, Any]:
    return asyncio.run(scrape_smn_argentina_async(conf))
