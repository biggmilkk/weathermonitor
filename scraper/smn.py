# scraper/smn.py
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
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
DEPARTMENTS_GEOJSON_PATH = "argentina_departments.geojson"

CAP_NS = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

TIMEOUT_TOTAL = 25
CONNECTOR_LIMIT = 10

logger = logging.getLogger(__name__)

# SMN sometimes surfaces public color words in pages/text.
# CAP-native severities are usually Extreme / Severe / Moderate / Minor.
SPANISH_SEVERITY_ORDER = {
    "Rojo": 4,
    "Naranja": 3,
    "Amarillo": 2,
    "Verde": 1,
    "Extreme": 4,
    "Severe": 3,
    "Moderate": 2,
    "Minor": 1,
}

COLOR_WORD_RE = re.compile(r"\b(rojo|naranja|amarillo|verde)\b", re.IGNORECASE)

# Stable manual event translation for bucket labels / grouping.
SMN_EVENT_ES_TO_EN = {
    "Tormentas": "Thunderstorms",
    "Lluvias": "Rain",
    "Viento": "Wind",
    "Nevadas": "Snow",
    "Frío": "Cold",
    "Calor": "Heat",
    "Zonda": "Zonda Wind",
    "Granizo": "Hail",
    "Niebla": "Fog",
    "Polvo": "Dust",
    "Viento Zonda": "Zonda Wind",
    "Tormenta": "Thunderstorms",
    "Lluvia": "Rain",
    "Nevada": "Snow",
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

def _event_to_english(event_es: str) -> str:
    event_es = _norm(event_es)
    if not event_es:
        return "Alert"
    return SMN_EVENT_ES_TO_EN.get(event_es, event_es)

def _extract_areas_from_text(text: str) -> list[str]:
    """
    Best-effort extraction of affected areas from RSS/HTML text.
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
    return areas[0] if areas else "Argentina"

def _xml_looks_like_cap(text: str) -> bool:
    t = (text or "")[:2000]
    return ("urn:oasis:names:tc:emergency:cap:1.2" in t) or ("<alert" in t and "<info" in t)

def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    return _norm(soup.get_text("\n", strip=True))

def _slug_hash(*parts: str) -> str:
    src = "|".join(_norm(p) for p in parts if _norm(p))
    return hashlib.sha1(src.encode("utf-8")).hexdigest()[:12]


# --------------------------------------------------------------------
# Geometry loaders / polygon helpers
# --------------------------------------------------------------------

def _extract_name(props: dict[str, Any], candidates: list[str]) -> str:
    for key in candidates:
        v = _norm(props.get(key))
        if v:
            return v
    return ""

@lru_cache(maxsize=1)
def _load_argentina_provinces(path: str = PROVINCES_GEOJSON_PATH) -> list[tuple[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    provinces: list[tuple[str, Any]] = []
    for feat in data.get("features", []):
        props = feat.get("properties", {}) or {}
        name = _extract_name(props, [
            "nombre",
            "name",
            "provincia",
            "nam",
            "prov_name",
        ])
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

@lru_cache(maxsize=1)
def _load_argentina_departments(path: str = DEPARTMENTS_GEOJSON_PATH) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    departments: list[dict[str, Any]] = []
    for feat in data.get("features", []):
        props = feat.get("properties", {}) or {}
        dept_name = _extract_name(props, [
            "nombre",
            "name",
            "departamento",
            "depto",
            "nomdep",
            "department",
            "departamen",
        ])
        prov_name = _extract_name(props, [
            "provincia_nombre",
            "provincia",
            "nomprov",
            "prov_name",
            "province",
            "nombre_provincia",
            "provincia_nom",
        ])
        geom = feat.get("geometry")
        if not dept_name or not geom:
            continue

        try:
            g = shape(geom)
            if not g.is_valid:
                g = g.buffer(0)
            if g.is_empty:
                continue
            departments.append({
                "department": dept_name,
                "province": prov_name,
                "geometry": g,
            })
        except Exception as e:
            logger.warning("[SMN] Department geometry parse failed for %s: %s", dept_name, e)

    return departments

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

def _match_provinces(poly) -> list[str]:
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

def _match_departments(poly) -> list[dict[str, str]]:
    if poly is None:
        return []

    matched: list[dict[str, str]] = []
    for item in _load_argentina_departments():
        try:
            geom = item["geometry"]
            if poly.intersects(geom):
                inter = poly.intersection(geom)
                if not inter.is_empty and inter.area > 0:
                    matched.append({
                        "department": item["department"],
                        "province": item["province"],
                    })
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
    event_es: str,
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
    area_descs: list[str],
    polygon_text: str,
) -> list[dict[str, Any]]:
    title = headline or fallback.get("title") or event_es or "Alerta SMN"

    # Keep CAP-native severity if available; fallback to color words if present in public text.
    severity_from_text = _guess_severity_from_text(title, headline, description)
    severity_final = severity or severity_from_text or ""

    event_es_final = event_es or _guess_event_from_title(title)
    event_en = _event_to_english(event_es_final)

    poly = _cap_polygon_to_shapely(polygon_text)

    matched_provinces = _match_provinces(poly)
    matched_departments = _match_departments(poly)

    # Group matched departments by province.
    dept_by_province: dict[str, list[str]] = defaultdict(list)
    for item in matched_departments:
        dept = _norm(item.get("department"))
        prov = _norm(item.get("province"))
        if not dept:
            continue
        dept_by_province[prov].append(dept)

    # If department file doesn't carry a province name, fall back to matched provinces later.
    out: list[dict[str, Any]] = []

    # Primary path: build one entry per province, with only that province's departments.
    province_candidates = sorted(set(
        [p for p in matched_provinces if _norm(p)] +
        [p for p in dept_by_province.keys() if _norm(p)]
    ))

    if province_candidates:
        for province_name in province_candidates:
            depts = sorted(set(dept_by_province.get(province_name, [])))
            region = ", ".join(depts) if depts else province_name

            stable_suffix = _slug_hash(identifier or fallback.get("link") or title, polygon_text, province_name)
            out.append({
                "id": f"{identifier or fallback.get('id') or fallback.get('link') or title}|{province_name}|{stable_suffix}",
                "identifier": identifier,
                "title": title,
                "headline": headline or title,
                "summary": description or fallback.get("summary") or "",
                "description": description or fallback.get("description") or "",
                "instruction": instruction,
                "event": event_en,           # English for bucket labels
                "event_es": event_es_final,  # keep original too
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
                "province": province_name,
                "province_name": province_name,
                "region": region,
                "areas": depts[:] if depts else [province_name],
                "polygon": polygon_text,
                "link": fallback.get("link") or "",  # keep specific CAP/RSS detail URL, not generic homepage
                "source": "SMN Argentina",
            })

        return out

    # Fallback path if geometry matching failed completely.
    named_areas = [a for a in area_descs if _norm(a)]
    if not named_areas:
        named_areas = _extract_areas_from_text(description)
    if not named_areas:
        named_areas = ["Argentina"]

    province_name = _province_from_areas(named_areas)
    region = ", ".join(named_areas)

    stable_suffix = _slug_hash(identifier or fallback.get("link") or title, polygon_text or region)
    return [{
        "id": f"{identifier or fallback.get('id') or fallback.get('link') or title}|{province_name}|{stable_suffix}",
        "identifier": identifier,
        "title": title,
        "headline": headline or title,
        "summary": description or fallback.get("summary") or "",
        "description": description or fallback.get("description") or "",
        "instruction": instruction,
        "event": event_en,
        "event_es": event_es_final,
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
        "province": province_name,
        "province_name": province_name,
        "region": region,
        "areas": named_areas[:],
        "polygon": polygon_text,
        "link": fallback.get("link") or "",
        "source": "SMN Argentina",
    }]

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
    event_es = _first_text(info, "cap:event", CAP_NS)
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
        event_es=event_es,
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
    event_es = _guess_event_from_title(title)
    event_en = _event_to_english(event_es)

    areas = _extract_areas_from_text(text) or ["Argentina"]
    province_name = _province_from_areas(areas)

    effective = ""
    expires = ""
    m_eff = re.search(r"(?:vigencia|desde)\s*:?[\s\-]*(.+?)(?:hasta|$)", text, flags=re.IGNORECASE)
    m_exp = re.search(r"(?:hasta)\s*:?[\s\-]*(.+?)(?:\.|$)", text, flags=re.IGNORECASE)
    if m_eff:
        effective = _norm(m_eff.group(1))
    if m_exp:
        expires = _norm(m_exp.group(1))

    stable_suffix = _slug_hash(fallback.get("id") or fallback.get("link") or title, ",".join(areas))
    return [{
        "id": f"{fallback.get('id') or fallback.get('link') or title}|{province_name}|{stable_suffix}",
        "identifier": "",
        "title": title,
        "headline": title,
        "summary": fallback.get("summary") or "",
        "description": text or fallback.get("description") or "",
        "instruction": "",
        "event": event_en,
        "event_es": event_es,
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
        "region": ", ".join(areas),
        "areas": areas[:],
        "polygon": "",
        "link": fallback.get("link") or "",
        "source": "SMN Argentina",
    }]


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
    event_es = _guess_event_from_title(d.get("title") or "")
    event_en = _event_to_english(event_es)
    severity = _guess_severity_from_text(d.get("title"), d.get("summary"), d.get("description"))
    areas = _extract_areas_from_text(d.get("description") or d.get("summary") or "") or ["Argentina"]
    province_name = _province_from_areas(areas)
    stable_suffix = _slug_hash(d.get("id") or d.get("link") or d.get("title") or "SMN", ",".join(areas))

    return [{
        "id": f"{d.get('id') or d.get('link') or d.get('title') or 'SMN'}|{province_name}|{stable_suffix}",
        "identifier": "",
        "title": d.get("title") or "",
        "headline": d.get("headline") or d.get("title") or "",
        "summary": d.get("summary") or "",
        "description": d.get("description") or "",
        "instruction": "",
        "event": event_en,
        "event_es": event_es,
        "severity": severity,
        "urgency": "",
        "certainty": "",
        "status": "",
        "msg_type": "",
        "scope": "",
        "category": "",
        "language": "es",
        "onset": "",
        "effective": "",
        "expires": "",
        "published": d.get("published") or "",
        "sender_name": "Servicio Meteorológico Nacional",
        "province": province_name,
        "province_name": province_name,
        "region": ", ".join(areas),
        "areas": areas[:],
        "polygon": "",
        "link": d.get("link") or "",
        "source": "SMN Argentina",
    }]

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
      4) resolve polygon -> provinces + departments using local GeoJSON files
      5) create one alert entry per intersected province
      6) within each province entry, store only the departments crossed in that province
      7) keep the specific RSS/CAP detail URL as the alert link
      8) use English event names for renderer bucket labels
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

    # Warm local geometry files early so issues are obvious.
    try:
        _load_argentina_provinces(PROVINCES_GEOJSON_PATH)
    except Exception as e:
        logger.warning("[SMN] Failed to load province GeoJSON (%s): %s", PROVINCES_GEOJSON_PATH, e)

    try:
        _load_argentina_departments(DEPARTMENTS_GEOJSON_PATH)
    except Exception as e:
        logger.warning("[SMN] Failed to load departments GeoJSON (%s): %s", DEPARTMENTS_GEOJSON_PATH, e)

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

            dedupe_key = _norm(item.get("id"))
            if dedupe_key and dedupe_key in seen_ids:
                continue
            if dedupe_key:
                seen_ids.add(dedupe_key)

            entries.append(item)

    entries = sorted(
        entries,
        key=lambda x: _norm(x.get("published") or x.get("effective") or x.get("onset")),
        reverse=True,
    )
    logger.warning("[SMN] Parsed %d alerts", len(entries))
    return {"entries": entries, "source": "SMN Argentina"}


def scrape_smn_argentina(conf: dict | None = None) -> dict[str, Any]:
    return asyncio.run(scrape_smn_argentina_async(conf))
