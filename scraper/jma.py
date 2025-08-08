import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime, timezone
import httpx

OFFICE_JSON = "https://www.jma.go.jp/bosai/warning/data/warning/{office}.json"
OFFICE_PAGE = "https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={office}"

# Phenomenon labels we care about (codes seen in JMA warnings JSONs)
PHENOMENON = {
    "04": "Flood",
    "10": "Heavy Rain",               # will expand to (Inundation/Landslide)
    "14": "Thunder Storm",
    "15": "High Wave",
    "19": "Storm Surge",
    "20": "Dense Fog",
    # Some offices expose "18" as explicit inundation (rare but present)
    "18": "Heavy Rain (Inundation)",
}

def _status_to_level(status: str) -> Optional[str]:
    """Map JMA status (JP) to the levels we keep. Skip advisory/clear/downgrade."""
    if not status:
        return None
    if "緊急" in status:                  # Emergency
        return "Emergency"
    if "特別警報" in status:               # Special warning (JMA English UI: Alert)
        return "Alert"
    if "注意報" in status:                 # Advisory → skip
        return None
    if "解除" in status:                   # Cleared → skip
        return None
    if "警報から注意報" in status:           # Downgraded to advisory → skip
        return None
    # Common "発表"(issued) / "継続"(continues) / anything containing 警報 → treat as Warning
    if "発表" in status or "継続" in status or "警報" in status:
        return "Warning"
    return None

def _utc_pub(jst_iso: str) -> str:
    """Convert +09:00 ISO to 'Fri, 08 Aug 2025 06:27 UTC'."""
    if not jst_iso:
        return ""
    try:
        dt = datetime.fromisoformat(jst_iso.replace("Z", "+00:00"))
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")
    except Exception:
        return jst_iso

def _load_region_map(path: str) -> Dict[str, str]:
    """Your curated area-code → 'Pref: Region' mapping. Only these are allowed."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Normalize keys to strings
            return {str(k): v for k, v in data.items()}
    except Exception as e:
        logging.warning(f"[JMA DEBUG] Failed to load region map '{path}': {e}")
        return {}

async def _fetch_office(client: httpx.AsyncClient, office: str) -> Optional[Dict[str, Any]]:
    url = OFFICE_JSON.format(office=office)
    try:
        r = await client.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.warning(f"[JMA DEBUG] fetch {office} failed: {e}")
        return None

def _heavy_rain_variant_from_warning(w: Dict[str, Any]) -> Optional[str]:
    # Check attentions like ["土砂災害注意","浸水注意"]
    vals = w.get("attentions")
    if isinstance(vals, list):
        joined = " ".join(str(v) for v in vals)
        if "浸水" in joined:
            return "Inundation"
        if "土砂" in joined:
            return "Landslide"
    return None

def _heavy_rain_variant_from_levels(w: Dict[str, Any]) -> Optional[str]:
    levels = w.get("levels")
    if not isinstance(levels, list):
        return None
    for lvl in levels:
        t = str(lvl.get("type", ""))
        if "浸水" in t:
            return "Inundation"
        if "土砂" in t:
            return "Landslide"
    return None

def _phenomenon_name(code: str, w: Dict[str, Any]) -> Optional[str]:
    if code == "10":
        variant = _heavy_rain_variant_from_warning(w) or _heavy_rain_variant_from_levels(w)
        if not variant:
            # If we can’t distinguish, skip to avoid ambiguous “Heavy Rain”
            return None
        return f"Heavy Rain ({variant})"
    if code == "18":
        return "Heavy Rain (Inundation)"
    return PHENOMENON.get(code)

def _iter_area_blocks(doc: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    pub = doc.get("reportDatetime", "")
    for block in doc.get("areaTypes", []):
        areas = block.get("areas")
        if isinstance(areas, list):
            yield pub, areas

async def scrape_jma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    conf required:
      - office_codes: list of office codes (e.g., ["011000","012000",...])
      - region_map_file: path to curated area codes JSON (only these areas are parsed)
    """
    office_codes: List[str] = conf.get("office_codes") or []
    region_map_path: str = conf.get("region_map_file") or "scraper/region_area_codes.json"

    if not office_codes:
        logging.warning("[JMA DEBUG] No office_codes configured.")
        return {"entries": [], "source": "JMA offices"}

    region_map = _load_region_map(region_map_path)
    if not region_map:
        logging.warning("[JMA DEBUG] Region map empty; nothing will match (0 entries expected).")

    allowed_codes: Set[str] = set(region_map.keys())

    dedupe: Set[Tuple[str, str, str]] = set()  # (area_code, phenomenon, level)
    entries: List[Dict[str, Any]] = []

    for office in office_codes:
        doc = await _fetch_office(client, office)
        if not doc:
            continue

        for pub_iso, areas in _iter_area_blocks(doc):
            published_str = _utc_pub(pub_iso) or _utc_pub(doc.get("reportDatetime") or "")

            for area in areas:
                area_code = str(area.get("code", ""))

                # *** Key filter: only parse areas the user cares about ***
                if area_code not in allowed_codes:
                    continue

                region_name = region_map.get(area_code, area_code)
                warnings = area.get("warnings")
                if not isinstance(warnings, list):
                    continue

                for w in warnings:
                    code = str(w.get("code", ""))
                    status = str(w.get("status", "") or "")
                    level = _status_to_level(status)
                    if level is None:
                        continue  # skip advisories/clears/etc.

                    pheno = _phenomenon_name(code, w)
                    if not pheno:
                        continue  # skip if no valid phenomenon resolution

                    key = (area_code, pheno, level)
                    if key in dedupe:
                        continue
                    dedupe.add(key)

                    entries.append({
                        "title": f"{level} – {pheno}",
                        "region": region_name,
                        "type": pheno,
                        "level": level,
                        "link": OFFICE_PAGE.format(office=office),
                        "published": published_str,
                    })

    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} filtered warnings/alerts")
    return {"entries": entries, "source": "JMA offices"}
