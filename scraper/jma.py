import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime, timezone
import httpx

OFFICE_JSON = "https://www.jma.go.jp/bosai/warning/data/warning/{office}.json"
OFFICE_PAGE = "https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={office}"

# Phenomenon base labels (we refine Heavy Rain to Inundation/Landslide below)
PHENOMENON = {
    "03": "Heavy Rain",                 # Many offices use 03 for the heavy-rain family
    "04": "Flood",
    "10": "Heavy Rain",                 # Some offices use 10 instead
    "14": "Thunder Storm",
    "15": "High Wave",
    "19": "Storm Surge",
    "20": "Dense Fog",
    "18": "Heavy Rain (Inundation)",    # Occasionally explicit
}

def _status_to_level(status: str) -> Optional[str]:
    """Keep only Warning / Alert / Emergency. Skip advisories/clears/downgrades."""
    if not status:
        return None
    if "緊急" in status:                      # Emergency
        return "Emergency"
    if "特別警報" in status:                   # Special warning (JMA English UI shows "Alert")
        return "Alert"
    if "注意報" in status:                     # Advisory → skip
        return None
    if "解除" in status:                       # Cleared → skip
        return None
    if "警報から注意報" in status:               # Downgraded to advisory → skip
        return None
    # Common: "発表"(issued) / "継続"(continues) / anything with "警報" → Warning
    if "発表" in status or "継続" in status or "警報" in status:
        return "Warning"
    return None

def _utc_pub(jst_iso: str) -> str:
    """Convert ISO with +09:00 to 'Fri, 08 Aug 2025 06:27 UTC'."""
    if not jst_iso:
        return ""
    try:
        dt = datetime.fromisoformat(jst_iso)
        if dt.tzinfo is None:
            # Assume JST if tz missing
            dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")
    except Exception:
        # Fallback: try replacing 'Z'
        try:
            dt = datetime.fromisoformat(jst_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
            return dt.strftime("%a, %d %b %Y %H:%M UTC")
        except Exception:
            return jst_iso

def _load_region_map(path: str) -> Dict[str, str]:
    """
    Load your curated Name->Code JSON and invert to Code->Name.
    Only areas present here will be emitted.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            name_to_code = json.load(f)
        code_to_name = {str(code): str(name) for name, code in name_to_code.items()}
        return code_to_name
    except Exception as e:
        logging.warning(f"[JMA DEBUG] Failed to load curated region map '{path}': {e}")
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

def _contains(text: Optional[str], *needles: str) -> bool:
    s = text or ""
    return any(n in s for n in needles)

def _heavy_rain_variant_from_any(w: Dict[str, Any]) -> Optional[str]:
    """
    Decide Heavy Rain subtype from condition/attentions/levels.
    Returns 'Inundation', 'Landslide', or None if we can’t tell.
    """
    # 1) condition (e.g., "土砂災害、浸水害")
    cond = w.get("condition")
    if _contains(cond, "浸水"):
        return "Inundation"
    if _contains(cond, "土砂"):
        return "Landslide"

    # 2) attentions list (e.g., ["土砂災害警戒","浸水警戒"])
    atts = w.get("attentions")
    if isinstance(atts, list):
        joined = " ".join(str(a) for a in atts)
        if "浸水" in joined:
            return "Inundation"
        if "土砂" in joined:
            return "Landslide"

    # 3) levels[].type strings
    levels = w.get("levels")
    if isinstance(levels, list):
        for lvl in levels:
            t = str(lvl.get("type", ""))
            if "浸水" in t:
                return "Inundation"
            if "土砂" in t:
                return "Landslide"

    return None

def _phenomenon_name(code: str, w: Dict[str, Any]) -> Optional[str]:
    base = PHENOMENON.get(code)
    if not base:
        return None

    # Resolve Heavy Rain subtype for 03 / 10
    if base == "Heavy Rain":
        variant = _heavy_rain_variant_from_any(w)
        if not variant:
            # Be conservative; skip ambiguous Heavy Rain without subtype
            return None
        return f"Heavy Rain ({variant})"

    return base

def _iter_area_blocks(doc: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    pub = doc.get("reportDatetime", "")
    for block in doc.get("areaTypes", []):
        areas = block.get("areas")
        if isinstance(areas, list):
            yield pub, areas

async def scrape_jma_async(conf: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    conf:
      - office_codes: list[str] of office codes (e.g. ["011000","012000",...])
      - region_map_file: path to your curated Name->Code JSON
    """
    office_codes: List[str] = conf.get("office_codes") or []
    region_map_path: str = conf.get("region_map_file") or "scraper/region_area_codes.json"

    if not office_codes:
        logging.warning("[JMA DEBUG] No office_codes configured.")
        return {"entries": [], "source": "JMA offices"}

    code_to_name = _load_region_map(region_map_path)
    if not code_to_name:
        logging.warning("[JMA DEBUG] Curated region map empty; nothing will match.")
        return {"entries": [], "source": "JMA offices"}

    allowed_codes: Set[str] = set(code_to_name.keys())
    dedupe: Set[Tuple[str, str, str]] = set()  # (area_code, phenomenon, level)
    entries: List[Dict[str, Any]] = []

    for office in office_codes:
        doc = await _fetch_office(client, office)
        if not doc:
            continue

        for pub_iso, areas in _iter_area_blocks(doc):
            published_str = _utc_pub(pub_iso)

            for area in areas:
                area_code = str(area.get("code", ""))
                if area_code not in allowed_codes:
                    continue  # only regions you care about

                region_name = code_to_name.get(area_code, area_code)
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
                        continue  # unknown/ambiguous

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
