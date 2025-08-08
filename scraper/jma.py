import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import httpx

# ---- Config helpers ----

def _load_region_map(path: str) -> Dict[str, str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"[JMA DEBUG] failed loading region map '{path}': {e}")
        return {}

# Phenomenon code → English
PHENOMENON_MAP = {
    "04": "Flood",        # (city-level JSON uses 04 for flood)
    "10": "Heavy Rain",   # split via levels/type to (Landslide)/(Inundation)
    "14": "Thunderstorm",
    "15": "High Wave",
    "18": "Flood",        # (some office JSONs use 18 for flood hazard)
    "19": "Storm Surge",
    "20": "Dense Fog",
}

OFFICE_PAGE = "https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={code}"
OFFICE_JSON = "https://www.jma.go.jp/bosai/warning/data/warning/{code}.json"

def _utc_pub(dt_jst_iso: str) -> str:
    # dt like 2025-08-08T15:27:00+09:00 → UTC RFC-ish
    try:
        dt = datetime.fromisoformat(dt_jst_iso.replace("Z","+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M UTC")
    except Exception:
        return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M UTC")

def _status_to_level(status: str) -> Optional[str]:
    """
    Return Advisory/Warning/Alert/Emergency or None if cleared.
    We *exclude* anything with 注意 (advisory) and anything 解除 (cleared).
    """
    if not status:
        return None
    if "解除" in status:
        return None
    if "特別警報" in status:
        return "Alert"
    if "緊急" in status:
        return "Emergency"
    if "注意" in status:   # contains advisory text
        return None
    # otherwise it's an active warning state like 発表/継続/警報から注意報(-> we drop; covered above)
    if "警報" in status or "発表" in status or "継続" in status:
        return "Warning"
    return None

def _heavy_rain_variant(warning_obj: Dict[str, Any]) -> List[str]:
    """
    Look inside levels[*].type to decide Landslide vs Inundation.
    Returns list of suffixes like ['Landslide'] or ['Inundation'] or both.
    """
    variants = set()
    for lvl in warning_obj.get("levels", []):
        t = lvl.get("type", "")
        if "土砂災害" in t:
            variants.add("Landslide")
        if "浸水" in t:
            variants.add("Inundation")
    # If no levels block but attentions hint at it:
    for att in warning_obj.get("attentions", []) or []:
        if "土砂" in att:
            variants.add("Landslide")
        if "浸水" in att:
            variants.add("Inundation")
    return list(variants)

def _phenomenon_name(code: str, warning_obj: Dict[str, Any]) -> Optional[str]:
    base = PHENOMENON_MAP.get(code)
    if not base:
        return None
    if code == "10":  # Heavy Rain → split
        variants = _heavy_rain_variant(warning_obj)
        if not variants:
            # fallback: unknown sub-type, skip to avoid wrong label
            return None
        # If both present, we will emit two entries upstream
        return "Heavy Rain"
    return base

async def _fetch_office(client: httpx.AsyncClient, code: str) -> Optional[Dict[str, Any]]:
    url = OFFICE_JSON.format(code=code)
    try:
        r = await client.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.warning(f"[JMA DEBUG] fetch {code} failed: {e}")
        return None

def _area_status_blocks(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Return the CURRENT status block: doc['areaTypes'][0]['areas'] (if present).
    """
    try:
        return (doc.get("areaTypes") or [])[0].get("areas") or []
    except Exception:
        return []

async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    conf must include:
      - office_codes: List[str]  # e.g. ["011000","012000",...]
      - region_map_file: str     # path to region_area_codes.json
    """
    office_codes: List[str] = conf.get("office_codes") or []
    region_map_path: str = conf.get("region_map_file") or "scraper/region_area_codes.json"

    region_map = _load_region_map(region_map_path)
    if not office_codes:
        logging.warning("[JMA DEBUG] No office_codes configured.")
        return {"entries": [], "source": "JMA offices"}

    entries: List[Dict[str, Any]] = []
    async with client:
        # Pull each office JSON and parse
        for office in office_codes:
            doc = await _fetch_office(client, office)
            if not doc:
                continue

            published = _utc_pub(doc.get("reportDatetime") or "")
            areas = _area_status_blocks(doc)
            for area in areas:
                area_code = str(area.get("code", ""))
                # Map to region name
                region_name = region_map.get(area_code)
                if not region_name:
                    # no mapping — skip to avoid showing codes
                    continue

                for w in area.get("warnings", []) or []:
                    code = str(w.get("code", ""))
                    status = w.get("status", "") or ""
                    level = _status_to_level(status)

                    # exclude advisories/cleared
                    if level is None:
                        continue

                    pheno = _phenomenon_name(code, w)
                    if not pheno:
                        continue

                    link = OFFICE_PAGE.format(code=office)

                    if code == "10":
                        # emit 1..2 entries based on variants
                        variants = _heavy_rain_variant(w)
                        for v in variants:
                            title = f"{level} – Heavy Rain ({v})"
                            entries.append({
                                "title": title,
                                "region": region_name,
                                "type": f"Heavy Rain ({v})",
                                "level": level,
                                "link": link,
                                "published": published,
                            })
                    else:
                        title = f"{level} – {pheno}"
                        entries.append({
                            "title": title,
                            "region": region_name,
                            "type": pheno,
                            "level": level,
                            "link": link,
                            "published": published,
                        })

    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} office warnings/alerts")
    return {"entries": entries, "source": "JMA offices"}
