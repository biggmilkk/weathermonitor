# scraper/jma.py
import json
import logging
from typing import Dict, List, Tuple, Optional, Set
from pathlib import Path

import httpx

JMA_AREA_JSON = "https://www.jma.go.jp/bosai/common/const/area.json"
JMA_WARNING_BASE = "https://www.jma.go.jp/bosai/warning/data/warning"

# Only show these hazards
INCLUDE_CODES = {"03", "04"}  # 03 = Heavy Rain (split by condition), 04 = Flood
# Ignore "Thunderstorm" (14), "洪水害危険度 outlook" (18), etc.

# English messages we want to show
HEAVY_RAIN_INUNDATION = "Heavy Rain (Inundation)"
HEAVY_RAIN_LANDSLIDE = "Heavy Rain (Landslide)"
FLOOD = "Flood"

# Simple, generic shape expected by the app:
# [{"title": "...", "region": "...", "summary": "...", "link": "...", "published": "..."}]


def _load_region_map_from_file(path: str) -> Dict[str, str]:
    """Load your master region map (name -> class10 code)."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


async def _fetch_area_json(client: httpx.AsyncClient) -> Optional[dict]:
    """Fetch JMA area.json for optional validation."""
    try:
        r = await client.get(JMA_AREA_JSON, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.warning(f"[JMA VALIDATION] Could not fetch area.json: {e}")
        return None


def _valid_class10_codes(area_json: dict) -> Set[str]:
    """Return the set of valid class10 codes from area.json."""
    try:
        class10s = area_json.get("class10s", {})
        return set(class10s.keys())
    except Exception:
        return set()


def _validate_region_map(region_map: Dict[str, str], area_json: Optional[dict]) -> Dict[str, str]:
    """
    Keep your names exactly as-is.
    If area_json is available, drop any entries whose code no longer exists.
    (We do not try to invent or rename anything.)
    """
    if not area_json:
        # No validation available; keep as-is
        return region_map

    valid_codes = _valid_class10_codes(area_json)
    out = {}
    dropped = []
    for name, code in region_map.items():
        if code in valid_codes:
            out[name] = code
        else:
            dropped.append((name, code))
    if dropped:
        for name, code in dropped:
            logging.warning(f"[JMA VALIDATION] Dropping '{name}' (unknown code {code}) per area.json")
    return out


def _office_url(office_code: str) -> str:
    return f"{JMA_WARNING_BASE}/{office_code}.json"


def _parse_heavy_rain_conditions(cond_text: Optional[str]) -> List[str]:
    """
    cond_text examples:
      - "土砂災害、浸水害"
      - "土砂災害"
      - None
    We map:
      - contains '浸水' -> Heavy Rain (Inundation)
      - contains '土砂' -> Heavy Rain (Landslide)
    If both present, return both (two entries).
    """
    out = []
    if not cond_text:
        return out
    if "浸水" in cond_text:
        out.append(HEAVY_RAIN_INUNDATION)
    if "土砂" in cond_text:
        out.append(HEAVY_RAIN_LANDSLIDE)
    return out


def _warnings_for_area(area_obj: dict) -> List[Tuple[str, dict]]:
    """
    From an 'area' item:
      {"code":"050010", "warnings":[ {...}, {...} ]}
    return a list of (msg, warning_dict) for included hazards.
    """
    results: List[Tuple[str, dict]] = []
    for w in area_obj.get("warnings", []):
        code = str(w.get("code", ""))
        if code not in INCLUDE_CODES:
            continue
        # Only show if status is "発表" (issued) or "継続" (continuing)
        status = w.get("status", "")
        if status not in ("発表", "継続"):
            continue

        if code == "03":  # Heavy Rain -> split by condition
            # condition like "土砂災害、浸水害", "土砂災害", ...
            for msg in _parse_heavy_rain_conditions(w.get("condition")):
                results.append((msg, w))
        elif code == "04":  # Flood
            results.append((FLOOD, w))
    return results


def _build_code_to_name(region_map: Dict[str, str]) -> Dict[str, str]:
    """Invert your region map: code -> name (names remain authoritative)."""
    return {code: name for name, code in region_map.items()}


async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Async scraper entry point (wired in scraper_registry as 'rss_jma').

    Expected in conf:
      - office_codes: List[str] (prefecture office codes like '050000', '460100', ...)
      - region_map_file: str (path to scraper/region_area_codes.json)
      - optionally: area_codes (already-loaded area.json content) -> not required
    """
    # 1) Load your master list (names -> class10 codes)
    try:
        region_map = _load_region_map_from_file(conf["region_map_file"])
    except Exception as e:
        logging.warning(f"[JMA] Failed to load region_map_file: {e}")
        return {"entries": [], "error": str(e), "source": conf}

    # 2) Optionally validate codes against live area.json
    area_json = await _fetch_area_json(client)
    region_map = _validate_region_map(region_map, area_json)
    allowed_code_to_name = _build_code_to_name(region_map)

    office_codes: List[str] = conf.get("office_codes", [])
    entries: List[dict] = []

    for office in office_codes:
        url = _office_url(office)
        try:
            r = await client.get(url, timeout=25)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logging.warning(f"[JMA FETCH ERROR] {office}: {e}")
            continue

        report_dt = data.get("reportDatetime") or data.get("reportDateTime") or ""
        publishing_office = data.get("publishingOffice", "")

        # We only need the "prefecture-level" areas block: it sits in areaTypes[0]["areas"]
        # (The second block in areaTypes is municipality-level; we ignore that.)
        area_types = data.get("areaTypes", [])
        if not area_types:
            continue
        pref_block = area_types[0]
        for area in pref_block.get("areas", []):
            code = str(area.get("code", ""))
            if code not in allowed_code_to_name:
                # Not in your interest list
                continue

            # Map warnings -> messages (split heavy rain)
            msgs = _warnings_for_area(area)
            if not msgs:
                continue

            region_name = allowed_code_to_name[code]
            # One entry per (region, message) so the UI prints them on separate lines
            for msg, _w in msgs:
                entries.append({
                    "title": f"Warning – {msg}",
                    "region": region_name,
                    "summary": "",  # keep clean; message is in title
                    "link": url,
                    "published": report_dt,
                })

    # Sort newest first by published timestamp string (they're ISO; string sort is fine)
    entries.sort(key=lambda x: x.get("published", ""), reverse=True)
    logging.warning(f"[JMA DEBUG] Async parsed {len(entries)} alerts")
    return {"entries": entries, "source": "JMA (office JSONs)"}
