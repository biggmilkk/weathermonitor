import json
import logging
from typing import Dict, List, Tuple, Optional, Set

import httpx

JMA_AREA_JSON = "https://www.jma.go.jp/bosai/common/const/area.json"
JMA_WARNING_BASE = "https://www.jma.go.jp/bosai/warning/data/warning"

# Only show these hazards
INCLUDE_CODES = {"03", "04"}  # 03 = Heavy Rain (split by condition), 04 = Flood

# English messages
HEAVY_RAIN_INUNDATION = "Heavy Rain (Inundation)"
HEAVY_RAIN_LANDSLIDE = "Heavy Rain (Landslide)"
FLOOD = "Flood"


def _load_region_map_from_file(path: str) -> Dict[str, str]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


async def _fetch_area_json(client: httpx.AsyncClient) -> Optional[dict]:
    try:
        r = await client.get(JMA_AREA_JSON, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.warning(f"[JMA VALIDATION] Could not fetch area.json: {e}")
        return None


def _valid_class10_codes(area_json: dict) -> Set[str]:
    try:
        class10s = area_json.get("class10s", {})
        return set(class10s.keys())
    except Exception:
        return set()


def _validate_region_map(region_map: Dict[str, str], area_json: Optional[dict]) -> Dict[str, str]:
    if not area_json:
        return region_map

    valid_codes = _valid_class10_codes(area_json)
    out: Dict[str, str] = {}
    dropped: List[Tuple[str, str]] = []
    for name, code in region_map.items():
        if code in valid_codes:
            out[name] = code
        else:
            dropped.append((name, code))
    for name, code in dropped:
        logging.warning(f"[JMA VALIDATION] Dropping '{name}' (unknown code {code}) per area.json")
    return out


def _office_json_url(office_code: str) -> str:
    return f"{JMA_WARNING_BASE}/{office_code}.json"


def _office_frontend_url(office_code: str) -> str:
    # Example:
    # https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code=050000
    return f"https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={office_code}"


def _parse_heavy_rain_conditions(cond_text: Optional[str]) -> List[str]:
    out: List[str] = []
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
        status = w.get("status", "")
        if status not in ("発表", "継続"):
            continue

        if code == "03":
            for msg in _parse_heavy_rain_conditions(w.get("condition")):
                results.append((msg, w))
        elif code == "04":
            results.append((FLOOD, w))
    return results


def _build_code_to_name(region_map: Dict[str, str]) -> Dict[str, str]:
    return {code: name for name, code in region_map.items()}


async def _fetch_office_json(
    client: httpx.AsyncClient,
    office: str,
    allowed_code_to_name: Dict[str, str],
) -> List[dict]:
    """Fetch and parse a single office JSON (unbounded concurrency)."""
    url = _office_json_url(office)
    try:
        r = await client.get(url, timeout=25)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {office}: {e}")
        return []

    report_dt = data.get("reportDatetime") or data.get("reportDateTime") or ""
    area_types = data.get("areaTypes", [])
    if not area_types:
        return []

    pref_block = area_types[0]
    frontend_url = _office_frontend_url(office)
    office_entries: List[dict] = []

    for area in pref_block.get("areas", []):
        code = str(area.get("code", ""))
        if code not in allowed_code_to_name:
            continue
        msgs = _warnings_for_area(area)
        if not msgs:
            continue
        region_name = allowed_code_to_name[code]
        for msg, _w in msgs:
            office_entries.append({
                "title": f"Warning – {msg}",
                "region": region_name,
                "summary": "",
                "link": frontend_url,   # human-friendly page
                "published": report_dt,
            })
    return office_entries


async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """Async JMA scraper — fetch all offices concurrently via shared client (no semaphore)."""
    try:
        region_map = _load_region_map_from_file(conf["region_map_file"])
    except Exception as e:
        logging.warning(f"[JMA] Failed to load region_map_file: {e}")
        return {"entries": [], "error": str(e), "source": conf}

    area_json = await _fetch_area_json(client)
    region_map = _validate_region_map(region_map, area_json)
    allowed_code_to_name = _build_code_to_name(region_map)

    office_codes: List[str] = conf.get("office_codes", [])
    tasks = [
        _fetch_office_json(client, office, allowed_code_to_name)
        for office in office_codes
    ]

    results = await asyncio.gather(*tasks, return_exceptions=False)

    # flatten and sort
    entries = [e for sub in results for e in sub]
    entries.sort(key=lambda x: x.get("published", ""), reverse=True)
    logging.warning(f"[JMA DEBUG] Parsed {len(entries)} alerts")
    return {"entries": entries, "source": "JMA (office JSONs)"}
