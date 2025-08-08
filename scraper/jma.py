# jma.py
# Async scraper for JMA warnings (class-10 regional layer), filtered to user-defined regions.
# Expected in conf (merged by loader):
#   conf["area_codes"] : contents of areacode.json (centers/offices/class10s/...)
#   conf["weather"]    : optional (not required here)
#   conf["region_map"] : dict[str name -> str class10_code]  # your master list of regions
#
# It fetches:
#   https://www.jma.go.jp/bosai/warning/data/warning/map.json
# then the needed office JSONs e.g.:
#   https://www.jma.go.jp/bosai/warning/data/warning/050000.json

import asyncio
import logging
from typing import Dict, List, Set, Tuple
import httpx

BASE = "https://www.jma.go.jp/bosai/warning/data/warning"
MAP_URL = f"{BASE}/map.json"

# Allowed JMA "code" values at class-10 layer
ALLOWED_CODES = {"03", "04"}  # 03=Heavy Rain, 04=Flood

# Render labels
LABEL_HEAVY_RAIN_LANDSLIDE = "Warning - Heavy Rain (Landslide)"
LABEL_HEAVY_RAIN_INUNDATION = "Warning - Heavy Rain (Inundation)"
LABEL_FLOOD = "Warning - Flood"

# Japanese keywords we key off for Heavy Rain conditions
KW_LANDSLIDE = "土砂災害"
KW_INUNDATION = "浸水害"

# Fallback keywords sometimes appear in attentions (less strict)
ATTN_LANDSLIDE = "土砂"
ATTN_INUNDATION = "浸水"
ATTN_FLOOD = "洪水"

def _codes_from_region_map(region_map: Dict[str, str]) -> Set[str]:
    """
    Return the set of class-10 codes the user cares about, taken from their master region map.
    region_map format: { "Aomori: Tsugaru": "020010", ... }
    """
    return {str(v).strip() for v in (region_map or {}).values() if str(v).strip()}

def _class10_to_display(region_map: Dict[str, str]) -> Dict[str, str]:
    """
    Reverse mapping: class10 code -> display name.
    Keeps user's names as source of truth.
    """
    rev = {}
    for display, code in (region_map or {}).items():
        if code:
            rev[str(code).strip()] = display
    return rev

def _offices_covering_codes(area_codes: dict, target_class10: Set[str]) -> Dict[str, List[str]]:
    """
    Build office -> [class10 codes under that office that we care about].
    Uses areacode.json structure (offices -> children (class10s)).
    """
    wanted_by_office: Dict[str, List[str]] = {}
    offices = (area_codes or {}).get("offices", {})
    class10s = (area_codes or {}).get("class10s", {})

    # Build parent office map for class10 codes we care about
    for code in target_class10:
        cinfo = class10s.get(code)
        if not cinfo:
            continue
        office = cinfo.get("parent")  # office code like "050000"
        if not office:
            continue
        wanted_by_office.setdefault(office, []).append(code)
    return wanted_by_office

def _extract_labels_from_warning(w: dict) -> Set[str]:
    """
    From a single JMA warning object at class-10 level, return label set we should emit.
    Handles:
      - code "03": Heavy Rain -> split by condition (inundation / landslide)
      - code "04": Flood
    Ignores everything else.
    """
    labels: Set[str] = set()
    code = str(w.get("code", "")).strip()

    if code not in ALLOWED_CODES:
        return labels

    if code == "04":
        # Flood warning
        labels.add(LABEL_FLOOD)
        return labels

    if code == "03":
        # Heavy Rain: split by condition; fallback to attentions
        cond = w.get("condition", "") or ""
        atts = w.get("attentions") or []

        has_landslide = (KW_LANDSLIDE in cond) or any(ATTN_LANDSLIDE in a for a in atts)
        has_inundation = (KW_INUNDATION in cond) or any(ATTN_INUNDATION in a for a in atts)

        if has_landslide:
            labels.add(LABEL_HEAVY_RAIN_LANDSLIDE)
        if has_inundation:
            labels.add(LABEL_HEAVY_RAIN_INUNDATION)

        # If neither keyword found, don't emit a vague "Heavy Rain" — stay precise.
        return labels

    return labels

def _parse_office_payload(payload: dict,
                          wanted_codes: Set[str],
                          code_to_display: Dict[str, str]) -> Tuple[List[dict], str]:
    """
    Given one office JSON payload, return entries list and reportDatetime string.
    Only considers areaTypes[0].areas (class-10).
    Produces one entry per (region, label) pair.
    """
    entries: List[dict] = []
    report_dt = payload.get("reportDatetime", "")

    area_types = payload.get("areaTypes") or []
    if not area_types:
        return entries, report_dt

    # class-10 slice should be first
    class10_block = area_types[0]
    areas = class10_block.get("areas") or []

    # Aggregate per region
    per_region: Dict[str, Set[str]] = {}

    for a in areas:
        code = str(a.get("code", "")).strip()
        if not code or code not in wanted_codes:
            continue

        display = code_to_display.get(code)
        if not display:
            # If user didn't supply a name for this code, skip (names are master)
            continue

        for w in (a.get("warnings") or []):
            labels = _extract_labels_from_warning(w)
            if not labels:
                continue
            per_region.setdefault(display, set()).update(labels)

    # Build entries: one per (region, label)
    for region_name, labels in per_region.items():
        for label in sorted(labels):
            entries.append({
                "title": label.replace(" - ", " – "),  # typographic dash to match your UI
                "region": region_name,
                "summary": "",
                "link": "",   # could deep-link to JMA if you want
                "published": report_dt,
            })

    return entries, report_dt

async def _fetch_json(client: httpx.AsyncClient, url: str) -> dict:
    r = await client.get(url, timeout=20.0)
    r.raise_for_status()
    return r.json()

async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Main entry — called by scraper_registry via loader that injects:
      - conf["area_codes"] (areacode.json)
      - conf["region_map"] (your master names -> codes)
    Returns: {"entries": [...], "source": {...}} compatible with the app.
    """
    try:
        area_codes = conf.get("area_codes") or {}
        region_map = conf.get("region_map") or {}

        # Master list of class10 codes (from the user's region_map)
        wanted_class10: Set[str] = _codes_from_region_map(region_map)
        code_to_display = _class10_to_display(region_map)

        if not wanted_class10:
            # Nothing to look for
            return {"entries": [], "source": {"map": MAP_URL, "offices": []}}

        # Map which offices we need to query
        by_office = _offices_covering_codes(area_codes, wanted_class10)
        if not by_office:
            return {"entries": [], "source": {"map": MAP_URL, "offices": []}}

        # Use map.json to ensure the office is currently active/present
        try:
            map_json = await _fetch_json(client, MAP_URL)
        except Exception as e:
            logging.warning(f"[JMA] map.json fetch failed: {e}")
            map_json = {}

        present_offices = set(str(k) for k in (map_json or {}).keys()) if isinstance(map_json, dict) else set()
        offices_to_fetch = [o for o in by_office.keys() if (not present_offices) or (o in present_offices)]

        # Fetch all office payloads concurrently
        async def fetch_office(office_code: str):
            url = f"{BASE}/{office_code}.json"
            try:
                data = await _fetch_json(client, url)
                return office_code, data, None
            except Exception as ex:
                return office_code, None, ex

        results = await asyncio.gather(*[fetch_office(o) for o in offices_to_fetch])

        entries: List[dict] = []
        for office_code, payload, err in results:
            if err or not payload:
                logging.warning(f"[JMA] fetch {office_code} failed: {err}")
                continue
            # Narrow wanted codes under this office to speed filtering
            wanted_here = set(by_office.get(office_code, []))
            if not wanted_here:
                continue
            office_entries, _ = _parse_office_payload(payload, wanted_here, code_to_display)
            entries.extend(office_entries)

        # Sort newest first by published (ISO-like string compare OK here)
        entries.sort(key=lambda x: x.get("published", ""), reverse=True)

        return {
            "entries": entries,
            "source": {
                "map": MAP_URL,
                "offices": offices_to_fetch,
                "filtered_codes": sorted(list(wanted_class10)),
            },
        }

    except Exception as e:
        logging.warning(f"[JMA] fatal error: {e}")
        return {"entries": [], "error": str(e), "source": {"map": MAP_URL}}
