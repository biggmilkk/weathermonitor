import logging
from typing import Dict, List, Tuple, Iterable
from datetime import datetime, timezone
import httpx

# ---- Defaults (can be overridden by conf) ----

# Which JMA hazard codes we will emit.
# 10: 土砂災害(Heavy Rain Landslide), 17: 浸水害(Heavy Rain Inundation), 18: 洪水(Flood)
DEFAULT_ALLOWED_CODES = {"10", "17", "18"}

# Fallback labels for codes we care about. (The loader may also provide a richer map via conf["weather"])
DEFAULT_WEATHER_MAP = {
    "10": "Heavy Rain (Landslide)",
    "17": "Heavy Rain (Inundation)",
    "18": "Flood",
    # "14": "Thunderstorm",  # intentionally not in DEFAULT_ALLOWED_CODES
    # "19": "Storm Surge",   # intentionally not in DEFAULT_ALLOWED_CODES
    # "20": "Dense Fog",
}


def _office_area_url_from_map_url(map_url: str, office_code: str) -> str:
    """
    Given map.json URL, build the per-office JSON URL:
    e.g. https://www.jma.go.jp/bosai/warning/data/warning/map.json
      →  https://www.jma.go.jp/bosai/warning/data/warning/area/{office_code}.json
    """
    # Very defensive: just replace the last 'map.json' with f"area/{office_code}.json"
    if map_url.endswith("map.json"):
        return map_url.rsplit("/", 1)[0] + f"/area/{office_code}.json"
    # Fallback (current JMA layout expectation):
    return "https://www.jma.go.jp/bosai/warning/data/warning/area/" + office_code + ".json"


def _iso8601_or_fallback(ts: str) -> str:
    """
    Return an ISO8601 UTC string from a JMA timestamp if possible, else original string.
    """
    try:
        # JMA timestamps look like "2025-08-08T04:17:00+09:00"
        dt = datetime.fromisoformat(ts)
        # normalize to UTC for consistent display
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%a, %d %b %Y %H:%M UTC")
    except Exception:
        return ts


def _collect_active_area_warnings(area_obj: dict) -> List[Tuple[str, str]]:
    """
    From a single area object, pull out active warnings as (code, status) pairs.
    Status strings include:
      - 発表 (issued), 継続 (continuing), 解除 (cancelled), など
    We treat anything not "解除" as active.
    """
    out = []
    for w in area_obj.get("warnings", []) or []:
        code = str(w.get("code", "")).strip()
        status = (w.get("status") or "").strip()
        if not code:
            continue
        if status == "解除":
            continue
        out.append((code, status))
    return out


def _office_and_class10_lookup(area_codes: dict):
    """
    Build helpers:
    - office_by_class10: class10 code -> its parent office code
    - class10_name_en: class10 code -> human readable "Prefecture: RegionName"
    - office_name_en: office code -> prefecture English name
    """
    offices = area_codes.get("offices", {})
    class10s = area_codes.get("class10s", {})

    office_name_en = {}
    office_children = {}
    for off_code, off in offices.items():
        office_name_en[off_code] = off.get("enName") or off.get("name") or off_code
        office_children[off_code] = off.get("children", []) or []

    class10_name_en = {}
    office_by_class10 = {}
    # We want "{Prefecture enName}: {class10 enName}" for the region label
    for off_code, children in office_children.items():
        pref_name = office_name_en.get(off_code, off_code)
        for class10_code in children:
            c10 = class10s.get(class10_code, {})
            region_en = c10.get("enName") or c10.get("name") or class10_code
            class10_name_en[class10_code] = f"{pref_name}: {region_en}"
            office_by_class10[class10_code] = off_code

    return office_by_class10, class10_name_en, office_name_en


async def _fetch_office_json(client: httpx.AsyncClient, url: str, office_code: str) -> dict:
    """
    Fetch per-office JSON. Returns {} on failure.
    """
    src = _office_area_url_from_map_url(url, office_code)
    try:
        resp = await client.get(src, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] office {office_code} url={src} err={e}")
        return {}


def _enumerate_area_rows(area_types_section: List[dict]) -> Iterable[dict]:
    """
    The office file has:
      "areaTypes": [
         { "areas": [ { "code": "290010", "warnings": [...] }, ... ] },
         { "areas": [ { "code": "2920101", ... }, ... ] }
      ]
    We only want the first level of areas (class10 codes, e.g., 290010/290020).
    We'll take the **first** areaTypes block, which maps to class10s consistently.
    """
    if not area_types_section:
        return []
    # The first block corresponds to class10 regions (office-level breakdown)
    first = area_types_section[0] or {}
    return (first.get("areas") or [])


def _region_display_name(class10_code: str, class10_name_en: dict) -> str:
    """
    Resolve the display name for a class10 region: "Prefecture: Region".
    """
    return class10_name_en.get(class10_code, class10_code)


def _title_for_hazard(code: str, weather_map: Dict[str, str]) -> str:
    """
    Pick a user-facing title for the hazard code.
    """
    # Prefer the mapping loaded from conf["weather"], else fallback
    return weather_map.get(code) or DEFAULT_WEATHER_MAP.get(code) or f"Code {code}"


async def scrape_jma_async(conf: dict, client: httpx.AsyncClient) -> dict:
    """
    Asynchronous scraper for JMA warnings (warning *levels* filtered by allowlist).
    Expected conf (after loader merges):
      - url: map.json URL
      - area_codes: full area.json structure (already loaded by loader)
      - weather: optional mapping from hazard code -> label
      - allowed_codes: optional list/iterable of hazard codes to emit
    Emits generic entries:
      {
        "title": "Warning – Heavy Rain (Inundation)",
        "region": "Nara: Northern Region",
        "link": "https://www.jma.go.jp/bosai/warning/#area_type=class10s&area_code=290010",
        "published": "Fri, 08 Aug 2025 07:05 UTC"
      }
    """
    base_url = conf.get("url", "https://www.jma.go.jp/bosai/warning/data/warning/map.json")
    area_codes = conf.get("area_codes") or {}
    weather_map = dict(DEFAULT_WEATHER_MAP)
    weather_map.update(conf.get("weather") or {})

    # Only emit hazards we care about
    allowed_codes = set(map(str, conf.get("allowed_codes", DEFAULT_ALLOWED_CODES)))

    # Build lookups
    office_by_class10, class10_name_en, office_name_en = _office_and_class10_lookup(area_codes)

    # Enumerate all offices from area.json and fetch their JSONs
    offices = area_codes.get("offices", {}) or {}
    office_codes = list(offices.keys())

    parsed_entries: List[dict] = []
    added_counter = 0

    # Helpful debug about what we think is allowed
    logging.warning(f"[JMA DEBUG] allowed hazard codes = {sorted(allowed_codes)}")

    for office_code in office_codes:
        # Pull per-office details
        office_json = await _fetch_office_json(client, base_url, office_code)
        if not office_json:
            continue

        # Minimal sanity check
        report_dt = office_json.get("reportDatetime", "")
        area_types = office_json.get("areaTypes", [])
        rows = list(_enumerate_area_rows(area_types))
        uniq_row_codes = {str(r.get("code", "")) for r in rows if r.get("code")}
        logging.warning(
            f"[JMA DEBUG] office {office_code}: present area rows={len(rows)}, unique area codes={len(uniq_row_codes)}"
        )

        # Iterate each class10 area row within the office
        office_added_before = added_counter
        for area_obj in rows:
            class10_code = str(area_obj.get("code", "")).strip()
            if not class10_code:
                continue

            # Active warnings
            active_pairs = _collect_active_area_warnings(area_obj)

            # Keep only hazards you care about
            active_pairs = [(hz, st) for (hz, st) in active_pairs if hz in allowed_codes]
            if not active_pairs:
                continue

            # Prepare presentation piece
            region_label = _region_display_name(class10_code, class10_name_en)
            published = _iso8601_or_fallback(report_dt)
            # JMA web viewer deeplink for class10 region
            link = f"https://www.jma.go.jp/bosai/warning/#area_type=class10s&area_code={class10_code}"

            # Emit one entry per hazard code for this region
            for hazard_code, _status in active_pairs:
                title = f"Warning – {_title_for_hazard(hazard_code, weather_map)}"
                parsed_entries.append(
                    {
                        "title": title,
                        "region": region_label,
                        "link": link,
                        "published": published,
                    }
                )
                added_counter += 1

        logging.warning(
            f"[JMA DEBUG] office {office_code}: added in office={added_counter - office_added_before}, added so far={added_counter}"
        )

    logging.warning(f"[JMA DEBUG] FINAL parsed entries={len(parsed_entries)}")

    return {
        "entries": parsed_entries,
        "source": base_url,
    }
