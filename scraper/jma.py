import json
import logging
from datetime import datetime
from typing import Any, Dict, List


async def scrape_jma_async(conf: Dict[str, Any], client) -> Dict[str, Any]:
    """
    Fetches the JMA warning map JSON, extracts the latest report, and returns only active warnings
    in a human-readable format.
    Expects conf to include:
    - url: URL to JMA map.json
    - area_codes: dict of area code -> {name, enName, ...}
    - weather: list (from weather.json) containing one item with key "elem" and its values array
    """
    url = conf.get("url")
    if not url:
        raise ValueError("Missing 'url' in JMA config")

    try:
        # client.get returns a coroutine; await it directly rather than using async with
        resp = await client.get(url)
        data = await resp.json()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {}

    # Ensure we have a list of entries
    data_list = data if isinstance(data, list) else [data]

    # Filter only entries that contain a reportDatetime
    entries = [item for item in data_list if isinstance(item, dict) and item.get("reportDatetime")]
    if not entries:
        logging.warning("[JMA FETCH ERROR] no reportDatetime entries")
        return {}

    # Select the latest report by datetime
    def _parse_dt(item: Dict[str, Any]) -> datetime:
        return datetime.fromisoformat(item["reportDatetime"])

    latest = max(entries, key=_parse_dt)
    report_dt = _parse_dt(latest)
    report_time = report_dt.strftime("%Y-%m-%d %H:%M %z")

    # Build mapping from numeric warning code to phenomenon name
    # weather.json provides mapping of phenomena values; numeric codes start at 10 for index 1
    phenomenon_map: Dict[str, str] = {}
    for item in conf.get("weather", []):
        if item.get("key") == "elem":
            for idx, phen in enumerate(item.get("values", [])):
                # skip the 'all' entry at index 0 if present
                code = str(idx + 9)
                phenomenon_map[code] = phen.get("enName")

    # Load area code mapping
    area_codes = conf.get("area_codes", {})

    warnings_list: List[Dict[str, str]] = []
    # Iterate through each area in the latest report
    for area_type in latest.get("areaTypes", []):
        for area in area_type.get("areas", []):
            area_code = area.get("code")
            area_info = area_codes.get(area_code, {})
            # Prefer English name if available
            area_name = area_info.get("enName") or area_info.get("name") or area_code

            for w in area.get("warnings", []):
                status = w.get("status")
                w_code = w.get("code")
                # Only include if warning is currently active
                if status not in ("継続", "発表"):
                    continue
                phenomenon = phenomenon_map.get(w_code, w_code)
                warnings_list.append({
                    "area": area_name,
                    "phenomenon": phenomenon,
                    "status": status
                })

    return {
        "title": "JMA Warnings",
        "url": url,
        "time": report_time,
        "warnings": warnings_list
    }
