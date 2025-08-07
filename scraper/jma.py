import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

async def scrape_jma_async(conf: Dict[str, Any], client) -> Dict[str, Any]:
    """
    Fetch JMA warning map JSON and produce a small feed with only the eight warning
    phenomena (Heavy Rain (Inundation), Heavy Rain (Landslide), Flood, Storm/Gale,
    High Wave, Storm Surge, Thunderstorm, Dense Fog), filtering out non‐warning levels.
    """
    # 1) Get the URL
    url = conf.get("url")
    if not url:
        logging.warning("[JMA FETCH ERROR] Missing 'url' in JMA config")
        return {}

    # 2) Fetch the live map.json
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {}

    # 3) Load local data files
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    try:
        with open(os.path.join(base, "areacode.json"), encoding="utf-8") as f:
            area_data = json.load(f)
        with open(os.path.join(base, "weather.json"), encoding="utf-8") as f:
            weather_sections = json.load(f)
    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] loading local JSONs: {e}")
        return {}

    # 4) Build a map of class10 (prefecture) and class15 (region)
    class10 = area_data.get("class10s", {})
    class15 = area_data.get("class15s", {})
    area_map: Dict[str, str] = {}
    for code, region_name in class15.items():
        pref_code = code[:2] + "0000"
        pref_name = class10.get(pref_code, "").strip()
        area_map[code] = f"{pref_name}: {region_name}" if pref_name else region_name

    # 5) Extract the flat list of phenomena (in order) and an English lookup
    phenomenon_order: List[str] = []
    weather_map: Dict[str, str] = {}
    for section in weather_sections:
        if section.get("key") == "elem":
            for item in section.get("values", []):
                val = item.get("value")
                en = item.get("enName")
                phenomenon_order.append(val)
                weather_map[val] = en
            break

    # 6) Define which codes are “warnings” and the severity→color mapping
    WARNING_PHENOMENA = {
        "inundation", "landslide", "flood", "wind",
        "wave", "tide", "thunder", "fog",
    }
    SEVERITY_COLOR = {
        2: "Yellow",   # Advisory
        3: "Orange",   # Warning
        4: "Red",      # Heavy Warning
    }

    # 7) Parse the issued‐at time
    time_str = data.get("time")
    try:
        dt_local = datetime.fromisoformat(time_str)
        dt_utc = dt_local.astimezone(timezone.utc)
        updated = dt_utc.strftime("%H:%M UTC %B %d")
    except Exception:
        updated = time_str or ""

    # 8) Build your list of items
    items: List[str] = []
    for area_entry in data.get("areas", []):
        # area_entry is [ areaCode, lvl0, lvl1, lvl2, ... ]
        area_code = area_entry[0]
        levels = area_entry[1:]
        area_label = area_map.get(area_code)
        if not area_label:
            continue

        for idx, lvl in enumerate(levels):
            # skip “no warning” or unknown
            if lvl not in SEVERITY_COLOR:
                continue
            phen = phenomenon_order[idx]
            if phen not in WARNING_PHENOMENA:
                continue
            color = SEVERITY_COLOR[lvl]
            name = weather_map.get(phen)
            if not name:
                continue

            items.append(f"● [{color}] {area_label} – {name}")

    return {
        "title": "JMA Warnings",
        "updated": updated,
        "items": items,
    }
