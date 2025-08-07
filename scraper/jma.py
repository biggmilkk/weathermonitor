# scraper/jma.py

import json
from dateutil import parser
from datetime import timezone
from typing import Any, Dict
from aiohttp import ClientResponse

async def scrape_jma_async(conf: Dict[str, Any], client) -> Dict[str, Any]:
    """
    Scrape the JMA warnings feed (map.json), map area codes to names and numeric codes
    to phenomenon keys, and return only active warnings per region.
    """
    url = conf.get("url")
    if not url:
        raise ValueError("Missing 'url' in JMA config")

    # 1) Fetch the JMA JSON
    resp: ClientResponse
    async with client.get(url) as resp:
        data = await resp.json()

    # 2) Parse and humanize the report time (to UTC)
    report_dt = parser.isoparse(data["reportDatetime"])
    report_utc = report_dt.astimezone(timezone.utc)
    report_str = report_utc.strftime("%Y-%m-%d %H:%M UTC")

    # 3) Flatten your area_codes JSON into a code → enName map
    raw_areas = conf["area_codes"]
    flat_areas: Dict[str, str] = {}
    for category in ("centers", "offices", "class20s"):
        for code, info in raw_areas.get(category, {}).items():
            flat_areas[code] = info.get("enName", code)

    # 4) Build weather_map: phenomenon key → English name
    weather_map: Dict[str, str] = {}
    for group in conf["weather"]:
        if group.get("key") == "elem":
            for v in group.get("values", []):
                weather_map[v["value"]] = v["enName"]

    # 5) Numeric JMA warning codes → your phenomenon keys
    PHENOMENON_CODE_MAP = {
        "10": "inundation",  # 大雨（浸水）
        "15": "landslide",   # 大雨（土砂災害）
        "14": "flood",       # 洪水
        "20": "wave",        # 波浪
        "19": "tide",        # 高潮
        # add more mappings here as needed...
    }

    # 6) Collect only active warnings per region
    items = []
    for area_group in data.get("areaTypes", []):
        for area in area_group.get("areas", []):
            code = area.get("code")
            region = flat_areas.get(code)
            if not region:
                continue

            active_warns = []
            for w in area.get("warnings", []):
                status = w.get("status")
                # only include 初回発表 or 継続
                if status not in ("発表", "継続"):
                    continue

                ph_code = w.get("code")
                key = PHENOMENON_CODE_MAP.get(ph_code)
                name = weather_map.get(key) if key else None
                if name and name not in active_warns:
                    active_warns.append(name)

            if active_warns:
                items.append({
                    "region": region,
                    "warnings": active_warns
                })

    return {
        "title": f"JMA Warnings ({report_str})",
        "items": items
    }
