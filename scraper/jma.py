import json
from datetime import datetime, timezone

async def scrape_jma_async(conf: dict, client) -> dict:
    """
    Scrapes JMA warning levels from the map.json feed.
    Only returns active warnings for the eight main phenomena.
    """
    url = conf.get("url")
    if not url:
        raise ValueError("Missing 'url' in JMA config")

    # Fetch the map.json
    resp = await client.get(url)
    data = await resp.json()

    # Parse publication time
    time_str = data.get("datetime") or data.get("reportDatetime") or data.get("time")
    if time_str:
        try:
            # ISO format with offset (e.g. 2023-08-06T05:34:00+09:00)
            dt = datetime.fromisoformat(time_str)
            pub_time = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        except ValueError:
            pub_time = time_str
    else:
        pub_time = ""

    # Build phenomenon code → English name map from weather.json
    phen_map = {}
    for block in conf.get("content", []):
        if block.get("key") == "elem":
            for v in block.get("values", []):
                phen_map[v["value"]] = v["enName"]
            break

    # We only care about these eight codes
    target_codes = {
        "inundation",  # Heavy Rain (Inundation)
        "landslide",   # Heavy Rain (Landslide)
        "flood",       # Flood
        "wind",        # Storm/Gale
        "wave",        # High Wave
        "tide",        # Storm Surge
        "thunder",     # Thunderstorm
        "fog"          # Dense Fog
    }

    # Area code → name mapping from areacode.json
    area_codes = conf.get("area_codes", {})

    # Collect all active warnings
    warnings_list = []
    for phen_code, area_map in data.items():
        # skip non-phenomenon keys
        if phen_code in ("datetime", "reportDatetime", "time"):
            continue
        if phen_code not in target_codes:
            continue

        phen_name = phen_map.get(phen_code)
        if not phen_name:
            continue

        for area_code, level in area_map.items():
            # only include if there's any advisory/warning
            if not level:
                continue
            region = area_codes.get(area_code)
            if not region:
                continue
            warnings_list.append({
                "region": region,
                "phenomenon": phen_name,
                "level": level
            })

    # Format into feed entries
    entries = [
        f"● [Level {w['level']}] {w['region']}: {w['phenomenon']}"
        for w in warnings_list
    ]

    return {
        "title": "JMA Warnings",
        "url": url,
        "published": pub_time,
        "entries": entries
    }
