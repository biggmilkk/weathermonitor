import datetime
import json
import logging
import aiohttp

JP_TO_EN_LEVEL = {
    "注意報": "Advisory",
    "警報": "Warning",
    "特別警報": "Alert",       # Shown as "Alert" on JMA's English UI
    "緊急警報": "Emergency",    # Rare, but keep for completeness
}

# Map JMA phenomenon codes to English names
PHENOMENON_MAP = {
    "大雨": "Heavy Rain (Inundation)",
    "大雨（土砂災害）": "Heavy Rain (Landslide)",
    "洪水": "Flood",
    "暴風": "Storm",
    "暴風雪": "Storm",
    "強風": "Gale",
    "波浪": "High Wave",
    "高潮": "Storm Surge",
    "雷": "Thunder Storm",
    "濃霧": "Dense Fog",
}

KEEP_LEVELS = {"Warning", "Alert", "Emergency"}

async def scrape_jma_async(conf, client=None):
    """
    Scrape JMA warnings via backend JSON.
    Only keeps Warning, Alert, and Emergency levels.
    """
    entries = []
    try:
        base_url = "https://www.jma.go.jp/bosai/warning/data/warning"

        async with aiohttp.ClientSession() as session:
            # 1. Load the main map.json listing all offices
            async with session.get(f"{base_url}/map.json") as resp:
                map_json = await resp.json()

            # 2. Iterate through each office in the map
            for office_code, office_data in map_json.items():
                # Only process offices that have warning info
                if "warnings" not in office_data:
                    continue

                office_name = office_data.get("name", "")
                area_name = office_data.get("enName") or office_name

                # 3. Get detailed warnings for this office
                detail_url = f"{base_url}/{office_code}.json"
                async with session.get(detail_url) as dresp:
                    detail_json = await dresp.json()

                # 4. The structure: [time_series, area_series, warning_series...]
                time_series = detail_json.get("timeSeries", [])
                if not time_series:
                    continue

                for ts_block in time_series:
                    areas = ts_block.get("areas", [])
                    for area in areas:
                        region_name = area.get("name") or area_name
                        warns = area.get("warnings") or []
                        for warn in warns:
                            level_jp = warn.get("status") or ""
                            level_en = JP_TO_EN_LEVEL.get(level_jp, level_jp)

                            # Filter out advisories
                            if level_en not in KEEP_LEVELS:
                                continue

                            code = warn.get("code", "")
                            phenomenon = PHENOMENON_MAP.get(code, code)

                            # Build the feed entry
                            entries.append({
                                "title": f"{level_en} - {phenomenon}",
                                "region": f"{area_name}: {region_name}",
                                "level": level_en,
                                "type": phenomenon,
                                "summary": "",
                                "published": datetime.datetime.utcnow().isoformat() + "Z",
                                "link": f"https://www.jma.go.jp/bosai/warning/#lang=en&area_type=offices&area_code={office_code}"
                            })

        logging.warning(f"[JMA DEBUG] Parsed {len(entries)} alerts (Warning/Alert/Emergency only)")
        return {"entries": entries}

    except Exception as e:
        logging.warning(f"[JMA FETCH ERROR] {e}")
        return {"entries": []}
