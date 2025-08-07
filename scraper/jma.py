import os
import json
from typing import Any, Dict, List

# Status code to level name mapping
STATUS_MAP = {
    1: "Advisory",
    2: "Warning",
    3: "Emergency Warning"
}

# Directory of this script
HERE = os.path.dirname(__file__)

# Load phenomenon code to English name mapping from weather.json
with open(os.path.join(HERE, "weather.json"), encoding="utf-8") as f:
    weather_data = json.load(f)
WEATHER_MAPPING: Dict[str, str] = {
    v["value"]: v["enName"]
    for v in weather_data[0]["values"]
}

# Load area code hierarchy from areacode.json and flatten to class10 mapping
with open(os.path.join(HERE, "areacode.json"), encoding="utf-8") as f:
    areacode = json.load(f)
REGION_MAPPING: Dict[str, str] = {
    code: info["enName"]
    for code, info in areacode["class10s"].items()
}

async def scrape_jma_async(conf: Dict[str, Any], client) -> Dict[str, Any]:
    """
    Fetch JMA warning map and return list of region-level warnings.
    Expects conf to optionally contain:
      - url: override for the JMA map.json endpoint
    """
    url = conf.get(
        "url",
        "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
    )
    resp = await client.get(url)
    data = await resp.json()

    # Extract timestamp if present
    timestamp = (
        data.get("time", {}).get("time")
        or data.get("reportDatetime")
        or data.get("generated_at")
    )

    results: List[Dict[str, str]] = []
    
    # Case 1: data contains 'map' as dict of code->{phenomenon: status}
    if "map" in data and isinstance(data["map"], dict):
        for code, warnings_dict in data["map"].items():
            region = REGION_MAPPING.get(code)
            if not region:
                continue
            for phen_code, status in warnings_dict.items():
                if status <= 0:
                    continue
                level = STATUS_MAP.get(status, f"Level {status}")
                phen_name = WEATHER_MAPPING.get(phen_code, phen_code)
                results.append({
                    "region": region,
                    "phenomenon": phen_name,
                    "level": level
                })
    # Case 2: data contains list of areas under 'areas'
    else:
        areas = data.get("areas", [])
        for area in areas:
            code = area.get("code")
            region = REGION_MAPPING.get(code)
            if not region:
                continue
            for w in area.get("warnings", []):
                status = w.get("status", 0)
                if status <= 0:
                    continue
                level = STATUS_MAP.get(status, f"Level {status}")
                phen_code = w.get("code")
                phen_name = WEATHER_MAPPING.get(phen_code, phen_code)
                results.append({
                    "region": region,
                    "phenomenon": phen_name,
                    "level": level
                })

    return {
        "title": "JMA Warnings",
        "timestamp": timestamp,
        "data": results
    }
