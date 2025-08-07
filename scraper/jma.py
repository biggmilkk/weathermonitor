import json
from pathlib import Path
from typing import Any, Dict


async def scrape_jma_async(conf: Dict[str, Any], client) -> Dict[str, Any]:
    """
    Scrapes JMA warnings GeoJSON and returns a mapping of areas to current warning/advisory statuses.
    Only phenomena with active advisories or warnings are included.
    """
    # Fetch the JMA warnings map
    url = conf.get(
        "url",
        "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
    )
    resp = await client.get(url)
    resp.raise_for_status()
    data = resp.json()

    # Load local mapping files
    base_dir = Path(__file__).resolve().parents[1] / "data"
    with open(base_dir / "areacode.json", encoding="utf-8") as f:
        area_map = json.load(f)
    with open(base_dir / "weather.json", encoding="utf-8") as f:
        weather_map = json.load(f)

    # Extract phenomenon definitions
    elems = next(
        (e["values"] for e in weather_map if e.get("key") == "elem"), []
    )
    # Level mapping: 1 = Advisory, 2 = Warning
    status_map = {1: "advisory", 2: "warning"}

    result: Dict[str, Any] = {
        "source": url,
        "updated": data.get("updated"),
        "areas": {}
    }

    # Iterate each geographic feature
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        # Area code used as key in areacode.json
        code = str(props.get("code", ""))
        # Try to resolve to a human-readable name via class10s (regions), offices, or centers
        area_info = (
            area_map.get("class10s", {}).get(code)
            or area_map.get("offices", {}).get(code)
            or area_map.get("centers", {}).get(code)
        )
        if area_info:
            area_name = area_info.get("enName") or area_info.get("name")
        else:
            area_name = code

        # Collect active statuses for this area
        statuses: Dict[str, str] = {}
        for idx, elem in enumerate(elems):
            # Skip the "all" catch-all element at index 0
            if idx == 0:
                continue
            key = f"w{idx:02d}"
            level_code = props.get(key)
            # Only include advisory or warning levels
            if isinstance(level_code, int) and level_code in status_map:
                # elem['value'] is the phenomenon code (e.g., "inundation")
                statuses[elem["value"]] = status_map[level_code]

        if statuses:
            result["areas"][area_name] = statuses

    return result
