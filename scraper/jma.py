import json
from datetime import datetime
from typing import Any, Dict


async def scrape_jma_async(conf: Dict[str, Any], client) -> Dict[str, Any]:
    """
    Scrapes JMA warning feed and returns human-readable warnings.
    """
    # URL for JMA map.json warnings
    url = conf.get("url", "https://www.jma.go.jp/bosai/warning/data/warning/map.json")

    # Fetch JSON data
    resp = await client.get(url)
    data = await resp.json()

    # Validate data
    if not isinstance(data, list) or not data:
        raise ValueError("Unexpected data format from JMA warnings")

    # Select latest report by datetime
    latest = max(data, key=lambda x: x.get("reportDatetime", ""))
    report_dt = latest.get("reportDatetime")
    # Parse into datetime
    updated_dt = datetime.fromisoformat(report_dt.replace('Z', '+00:00'))

    # Prepare mappings loaded via loader
    area_codes: Dict[str, Dict[str, Any]] = conf.get("area_codes", {})
    weather_list = conf.get("weather", [])

    # Build phenomenon mapping: value code -> names
    ph_map: Dict[str, Dict[str, Any]] = {}
    for item in weather_list:
        if item.get("key") == "elem":
            for v in item.get("values", []):
                ph_map[v.get("value")] = v
            break

    # Code types correspond to areaTypes order
    code_types = ["class10s", "class20s"]

    # Collect warnings
    warnings = []
    for idx, area_type in enumerate(latest.get("areaTypes", [])):
        code_type = code_types[idx] if idx < len(code_types) else None
        for area in area_type.get("areas", []):
            area_code = area.get("code")
            # Resolve human name for area
            name = area_code
            if code_type and code_type in area_codes:
                info = area_codes[code_type].get(area_code, {})
                name = info.get("name") or info.get("enName") or area_code

            # Iterate warnings for this area
            for w in area.get("warnings", []):
                ph_code = w.get("code")
                status = w.get("status")
                # Only include active warnings (status not None or empty)
                if not status or status.lower() in ("解除", "cancelled", "cleared"):
                    continue

                ph_info = ph_map.get(ph_code, {})
                ph_name = ph_info.get("enName") or ph_info.get("name") or ph_code

                warnings.append({
                    "region": name,
                    "phenomenon": ph_name,
                    "status": status,
                })

    return {
        "title": "JMA Warnings",
        "updated": updated_dt.isoformat(),
        "warnings": warnings,
    }
