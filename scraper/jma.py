# scraper/jma.py

import httpx
from dateutil import parser as dateparser

async def scrape_jma_table_async(conf: dict, client: httpx.AsyncClient):
    """
    Fetch JMA warning/map.json, extract any non‐zero warning levels,
    and return them as a list of dict entries.
    """
    url = conf.get("url", "https://www.jma.go.jp/bosai/warning/data/warning/map.json")
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return {
            "entries": [],
            "error": f"JMA fetch failed: {e}",
            "source": conf,
        }

    entries = []
    for prefecture_code, region in data.items():
        # parse the timestamp
        ts = region.get("time")
        try:
            published = dateparser.parse(ts).isoformat()
        except Exception:
            published = None

        # each region has an "areas" dict mapping area_name -> { type: level, ... }
        for area_name, warns in region.get("areas", {}).items():
            for warning_type, level in warns.items():
                if level and level > 0:
                    # build a unique ID for deduplication
                    uid = f"jma|{prefecture_code}|{area_name}|{warning_type}|{published}"
                    entries.append({
                        "id":       uid,
                        "title":    f"{area_name}: {warning_type} (level {level})",
                        "description": f"JMA advisory: {warning_type} level {level} in {area_name}",
                        "link":     url,
                        "published": published,
                        # custom fields for downstream renderers if you like:
                        "pref_code": prefecture_code,
                        "area":      area_name,
                        "type":      warning_type,
                        "level":     level,
                    })

    return {
        "entries": entries,
        "source":  conf,
    }
