# scraper/jma.py

import httpx
from dateutil import parser as dateparser

async def scrape_jma_table_async(conf: dict, client: httpx.AsyncClient):
    """
    Fetch JMA warning/map.json, extract any non‐zero warning levels,
    and return them under a dict 'alerts' keyed by conf['key'].
    Each alert item has: id, region, type, description, link, published.
    """
    url = conf.get(
        "url",
        "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
    )
    feed_key = conf.get("key", "rss_jma")

    # 1) Fetch & parse JSON
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return {
            "alerts": {},
            "error": f"JMA fetch failed: {e}",
            "source": conf,
        }

    # 2) Ensure it's a dict
    if not isinstance(data, dict):
        return {
            "alerts": {},
            "error": f"Unexpected JSON structure: expected object, got {type(data).__name__}",
            "source": conf,
        }

    entries = []

    # 3) Walk prefectures → areas → warning types
    for pref_code, region in data.items():
        ts = region.get("time")
        try:
            published = dateparser.parse(ts).isoformat()
        except Exception:
            published = None

        areas = region.get("areas", {})
        if not isinstance(areas, dict):
            continue

        for area_name, warns in areas.items():
            if not isinstance(warns, dict):
                continue
            for warning_type, level in warns.items():
                # only numeric levels > 0
                if isinstance(level, (int, float)) and level > 0:
                    uid = f"jma|{pref_code}|{area_name}|{warning_type}|{published}"
                    entries.append({
                        "id":          uid,
                        "region":      area_name,
                        "type":        warning_type,
                        "description": f"{warning_type} level {level} in {area_name}",
                        "link":        url,
                        "published":   published,
                    })

    # 4) Return under an 'alerts' dict so downstream code can do:
    #      for alerts in country.get("alerts", {}).values(): ...
    return {
        "alerts": { feed_key: entries },
        "source":  conf,
    }
